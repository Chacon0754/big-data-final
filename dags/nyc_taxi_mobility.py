import csv
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlopen

from airflow.sdk import Param, TaskGroup, dag, get_current_context, task

from src.mobility_stages import STAGES, stage_notebook

from src.tlc_period import MONTH_PATTERN, configured_months, month_range

WORKSPACE = Path("/workspace")
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
NOTEBOOK_BIN = Path("/opt/notebook-venv/bin")
DEFAULT_START_MONTH, DEFAULT_END_MONTH = configured_months()


def is_valid_input(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size < 12:
        return False

    if path.suffix == ".parquet":
        with path.open("rb") as file:
            header: bytes = file.read(4)
            file.seek(-4, os.SEEK_END)
            return header == file.read(4) == b"PAR1"

    with path.open(encoding="utf-8-sig", newline="") as file:
        return next(csv.reader(file), []) == [
            "LocationID", "Borough", "Zone", "service_zone"
        ]


def fetch_input(url: str, path: Path) -> None:
    if is_valid_input(path):
        print(f"Reused {path.name} ({path.stat().st_size:,} bytes)", flush=True)
        return

    temporary_file = NamedTemporaryFile(
        dir=path.parent, prefix=".download-", suffix=path.suffix, delete=False
    )
    temporary_path: Path = Path(temporary_file.name)

    try:
        with temporary_file, urlopen(url, timeout=90) as response:
            shutil.copyfileobj(response, temporary_file)
        if not is_valid_input(temporary_path):
            raise ValueError(f"Invalid data downloaded for {path.name}")
        temporary_path.chmod(0o644)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)

    print(f"Downloaded {path.name} ({path.stat().st_size:,} bytes)", flush=True)


@dag(
    dag_id="nyc_taxi_mobility",
    description="Download TLC inputs, execute the Spark notebook and publish to MongoDB",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_month": Param(
            DEFAULT_START_MONTH, type="string", pattern=MONTH_PATTERN,
            title="Mes inicial (AAAA-MM)",
        ),
        "end_month": Param(
            DEFAULT_END_MONTH, type="string", pattern=MONTH_PATTERN,
            title="Mes final incluido (AAAA-MM)",
        ),
    },
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["nyc-taxi", "spark", "mongodb"],
)
def mobility_pipeline() -> None:
    @task
    def ingest_tlc() -> None:
        params = get_current_context()["params"]
        _, _, source_months = month_range(params["start_month"], params["end_month"])
        print(
            f"Ingesting {len(source_months)} month(s): {source_months[0]} through {source_months[-1]}"
        )

        raw_dir: Path = WORKSPACE / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        for month in source_months:
            file_name: str = f"yellow_tripdata_{month}.parquet"
            fetch_input(f"{TLC_BASE}/trip-data/{file_name}", raw_dir / file_name)

        fetch_input(
            f"{TLC_BASE}/misc/taxi_zone_lookup.csv", raw_dir / "taxi_zone_lookup.csv"
        )

    @task(execution_timeout=timedelta(hours=2))
    def run_notebook_stage(stage: str) -> str:
        context = get_current_context()
        run_id: str = context["run_id"]
        dag_run = context["dag_run"]
        started_at: datetime = dag_run.start_date or dag_run.run_after
        params = context["params"]
        run_dir: Path = WORKSPACE / "data" / "processed" / "airflow" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        source: Path = WORKSPACE / "notebooks" / "01_nyc_taxi_analysis.ipynb"
        analysis_dir: Path = run_dir / "analysis"
        notebooks_dir: Path = run_dir / "notebooks"
        stage_source: Path = notebooks_dir / f"{stage}.ipynb"
        executed: Path = notebooks_dir / f"{stage}.executed.ipynb"
        report_path: Path = run_dir / f"{run_id}.json"
        output: Path = analysis_dir / STAGES[stage] if STAGES[stage] else report_path
        stage_notebook(source, stage_source, stage)

        subprocess.run(
            [
                str(NOTEBOOK_BIN / "papermill"),
                str(stage_source), str(executed),
                "--kernel", "spark-python3",
                "--cwd", str(WORKSPACE),
                "--log-output", "--no-progress-bar",
            ],
            cwd=WORKSPACE,
            env={
                **os.environ,
                "TLC_START_MONTH": params["start_month"],
                "TLC_END_MONTH": params["end_month"],
                "TLC_DAG_RUN_ID": run_id,
                "TLC_DAG_RUN_STARTED_AT": started_at.astimezone(timezone.utc).isoformat(),
                "SPARK_CONSOLE_PROGRESS": "false",
                "MOBILITY_ANALYSIS_DIR": str(analysis_dir),
                "MOBILITY_REPORT_PATH": str(report_path),
            },
            stderr=subprocess.STDOUT,
            check=True,
        )

        if not output.is_file():
            raise FileNotFoundError(output)

        print(f"Executed notebook: {executed}", flush=True)
        return str(output)

    @task
    def publish_to_mongo(report_path: str) -> None:
        subprocess.run(
            [
                str(NOTEBOOK_BIN / "python"),
                "-m", "src.publish_report",
                "--report-path", report_path,
            ],
            cwd=WORKSPACE,
            check=True,
        )

    ingestion = ingest_tlc()
    with TaskGroup(group_id="run_spark_notebook", tooltip="Prepare, analyze and export"):
        prepared = run_notebook_stage.override(task_id="prepare_trips")("prepare_trips")
        demand = run_notebook_stage.override(task_id="analyze_demand")("analyze_demand")
        metrics = run_notebook_stage.override(task_id="analyze_trip_metrics")("analyze_trip_metrics")
        report_path = run_notebook_stage.override(task_id="export_report")("export_report")
    publication = publish_to_mongo(report_path)
    ingestion >> prepared >> demand >> metrics >> report_path >> publication


nyc_taxi_mobility = mobility_pipeline()
