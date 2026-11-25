import csv
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlopen

from airflow.sdk import dag, get_current_context, task

WORKSPACE = Path("/workspace")
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
NOTEBOOK_BIN = Path("/opt/notebook-venv/bin")


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
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["nyc-taxi", "spark", "mongodb"],
)
def mobility_pipeline() -> None:
    @task
    def ingest_tlc() -> None:
        year: int = int(os.environ.get("TLC_ANALYSIS_YEAR", "2026"))
        last_month: int = int(os.environ.get("TLC_LAST_MONTH", "9"))
        if not 1 <= last_month <= 12:
            raise ValueError("TLC_LAST_MONTH must be between 1 and 12")

        raw_dir: Path = WORKSPACE / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        for month in range(1, last_month + 1):
            file_name: str = f"yellow_tripdata_{year}-{month:02d}.parquet"
            fetch_input(f"{TLC_BASE}/trip-data/{file_name}", raw_dir / file_name)

        fetch_input(
            f"{TLC_BASE}/misc/taxi_zone_lookup.csv", raw_dir / "taxi_zone_lookup.csv"
        )

    @task(execution_timeout=timedelta(minutes=45))
    def run_spark_notebook() -> str:
        run_id: str = get_current_context()["run_id"]
        run_dir: Path = WORKSPACE / "data" / "processed" / "airflow" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        source: Path = WORKSPACE / "notebooks" / "01_nyc_taxi_analysis.ipynb"
        executed: Path = run_dir / "01_nyc_taxi_analysis.executed.ipynb"
        report_path: Path = run_dir / "mobility_report.json"

        subprocess.run(
            [
                str(NOTEBOOK_BIN / "papermill"),
                str(source), str(executed),
                "--kernel", "spark-python3",
                "--cwd", str(WORKSPACE),
                "--log-output", "--no-progress-bar",
                "--execution-timeout", "900",
            ],
            cwd=WORKSPACE,
            env={**os.environ, "MOBILITY_REPORT_PATH": str(report_path)},
            check=True,
        )

        if not report_path.is_file():
            raise FileNotFoundError(report_path)

        print(f"Executed notebook: {executed}", flush=True)
        return str(report_path)

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
    report_path = run_spark_notebook()
    ingestion >> report_path
    publish_to_mongo(report_path)


nyc_taxi_mobility = mobility_pipeline()