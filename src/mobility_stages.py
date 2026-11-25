import json
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

STAGES: dict[str, str] = {
    "prepare_trips": "preparation.json",
    "analyze_demand": "demand.json",
    "analyze_trip_metrics": "trip_metrics.json",
    "export_report": "",
}


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def encode_decimal(value: object) -> dict[str, str]:
    if isinstance(value, Decimal):
        return {"$numberDecimal": str(value)}
    raise TypeError(f"Unsupported value: {type(value).__name__}")


def write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False
    )
    temporary_path = Path(output.name)

    try:
        with output:
            json.dump(document, output, default=encode_decimal, allow_nan=False,
                      indent=2, ensure_ascii=False)
            output.write("\n")
        temporary_path.chmod(0o644)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def stage_notebook(source: Path, destination: Path, stage: str) -> None:
    if stage not in STAGES:
        raise ValueError(f"Unknown mobility analysis stage: {stage}")

    notebook = read_json(source)
    notebook["cells"] = [
        cell
        for cell in notebook["cells"]
        if stage in cell["metadata"].get("mobility_stages", [])
    ]
    if not any(cell["cell_type"] == "code" for cell in notebook["cells"]):
        raise ValueError(f"No cells configured for mobility analysis stage: {stage}")

    for cell in notebook["cells"]:
        cell["metadata"].pop("execution", None)
        if cell["cell_type"] == "code":
            cell["execution_count"] = None
            cell["outputs"] = []

    notebook["metadata"].pop("papermill", None)
    write_json(destination, notebook)
