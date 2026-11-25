import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

from bson.json_util import loads
from pymongo import MongoClient


def publish_report(report_path: Path, mongo_uri: str) -> None:
    report: object = loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("The report must be a JSON object")

    report_id: object = report.get("_id")
    valid_trip_count: object = report.get("valid_trip_count")

    if not isinstance(report_id, str) or not report_id:
        raise ValueError("The report must have a nonempty period ID")
    if not isinstance(valid_trip_count, int) or valid_trip_count <= 0:
        raise ValueError("The report must have a positive valid trip count")
    if not isinstance(report.get("indicators"), dict) or not report["indicators"]:
        raise ValueError("The report must contain indicator tables")

    client: MongoClient[dict[str, Any]] = MongoClient(
        mongo_uri, serverSelectionTimeoutMS=5_000
    )
    with client:
        result = client["nyc_taxi"]["mobility_reports"].replace_one(
            {"_id": report_id}, report, upsert=True
        )

    print(f"Published {report_id} with {valid_trip_count:,} valid trips")
    print(f"matched={result.matched_count} modified={result.modified_count}")
    print(f"upserted_id={result.upserted_id}")


def main() -> None:
    parser = ArgumentParser(description="Publish the Spark mobility report")
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("data/processed/mobility_report.json"),
    )
    report_path: Path = parser.parse_args().report_path
    mongo_uri: str = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27087")
    publish_report(report_path, mongo_uri)


if __name__ == "__main__":
    main()