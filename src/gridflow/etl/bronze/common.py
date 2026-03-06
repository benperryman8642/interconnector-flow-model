from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from gridflow.common.partitions import build_partitioned_file_path, year_month_day_string
from gridflow.common.time import parse_date


def daily_bronze_file_paths(
    bronze_root: Path,
    source_name: str,
    dataset_name: str,
    day: str | date | datetime,
    raw_extension: str,
    flat_extension: str = "parquet",
) -> dict[str, Path]:
    """
    Build standard bronze paths for a daily-ingested dataset.

    Example output:
      data/bronze/elexon/fuelhh/raw/year=2026/month=03/fuelhh_2026-03-01.json
      data/bronze/elexon/fuelhh/flat/year=2026/month=03/fuelhh_2026-03-01.parquet
    """
    parsed_day = parse_date(day)
    day_str = year_month_day_string(parsed_day)

    raw_filename = f"{dataset_name}_{day_str}.{raw_extension}"
    flat_filename = f"{dataset_name}_{day_str}.{flat_extension}"

    raw_path = build_partitioned_file_path(
        bronze_root,
        source_name,
        dataset_name,
        "raw",
        partition_date=parsed_day,
        filename=raw_filename,
    )

    flat_path = build_partitioned_file_path(
        bronze_root,
        source_name,
        dataset_name,
        "flat",
        partition_date=parsed_day,
        filename=flat_filename,
    )

    return {
        "raw": raw_path,
        "flat": flat_path,
    }


def should_skip_existing(paths: dict[str, Path], overwrite: bool = False) -> bool:
    """
    Skip if all expected outputs already exist and overwrite is False.
    """
    if overwrite:
        return False
    return all(path.exists() for path in paths.values())


def build_ingest_status_row(
    *,
    run_timestamp_utc: str,
    dataset: str,
    settlement_date: str,
    status: str,
    raw_json_path: str,
    flat_parquet_path: str,
    fuel_type: str | None = None,
    row_count: int | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """
    Standard row shape for bronze ingest manifests.
    """
    return {
        "run_timestamp_utc": run_timestamp_utc,
        "dataset": dataset,
        "settlement_date": settlement_date,
        "fuel_type": fuel_type,
        "status": status,
        "row_count": row_count,
        "raw_json_path": raw_json_path,
        "flat_parquet_path": flat_parquet_path,
        "error": error,
    }