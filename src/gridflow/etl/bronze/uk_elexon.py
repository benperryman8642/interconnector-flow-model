from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from gridflow.common.io import write_json, write_parquet
from gridflow.common.manifests import append_manifest_rows
from gridflow.common.paths import BRONZE_ROOT, bronze_path
from gridflow.common.time import coerce_date_string, inclusive_date_range, parse_date
from gridflow.config.sources import ELEXON, ELEXON_DATASETS, build_url
from gridflow.etl.bronze.common import (
    build_ingest_status_row,
    daily_bronze_file_paths,
    should_skip_existing,
)


def _default_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "gridflow/0.1.0",
    }


def _fuelhh_daily_paths(day: str | date | datetime) -> dict[str, Path]:
    return daily_bronze_file_paths(
        bronze_root=BRONZE_ROOT,
        source_name="elexon",
        dataset_name="fuelhh",
        day=day,
        raw_extension="json",
        flat_extension="parquet",
    )


def _fuelhh_manifest_path() -> Path:
    return bronze_path("elexon", "fuelhh", "manifests", "fuelhh_ingest_log.parquet")


def get_dataset_json(
    dataset_key: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int | None = None,
) -> dict[str, Any] | list[Any]:
    if dataset_key not in ELEXON_DATASETS:
        raise ValueError(f"Unknown Elexon dataset_key: {dataset_key}")

    dataset = ELEXON_DATASETS[dataset_key]
    url = build_url(ELEXON, dataset.path)

    response = requests.get(
        url,
        params=params or {},
        headers=_default_headers(),
        timeout=timeout_seconds or ELEXON.timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def extract_records(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        raise TypeError("Unexpected payload type")

    for key in ("data", "result", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    return [payload]


def payload_to_frame(payload: dict[str, Any] | list[Any]) -> pd.DataFrame:
    records = extract_records(payload)
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records)


def fetch_fuelhh(
    publish_date_time_from: str | None = None,
    publish_date_time_to: str | None = None,
    settlement_date_from: str | date | datetime | None = None,
    settlement_date_to: str | date | datetime | None = None,
    fuel_type: str | None = None,
) -> tuple[dict[str, Any] | list[Any], pd.DataFrame]:
    """
    Fetch half-hourly generation outturn by fuel type from the Elexon
    FUELHH stream endpoint.
    """
    using_publish_filters = bool(publish_date_time_from or publish_date_time_to)
    using_settlement_filters = bool(settlement_date_from or settlement_date_to)

    if using_publish_filters and using_settlement_filters:
        raise ValueError(
            "Settlement date filters cannot be combined with publish date filters "
            "for Elexon FUELHH."
        )

    dataset = ELEXON_DATASETS["fuelhh"]
    if not dataset.stream_path:
        raise ValueError("Elexon FUELHH stream_path is not configured.")

    params: dict[str, Any] = {}

    if publish_date_time_from:
        params["publishDateTimeFrom"] = publish_date_time_from
    if publish_date_time_to:
        params["publishDateTimeTo"] = publish_date_time_to

    settlement_date_from_str = coerce_date_string(settlement_date_from)
    settlement_date_to_str = coerce_date_string(settlement_date_to)

    if settlement_date_from_str:
        params["settlementDateFrom"] = settlement_date_from_str
    if settlement_date_to_str:
        params["settlementDateTo"] = settlement_date_to_str
    if fuel_type:
        params["fuelType"] = fuel_type

    url = build_url(ELEXON, dataset.stream_path)

    response = requests.get(
        url,
        params=params,
        headers=_default_headers(),
        timeout=ELEXON.timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json()
    df = payload_to_frame(payload)
    return payload, df


def save_bronze_fuelhh_day(
    day: str | date | datetime,
    df: pd.DataFrame,
    payload: dict[str, Any] | list[Any],
) -> dict[str, str]:
    paths = _fuelhh_daily_paths(day)

    write_json(payload, paths["raw"])
    write_parquet(df, paths["flat"])

    return {
        "raw_json": str(paths["raw"]),
        "flat_parquet": str(paths["flat"]),
    }


def run_fuelhh_ingest(
    settlement_date_from: str | date | datetime,
    settlement_date_to: str | date | datetime,
    fuel_type: str | None = None,
) -> pd.DataFrame:
    payload, df = fetch_fuelhh(
        settlement_date_from=settlement_date_from,
        settlement_date_to=settlement_date_to,
        fuel_type=fuel_type,
    )
    start_day = parse_date(settlement_date_from)
    save_bronze_fuelhh_day(day=start_day, df=df, payload=payload)
    return df


def ingest_fuelhh_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    fuel_type: str | None = None,
    overwrite: bool = False,
) -> pd.DataFrame:
    """
    Historical bronze ingestion for Elexon FUELHH.

    Strategy:
    - loop day by day
    - save one raw JSON file and one flat parquet per day
    - partition by month
    - append a manifest log
    """
    manifest_rows: list[dict[str, Any]] = []
    run_timestamp = pd.Timestamp.utcnow().isoformat()

    for day in inclusive_date_range(date_from, date_to):
        paths = _fuelhh_daily_paths(day)
        day_str = day.isoformat()

        if should_skip_existing(paths, overwrite=overwrite):
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset="fuelhh",
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="skipped_exists",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
                    error=None,
                )
            )
            print(f"[SKIP] {day_str} already exists")
            continue

        try:
            payload, df = fetch_fuelhh(
                settlement_date_from=day,
                settlement_date_to=day,
                fuel_type=fuel_type,
            )

            save_paths = save_bronze_fuelhh_day(day=day, df=df, payload=payload)

            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset="fuelhh",
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="success",
                    row_count=len(df),
                    raw_json_path=save_paths["raw_json"],
                    flat_parquet_path=save_paths["flat_parquet"],
                    error=None,
                )
            )
            print(f"[OK]   {day_str} rows={len(df)}")

        except Exception as exc:
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset="fuelhh",
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="error",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
                    error=str(exc),
                )
            )
            print(f"[ERR]  {day_str} error={exc}")

    return append_manifest_rows(_fuelhh_manifest_path(), manifest_rows)