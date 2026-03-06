from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from gridflow.common.io import write_json, write_parquet
from gridflow.common.manifests import append_manifest_rows
from gridflow.common.paths import BRONZE_ROOT, bronze_path
from gridflow.common.time import inclusive_date_range
from gridflow.config.sources import ELEXON, ELEXON_DATASETS, build_url
from gridflow.etl.bronze.common import (
    build_ingest_status_row,
    daily_bronze_file_paths,
    should_skip_existing,
)


Payload = dict[str, Any] | list[Any]
FetchDayFn = Callable[[date], tuple[Payload, pd.DataFrame]]


def _default_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "gridflow/0.1.0",
    }


def _dataset_daily_paths(dataset_name: str, day: str | date | datetime) -> dict[str, Path]:
    return daily_bronze_file_paths(
        bronze_root=BRONZE_ROOT,
        source_name="elexon",
        dataset_name=dataset_name,
        day=day,
        raw_extension="json",
        flat_extension="parquet",
    )


def _dataset_manifest_path(dataset_name: str) -> Path:
    return bronze_path("elexon", dataset_name, "manifests", f"{dataset_name}_ingest_log.parquet")


def _utc_day_bounds(day: date) -> tuple[str, str]:
    start_dt = datetime.combine(day, time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = datetime.combine(day, time.max.replace(microsecond=0)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return start_dt, end_dt


def _request_json(url: str, params: dict[str, Any]) -> Payload:
    response = requests.get(
        url,
        params=params,
        headers=_default_headers(),
        timeout=ELEXON.timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def extract_records(payload: Payload) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        raise TypeError("Unexpected payload type")

    for key in ("data", "result", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    return [payload]


def payload_to_frame(payload: Payload) -> pd.DataFrame:
    records = extract_records(payload)
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records)


def fetch_fuelhh_day(day: date, fuel_type: str | None = None) -> tuple[Payload, pd.DataFrame]:
    dataset = ELEXON_DATASETS["fuelhh"]
    if not dataset.stream_path:
        raise ValueError("Elexon FUELHH stream_path is not configured.")

    url = build_url(ELEXON, dataset.stream_path)
    params: dict[str, Any] = {
        "settlementDateFrom": day.isoformat(),
        "settlementDateTo": day.isoformat(),
    }
    if fuel_type:
        params["fuelType"] = fuel_type

    payload = _request_json(url, params)
    return payload, payload_to_frame(payload)


def fetch_indo_day(day: date) -> tuple[Payload, pd.DataFrame]:
    dataset = ELEXON_DATASETS["indo"]
    url = build_url(ELEXON, dataset.stream_path or dataset.path)

    publish_from, publish_to = _utc_day_bounds(day)
    params = {
        "publishDateTimeFrom": publish_from,
        "publishDateTimeTo": publish_to,
    }

    payload = _request_json(url, params)
    return payload, payload_to_frame(payload)


def fetch_itsdo_day(day: date) -> tuple[Payload, pd.DataFrame]:
    dataset = ELEXON_DATASETS["itsdo"]
    url = build_url(ELEXON, dataset.path)

    publish_from, publish_to = _utc_day_bounds(day)
    params = {
        "publishDateTimeFrom": publish_from,
        "publishDateTimeTo": publish_to,
    }

    payload = _request_json(url, params)
    return payload, payload_to_frame(payload)


def fetch_mid_day(day: date) -> tuple[Payload, pd.DataFrame]:
    dataset = ELEXON_DATASETS["mid"]
    if not dataset.stream_path:
        raise ValueError("Elexon MID stream_path is not configured.")

    url = build_url(ELEXON, dataset.stream_path)
    from_ts, to_ts = _utc_day_bounds(day)
    params = {
        "from": from_ts,
        "to": to_ts,
    }

    payload = _request_json(url, params)
    return payload, payload_to_frame(payload)


def save_bronze_elexon_day(
    dataset_name: str,
    day: str | date | datetime,
    df: pd.DataFrame,
    payload: Payload,
) -> dict[str, str]:
    paths = _dataset_daily_paths(dataset_name, day)

    write_json(payload, paths["raw"])
    write_parquet(df, paths["flat"])

    return {
        "raw_json": str(paths["raw"]),
        "flat_parquet": str(paths["flat"]),
    }


def ingest_elexon_daily_history(
    *,
    dataset_name: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    fetch_day_fn: FetchDayFn,
    overwrite: bool = False,
    fuel_type: str | None = None,
) -> pd.DataFrame:
    manifest_rows: list[dict[str, Any]] = []
    run_timestamp = pd.Timestamp.utcnow().isoformat()

    for day in inclusive_date_range(date_from, date_to):
        paths = _dataset_daily_paths(dataset_name, day)
        day_str = day.isoformat()

        if should_skip_existing(paths, overwrite=overwrite):
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=dataset_name,
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="skipped_exists",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
                    error=None,
                )
            )
            print(f"[SKIP] {dataset_name} {day_str} already exists")
            continue

        try:
            payload, df = fetch_day_fn(day)
            save_paths = save_bronze_elexon_day(dataset_name, day, df, payload)

            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=dataset_name,
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="success",
                    row_count=len(df),
                    raw_json_path=save_paths["raw_json"],
                    flat_parquet_path=save_paths["flat_parquet"],
                    error=None,
                )
            )
            print(f"[OK]   {dataset_name} {day_str} rows={len(df)}")

        except Exception as exc:
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=dataset_name,
                    settlement_date=day_str,
                    fuel_type=fuel_type,
                    status="error",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
                    error=str(exc),
                )
            )
            print(f"[ERR]  {dataset_name} {day_str} error={exc}")

    return append_manifest_rows(_dataset_manifest_path(dataset_name), manifest_rows)


def ingest_fuelhh_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    fuel_type: str | None = None,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_elexon_daily_history(
        dataset_name="fuelhh",
        date_from=date_from,
        date_to=date_to,
        fetch_day_fn=lambda day: fetch_fuelhh_day(day, fuel_type=fuel_type),
        overwrite=overwrite,
        fuel_type=fuel_type,
    )


def ingest_indo_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_elexon_daily_history(
        dataset_name="indo",
        date_from=date_from,
        date_to=date_to,
        fetch_day_fn=fetch_indo_day,
        overwrite=overwrite,
    )


def ingest_itsdo_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_elexon_daily_history(
        dataset_name="itsdo",
        date_from=date_from,
        date_to=date_to,
        fetch_day_fn=fetch_itsdo_day,
        overwrite=overwrite,
    )


def ingest_mid_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_elexon_daily_history(
        dataset_name="mid",
        date_from=date_from,
        date_to=date_to,
        fetch_day_fn=fetch_mid_day,
        overwrite=overwrite,
    )


def ingest_elexon_core_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
    fuel_type: str | None = None,
) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}

    print("\n=== Ingesting FUELHH ===")
    results["fuelhh"] = ingest_fuelhh_history(
        date_from=date_from,
        date_to=date_to,
        fuel_type=fuel_type,
        overwrite=overwrite,
    )

    print("\n=== Ingesting INDO ===")
    results["indo"] = ingest_indo_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print("\n=== Ingesting ITSDO ===")
    results["itsdo"] = ingest_itsdo_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print("\n=== Ingesting MID ===")
    results["mid"] = ingest_mid_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    return results