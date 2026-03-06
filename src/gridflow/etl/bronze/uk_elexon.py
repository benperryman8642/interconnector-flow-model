from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests

from gridflow.common.io import write_json, write_parquet
from gridflow.common.paths import bronze_path
from gridflow.config.sources import ELEXON, ELEXON_DATASETS, build_url


@dataclass(frozen=True)
class ElexonRequest:
    dataset_key: str
    params: dict[str, Any] | None = None


def _coerce_date(value: str | date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _default_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "gridflow/0.1.0",
    }


def get_dataset_json(
    dataset_key: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int | None = None,
) -> dict[str, Any] | list[Any]:
    if dataset_key not in ELEXON_DATASETS:
        raise ValueError(f"Unknown Elexon dataset_key: {dataset_key}")

    dataset = ELEXON_DATASETS[dataset_key]
    url = build_url(ELEXON, dataset.stream_path)

    response = requests.get(
        url,
        params=params or {},
        headers=_default_headers(),
        timeout=timeout_seconds or ELEXON.timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def extract_records(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """
    Elexon endpoints can wrap data differently. This tries a few common shapes.
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        raise TypeError("Unexpected payload type")

    # Common candidate keys seen across APIs / wrappers
    for key in ("data", "result", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    # Sometimes the payload itself is already a record-like dict.
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

    Notes:
    - Uses the /stream endpoint, which is the documented JSON-optimised route.
    - Settlement date filters cannot be combined with publish date filters.
    """
    using_publish_filters = bool(publish_date_time_from or publish_date_time_to)
    using_settlement_filters = bool(settlement_date_from or settlement_date_to)

    if using_publish_filters and using_settlement_filters:
        raise ValueError(
            "Settlement date filters cannot be combined with publish date filters "
            "for Elexon FUELHH."
        )

    if "fuelhh" not in ELEXON_DATASETS:
        raise ValueError("Missing Elexon dataset definition for 'fuelhh'.")

    dataset = ELEXON_DATASETS["fuelhh"]
    if not dataset.stream_path:
        raise ValueError("Elexon FUELHH stream_path is not configured.")

    params: dict[str, Any] = {}

    if publish_date_time_from:
        params["publishDateTimeFrom"] = publish_date_time_from
    if publish_date_time_to:
        params["publishDateTimeTo"] = publish_date_time_to

    settlement_date_from_str = _coerce_date(settlement_date_from)
    settlement_date_to_str = _coerce_date(settlement_date_to)

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


def save_bronze_fuelhh(
    df: pd.DataFrame,
    payload: dict[str, Any] | list[Any],
    run_label: str,
) -> dict[str, str]:
    """
    Writes both raw JSON and flattened parquet for inspection/debugging.
    """
    raw_json_path = bronze_path("elexon", "fuelhh", "raw", f"{run_label}.json")
    flat_parquet_path = bronze_path("elexon", "fuelhh", "flat", f"{run_label}.parquet")

    write_json(payload, raw_json_path)
    write_parquet(df, flat_parquet_path)

    return {
        "raw_json": str(raw_json_path),
        "flat_parquet": str(flat_parquet_path),
    }


def run_fuelhh_ingest(
    settlement_date_from: str | date | datetime,
    settlement_date_to: str | date | datetime,
    fuel_type: str | None = None,
    run_label: str | None = None,
) -> pd.DataFrame:
    payload, df = fetch_fuelhh(
        settlement_date_from=settlement_date_from,
        settlement_date_to=settlement_date_to,
        fuel_type=fuel_type,
    )

    if run_label is None:
        run_label = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")

    save_bronze_fuelhh(df=df, payload=payload, run_label=run_label)
    return df


if __name__ == "__main__":
    df = run_fuelhh_ingest(
        settlement_date_from="2026-03-01",
        settlement_date_to="2026-03-02",
    )
    print(df.head())
    print(f"Rows fetched: {len(df)}")
    print(f"Columns: {list(df.columns)}")