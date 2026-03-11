from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from dotenv import load_dotenv

from gridflow.common.http import ENTSOE_HTTP_CONFIG, get_with_retries
from gridflow.common.io import write_parquet
from gridflow.common.manifests import append_manifest_rows
from gridflow.common.paths import BRONZE_ROOT, bronze_path
from gridflow.common.time import inclusive_date_range
from gridflow.config.sources import ENTSOE, ENTSOE_DATASETS, ENTSOE_ZONES
from gridflow.etl.bronze.common import build_ingest_status_row

load_dotenv()

Payload = str
FetchChunkFn = Callable[[str, str | date | datetime, str | date | datetime], tuple[str, pd.DataFrame]]


def _get_entsoe_token() -> str:
    token = os.getenv("ENTSOE_SECURITY_TOKEN")
    if not token:
        raise ValueError(
            "ENTSOE_SECURITY_TOKEN is not set. Add it to your environment or .env before calling ENTSO-E."
        )
    return token


def _xml_namespace(root: ET.Element) -> dict[str, str]:
    if root.tag.startswith("{"):
        uri = root.tag.split("}")[0].strip("{")
        return {"ns": uri}
    return {"ns": ""}


def _period_str(value: str | date | datetime) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y%m%d%H%M")


def _default_headers() -> dict[str, str]:
    return {
        "Accept": "application/xml",
        "User-Agent": "gridflow/0.1.0",
    }


def _dataset_flat_path(dataset_name: str, zone: str, day: str | date | datetime) -> Path:
    day = pd.Timestamp(day).date()
    dataset_key = f"{dataset_name}__{zone.lower()}"

    return bronze_path(
        "entsoe",
        dataset_key,
        "flat",
        f"year={day:%Y}",
        f"month={day:%m}",
        f"{dataset_key}_{day.isoformat()}.parquet",
    )


def _dataset_manifest_path(dataset_name: str, zone: str) -> Path:
    dataset_key = f"{dataset_name}__{zone.lower()}"
    return bronze_path(
        "entsoe",
        dataset_key,
        "manifests",
        f"{dataset_key}_ingest_log.parquet",
    )


def _chunk_raw_xml_path(
    dataset_name: str,
    zone: str,
    chunk_start: date,
    chunk_end: date,
) -> Path:
    dataset_key = f"{dataset_name}__{zone.lower()}"
    return bronze_path(
        "entsoe",
        dataset_key,
        "raw_chunks",
        f"year={chunk_start:%Y}",
        f"month={chunk_start:%m}",
        f"{dataset_key}_{chunk_start.isoformat()}__{chunk_end.isoformat()}.xml",
    )


def _request_xml(params: dict[str, Any]) -> str:
    query_params = {"securityToken": _get_entsoe_token(), **params}
    response = get_with_retries(
        source_name=ENTSOE.name,
        url=ENTSOE.base_url,
        config=ENTSOE_HTTP_CONFIG,
        params=query_params,
        headers=_default_headers(),
        timeout=ENTSOE.timeout_seconds,
    )
    return response.text


def _parse_entsoe_points(xml_text: str) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    ns = _xml_namespace(root)

    rows: list[dict[str, Any]] = []

    for ts in root.findall(".//ns:TimeSeries", ns):
        business_type = ts.findtext("ns:businessType", default=None, namespaces=ns)
        psr_type = ts.findtext(".//ns:MktPSRType/ns:psrType", default=None, namespaces=ns)
        in_domain = ts.findtext("ns:in_Domain.mRID", default=None, namespaces=ns)
        out_domain = ts.findtext("ns:outBiddingZone_Domain.mRID", default=None, namespaces=ns)
        currency_unit = ts.findtext("ns:currency_Unit.name", default=None, namespaces=ns)
        price_unit = ts.findtext("ns:price_Measure_Unit.name", default=None, namespaces=ns)
        quantity_unit = ts.findtext("ns:quantity_Measure_Unit.name", default=None, namespaces=ns)

        for period in ts.findall("ns:Period", ns):
            start = period.findtext("ns:timeInterval/ns:start", default=None, namespaces=ns)
            end = period.findtext("ns:timeInterval/ns:end", default=None, namespaces=ns)
            resolution = period.findtext("ns:resolution", default=None, namespaces=ns)

            for point in period.findall("ns:Point", ns):
                rows.append(
                    {
                        "businessType": business_type,
                        "psrType": psr_type,
                        "inDomain": in_domain,
                        "outDomain": out_domain,
                        "currencyUnit": currency_unit,
                        "priceMeasureUnit": price_unit,
                        "quantityMeasureUnit": quantity_unit,
                        "periodStart": start,
                        "periodEnd": end,
                        "resolution": resolution,
                        "position": point.findtext("ns:position", default=None, namespaces=ns),
                        "quantity": point.findtext("ns:quantity", default=None, namespaces=ns),
                        "price.amount": point.findtext("ns:price.amount", default=None, namespaces=ns),
                    }
                )

    return pd.DataFrame(rows)


def _parse_resolution_to_timedelta(resolution: str) -> pd.Timedelta:
    if resolution == "PT15M":
        return pd.Timedelta(minutes=15)
    if resolution == "PT30M":
        return pd.Timedelta(minutes=30)
    if resolution in {"PT60M", "PT1H"}:
        return pd.Timedelta(hours=1)
    raise ValueError(f"Unsupported ENTSOE resolution: {resolution}")


def _entsoe_position_timestamp(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")

    required = {"periodStart", "resolution", "position"}
    missing = required - set(df.columns)
    if missing:
        return pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns, UTC]")

    period_start = pd.to_datetime(df["periodStart"], utc=True, errors="coerce")
    position = pd.to_numeric(df["position"], errors="coerce")

    timedeltas = []
    for res, pos in zip(df["resolution"], position):
        if pd.isna(res) or pd.isna(pos):
            timedeltas.append(pd.NaT)
            continue

        try:
            step = _parse_resolution_to_timedelta(str(res))
        except ValueError:
            timedeltas.append(pd.NaT)
            continue

        timedeltas.append((int(pos) - 1) * step)

    return period_start + pd.to_timedelta(timedeltas)


def _daily_timestamp_filter(df: pd.DataFrame, day: date) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    timestamp_utc = _entsoe_position_timestamp(df)
    out = df.copy()
    out["_timestamp_utc"] = timestamp_utc
    out = out[out["_timestamp_utc"].dt.date == day].copy()
    out = out.drop(columns=["_timestamp_utc"], errors="ignore")
    return out


def _chunk_date_range(
    start_day: date,
    end_day: date,
    chunk_days: int,
) -> list[tuple[date, date]]:
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")

    chunks: list[tuple[date, date]] = []
    current = start_day

    while current <= end_day:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_day)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    return chunks


def _flat_exists(dataset_name: str, zone: str, day: date, overwrite: bool) -> bool:
    if overwrite:
        return False
    return _dataset_flat_path(dataset_name, zone, day).exists()


def fetch_actual_total_load(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
) -> tuple[Payload, pd.DataFrame]:
    if zone not in ENTSOE_ZONES:
        raise ValueError(f"Unknown ENTSOE zone: {zone}")

    dataset = ENTSOE_DATASETS["actual_total_load"]
    params = {
        "documentType": dataset.document_type,
        "processType": dataset.process_type,
        "outBiddingZone_Domain": ENTSOE_ZONES[zone],
        "periodStart": _period_str(date_from),
        "periodEnd": _period_str(date_to),
    }

    xml_text = _request_xml(params)
    df = _parse_entsoe_points(xml_text)
    return xml_text, df


def fetch_generation_per_type(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
) -> tuple[Payload, pd.DataFrame]:
    if zone not in ENTSOE_ZONES:
        raise ValueError(f"Unknown ENTSOE zone: {zone}")

    dataset = ENTSOE_DATASETS["generation_per_type"]
    params = {
        "documentType": dataset.document_type,
        "processType": dataset.process_type,
        "in_Domain": ENTSOE_ZONES[zone],
        "periodStart": _period_str(date_from),
        "periodEnd": _period_str(date_to),
    }

    xml_text = _request_xml(params)
    df = _parse_entsoe_points(xml_text)
    return xml_text, df


def fetch_energy_prices(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
) -> tuple[Payload, pd.DataFrame]:
    if zone not in ENTSOE_ZONES:
        raise ValueError(f"Unknown ENTSOE zone: {zone}")

    dataset = ENTSOE_DATASETS["energy_prices"]
    params = {
        "documentType": dataset.document_type,
        "in_Domain": ENTSOE_ZONES[zone],
        "out_Domain": ENTSOE_ZONES[zone],
        "periodStart": _period_str(date_from),
        "periodEnd": _period_str(date_to),
    }

    xml_text = _request_xml(params)
    df = _parse_entsoe_points(xml_text)
    return xml_text, df


def _save_chunk_raw_xml(
    dataset_name: str,
    zone: str,
    chunk_start: date,
    chunk_end: date,
    xml_text: str,
) -> Path:
    path = _chunk_raw_xml_path(dataset_name, zone, chunk_start, chunk_end)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(xml_text, encoding="utf-8")
    return path


def _save_daily_flat_parquet(
    dataset_name: str,
    zone: str,
    day: date,
    df: pd.DataFrame,
) -> Path:
    path = _dataset_flat_path(dataset_name, zone, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(df, path)
    return path


def ingest_entsoe_chunked_history(
    *,
    dataset_name: str,
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    fetch_chunk_fn: FetchChunkFn,
    chunk_days: int,
    overwrite: bool = False,
) -> pd.DataFrame:
    all_days = inclusive_date_range(date_from, date_to)
    start_day = all_days[0]
    end_day = all_days[-1]

    manifest_rows: list[dict[str, Any]] = []
    run_timestamp = pd.Timestamp.utcnow().isoformat()

    for chunk_start, chunk_end in _chunk_date_range(start_day, end_day, chunk_days=chunk_days):
        print(f"\n[CHUNK] {dataset_name} {zone} {chunk_start} -> {chunk_end}")

        # ENTSOE periodEnd is effectively exclusive, so request next-day midnight.
        chunk_request_end = chunk_end + timedelta(days=1)

        try:
            xml_text, chunk_df = fetch_chunk_fn(zone, chunk_start, chunk_request_end)
            raw_chunk_path = _save_chunk_raw_xml(
                dataset_name=dataset_name,
                zone=zone,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                xml_text=xml_text,
            )
        except Exception as exc:
            for day in inclusive_date_range(chunk_start, chunk_end):
                flat_path = _dataset_flat_path(dataset_name, zone, day)
                manifest_rows.append(
                    build_ingest_status_row(
                        run_timestamp_utc=run_timestamp,
                        dataset=f"{dataset_name}__{zone.lower()}",
                        settlement_date=day.isoformat(),
                        fuel_type=None,
                        status="error",
                        row_count=None,
                        raw_json_path=str(_chunk_raw_xml_path(dataset_name, zone, chunk_start, chunk_end)),
                        flat_parquet_path=str(flat_path),
                        error=str(exc),
                    )
                )
                print(f"[ERR]  {dataset_name} {zone} {day.isoformat()} error={exc}")
            continue

        for day in inclusive_date_range(chunk_start, chunk_end):
            day_str = day.isoformat()
            flat_path = _dataset_flat_path(dataset_name, zone, day)

            if _flat_exists(dataset_name, zone, day, overwrite=overwrite):
                manifest_rows.append(
                    build_ingest_status_row(
                        run_timestamp_utc=run_timestamp,
                        dataset=f"{dataset_name}__{zone.lower()}",
                        settlement_date=day_str,
                        fuel_type=None,
                        status="skipped_exists",
                        row_count=None,
                        raw_json_path=str(raw_chunk_path),
                        flat_parquet_path=str(flat_path),
                        error=None,
                    )
                )
                print(f"[SKIP] {dataset_name} {zone} {day_str} already exists")
                continue

            try:
                day_df = _daily_timestamp_filter(chunk_df, day)
                saved_flat_path = _save_daily_flat_parquet(
                    dataset_name=dataset_name,
                    zone=zone,
                    day=day,
                    df=day_df,
                )

                status = "no_data" if day_df.empty else "success"

                manifest_rows.append(
                    build_ingest_status_row(
                        run_timestamp_utc=run_timestamp,
                        dataset=f"{dataset_name}__{zone.lower()}",
                        settlement_date=day_str,
                        fuel_type=None,
                        status=status,
                        row_count=len(day_df),
                        raw_json_path=str(raw_chunk_path),
                        flat_parquet_path=str(saved_flat_path),
                        error=None,
                    )
                )
                print(f"[OK]   {dataset_name} {zone} {day_str} rows={len(day_df)}")

            except Exception as exc:
                manifest_rows.append(
                    build_ingest_status_row(
                        run_timestamp_utc=run_timestamp,
                        dataset=f"{dataset_name}__{zone.lower()}",
                        settlement_date=day_str,
                        fuel_type=None,
                        status="error",
                        row_count=None,
                        raw_json_path=str(raw_chunk_path),
                        flat_parquet_path=str(flat_path),
                        error=str(exc),
                    )
                )
                print(f"[ERR]  {dataset_name} {zone} {day_str} error={exc}")

    return append_manifest_rows(_dataset_manifest_path(dataset_name, zone), manifest_rows)


def ingest_actual_total_load_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
    chunk_days: int = 30,
) -> pd.DataFrame:
    return ingest_entsoe_chunked_history(
        dataset_name="actual_total_load",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_chunk_fn=fetch_actual_total_load,
        chunk_days=chunk_days,
        overwrite=overwrite,
    )


def ingest_generation_per_type_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
    chunk_days: int = 14,
) -> pd.DataFrame:
    return ingest_entsoe_chunked_history(
        dataset_name="generation_per_type",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_chunk_fn=fetch_generation_per_type,
        chunk_days=chunk_days,
        overwrite=overwrite,
    )


def ingest_energy_prices_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
    chunk_days: int = 30,
) -> pd.DataFrame:
    return ingest_entsoe_chunked_history(
        dataset_name="energy_prices",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_chunk_fn=fetch_energy_prices,
        chunk_days=chunk_days,
        overwrite=overwrite,
    )


def ingest_entsoe_core_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}

    print(f"\n=== Ingesting ENTSOE ACTUAL_TOTAL_LOAD {zone} ===")
    results["actual_total_load"] = ingest_actual_total_load_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
        chunk_days=30,
    )

    print(f"\n=== Ingesting ENTSOE GENERATION_PER_TYPE {zone} ===")
    results["generation_per_type"] = ingest_generation_per_type_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
        chunk_days=14,
    )

    print(f"\n=== Ingesting ENTSOE ENERGY_PRICES {zone} ===")
    results["energy_prices"] = ingest_energy_prices_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
        chunk_days=30,
    )

    return results


def ingest_entsoe_core_history_all_zones(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> dict[str, dict[str, pd.DataFrame]]:
    results: dict[str, dict[str, pd.DataFrame]] = {}

    for zone in ENTSOE_ZONES:
        print(f"\n######## ENTSOE ZONE: {zone} ########")
        results[zone] = ingest_entsoe_core_history(
            zone=zone,
            date_from=date_from,
            date_to=date_to,
            overwrite=overwrite,
        )

    return results