from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from dotenv import load_dotenv
from gridflow.common.io import write_json, write_parquet
from gridflow.common.manifests import append_manifest_rows
from gridflow.common.paths import BRONZE_ROOT, bronze_path
from gridflow.common.time import inclusive_date_range
from gridflow.config.sources import ENTSOE, ENTSOE_DATASETS, ENTSOE_ZONES
from gridflow.etl.bronze.common import (
    build_ingest_status_row,
    daily_bronze_file_paths,
    should_skip_existing,
)

ENTSOE_NAMESPACE = {"ns": "urn:iec62325.351:tc57wg16:451-6:publicationdocument:7:0"}
load_dotenv()

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


def _dataset_daily_paths(dataset_name: str, zone: str, day: str | date | datetime) -> dict[str, Path]:
    return daily_bronze_file_paths(
        bronze_root=BRONZE_ROOT,
        source_name="entsoe",
        dataset_name=f"{dataset_name}__{zone.lower()}",
        day=day,
        raw_extension="xml",
        flat_extension="parquet",
    )


def _dataset_manifest_path(dataset_name: str, zone: str) -> Path:
    return bronze_path(
        "entsoe",
        f"{dataset_name}__{zone.lower()}",
        "manifests",
        f"{dataset_name}__{zone.lower()}_ingest_log.parquet",
    )


def _request_xml(params: dict[str, Any]) -> str:
    query_params = {"securityToken": _get_entsoe_token(), **params}
    response = requests.get(
        ENTSOE.base_url,
        params=query_params,
        headers=_default_headers(),
        timeout=ENTSOE.timeout_seconds,
    )
    response.raise_for_status()
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


def fetch_actual_total_load(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
) -> tuple[str, pd.DataFrame]:
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
) -> tuple[str, pd.DataFrame]:
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
) -> tuple[str, pd.DataFrame]:
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


def save_bronze_entsoe_day(
    dataset_name: str,
    zone: str,
    day: str | date | datetime,
    df: pd.DataFrame,
    xml_text: str,
) -> dict[str, str]:
    paths = _dataset_daily_paths(dataset_name, zone, day)
    paths["raw"].parent.mkdir(parents=True, exist_ok=True)
    paths["flat"].parent.mkdir(parents=True, exist_ok=True)

    paths["raw"].write_text(xml_text, encoding="utf-8")
    write_parquet(df, paths["flat"])

    return {
        "raw_xml": str(paths["raw"]),
        "flat_parquet": str(paths["flat"]),
    }


def ingest_entsoe_daily_history(
    *,
    dataset_name: str,
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    fetch_fn,
    overwrite: bool = False,
) -> pd.DataFrame:
    manifest_rows: list[dict[str, Any]] = []
    run_timestamp = pd.Timestamp.utcnow().isoformat()

    for day in inclusive_date_range(date_from, date_to):
        next_day = day + pd.Timedelta(days=1)
        paths = _dataset_daily_paths(dataset_name, zone, day)
        day_str = day.isoformat()

        if should_skip_existing(paths, overwrite=overwrite):
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=f"{dataset_name}__{zone.lower()}",
                    settlement_date=day_str,
                    fuel_type=None,
                    status="skipped_exists",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
                    error=None,
                )
            )
            print(f"[SKIP] {dataset_name} {zone} {day_str} already exists")
            continue

        try:
            xml_text, df = fetch_fn(zone, day, next_day)

            save_paths = save_bronze_entsoe_day(
                dataset_name=dataset_name,
                zone=zone,
                day=day,
                df=df,
                xml_text=xml_text,
            )

            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=f"{dataset_name}__{zone.lower()}",
                    settlement_date=day_str,
                    fuel_type=None,
                    status="success",
                    row_count=len(df),
                    raw_json_path=save_paths["raw_xml"],
                    flat_parquet_path=save_paths["flat_parquet"],
                    error=None,
                )
            )
            print(f"[OK]   {dataset_name} {zone} {day_str} rows={len(df)}")

        except Exception as exc:
            manifest_rows.append(
                build_ingest_status_row(
                    run_timestamp_utc=run_timestamp,
                    dataset=f"{dataset_name}__{zone.lower()}",
                    settlement_date=day_str,
                    fuel_type=None,
                    status="error",
                    row_count=None,
                    raw_json_path=str(paths["raw"]),
                    flat_parquet_path=str(paths["flat"]),
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
) -> pd.DataFrame:
    return ingest_entsoe_daily_history(
        dataset_name="actual_total_load",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_fn=fetch_actual_total_load,
        overwrite=overwrite,
    )


def ingest_generation_per_type_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_entsoe_daily_history(
        dataset_name="generation_per_type",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_fn=fetch_generation_per_type,
        overwrite=overwrite,
    )


def ingest_energy_prices_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    return ingest_entsoe_daily_history(
        dataset_name="energy_prices",
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        fetch_fn=fetch_energy_prices,
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
    )

    print(f"\n=== Ingesting ENTSOE GENERATION_PER_TYPE {zone} ===")
    results["generation_per_type"] = ingest_generation_per_type_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print(f"\n=== Ingesting ENTSOE ENERGY_PRICES {zone} ===")
    results["energy_prices"] = ingest_energy_prices_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
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