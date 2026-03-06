from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from gridflow.common.io import read_parquet, write_parquet
from gridflow.common.paths import BRONZE_ROOT, SILVER_ROOT, bronze_path, silver_path
from gridflow.common.time import inclusive_date_range
from gridflow.etl.silver.standardise import (
    add_source_column,
    drop_rows_with_nulls,
    ensure_numeric,
    parse_utc_column,
    reorder_columns,
    require_columns,
)


INTERCONNECTOR_FUEL_MAP: dict[str, str] = {
    "INTELEC": "ELECLINK",
    "INTEW": "EAST_WEST",
    "INTFR": "IFA",
    "INTGRNL": "GREENLINK",
    "INTIFA2": "IFA2",
    "INTIRL": "MOYLE_IRL",
    "INTNED": "BRITNED",
    "INTNEM": "NEMO",
    "INTNSL": "NORTH_SEA_LINK",
    "INTVKL": "VIKING",
}


def _bronze_daily_path(dataset_name: str, day: str | date | datetime) -> Path:
    day = pd.Timestamp(day).date()
    return bronze_path(
        "elexon",
        dataset_name,
        "flat",
        f"year={day:%Y}",
        f"month={day:%m}",
        f"{dataset_name}_{day.isoformat()}.parquet",
    )


def _silver_daily_path(dataset_name: str, day: str | date | datetime) -> Path:
    day = pd.Timestamp(day).date()
    return silver_path(
        "elexon",
        dataset_name,
        f"year={day:%Y}",
        f"month={day:%m}",
        f"{dataset_name}_{day.isoformat()}.parquet",
    )


def _load_bronze_day(dataset_name: str, day: str | date | datetime) -> pd.DataFrame:
    path = _bronze_daily_path(dataset_name, day)
    if not path.exists():
        raise FileNotFoundError(f"Bronze file not found: {path}")
    return read_parquet(path)


def _save_silver_day(dataset_name: str, day: str | date | datetime, df: pd.DataFrame) -> Path:
    path = _silver_daily_path(dataset_name, day)
    write_parquet(df, path)
    return path


def clean_elexon_fuelhh(df: pd.DataFrame) -> pd.DataFrame:
    require_columns(
        df,
        [
            "publishTime",
            "startTime",
            "settlementDate",
            "settlementPeriod",
            "fuelType",
            "generation",
        ],
        "elexon_fuelhh",
    )

    out = df.copy()

    out["publish_time_utc"] = parse_utc_column(out, "publishTime")
    out["timestamp_utc"] = parse_utc_column(out, "startTime")
    out["settlement_date"] = pd.to_datetime(out["settlementDate"], errors="coerce").dt.date
    out["settlement_period"] = pd.to_numeric(out["settlementPeriod"], errors="coerce").astype("Int64")
    out["fuel_type"] = out["fuelType"].astype("string")
    out["generation_mw"] = pd.to_numeric(out["generation"], errors="coerce")

    out["is_interconnector"] = out["fuel_type"].isin(INTERCONNECTOR_FUEL_MAP.keys())
    out["interconnector_name"] = out["fuel_type"].map(INTERCONNECTOR_FUEL_MAP).astype("string")
    out["country_code"] = "GB"

    out = add_source_column(out, "elexon")
    out = drop_rows_with_nulls(
        out,
        ["timestamp_utc", "settlement_date", "settlement_period", "fuel_type", "generation_mw"],
    )

    out = out.drop_duplicates(
        subset=["timestamp_utc", "settlement_period", "fuel_type"],
        keep="last",
    ).copy()

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "publish_time_utc",
            "settlement_date",
            "settlement_period",
            "country_code",
            "fuel_type",
            "is_interconnector",
            "interconnector_name",
            "generation_mw",
            "source",
        ],
    )

    return out.reset_index(drop=True)


def clean_elexon_demand_actual_total(df: pd.DataFrame) -> pd.DataFrame:
    require_columns(
        df,
        [
            "publishTime",
            "startTime",
            "settlementDate",
            "settlementPeriod",
            "quantity",
        ],
        "elexon_demand_actual_total",
    )

    out = df.copy()

    out["publish_time_utc"] = parse_utc_column(out, "publishTime")
    out["timestamp_utc"] = parse_utc_column(out, "startTime")
    out["settlement_date"] = pd.to_datetime(out["settlementDate"], errors="coerce").dt.date
    out["settlement_period"] = pd.to_numeric(out["settlementPeriod"], errors="coerce").astype("Int64")
    out["demand_mw"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["country_code"] = "GB"

    out = add_source_column(out, "elexon")
    out = drop_rows_with_nulls(
        out,
        ["timestamp_utc", "settlement_date", "settlement_period", "demand_mw"],
    )

    # Keep latest published record if duplicates exist
    out = (
        out.sort_values("publish_time_utc")
        .drop_duplicates(subset=["timestamp_utc", "settlement_period"], keep="last")
        .copy()
    )

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "publish_time_utc",
            "settlement_date",
            "settlement_period",
            "country_code",
            "demand_mw",
            "source",
        ],
    )

    return out.reset_index(drop=True)


def clean_elexon_mid(df: pd.DataFrame) -> pd.DataFrame:
    require_columns(
        df,
        [
            "startTime",
            "settlementDate",
            "settlementPeriod",
            "dataProvider",
            "price",
            "volume",
        ],
        "elexon_mid",
    )

    out = df.copy()

    out["timestamp_utc"] = parse_utc_column(out, "startTime")
    out["settlement_date"] = pd.to_datetime(out["settlementDate"], errors="coerce").dt.date
    out["settlement_period"] = pd.to_numeric(out["settlementPeriod"], errors="coerce").astype("Int64")
    out["data_provider"] = out["dataProvider"].astype("string")
    out["price_gbp_mwh"] = pd.to_numeric(out["price"], errors="coerce")
    out["volume_mwh"] = pd.to_numeric(out["volume"], errors="coerce")
    out["country_code"] = "GB"

    out = add_source_column(out, "elexon")
    out = drop_rows_with_nulls(
        out,
        ["timestamp_utc", "settlement_date", "settlement_period", "data_provider", "price_gbp_mwh"],
    )

    out = out.drop_duplicates(
        subset=["timestamp_utc", "settlement_period", "data_provider"],
        keep="last",
    ).copy()

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "settlement_date",
            "settlement_period",
            "country_code",
            "data_provider",
            "price_gbp_mwh",
            "volume_mwh",
            "source",
        ],
    )

    return out.reset_index(drop=True)


def run_fuelhh_silver_day(day: str | date | datetime, overwrite: bool = False) -> Path:
    target = _silver_daily_path("fuelhh", day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("fuelhh", day)
    silver_df = clean_elexon_fuelhh(bronze_df)
    return _save_silver_day("fuelhh", day, silver_df)


def run_demand_actual_total_silver_day(day: str | date | datetime, overwrite: bool = False) -> Path:
    target = _silver_daily_path("demand_actual_total", day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("demand_actual_total", day)
    silver_df = clean_elexon_demand_actual_total(bronze_df)
    return _save_silver_day("demand_actual_total", day, silver_df)


def run_mid_silver_day(day: str | date | datetime, overwrite: bool = False) -> Path:
    target = _silver_daily_path("mid", day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("mid", day)
    silver_df = clean_elexon_mid(bronze_df)
    return _save_silver_day("mid", day, silver_df)


def run_fuelhh_silver_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_fuelhh_silver_day(day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] fuelhh silver {day} -> {path}")
    return outputs


def run_demand_actual_total_silver_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_demand_actual_total_silver_day(day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] demand_actual_total silver {day} -> {path}")
    return outputs


def run_mid_silver_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_mid_silver_day(day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] mid silver {day} -> {path}")
    return outputs


def run_elexon_core_silver_history(
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> dict[str, list[Path]]:
    results: dict[str, list[Path]] = {}

    print("\n=== Building FUELHH silver ===")
    results["fuelhh"] = run_fuelhh_silver_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print("\n=== Building DEMAND_ACTUAL_TOTAL silver ===")
    results["demand_actual_total"] = run_demand_actual_total_silver_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print("\n=== Building MID silver ===")
    results["mid"] = run_mid_silver_history(
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    return results