from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from gridflow.common.io import read_parquet, write_parquet
from gridflow.common.paths import bronze_path, silver_path
from gridflow.common.time import inclusive_date_range
from gridflow.etl.silver.standardise import (
    add_source_column,
    drop_rows_with_nulls,
    reorder_columns,
    require_columns,
)


def _bronze_daily_path(dataset_name: str, zone: str, day: str | date | datetime) -> Path:
    day = pd.Timestamp(day).date()
    zone_key = zone.lower()

    return bronze_path(
        "entsoe",
        f"{dataset_name}__{zone_key}",
        "flat",
        f"year={day:%Y}",
        f"month={day:%m}",
        f"{dataset_name}__{zone_key}_{day.isoformat()}.parquet",
    )


def _silver_daily_path(dataset_name: str, zone: str, day: str | date | datetime) -> Path:
    day = pd.Timestamp(day).date()
    zone_key = zone.lower()

    return silver_path(
        "entsoe",
        zone_key,
        dataset_name,
        f"year={day:%Y}",
        f"month={day:%m}",
        f"{dataset_name}_{day.isoformat()}.parquet",
    )


def _load_bronze_day(dataset_name: str, zone: str, day: str | date | datetime) -> pd.DataFrame:
    path = _bronze_daily_path(dataset_name, zone, day)
    if not path.exists():
        raise FileNotFoundError(f"Bronze file not found: {path}")
    return read_parquet(path)


def _save_silver_day(dataset_name: str, zone: str, day: str | date | datetime, df: pd.DataFrame) -> Path:
    path = _silver_daily_path(dataset_name, zone, day)
    write_parquet(df, path)
    return path


def _parse_resolution_to_timedelta(resolution: str) -> pd.Timedelta:
    if resolution == "PT15M":
        return pd.Timedelta(minutes=15)
    if resolution == "PT30M":
        return pd.Timedelta(minutes=30)
    if resolution in {"PT60M", "PT1H"}:
        return pd.Timedelta(hours=1)
    raise ValueError(f"Unsupported ENTSOE resolution: {resolution}")


def _entsoe_position_timestamp(df: pd.DataFrame) -> pd.Series:
    require_columns(
        df,
        ["periodStart", "resolution", "position"],
        "entsoe_timestamp_builder",
    )

    period_start = pd.to_datetime(df["periodStart"], utc=True, errors="coerce")
    position = pd.to_numeric(df["position"], errors="coerce")

    timedeltas = []
    for res, pos in zip(df["resolution"], position):
        if pd.isna(pos) or pd.isna(res):
            timedeltas.append(pd.NaT)
            continue
        step = _parse_resolution_to_timedelta(str(res))
        timedeltas.append((int(pos) - 1) * step)

    return period_start + pd.to_timedelta(timedeltas)


# ---------------------------------------------------------------------
# Empty-schema helpers
# ---------------------------------------------------------------------


def _empty_actual_total_load_df(zone: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_start_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_end_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "resolution": pd.Series(dtype="string"),
            "position": pd.Series(dtype="Int64"),
            "zone_code": pd.Series(dtype="string"),
            "area_code": pd.Series(dtype="string"),
            "load_mw": pd.Series(dtype="float64"),
            "unit": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
        }
    ).assign(zone_code=zone, source="entsoe")


def _empty_generation_per_type_df(zone: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_start_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_end_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "resolution": pd.Series(dtype="string"),
            "position": pd.Series(dtype="Int64"),
            "zone_code": pd.Series(dtype="string"),
            "area_code": pd.Series(dtype="string"),
            "psr_type": pd.Series(dtype="string"),
            "business_type": pd.Series(dtype="string"),
            "generation_mw": pd.Series(dtype="float64"),
            "unit": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
        }
    ).assign(zone_code=zone, source="entsoe")


def _empty_energy_prices_df(zone: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_start_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_end_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "resolution": pd.Series(dtype="string"),
            "position": pd.Series(dtype="Int64"),
            "zone_code": pd.Series(dtype="string"),
            "in_area_code": pd.Series(dtype="string"),
            "out_area_code": pd.Series(dtype="string"),
            "price": pd.Series(dtype="float64"),
            "currency_unit": pd.Series(dtype="string"),
            "price_measure_unit": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
        }
    ).assign(zone_code=zone, source="entsoe")


def clean_entsoe_actual_total_load(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    if df.empty:
        return _empty_actual_total_load_df(zone)

    require_columns(
        df,
        ["periodStart", "periodEnd", "resolution", "position", "quantity"],
        "entsoe_actual_total_load",
    )

    out = df.copy()

    out["timestamp_utc"] = _entsoe_position_timestamp(out)
    out["period_start_utc"] = pd.to_datetime(out["periodStart"], utc=True, errors="coerce")
    out["period_end_utc"] = pd.to_datetime(out["periodEnd"], utc=True, errors="coerce")
    out["resolution"] = out["resolution"].astype("string")
    out["position"] = pd.to_numeric(out["position"], errors="coerce").astype("Int64")
    out["load_mw"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["zone_code"] = zone
    out["area_code"] = out.get("outDomain", pd.Series(dtype="string")).astype("string")
    out["unit"] = out.get("quantityMeasureUnit", pd.Series(dtype="string")).astype("string")

    out = add_source_column(out, "entsoe")
    out = drop_rows_with_nulls(out, ["timestamp_utc", "position", "load_mw"])

    out = (
        out.sort_values(["timestamp_utc", "position"])
        .drop_duplicates(subset=["timestamp_utc"], keep="last")
        .reset_index(drop=True)
    )

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "period_start_utc",
            "period_end_utc",
            "resolution",
            "position",
            "zone_code",
            "area_code",
            "load_mw",
            "unit",
            "source",
        ],
    )

    return out


def clean_entsoe_generation_per_type(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    if df.empty:
        return _empty_generation_per_type_df(zone)

    require_columns(
        df,
        ["periodStart", "periodEnd", "resolution", "position", "quantity"],
        "entsoe_generation_per_type",
    )

    out = df.copy()

    out["timestamp_utc"] = _entsoe_position_timestamp(out)
    out["period_start_utc"] = pd.to_datetime(out["periodStart"], utc=True, errors="coerce")
    out["period_end_utc"] = pd.to_datetime(out["periodEnd"], utc=True, errors="coerce")
    out["resolution"] = out["resolution"].astype("string")
    out["position"] = pd.to_numeric(out["position"], errors="coerce").astype("Int64")
    out["generation_mw"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["psr_type"] = out.get("psrType", pd.Series(dtype="string")).astype("string")
    out["business_type"] = out.get("businessType", pd.Series(dtype="string")).astype("string")
    out["zone_code"] = zone
    out["area_code"] = out.get("inDomain", pd.Series(dtype="string")).astype("string")
    out["unit"] = out.get("quantityMeasureUnit", pd.Series(dtype="string")).astype("string")

    out = add_source_column(out, "entsoe")
    out = drop_rows_with_nulls(out, ["timestamp_utc", "position", "generation_mw"])

    out = (
        out.sort_values(["timestamp_utc", "position", "psr_type"])
        .drop_duplicates(subset=["timestamp_utc", "psr_type"], keep="last")
        .reset_index(drop=True)
    )

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "period_start_utc",
            "period_end_utc",
            "resolution",
            "position",
            "zone_code",
            "area_code",
            "psr_type",
            "business_type",
            "generation_mw",
            "unit",
            "source",
        ],
    )

    return out


def clean_entsoe_energy_prices(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    if df.empty:
        return _empty_energy_prices_df(zone)

    require_columns(
        df,
        ["periodStart", "periodEnd", "resolution", "position", "price.amount"],
        "entsoe_energy_prices",
    )

    out = df.copy()

    out["timestamp_utc"] = _entsoe_position_timestamp(out)
    out["period_start_utc"] = pd.to_datetime(out["periodStart"], utc=True, errors="coerce")
    out["period_end_utc"] = pd.to_datetime(out["periodEnd"], utc=True, errors="coerce")
    out["resolution"] = out["resolution"].astype("string")
    out["position"] = pd.to_numeric(out["position"], errors="coerce").astype("Int64")
    out["price"] = pd.to_numeric(out["price.amount"], errors="coerce")
    out["zone_code"] = zone
    out["in_area_code"] = out.get("inDomain", pd.Series(dtype="string")).astype("string")
    out["out_area_code"] = out.get("outDomain", pd.Series(dtype="string")).astype("string")
    out["currency_unit"] = out.get("currencyUnit", pd.Series(dtype="string")).astype("string")
    out["price_measure_unit"] = out.get("priceMeasureUnit", pd.Series(dtype="string")).astype("string")

    out = add_source_column(out, "entsoe")
    out = drop_rows_with_nulls(out, ["timestamp_utc", "position", "price"])

    out = (
        out.sort_values(["timestamp_utc", "position"])
        .drop_duplicates(subset=["timestamp_utc"], keep="last")
        .reset_index(drop=True)
    )

    out = reorder_columns(
        out,
        [
            "timestamp_utc",
            "period_start_utc",
            "period_end_utc",
            "resolution",
            "position",
            "zone_code",
            "in_area_code",
            "out_area_code",
            "price",
            "currency_unit",
            "price_measure_unit",
            "source",
        ],
    )

    return out


def run_actual_total_load_silver_day(
    zone: str,
    day: str | date | datetime,
    overwrite: bool = False,
) -> Path:
    target = _silver_daily_path("actual_total_load", zone, day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("actual_total_load", zone, day)
    silver_df = clean_entsoe_actual_total_load(bronze_df, zone=zone)
    return _save_silver_day("actual_total_load", zone, day, silver_df)


def run_generation_per_type_silver_day(
    zone: str,
    day: str | date | datetime,
    overwrite: bool = False,
) -> Path:
    target = _silver_daily_path("generation_per_type", zone, day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("generation_per_type", zone, day)
    silver_df = clean_entsoe_generation_per_type(bronze_df, zone=zone)
    return _save_silver_day("generation_per_type", zone, day, silver_df)


def run_energy_prices_silver_day(
    zone: str,
    day: str | date | datetime,
    overwrite: bool = False,
) -> Path:
    target = _silver_daily_path("energy_prices", zone, day)
    if target.exists() and not overwrite:
        return target

    bronze_df = _load_bronze_day("energy_prices", zone, day)
    silver_df = clean_entsoe_energy_prices(bronze_df, zone=zone)
    return _save_silver_day("energy_prices", zone, day, silver_df)


def run_actual_total_load_silver_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_actual_total_load_silver_day(zone, day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] actual_total_load silver {zone} {day} -> {path}")
    return outputs


def run_generation_per_type_silver_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_generation_per_type_silver_day(zone, day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] generation_per_type silver {zone} {day} -> {path}")
    return outputs


def run_energy_prices_silver_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> list[Path]:
    outputs: list[Path] = []
    for day in inclusive_date_range(date_from, date_to):
        path = run_energy_prices_silver_day(zone, day, overwrite=overwrite)
        outputs.append(path)
        print(f"[OK] energy_prices silver {zone} {day} -> {path}")
    return outputs


def run_entsoe_core_silver_history(
    zone: str,
    date_from: str | date | datetime,
    date_to: str | date | datetime,
    overwrite: bool = False,
) -> dict[str, list[Path]]:
    results: dict[str, list[Path]] = {}

    print(f"\n=== Building ENTSOE actual_total_load silver for {zone} ===")
    results["actual_total_load"] = run_actual_total_load_silver_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print(f"\n=== Building ENTSOE generation_per_type silver for {zone} ===")
    results["generation_per_type"] = run_generation_per_type_silver_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    print(f"\n=== Building ENTSOE energy_prices silver for {zone} ===")
    results["energy_prices"] = run_energy_prices_silver_history(
        zone=zone,
        date_from=date_from,
        date_to=date_to,
        overwrite=overwrite,
    )

    return results