from __future__ import annotations

from typing import Iterable

import pandas as pd


def require_columns(df: pd.DataFrame, required: Iterable[str], dataset_name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            f"{dataset_name}: missing required columns: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def parse_utc_column(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(df[column], utc=True, errors="coerce")


def add_source_column(df: pd.DataFrame, source: str) -> pd.DataFrame:
    out = df.copy()
    out["source"] = source
    return out


def reorder_columns(df: pd.DataFrame, preferred_order: list[str]) -> pd.DataFrame:
    existing_preferred = [c for c in preferred_order if c in df.columns]
    remaining = [c for c in df.columns if c not in existing_preferred]
    return df[existing_preferred + remaining]


def ensure_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def drop_rows_with_nulls(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    return df.dropna(subset=[c for c in subset if c in df.columns]).copy()