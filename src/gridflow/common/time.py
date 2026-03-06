from __future__ import annotations

import pandas as pd


CANONICAL_TIMEZONE = "UTC"


def to_utc_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(CANONICAL_TIMEZONE)
    else:
        ts = ts.tz_convert(CANONICAL_TIMEZONE)
    return ts


def ensure_utc_series(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series, utc=True)
    return series


def floor_to_frequency(series: pd.Series, freq: str) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.floor(freq)