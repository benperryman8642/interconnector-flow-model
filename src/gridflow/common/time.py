from __future__ import annotations

import pandas as pd

CANONICAL_TIMEZONE = "UTC"

from datetime import date, datetime, timedelta
from typing import Iterable


def coerce_date_string(value: str | date | datetime | None) -> str | None:
    """
    Convert a date-like value to ISO yyyy-mm-dd string.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def parse_date(value: str | date | datetime) -> date:
    """
    Convert a date-like value into a datetime.date.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def inclusive_date_range(
    start_date: str | date | datetime,
    end_date: str | date | datetime,
) -> list[date]:
    """
    Inclusive daily date range from start_date to end_date.
    """
    start = parse_date(start_date)
    end = parse_date(end_date)

    if end < start:
        raise ValueError("end_date must be on or after start_date")

    n_days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(n_days)]


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