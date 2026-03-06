from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from gridflow.common.time import parse_date


def year_month_parts(value: str | date | datetime) -> tuple[str, str]:
    """
    Return partition folder parts like:
    ('year=2026', 'month=03')
    """
    day = parse_date(value)
    return (f"year={day:%Y}", f"month={day:%m}")


def year_month_day_string(value: str | date | datetime) -> str:
    """
    Return yyyy-mm-dd string for filenames.
    """
    return parse_date(value).isoformat()


def build_partitioned_file_path(
    root: Path,
    *prefix_parts: str,
    partition_date: str | date | datetime,
    filename: str,
) -> Path:
    """
    Build a path like:
    root / prefix_parts... / year=YYYY / month=MM / filename
    """
    year_part, month_part = year_month_parts(partition_date)
    return root.joinpath(*prefix_parts, year_part, month_part, filename)