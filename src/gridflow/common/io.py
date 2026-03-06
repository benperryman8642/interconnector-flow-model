from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_parent_dir(path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_parquet(df: pd.DataFrame, path: str | Path, index: bool = False) -> Path:
    path = ensure_parent_dir(path)
    df.to_parquet(path, index=index)
    return path


def read_parquet(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    path = Path(path)
    return pd.read_parquet(path, **kwargs)


def write_csv(df: pd.DataFrame, path: str | Path, index: bool = False) -> Path:
    path = ensure_parent_dir(path)
    df.to_csv(path, index=index)
    return path


def read_csv(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    path = Path(path)
    return pd.read_csv(path, **kwargs)


def write_json(data: Any, path: str | Path, indent: int = 2) -> Path:
    path = ensure_parent_dir(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
    return path


def read_json(path: str | Path) -> Any:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)