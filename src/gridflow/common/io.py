"""Simple IO helpers (placeholder)"""
from pathlib import Path

def write_parquet(df, path: str):
    # placeholder, using pandas/pyarrow in real project
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"parquet-placeholder")
