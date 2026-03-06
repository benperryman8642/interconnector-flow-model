from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from gridflow.common.io import read_parquet, write_parquet


def append_manifest_rows(
    manifest_path: str | Path,
    rows: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Append rows to a parquet manifest file and return the combined manifest.
    """
    manifest_path = Path(manifest_path)
    new_df = pd.DataFrame(rows)

    if manifest_path.exists():
        existing_df = read_parquet(manifest_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    write_parquet(combined_df, manifest_path)
    return combined_df