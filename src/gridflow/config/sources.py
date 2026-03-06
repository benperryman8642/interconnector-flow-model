from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class ApiSource:
    name: str
    base_url: str
    timeout_seconds: int = 30


@dataclass(frozen=True)
class ElexonDataset:
    name: str
    path: str
    stream_path: str | None = None


# Core source definitions
ELEXON: Final[ApiSource] = ApiSource(
    name="elexon",
    base_url="https://data.elexon.co.uk/bmrs/api/v1",
    timeout_seconds=30,
)


# Dataset definitions
ELEXON_DATASETS: Final[dict[str, ElexonDataset]] = {
    "fuelhh": ElexonDataset(
        name="fuelhh",
        path="/datasets/FUELHH",
        stream_path="/datasets/FUELHH/stream",
    ),
    "indo": ElexonDataset(
        name="indo",
        path="/demand/outturn",
        stream_path="/demand/outturn/stream",
    ),
    "itsdo": ElexonDataset(
        name="itsdo",
        path="/demand/peak",
        stream_path=None,
    ),
    "mid": ElexonDataset(
        name="mid",
        path="/datasets/MID",
        stream_path="/datasets/MID/stream",
    ),
    "metadata_latest": ElexonDataset(
        name="metadata_latest",
        path="/datasets/metadata/latest",
        stream_path=None,
    ),
}


def build_url(source: ApiSource, path: str) -> str:
    return f"{source.base_url.rstrip('/')}/{path.lstrip('/')}"