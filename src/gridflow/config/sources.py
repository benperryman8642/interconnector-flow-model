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


@dataclass(frozen=True)
class EntsoeDataset:
    name: str
    document_type: str
    process_type: str | None = None


ELEXON: Final[ApiSource] = ApiSource(
    name="elexon",
    base_url="https://data.elexon.co.uk/bmrs/api/v1",
    timeout_seconds=30,
)

ENTSOE: Final[ApiSource] = ApiSource(
    name="entsoe",
    base_url="https://web-api.tp.entsoe.eu/api",
    timeout_seconds=60,
)


ELEXON_DATASETS: Final[dict[str, ElexonDataset]] = {
    "fuelhh": ElexonDataset(
        name="fuelhh",
        path="/datasets/FUELHH",
        stream_path="/datasets/FUELHH/stream",
    ),
    "demand_actual_total": ElexonDataset(
        name="demand_actual_total",
        path="/demand/actual/total",
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


ENTSOE_ZONES: Final[dict[str, str]] = {
    "FR": "10YFR-RTE------C",
    "IE_SEM": "10Y1001A1001A59C",
    "BE": "10YBE----------2",
    "NL": "10YNL----------L",

    "DE_LU": "10Y1001A1001A82H",

    "NO1": "10YNO-1--------2",
    "NO2": "10YNO-2--------T",
    "NO3": "10YNO-3--------J",
    "NO4": "10YNO-4--------9",
    "NO5": "10Y1001A1001A48H",

    "DK1": "10YDK-1--------W",
    "DK2": "10YDK-2--------M",

    "SE1": "10Y1001A1001A44P",
    "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L",
    "SE4": "10Y1001A1001A47J",
}




ENTSOE_DATASETS: Final[dict[str, EntsoeDataset]] = {
    "actual_total_load": EntsoeDataset(
        name="actual_total_load",
        document_type="A65",
        process_type="A16",
    ),
    "generation_per_type": EntsoeDataset(
        name="generation_per_type",
        document_type="A75",
        process_type="A16",
    ),
    "energy_prices": EntsoeDataset(
        name="energy_prices",
        document_type="A44",
        process_type=None,
    ),
}


def build_url(source: ApiSource, path: str) -> str:
    return f"{source.base_url.rstrip('/')}/{path.lstrip('/')}"