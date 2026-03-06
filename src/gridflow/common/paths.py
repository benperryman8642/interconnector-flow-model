from __future__ import annotations

from pathlib import Path


def _find_project_root() -> Path:
    """
    Find the project root by walking up from this file.
    Assumes this file lives at: src/gridflow/common/paths.py
    """
    return Path(__file__).resolve().parents[3]


PROJECT_ROOT = _find_project_root()

CONFIG_ROOT = PROJECT_ROOT / "config"
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_ROOT = DATA_ROOT / "bronze"
SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"

OUTPUTS_ROOT = PROJECT_ROOT / "outputs"
FIGURES_ROOT = OUTPUTS_ROOT / "figures"
FORECASTS_ROOT = OUTPUTS_ROOT / "forecasts"
MODELS_ROOT = OUTPUTS_ROOT / "models"
REPORTS_ROOT = OUTPUTS_ROOT / "reports"

NOTEBOOKS_ROOT = PROJECT_ROOT / "notebooks"
TESTS_ROOT = PROJECT_ROOT / "tests"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def bronze_path(*parts: str, ensure: bool = False) -> Path:
    path = BRONZE_ROOT.joinpath(*parts)
    return ensure_dir(path) if ensure else path


def silver_path(*parts: str, ensure: bool = False) -> Path:
    path = SILVER_ROOT.joinpath(*parts)
    return ensure_dir(path) if ensure else path


def gold_path(*parts: str, ensure: bool = False) -> Path:
    path = GOLD_ROOT.joinpath(*parts)
    return ensure_dir(path) if ensure else path


def output_path(*parts: str, ensure: bool = False) -> Path:
    path = OUTPUTS_ROOT.joinpath(*parts)
    return ensure_dir(path) if ensure else path