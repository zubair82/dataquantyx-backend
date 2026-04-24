from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BACKEND_DIR / "data"
PLOTS_DIR = BACKEND_DIR / "plots"
REPORTS_DIR = BACKEND_DIR / "reports"
DB_DIR = BACKEND_DIR / "db"
DB_PATH = DB_DIR / "data_quantyx.db"


def resolve_storage_path(path_value):
    path = Path(path_value)
    if path.is_absolute():
        return path
    return BACKEND_DIR / path


def public_asset_url(asset_type: str, filename: str) -> str:
    return f"/{asset_type}/{filename}"
