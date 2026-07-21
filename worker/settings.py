"""
file-processor service configuration.

file-processor is an independent HTTP service: core-api POSTs files to /process
(no shared filesystem). It also watches local drop folders. Common settings
(DBSettings, logging, DEV_MODE, ROOT) come from the local ``Config`` package.
"""
import os
from pathlib import Path

# --- Load THIS service's own .env before importing the shared Config ---------
# Each segment owns its environment file (repo-root .env[.dev]);
# they do NOT share a single root .env. We point the shared Config at our file
# via ENV_PATH / ENV_DEV_PATH (which Config already honours). In containers the
# vars are usually injected directly (compose env_file / --env-file).
_SERVICE_ROOT = Path(__file__).resolve().parents[1]
_DEV = os.getenv("DEV_MODE", "false").lower() in ("1", "true", "yes", "on")
_ENV_VAR = "ENV_DEV_PATH" if _DEV else "ENV_PATH"
if not os.getenv(_ENV_VAR):
    _env_file = _SERVICE_ROOT / (".env.dev" if _DEV else ".env")
    if _env_file.exists():
        os.environ[_ENV_VAR] = str(_env_file)
# -----------------------------------------------------------------------------

from Config import ROOT, require_env

# HTTP server (receives files from core-api)
HOST: str = require_env("HOST", "0.0.0.0")
PORT: int = int(require_env("PORT", 8000))

# Token core-api must present (X-Service-Token) when forwarding a file to /process.
SERVICE_TOKEN: str = require_env("SERVICE_TOKEN", "")

# Processing queue: how many files are ingested concurrently (bounded by the worker pool).
FP_WORKERS: int = int(require_env("FP_WORKERS", 2))

# Watched input folders (manual local drops)
FILES_PATH: Path = ROOT / "input_files"
EXCEL_FILES_PATH: Path = FILES_PATH / "excel_db"
CIRIUM_FILES_PATH: Path = EXCEL_FILES_PATH / "cirium"                       # drop -> plan_type Commercial
CIRIUM_BUSINESS_FILES_PATH: Path = EXCEL_FILES_PATH / "cirium_business"     # drop -> plan_type Business&Helicopters
NOPASSED_PATH: Path = ROOT / "nopassed"
# Files received over HTTP land here (this service's OWN local storage).
INTAKE_PATH: Path = ROOT / "intake"

for _p in (FILES_PATH, EXCEL_FILES_PATH, CIRIUM_FILES_PATH, CIRIUM_BUSINESS_FILES_PATH,
           NOPASSED_PATH, INTAKE_PATH):
    _p.mkdir(parents=True, exist_ok=True)
