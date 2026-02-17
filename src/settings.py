from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    project_root: Path
    db_path: Path
    data_dir: Path
    raw_dir: Path
    out_dir: Path
    config_dir: Path
    seed_accounts_path: Path
    signal_registry_path: Path
    source_registry_path: Path
    thresholds_path: Path
    keyword_lexicon_path: Path
    account_source_handles_path: Path
    google_sheet_id: str | None
    google_service_account_file: Path | None
    run_timezone: str
    enable_live_crawl: bool
    http_timeout_seconds: int
    http_user_agent: str
    live_max_accounts: int
    auto_discover_job_handles: bool
    live_max_jobs_per_source: int


def load_settings(project_root: Path | None = None) -> Settings:
    default_root = Path(__file__).resolve().parents[1]
    env_root = os.getenv("SIGNALS_PROJECT_ROOT")
    root = project_root or Path(env_root) if env_root else (project_root or default_root)

    _load_dotenv(root / ".env")

    db_path = Path(os.getenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db")))
    config_dir = root / "config"
    data_dir = root / "data"

    google_service_account_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    google_service_account_file = Path(google_service_account_raw) if google_service_account_raw else None

    timeout_raw = os.getenv("SIGNALS_HTTP_TIMEOUT_SECONDS", "10")
    try:
        timeout_seconds = max(1, int(timeout_raw))
    except ValueError:
        timeout_seconds = 10

    max_accounts_raw = os.getenv("SIGNALS_LIVE_MAX_ACCOUNTS", "100")
    try:
        live_max_accounts = max(1, int(max_accounts_raw))
    except ValueError:
        live_max_accounts = 100

    max_jobs_raw = os.getenv("SIGNALS_LIVE_MAX_JOBS_PER_SOURCE", "60")
    try:
        live_max_jobs_per_source = max(1, int(max_jobs_raw))
    except ValueError:
        live_max_jobs_per_source = 60

    return Settings(
        project_root=root,
        db_path=db_path,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        out_dir=data_dir / "out",
        config_dir=config_dir,
        seed_accounts_path=config_dir / "seed_accounts.csv",
        signal_registry_path=config_dir / "signal_registry.csv",
        source_registry_path=config_dir / "source_registry.csv",
        thresholds_path=config_dir / "thresholds.csv",
        keyword_lexicon_path=config_dir / "keyword_lexicon.csv",
        account_source_handles_path=config_dir / "account_source_handles.csv",
        google_sheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip() or None,
        google_service_account_file=google_service_account_file,
        run_timezone=os.getenv("SIGNALS_RUN_TIMEZONE", "America/Los_Angeles"),
        enable_live_crawl=_parse_bool(os.getenv("SIGNALS_ENABLE_LIVE_CRAWL"), default=False),
        http_timeout_seconds=timeout_seconds,
        http_user_agent=os.getenv("SIGNALS_HTTP_USER_AGENT", "zopdev-signals/0.1"),
        live_max_accounts=live_max_accounts,
        auto_discover_job_handles=_parse_bool(os.getenv("SIGNALS_AUTO_DISCOVER_JOB_HANDLES"), default=False),
        live_max_jobs_per_source=live_max_jobs_per_source,
    )
