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
    watchlist_accounts_path: Path
    signal_registry_path: Path
    source_registry_path: Path
    thresholds_path: Path
    keyword_lexicon_path: Path
    account_source_handles_path: Path
    signal_classes_path: Path
    account_profiles_path: Path
    discovery_thresholds_path: Path
    discovery_blocklist_path: Path
    google_sheet_id: str | None
    google_service_account_file: Path | None
    run_timezone: str
    enable_live_crawl: bool
    http_timeout_seconds: int
    http_user_agent: str
    http_proxy_url: str
    respect_robots_txt: bool
    min_domain_request_interval_ms: int
    live_max_accounts: int
    auto_discover_job_handles: bool
    live_max_jobs_per_source: int
    discovery_webhook_token: str
    discovery_event_batch_size: int
    discovery_lookback_days: int
    watchlist_query_workers: int
    watchlist_country_query_timeout_seconds: int


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

    min_interval_raw = os.getenv("SIGNALS_MIN_DOMAIN_REQUEST_INTERVAL_MS", "2000")
    try:
        min_interval_ms = max(0, int(min_interval_raw))
    except ValueError:
        min_interval_ms = 2000

    max_accounts_raw = os.getenv("SIGNALS_LIVE_MAX_ACCOUNTS", "1000")
    try:
        live_max_accounts = max(1, int(max_accounts_raw))
    except ValueError:
        live_max_accounts = 100

    max_jobs_raw = os.getenv("SIGNALS_LIVE_MAX_JOBS_PER_SOURCE", "60")
    try:
        live_max_jobs_per_source = max(1, int(max_jobs_raw))
    except ValueError:
        live_max_jobs_per_source = 60

    discovery_batch_raw = os.getenv("SIGNALS_DISCOVERY_EVENT_BATCH_SIZE", "500")
    try:
        discovery_event_batch_size = max(1, int(discovery_batch_raw))
    except ValueError:
        discovery_event_batch_size = 500

    discovery_lookback_raw = os.getenv("SIGNALS_DISCOVERY_LOOKBACK_DAYS", "120")
    try:
        discovery_lookback_days = max(1, int(discovery_lookback_raw))
    except ValueError:
        discovery_lookback_days = 120

    watchlist_workers_raw = os.getenv("SIGNALS_WATCHLIST_QUERY_WORKERS", "8")
    try:
        watchlist_query_workers = max(1, int(watchlist_workers_raw))
    except ValueError:
        watchlist_query_workers = 8

    watchlist_timeout_raw = os.getenv("SIGNALS_WATCHLIST_COUNTRY_TIMEOUT_SECONDS", "120")
    try:
        watchlist_country_query_timeout_seconds = max(15, int(watchlist_timeout_raw))
    except ValueError:
        watchlist_country_query_timeout_seconds = 120

    return Settings(
        project_root=root,
        db_path=db_path,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        out_dir=data_dir / "out",
        config_dir=config_dir,
        seed_accounts_path=config_dir / "seed_accounts.csv",
        watchlist_accounts_path=config_dir / "watchlist_accounts.csv",
        signal_registry_path=config_dir / "signal_registry.csv",
        source_registry_path=config_dir / "source_registry.csv",
        thresholds_path=config_dir / "thresholds.csv",
        keyword_lexicon_path=config_dir / "keyword_lexicon.csv",
        account_source_handles_path=config_dir / "account_source_handles.csv",
        signal_classes_path=config_dir / "signal_classes.csv",
        account_profiles_path=config_dir / "account_profiles.csv",
        discovery_thresholds_path=config_dir / "discovery_thresholds.csv",
        discovery_blocklist_path=config_dir / "discovery_blocklist.csv",
        google_sheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip() or None,
        google_service_account_file=google_service_account_file,
        run_timezone=os.getenv("SIGNALS_RUN_TIMEZONE", "America/Los_Angeles"),
        enable_live_crawl=_parse_bool(os.getenv("SIGNALS_ENABLE_LIVE_CRAWL"), default=False),
        http_timeout_seconds=timeout_seconds,
        http_user_agent=os.getenv("SIGNALS_HTTP_USER_AGENT", "zopdev-signals/0.1"),
        http_proxy_url=os.getenv("SIGNALS_HTTP_PROXY_URL", "").strip(),
        respect_robots_txt=_parse_bool(os.getenv("SIGNALS_RESPECT_ROBOTS_TXT"), default=True),
        min_domain_request_interval_ms=min_interval_ms,
        live_max_accounts=live_max_accounts,
        auto_discover_job_handles=_parse_bool(os.getenv("SIGNALS_AUTO_DISCOVER_JOB_HANDLES"), default=False),
        live_max_jobs_per_source=live_max_jobs_per_source,
        discovery_webhook_token=os.getenv("SIGNALS_DISCOVERY_WEBHOOK_TOKEN", "").strip(),
        discovery_event_batch_size=discovery_event_batch_size,
        discovery_lookback_days=discovery_lookback_days,
        watchlist_query_workers=watchlist_query_workers,
        watchlist_country_query_timeout_seconds=watchlist_country_query_timeout_seconds,
    )
