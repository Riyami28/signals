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
    pg_dsn: str
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
    source_execution_policy_path: Path
    account_source_handles_path: Path
    signal_classes_path: Path
    account_profiles_path: Path
    discovery_thresholds_path: Path
    discovery_blocklist_path: Path
    promotion_policy_path: Path
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
    stage_timeout_seconds: int
    retry_attempt_limit: int
    watchlist_query_workers: int
    watchlist_country_query_timeout_seconds: int
    gchat_webhook_url: str
    alert_email_to: str
    alert_email_from: str
    alert_smtp_host: str
    alert_smtp_port: int
    alert_smtp_user: str
    alert_smtp_password: str
    alert_retry_depth_threshold: int
    alert_min_high_precision: float
    alert_min_medium_precision: float
    ops_metrics_lookback_days: int


def load_settings(project_root: Path | None = None) -> Settings:
    default_root = Path(__file__).resolve().parents[1]
    env_root = os.getenv("SIGNALS_PROJECT_ROOT")
    root = project_root or Path(env_root) if env_root else (project_root or default_root)

    _load_dotenv(root / ".env")

    pg_dsn = os.getenv("SIGNALS_PG_DSN", "").strip()
    if not pg_dsn:
        pg_host = os.getenv("SIGNALS_PG_HOST", "127.0.0.1").strip()
        pg_port = os.getenv("SIGNALS_PG_PORT", "55432").strip()
        pg_user = os.getenv("SIGNALS_PG_USER", "signals").strip()
        pg_password = os.getenv("SIGNALS_PG_PASSWORD", "signals_dev_password").strip()
        pg_database = os.getenv("SIGNALS_PG_DB", "signals").strip()
        pg_dsn = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
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

    stage_timeout_raw = os.getenv("SIGNALS_STAGE_TIMEOUT_SECONDS", "1800")
    try:
        stage_timeout_seconds = max(10, int(stage_timeout_raw))
    except ValueError:
        stage_timeout_seconds = 1800

    retry_attempt_limit_raw = os.getenv("SIGNALS_RETRY_ATTEMPT_LIMIT", "3")
    try:
        retry_attempt_limit = max(1, int(retry_attempt_limit_raw))
    except ValueError:
        retry_attempt_limit = 3

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

    smtp_port_raw = os.getenv("SIGNALS_ALERT_SMTP_PORT", "587")
    try:
        smtp_port = max(1, int(smtp_port_raw))
    except ValueError:
        smtp_port = 587

    retry_depth_threshold_raw = os.getenv("SIGNALS_ALERT_RETRY_DEPTH_THRESHOLD", "2")
    try:
        alert_retry_depth_threshold = max(1, int(retry_depth_threshold_raw))
    except ValueError:
        alert_retry_depth_threshold = 2

    high_precision_raw = os.getenv("SIGNALS_ALERT_MIN_HIGH_PRECISION", "0.65")
    try:
        alert_min_high_precision = max(0.0, min(1.0, float(high_precision_raw)))
    except ValueError:
        alert_min_high_precision = 0.65

    medium_precision_raw = os.getenv("SIGNALS_ALERT_MIN_MEDIUM_PRECISION", "0.55")
    try:
        alert_min_medium_precision = max(0.0, min(1.0, float(medium_precision_raw)))
    except ValueError:
        alert_min_medium_precision = 0.55

    ops_lookback_raw = os.getenv("SIGNALS_OPS_METRICS_LOOKBACK_DAYS", "14")
    try:
        ops_metrics_lookback_days = max(1, int(ops_lookback_raw))
    except ValueError:
        ops_metrics_lookback_days = 14

    return Settings(
        project_root=root,
        pg_dsn=pg_dsn,
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
        source_execution_policy_path=config_dir / "source_execution_policy.csv",
        account_source_handles_path=config_dir / "account_source_handles.csv",
        signal_classes_path=config_dir / "signal_classes.csv",
        account_profiles_path=config_dir / "account_profiles.csv",
        discovery_thresholds_path=config_dir / "discovery_thresholds.csv",
        discovery_blocklist_path=config_dir / "discovery_blocklist.csv",
        promotion_policy_path=config_dir / "promotion_policy.csv",
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
        stage_timeout_seconds=stage_timeout_seconds,
        retry_attempt_limit=retry_attempt_limit,
        watchlist_query_workers=watchlist_query_workers,
        watchlist_country_query_timeout_seconds=watchlist_country_query_timeout_seconds,
        gchat_webhook_url=os.getenv("SIGNALS_GCHAT_WEBHOOK_URL", "").strip(),
        alert_email_to=os.getenv("SIGNALS_ALERT_EMAIL_TO", "").strip(),
        alert_email_from=os.getenv("SIGNALS_ALERT_EMAIL_FROM", "").strip(),
        alert_smtp_host=os.getenv("SIGNALS_ALERT_SMTP_HOST", "").strip(),
        alert_smtp_port=smtp_port,
        alert_smtp_user=os.getenv("SIGNALS_ALERT_SMTP_USER", "").strip(),
        alert_smtp_password=os.getenv("SIGNALS_ALERT_SMTP_PASSWORD", "").strip(),
        alert_retry_depth_threshold=alert_retry_depth_threshold,
        alert_min_high_precision=alert_min_high_precision,
        alert_min_medium_precision=alert_min_medium_precision,
        ops_metrics_lookback_days=ops_metrics_lookback_days,
    )
