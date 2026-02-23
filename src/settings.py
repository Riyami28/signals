from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIGNALS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- core paths (derived from project_root in validator) ---
    project_root: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[1])
    pg_dsn: str = Field(default="")
    pg_host: str = Field(default="127.0.0.1")
    pg_port: str = Field(default="55432")
    pg_user: str = Field(default="signals")
    pg_password: str = Field(default="signals_dev_password")
    pg_db: str = Field(default="signals")

    data_dir: Path = Field(default=Path(""))
    raw_dir: Path = Field(default=Path(""))
    out_dir: Path = Field(default=Path(""))
    config_dir: Path = Field(default=Path(""))

    # --- config file paths (derived) ---
    seed_accounts_path: Path = Field(default=Path(""))
    watchlist_accounts_path: Path = Field(default=Path(""))
    signal_registry_path: Path = Field(default=Path(""))
    source_registry_path: Path = Field(default=Path(""))
    thresholds_path: Path = Field(default=Path(""))
    keyword_lexicon_path: Path = Field(default=Path(""))
    source_execution_policy_path: Path = Field(default=Path(""))
    account_source_handles_path: Path = Field(default=Path(""))
    signal_classes_path: Path = Field(default=Path(""))
    account_profiles_path: Path = Field(default=Path(""))
    discovery_thresholds_path: Path = Field(default=Path(""))
    discovery_blocklist_path: Path = Field(default=Path(""))
    promotion_policy_path: Path = Field(default=Path(""))

    # --- Google Sheets ---
    google_sheet_id: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_SPREADSHEET_ID")
    google_service_account_file: Optional[Path] = Field(default=None, alias="GOOGLE_SERVICE_ACCOUNT_FILE")

    # --- runtime ---
    run_timezone: str = Field(default="America/Los_Angeles")
    enable_live_crawl: bool = Field(default=False)
    http_timeout_seconds: int = Field(default=10, ge=1)
    http_user_agent: str = Field(default="zopdev-signals/0.1")
    http_proxy_url: str = Field(default="")
    respect_robots_txt: bool = Field(default=True)
    min_domain_request_interval_ms: int = Field(default=2000, ge=0)
    live_max_accounts: int = Field(default=1000, ge=1)
    auto_discover_job_handles: bool = Field(default=False)
    live_max_jobs_per_source: int = Field(default=60, ge=1)
    discovery_webhook_token: str = Field(default="")
    discovery_event_batch_size: int = Field(default=500, ge=1)
    discovery_lookback_days: int = Field(default=120, ge=1)
    stage_timeout_seconds: int = Field(default=1800, ge=10)
    retry_attempt_limit: int = Field(default=3, ge=1)
    watchlist_query_workers: int = Field(default=8, ge=1)
    watchlist_country_query_timeout_seconds: int = Field(default=120, ge=15, alias="SIGNALS_WATCHLIST_COUNTRY_TIMEOUT_SECONDS")

    # --- alerting ---
    gchat_webhook_url: str = Field(default="")
    alert_email_to: str = Field(default="")
    alert_email_from: str = Field(default="")
    alert_smtp_host: str = Field(default="")
    alert_smtp_port: int = Field(default=587, ge=1)
    alert_smtp_user: str = Field(default="")
    alert_smtp_password: str = Field(default="")
    alert_retry_depth_threshold: int = Field(default=2, ge=1)
    alert_min_high_precision: float = Field(default=0.65, ge=0.0, le=1.0)
    alert_min_medium_precision: float = Field(default=0.55, ge=0.0, le=1.0)
    ops_metrics_lookback_days: int = Field(default=14, ge=1)

    # --- new fields for LLM research ---
    claude_api_key: str = Field(default="")
    claude_model: str = Field(default="claude-sonnet-4-5")
    research_max_accounts: int = Field(default=20, ge=1, le=200)
    research_stale_days: int = Field(default=30, ge=1)
    research_timeout_seconds: int = Field(default=120, ge=10)

    # --- new fields for waterfall enrichment ---
    clearbit_api_key: str = Field(default="")
    hunter_api_key: str = Field(default="")

    @model_validator(mode="after")
    def _derive_paths_and_dsn(self) -> "Settings":
        root = self.project_root

        # Derive pg_dsn from components if not explicitly set.
        if not self.pg_dsn:
            self.pg_dsn = (
                f"postgresql://{self.pg_user}:{self.pg_password}"
                f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
            )

        # Derive directory paths from project_root.
        config_dir = root / "config"
        data_dir = root / "data"
        if self.config_dir == Path(""):
            self.config_dir = config_dir
        if self.data_dir == Path(""):
            self.data_dir = data_dir
        if self.raw_dir == Path(""):
            self.raw_dir = data_dir / "raw"
        if self.out_dir == Path(""):
            self.out_dir = data_dir / "out"

        # Derive config file paths.
        cd = self.config_dir
        if self.seed_accounts_path == Path(""):
            self.seed_accounts_path = cd / "seed_accounts.csv"
        if self.watchlist_accounts_path == Path(""):
            self.watchlist_accounts_path = cd / "watchlist_accounts.csv"
        if self.signal_registry_path == Path(""):
            self.signal_registry_path = cd / "signal_registry.csv"
        if self.source_registry_path == Path(""):
            self.source_registry_path = cd / "source_registry.csv"
        if self.thresholds_path == Path(""):
            self.thresholds_path = cd / "thresholds.csv"
        if self.keyword_lexicon_path == Path(""):
            self.keyword_lexicon_path = cd / "keyword_lexicon.csv"
        if self.source_execution_policy_path == Path(""):
            self.source_execution_policy_path = cd / "source_execution_policy.csv"
        if self.account_source_handles_path == Path(""):
            self.account_source_handles_path = cd / "account_source_handles.csv"
        if self.signal_classes_path == Path(""):
            self.signal_classes_path = cd / "signal_classes.csv"
        if self.account_profiles_path == Path(""):
            self.account_profiles_path = cd / "account_profiles.csv"
        if self.discovery_thresholds_path == Path(""):
            self.discovery_thresholds_path = cd / "discovery_thresholds.csv"
        if self.discovery_blocklist_path == Path(""):
            self.discovery_blocklist_path = cd / "discovery_blocklist.csv"
        if self.promotion_policy_path == Path(""):
            self.promotion_policy_path = cd / "promotion_policy.csv"

        # Normalize empty google_sheet_id to None.
        if self.google_sheet_id is not None and not self.google_sheet_id.strip():
            self.google_sheet_id = None

        return self


def load_settings(project_root: Path | None = None) -> Settings:
    """Backward-compatible factory. Prefer ``Settings()`` for new code."""
    kwargs: dict = {}
    if project_root is not None:
        kwargs["project_root"] = project_root
        # When a custom root is given (tests, webhook), read .env from that root.
        kwargs["_env_file"] = str(project_root / ".env")
    return Settings(**kwargs)
