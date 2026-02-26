from __future__ import annotations

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL UNIQUE,
  source_type TEXT NOT NULL CHECK (source_type IN ('seed', 'discovered')),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signal_observations (
  obs_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight', 'shared')),
  source TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  evidence_url TEXT,
  evidence_text TEXT,
  document_id TEXT NOT NULL DEFAULT '',
  mention_id TEXT NOT NULL DEFAULT '',
  evidence_sentence TEXT NOT NULL DEFAULT '',
  evidence_sentence_en TEXT NOT NULL DEFAULT '',
  matched_phrase TEXT NOT NULL DEFAULT '',
  language TEXT NOT NULL DEFAULT '',
  speaker_name TEXT NOT NULL DEFAULT '',
  speaker_role TEXT NOT NULL DEFAULT '',
  evidence_quality REAL NOT NULL DEFAULT 0.0,
  relevance_score REAL NOT NULL DEFAULT 0.0,
  confidence REAL NOT NULL,
  source_reliability REAL NOT NULL,
  raw_payload_hash TEXT NOT NULL,
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_observation_dedupe
ON signal_observations(account_id, signal_code, source, observed_at, raw_payload_hash);

CREATE INDEX IF NOT EXISTS idx_signal_observations_account_observed
ON signal_observations(account_id, observed_at);

CREATE TABLE IF NOT EXISTS score_runs (
  run_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  error_summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_score_runs_date ON score_runs(run_date);

CREATE TABLE IF NOT EXISTS score_components (
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight')),
  signal_code TEXT NOT NULL,
  component_score REAL NOT NULL,
  PRIMARY KEY (run_id, account_id, product, signal_code),
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE IF NOT EXISTS account_scores (
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight')),
  score REAL NOT NULL,
  tier TEXT NOT NULL CHECK (tier IN ('high', 'medium', 'low')),
  tier_v2 TEXT NOT NULL DEFAULT 'tier_4' CHECK (tier_v2 IN ('tier_1', 'tier_2', 'tier_3', 'tier_4')),
  top_reasons_json TEXT NOT NULL,
  delta_7d REAL NOT NULL,
  dimension_scores_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (run_id, account_id, product),
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE IF NOT EXISTS review_labels (
  review_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected', 'needs_more_info')),
  reviewer TEXT NOT NULL,
  notes TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX IF NOT EXISTS idx_review_labels_run ON review_labels(run_id);

CREATE TABLE IF NOT EXISTS source_metrics (
  run_date TEXT NOT NULL,
  source TEXT NOT NULL,
  approved_rate REAL NOT NULL,
  sample_size INTEGER NOT NULL,
  PRIMARY KEY (run_date, source)
);

CREATE TABLE IF NOT EXISTS crawl_checkpoints (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  last_crawled_at TEXT NOT NULL,
  PRIMARY KEY (source, account_id, endpoint)
);

CREATE TABLE IF NOT EXISTS crawl_attempts (
  attempt_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  attempted_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('success', 'http_error', 'exception', 'skipped')),
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_crawl_attempts_attempted_at ON crawl_attempts(attempted_at);
CREATE INDEX IF NOT EXISTS idx_crawl_attempts_source_attempted_at ON crawl_attempts(source, attempted_at);

CREATE TABLE IF NOT EXISTS external_discovery_events (
  event_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE,
  observed_at TEXT NOT NULL,
  title TEXT NOT NULL,
  text TEXT NOT NULL,
  url TEXT NOT NULL,
  entry_url TEXT NOT NULL DEFAULT '',
  url_type TEXT NOT NULL DEFAULT '',
  language_hint TEXT NOT NULL DEFAULT '',
  author_hint TEXT NOT NULL DEFAULT '',
  published_at_hint TEXT NOT NULL DEFAULT '',
  company_name_hint TEXT NOT NULL,
  domain_hint TEXT NOT NULL,
  raw_payload_json TEXT NOT NULL,
  ingested_at TEXT NOT NULL,
  processing_status TEXT NOT NULL CHECK (processing_status IN ('pending', 'processed', 'failed')),
  processed_run_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_external_discovery_events_status_observed
ON external_discovery_events(processing_status, observed_at);

CREATE TABLE IF NOT EXISTS discovery_runs (
  discovery_run_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  score_run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  source_events_processed INTEGER NOT NULL,
  observations_inserted INTEGER NOT NULL,
  total_candidates INTEGER NOT NULL,
  crm_eligible_candidates INTEGER NOT NULL,
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_discovery_runs_date ON discovery_runs(run_date);

CREATE TABLE IF NOT EXISTS discovery_candidates (
  discovery_run_id TEXT NOT NULL,
  score_run_id TEXT NOT NULL,
  run_date TEXT NOT NULL,
  account_id TEXT NOT NULL,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  best_product TEXT NOT NULL,
  score REAL NOT NULL,
  tier TEXT NOT NULL,
  confidence_band TEXT NOT NULL CHECK (confidence_band IN ('high', 'medium', 'explore')),
  cpg_like_group_count INTEGER NOT NULL,
  primary_signal_count INTEGER NOT NULL,
  source_count INTEGER NOT NULL,
  has_poc_progression_first_party INTEGER NOT NULL,
  relationship_stage TEXT NOT NULL,
  vertical_tag TEXT NOT NULL,
  is_self INTEGER NOT NULL,
  exclude_from_crm INTEGER NOT NULL,
  eligible_for_crm INTEGER NOT NULL,
  novelty_score REAL NOT NULL,
  rank_score REAL NOT NULL,
  reasons_json TEXT NOT NULL,
  PRIMARY KEY (discovery_run_id, account_id)
);

CREATE TABLE IF NOT EXISTS discovery_evidence (
  discovery_run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  source TEXT NOT NULL,
  evidence_url TEXT NOT NULL,
  evidence_text TEXT NOT NULL,
  component_score REAL NOT NULL,
  PRIMARY KEY (discovery_run_id, account_id, signal_code, source, evidence_url)
);

CREATE TABLE IF NOT EXISTS crawl_frontier (
  frontier_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  url_type TEXT NOT NULL CHECK (url_type IN ('article', 'listing', 'profile', 'other')),
  depth INTEGER NOT NULL,
  priority REAL NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'fetched', 'parsed', 'failed', 'skipped')),
  retry_count INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_attempt_at TEXT NOT NULL,
  last_error TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  UNIQUE(run_date, canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_status_priority
ON crawl_frontier(status, priority DESC, first_seen_at ASC);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_run_date
ON crawl_frontier(run_date);

CREATE TABLE IF NOT EXISTS documents (
  document_id TEXT PRIMARY KEY,
  frontier_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL UNIQUE,
  content_sha256 TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  author TEXT NOT NULL,
  published_at TEXT NOT NULL,
  section TEXT NOT NULL,
  language TEXT NOT NULL,
  body_text TEXT NOT NULL,
  body_text_en TEXT NOT NULL,
  raw_html TEXT NOT NULL,
  parser_version TEXT NOT NULL,
  evidence_quality REAL NOT NULL,
  relevance_score REAL NOT NULL,
  fetched_with TEXT NOT NULL,
  outbound_links_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_account_created
ON documents(account_id, created_at DESC);

CREATE TABLE IF NOT EXISTS document_mentions (
  mention_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  matched_phrase TEXT NOT NULL,
  evidence_sentence TEXT NOT NULL,
  evidence_sentence_en TEXT NOT NULL,
  language TEXT NOT NULL,
  speaker_name TEXT NOT NULL,
  speaker_role TEXT NOT NULL,
  confidence REAL NOT NULL,
  evidence_quality REAL NOT NULL,
  relevance_score REAL NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(document_id, signal_code, matched_phrase)
);

CREATE INDEX IF NOT EXISTS idx_document_mentions_account_signal
ON document_mentions(account_id, signal_code);

CREATE TABLE IF NOT EXISTS observation_lineage (
  obs_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  mention_id TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  run_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observation_lineage_run_date
ON observation_lineage(run_date);

CREATE TABLE IF NOT EXISTS people_watchlist (
  watch_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  role_weight REAL NOT NULL,
  source_url TEXT NOT NULL,
  is_active INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(account_id, person_name, role_title)
);

CREATE TABLE IF NOT EXISTS people_activity (
  activity_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  document_id TEXT NOT NULL,
  activity_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  published_at TEXT NOT NULL,
  url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(account_id, person_name, document_id, activity_type)
);

CREATE TABLE IF NOT EXISTS run_lock_events (
  event_id BIGSERIAL PRIMARY KEY,
  lock_name TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('acquired', 'released', 'busy', 'release_missed')),
  details TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_lock_events_lock_name_created
ON run_lock_events(lock_name, created_at);

CREATE TABLE IF NOT EXISTS stage_failures (
  failure_id BIGSERIAL PRIMARY KEY,
  run_type TEXT NOT NULL,
  run_date TEXT NOT NULL,
  stage TEXT NOT NULL,
  duration_seconds REAL NOT NULL DEFAULT 0,
  timed_out INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT NOT NULL DEFAULT '',
  retry_task_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stage_failures_run_date_created
ON stage_failures(run_date, created_at);

CREATE TABLE IF NOT EXISTS retry_queue (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'quarantined', 'failed')),
  due_at TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retry_queue_status_due
ON retry_queue(status, due_at);

CREATE TABLE IF NOT EXISTS quarantine_failures (
  quarantine_id BIGSERIAL PRIMARY KEY,
  task_id TEXT NOT NULL,
  task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT NOT NULL DEFAULT '',
  quarantined_at TEXT NOT NULL,
  resolved INTEGER NOT NULL DEFAULT 0,
  resolved_at TEXT NOT NULL DEFAULT '',
  resolution_note TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_quarantine_failures_resolved
ON quarantine_failures(resolved, quarantined_at);

CREATE TABLE IF NOT EXISTS ops_metrics (
  metric_id BIGSERIAL PRIMARY KEY,
  run_date TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  metric TEXT NOT NULL,
  value REAL NOT NULL,
  meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_ops_metrics_run_date_metric
ON ops_metrics(run_date, metric, recorded_at);

CREATE TABLE IF NOT EXISTS company_research (
    account_id          TEXT PRIMARY KEY REFERENCES accounts(account_id),
    research_brief      TEXT,
    research_profile    TEXT,
    enrichment_json     TEXT NOT NULL DEFAULT '{}',
    research_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (research_status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    researched_at       TEXT,
    model_used          TEXT,
    prompt_hash         TEXT,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contact_research (
    contact_id          TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL REFERENCES accounts(account_id),
    first_name          TEXT NOT NULL,
    last_name           TEXT NOT NULL,
    title               TEXT,
    email               TEXT,
    linkedin_url        TEXT,
    management_level    TEXT
        CHECK (management_level IN ('C-Level', 'VP', 'Director', 'Manager', 'IC')),
    year_joined         INTEGER,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contact_research_account
    ON contact_research(account_id);

CREATE TABLE IF NOT EXISTS contacts (
    contact_id          TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL,
    first_name          TEXT NOT NULL DEFAULT '',
    last_name           TEXT NOT NULL DEFAULT '',
    title               TEXT NOT NULL DEFAULT '',
    email               TEXT NOT NULL DEFAULT '',
    email_verified      BOOLEAN NOT NULL DEFAULT FALSE,
    phone               TEXT NOT NULL DEFAULT '',
    linkedin_url        TEXT NOT NULL DEFAULT '',
    enrichment_source   TEXT NOT NULL DEFAULT '',
    enriched_at         TEXT NOT NULL DEFAULT '',
    confidence          REAL NOT NULL DEFAULT 0.0,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX IF NOT EXISTS idx_contacts_account ON contacts(account_id);

CREATE TABLE IF NOT EXISTS research_runs (
    research_run_id     TEXT PRIMARY KEY,
    run_date            TEXT NOT NULL,
    score_run_id        TEXT NOT NULL,
    accounts_attempted  INTEGER NOT NULL DEFAULT 0,
    accounts_completed  INTEGER NOT NULL DEFAULT 0,
    accounts_failed     INTEGER NOT NULL DEFAULT 0,
    accounts_skipped    INTEGER NOT NULL DEFAULT 0,
    started_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed'))
);

CREATE TABLE IF NOT EXISTS account_labels (
    label_id            TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL,
    label               TEXT NOT NULL,
    reviewer            TEXT NOT NULL DEFAULT 'web_ui',
    notes               TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_account_labels_account ON account_labels(account_id);
CREATE INDEX IF NOT EXISTS idx_account_labels_label ON account_labels(label);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_run_id     TEXT PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    status              TEXT NOT NULL DEFAULT 'running',
    account_ids_json    TEXT NOT NULL DEFAULT '[]',
    stages_json         TEXT NOT NULL DEFAULT '[]',
    result_json         TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS dossiers (
    dossier_id          TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL,
    dossier_type        TEXT NOT NULL DEFAULT 'full'
        CHECK (dossier_type IN ('full', 'brief', 'summary', 'skipped')),
    version             INTEGER NOT NULL DEFAULT 1,
    sections_json       TEXT NOT NULL DEFAULT '[]',
    markdown            TEXT NOT NULL DEFAULT '',
    generated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX IF NOT EXISTS idx_dossiers_account ON dossiers(account_id);
"""
