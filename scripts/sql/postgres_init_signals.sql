CREATE SCHEMA IF NOT EXISTS signals;

CREATE TABLE IF NOT EXISTS signals.accounts (
  account_id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL UNIQUE,
  source_type TEXT NOT NULL CHECK (source_type IN ('seed', 'discovered')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals.account_metadata (
  account_id TEXT PRIMARY KEY REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  country TEXT NOT NULL DEFAULT '',
  region_group TEXT NOT NULL DEFAULT '',
  industry_label TEXT NOT NULL DEFAULT '',
  website_url TEXT NOT NULL DEFAULT '',
  wikidata_id TEXT NOT NULL DEFAULT '',
  sitelinks INTEGER NOT NULL DEFAULT 0,
  revenue_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  employees INTEGER NOT NULL DEFAULT 0,
  ranking_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  data_source TEXT NOT NULL DEFAULT 'migration',
  last_refreshed_on DATE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals.account_source_handles (
  account_id TEXT PRIMARY KEY REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  domain TEXT NOT NULL UNIQUE,
  company_name TEXT NOT NULL DEFAULT '',
  greenhouse_board TEXT NOT NULL DEFAULT '',
  lever_company TEXT NOT NULL DEFAULT '',
  careers_url TEXT NOT NULL DEFAULT '',
  website_url TEXT NOT NULL DEFAULT '',
  news_query TEXT NOT NULL DEFAULT '',
  news_rss TEXT NOT NULL DEFAULT '',
  reddit_query TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_domain ON signals.accounts(domain);
CREATE INDEX IF NOT EXISTS idx_account_metadata_country ON signals.account_metadata(country);
CREATE INDEX IF NOT EXISTS idx_account_metadata_region ON signals.account_metadata(region_group);

CREATE TABLE IF NOT EXISTS signals.stage_watchlist (
  company_name TEXT,
  domain TEXT,
  source_type TEXT,
  country TEXT,
  region_group TEXT,
  industry_label TEXT,
  website_url TEXT,
  wikidata_id TEXT,
  sitelinks TEXT,
  revenue_usd TEXT,
  employees TEXT,
  ranking_score TEXT,
  data_source TEXT,
  last_refreshed_on TEXT
);

CREATE TABLE IF NOT EXISTS signals.stage_handles (
  domain TEXT,
  company_name TEXT,
  greenhouse_board TEXT,
  lever_company TEXT,
  careers_url TEXT,
  website_url TEXT,
  news_query TEXT,
  news_rss TEXT,
  reddit_query TEXT
);

CREATE TABLE IF NOT EXISTS signals.external_discovery_events (
  event_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE,
  observed_at TIMESTAMPTZ NOT NULL,
  title TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  entry_url TEXT NOT NULL DEFAULT '',
  url_type TEXT NOT NULL DEFAULT '',
  language_hint TEXT NOT NULL DEFAULT '',
  author_hint TEXT NOT NULL DEFAULT '',
  published_at_hint TEXT NOT NULL DEFAULT '',
  company_name_hint TEXT NOT NULL DEFAULT '',
  domain_hint TEXT NOT NULL DEFAULT '',
  raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processing_status TEXT NOT NULL DEFAULT 'pending',
  processed_run_id TEXT NOT NULL DEFAULT '',
  processed_at TIMESTAMPTZ,
  error_summary TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_external_discovery_events_status_observed
ON signals.external_discovery_events(processing_status, observed_at);

CREATE TABLE IF NOT EXISTS signals.crawl_frontier (
  frontier_id TEXT PRIMARY KEY,
  run_date DATE NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL DEFAULT '',
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  url_type TEXT NOT NULL,
  depth INTEGER NOT NULL DEFAULT 0,
  priority DOUBLE PRECISION NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'pending',
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 2,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_attempt_at TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (run_date, canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_status_priority
ON signals.crawl_frontier(status, priority DESC, first_seen_at ASC);

CREATE TABLE IF NOT EXISTS signals.documents (
  document_id TEXT PRIMARY KEY,
  frontier_id TEXT NOT NULL REFERENCES signals.crawl_frontier(frontier_id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  title TEXT NOT NULL DEFAULT '',
  author TEXT NOT NULL DEFAULT '',
  published_at TEXT NOT NULL DEFAULT '',
  section TEXT NOT NULL DEFAULT '',
  language TEXT NOT NULL DEFAULT '',
  body_text TEXT NOT NULL DEFAULT '',
  body_text_en TEXT NOT NULL DEFAULT '',
  raw_html TEXT NOT NULL DEFAULT '',
  parser_version TEXT NOT NULL DEFAULT '',
  evidence_quality DOUBLE PRECISION NOT NULL DEFAULT 0,
  relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  fetched_with TEXT NOT NULL DEFAULT '',
  outbound_links_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_canonical_url
ON signals.documents(canonical_url);

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_content_sha256
ON signals.documents(content_sha256);

CREATE TABLE IF NOT EXISTS signals.document_mentions (
  mention_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES signals.documents(document_id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  signal_code TEXT NOT NULL,
  matched_phrase TEXT NOT NULL,
  evidence_sentence TEXT NOT NULL DEFAULT '',
  evidence_sentence_en TEXT NOT NULL DEFAULT '',
  language TEXT NOT NULL DEFAULT '',
  speaker_name TEXT NOT NULL DEFAULT '',
  speaker_role TEXT NOT NULL DEFAULT '',
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
  evidence_quality DOUBLE PRECISION NOT NULL DEFAULT 0,
  relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_document_mentions_doc_signal_phrase
ON signals.document_mentions(document_id, signal_code, lower(matched_phrase));

CREATE TABLE IF NOT EXISTS signals.observation_lineage (
  obs_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  document_id TEXT NOT NULL REFERENCES signals.documents(document_id) ON DELETE CASCADE,
  mention_id TEXT NOT NULL REFERENCES signals.document_mentions(mention_id) ON DELETE CASCADE,
  source_event_id TEXT NOT NULL DEFAULT '',
  run_date DATE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_observation_lineage_obs_id
ON signals.observation_lineage(obs_id);

CREATE TABLE IF NOT EXISTS signals.people_watchlist (
  watch_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  role_weight DOUBLE PRECISION NOT NULL DEFAULT 0,
  source_url TEXT NOT NULL DEFAULT '',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (account_id, person_name, role_title)
);

CREATE TABLE IF NOT EXISTS signals.people_activity (
  activity_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES signals.accounts(account_id) ON DELETE CASCADE,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  document_id TEXT NOT NULL REFERENCES signals.documents(document_id) ON DELETE CASCADE,
  activity_type TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  published_at TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (account_id, person_name, document_id, activity_type)
);
