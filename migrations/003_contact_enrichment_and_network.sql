-- Migration 003: Add enrichment columns to contact_research + create internal_network table
-- Fixes bug where upsert_contacts() writes to nonexistent email_verified/verification_status columns.
-- Adds decision maker workflow fields and warm path intelligence via internal_network.

SET search_path = signals, public;

-- Fix: add columns that upsert_contacts() already references
ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS verification_status TEXT NOT NULL DEFAULT '';

-- New columns for decision maker workflow
ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS enrichment_source TEXT NOT NULL DEFAULT '';

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS contact_status TEXT NOT NULL DEFAULT 'discovered';

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS semantic_role TEXT NOT NULL DEFAULT '';

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS authority_score REAL NOT NULL DEFAULT 0.0;

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS warmth_score REAL NOT NULL DEFAULT 0.0;

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS warm_path_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS department TEXT NOT NULL DEFAULT '';

ALTER TABLE contact_research
  ADD COLUMN IF NOT EXISTS updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Internal network table for warm path intelligence
CREATE TABLE IF NOT EXISTS internal_network (
    network_id              TEXT PRIMARY KEY,
    team_member             TEXT NOT NULL,
    connection_name         TEXT NOT NULL,
    connection_linkedin_url TEXT,
    connection_title        TEXT,
    connection_company      TEXT,
    past_companies          TEXT NOT NULL DEFAULT '',
    relationship_type       TEXT NOT NULL DEFAULT 'connection',
    imported_at             TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_internal_network_linkedin
    ON internal_network(connection_linkedin_url);

CREATE INDEX IF NOT EXISTS idx_internal_network_name
    ON internal_network(LOWER(connection_name));
