-- Migration 002: Add confidence band columns to account_scores
-- Tracks source diversity confidence per dimension for scoring validation.

SET search_path = signals, public;

ALTER TABLE account_scores
  ADD COLUMN IF NOT EXISTS confidence_band TEXT NOT NULL DEFAULT 'low'
    CHECK (confidence_band IN ('high', 'medium', 'low'));

ALTER TABLE account_scores
  ADD COLUMN IF NOT EXISTS dimension_confidence_json TEXT NOT NULL DEFAULT '{}';
