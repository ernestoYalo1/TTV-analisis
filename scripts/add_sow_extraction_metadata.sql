-- =============================================================================
-- Migration: add SOW extraction metadata columns
-- Run once in Supabase SQL Editor
-- =============================================================================
-- Adds three columns to track how each expected_go_live_sow value was derived:
--   sow_extraction_confidence — "high" | "medium" | "low" | "none"
--   sow_extraction_source     — short quote or description from Claude
--   sow_extracted_at          — timestamp when the extraction ran

ALTER TABLE ttv_account_mappings
    ADD COLUMN IF NOT EXISTS sow_extraction_confidence TEXT,
    ADD COLUMN IF NOT EXISTS sow_extraction_source     TEXT,
    ADD COLUMN IF NOT EXISTS sow_extracted_at          TIMESTAMPTZ;
