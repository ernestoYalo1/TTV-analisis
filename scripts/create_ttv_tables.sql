-- =============================================================================
-- TTV Analysis — Supabase Tables
-- Run once in Supabase SQL Editor (https://supabase.com/dashboard → SQL Editor)
-- =============================================================================

-- 1. Account ↔ Bot Mappings
-- Stores the manual link between Salesforce accounts and BigQuery bot_ids.
CREATE TABLE IF NOT EXISTS ttv_account_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_name TEXT NOT NULL,
    account_id TEXT,
    opportunity_name TEXT,
    opportunity_id TEXT UNIQUE NOT NULL,
    close_date DATE,
    amount NUMERIC DEFAULT 0,
    sf_url TEXT,
    bot_id TEXT,
    mapped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ttv_mappings_opportunity
    ON ttv_account_mappings (opportunity_id);

CREATE INDEX IF NOT EXISTS idx_ttv_mappings_bot
    ON ttv_account_mappings (bot_id);

-- 2. Milestone Cache
-- Precomputed TTV milestones so the dashboard loads without re-querying BigQuery.
CREATE TABLE IF NOT EXISTS ttv_milestones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    opportunity_id TEXT NOT NULL REFERENCES ttv_account_mappings(opportunity_id),
    bot_id TEXT NOT NULL,
    total_unique_contacts INTEGER DEFAULT 0,
    date_10 DATE,
    days_to_10 INTEGER,
    date_50 DATE,
    days_to_50 INTEGER,
    date_100 DATE,
    days_to_100 INTEGER,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(opportunity_id)
);

CREATE INDEX IF NOT EXISTS idx_ttv_milestones_opp
    ON ttv_milestones (opportunity_id);
