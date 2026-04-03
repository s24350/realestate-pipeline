-- init_schemas.sql
-- Creates all silver and gold tables.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS throughout.
-- Never drops existing tables (lecturer requirement: no full wipe).

-- ═══════════════════════════════════════════════════════════════════════════
-- SILVER LAYER
-- ═══════════════════════════════════════════════════════════════════════════

-- silver.land_registry_clean
-- One row per property transaction.
-- Merge key: transaction_id (UUID supplied by Land Registry)
CREATE TABLE IF NOT EXISTS silver.land_registry_clean (
    transaction_id       TEXT        PRIMARY KEY,
    transfer_date        DATE,
    price_gbp            NUMERIC(14,2),
    postcode             TEXT,
    property_type        CHAR(1),          -- D, S, T, F, O
    old_new              CHAR(1),          -- Y = new build, N = established
    duration             CHAR(1),          -- F = freehold, L = leasehold
    paon                 TEXT,
    saon                 TEXT,
    street               TEXT,
    locality             TEXT,
    town_city            TEXT,
    district             TEXT,
    county               TEXT,
    ppd_category         CHAR(1),
    record_status        CHAR(1),
    quarter_start        DATE,             -- first day of transaction quarter
    quarter_begin        DATE,             -- alias kept for clarity (same as quarter_start)
    quarter_label        TEXT,             -- e.g. '2025Q4'
    loaded_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lr_quarter_start
    ON silver.land_registry_clean (quarter_start);


-- silver.boe_monthly_clean
-- One row per month.
-- Merge key: month_start
CREATE TABLE IF NOT EXISTS silver.boe_monthly_clean (
    month_start                     DATE        PRIMARY KEY,
    year                            SMALLINT,
    month                           SMALLINT,
    quarter_start                   DATE,
    quarter_begin                   DATE,
    quarter_label                   TEXT,
    mfi_house_purchase              NUMERIC,
    mfi_remortgage                  NUMERIC,
    mfi_other_lending               NUMERIC,
    mfi_total_approvals             NUMERIC,
    other_spec_house_purchase       NUMERIC,
    other_spec_remortgage           NUMERIC,
    other_spec_other_lending        NUMERIC,
    other_spec_total_approvals      NUMERIC,
    total_house_purchase            NUMERIC,
    total_remortgage                NUMERIC,
    total_other_lending             NUMERIC,
    total_secured_lending           NUMERIC,
    loaded_at                       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_boe_quarter_start
    ON silver.boe_monthly_clean (quarter_start);


-- silver.mlar_long
-- Unpivoted MLAR data: one row per (source_sheet, category, quarter).
-- Merge key: (src, category, quarter_start)
CREATE TABLE IF NOT EXISTS silver.mlar_long (
    src              TEXT,
    category         TEXT,
    quarter_start    DATE,
    quarter_begin    DATE,
    quarter_label    TEXT,
    quarter_num      SMALLINT,
    year             SMALLINT,
    value            NUMERIC,
    loaded_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (src, category, quarter_start)
);

CREATE INDEX IF NOT EXISTS idx_mlar_quarter_start
    ON silver.mlar_long (quarter_start);


-- ═══════════════════════════════════════════════════════════════════════════
-- GOLD LAYER
-- ═══════════════════════════════════════════════════════════════════════════

-- gold.housing_credit_summary
-- One row per quarter — the main analytical fact table.
-- Merge key: quarter_start
CREATE TABLE IF NOT EXISTS gold.housing_credit_summary (
    quarter_start                           DATE        PRIMARY KEY,
    quarter_label                           TEXT,

    -- Land Registry aggregates
    transactions_total                      BIGINT,
    price_avg_gbp                           NUMERIC(18,2),
    price_median_gbp                        NUMERIC(18,2),
    price_min_gbp                           NUMERIC(18,2),
    price_max_gbp                           NUMERIC(18,2),

    -- BoE aggregates (averaged across months in the quarter)
    boe_total_house_purchase_avg            NUMERIC,
    boe_total_remortgage_avg                NUMERIC,
    boe_total_secured_lending_avg           NUMERIC,
    boe_mfi_total_approvals_avg             NUMERIC,

    -- MLAR figures (quarterly, in GBP — already ×1,000,000 from source)
    mlar_gross_advances_gbp                 NUMERIC,
    mlar_net_advances_gbp                   NUMERIC,
    mlar_new_commitments_gbp                NUMERIC,
    mlar_imp_repayment_pct                  NUMERIC(6,3),
    mlar_imp_interest_only_pct              NUMERIC(6,3),
    mlar_new_house_purchase_gbp             NUMERIC,
    mlar_new_remortgage_gbp                 NUMERIC,

    -- Data quality flags
    source_available_lr                     BOOLEAN,
    source_available_boe                    BOOLEAN,
    source_available_mlar                   BOOLEAN,

    loaded_at                               TIMESTAMP NOT NULL DEFAULT NOW()
);


-- gold.column_dictionary
-- Metadata table describing every column in the gold layer.
CREATE TABLE IF NOT EXISTS gold.column_dictionary (
    column_name     TEXT    PRIMARY KEY,
    table_name      TEXT    NOT NULL,
    source          TEXT,
    original_label  TEXT,
    unit            TEXT,
    transformation  TEXT,
    description     TEXT,
    example_value   TEXT,
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
