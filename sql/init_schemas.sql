-- init_schemas.sql
-- Creates all bronze, silver, gold, and meta tables.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS throughout.
-- Never drops existing tables (lecturer requirement: no full wipe).

-- ═══════════════════════════════════════════════════════════════════════════
-- BRONZE LAYER — raw data, all columns TEXT, no transformation
-- ═══════════════════════════════════════════════════════════════════════════

-- bronze.land_registry_raw
-- One row per property transaction, loaded via COPY from pp-complete.csv
-- or appended from pp-monthly-update-new-version.csv (incremental)
-- No header in source CSV — columns are positional
CREATE TABLE IF NOT EXISTS bronze.land_registry_raw (
    transaction_id   TEXT,
    price            TEXT,
    transfer_date    TEXT,
    postcode         TEXT,
    property_type    TEXT,
    old_new          TEXT,
    duration         TEXT,
    paon             TEXT,
    saon             TEXT,
    street           TEXT,
    locality         TEXT,
    town_city        TEXT,
    district         TEXT,
    county           TEXT,
    ppd_category     TEXT,
    record_status    TEXT
);

-- bronze.boe_raw
-- One row per month, loaded via Kafka (row data) or COPY
-- Column names = BoE series codes (positional mapping from CSV)
-- Source CSV has a header row — COPY uses HEADER true to skip it
CREATE TABLE IF NOT EXISTS bronze.boe_raw (
    date_col     TEXT,
    "LPMB23A"    TEXT,
    "LPMB26A"    TEXT,
    "LPMB3C8"    TEXT,
    "LPMB3SI"    TEXT,
    "LPMB3TI"    TEXT,
    "LPMB3VA"    TEXT,
    "LPMB4B3"    TEXT,
    "LPMB4B4"    TEXT,
    "LPMVTVX"    TEXT,
    "LPMVYVA"    TEXT,
    "LPMZ3UP"    TEXT,
    "LPMZ3UR"    TEXT
);

-- bronze.mlar_raw
-- Long format: one row per (source sheet, category, quarter)
-- Produced by mlar_parser.py which transposes the wide XLSX data
-- Source CSV has a header row — COPY uses HEADER true to skip it
CREATE TABLE IF NOT EXISTS bronze.mlar_raw (
    src          TEXT,
    category     TEXT,
    quarter      TEXT,
    value        TEXT
);


-- ═══════════════════════════════════════════════════════════════════════════
-- SILVER LAYER — typed, cleaned, temporal columns derived
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
-- One row per (source_sheet, category, quarter).
-- Merge key: (src, category, quarter_start)
CREATE TABLE IF NOT EXISTS silver.mlar_long (
    src              TEXT,
    category         TEXT,
    quarter_start    DATE,
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
-- GOLD LAYER — aggregated, joined on quarter_start
-- Column naming: friendly_name__SOURCE_CODE__unit
-- Mixed-case columns must be double-quoted
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.housing_credit_summary (
    quarter_start__date                             DATE        PRIMARY KEY,
    quarter_label                                   TEXT,

    -- Land Registry aggregates
    "transactions_total__LR__count"                 BIGINT,
    "price_avg__LR__gbp"                            NUMERIC(18,2),
    "price_median__LR__gbp"                         NUMERIC(18,2),
    "price_min__LR__gbp"                            NUMERIC(18,2),
    "price_max__LR__gbp"                            NUMERIC(18,2),

    -- BoE aggregates (averaged across months in the quarter)
    "boe_house_purchase__LPMVTVX__count"            NUMERIC,
    "boe_remortgage__LPMB4B3__count"                NUMERIC,
    "boe_total_secured_lending__LPMB3C8__count"     NUMERIC,
    "boe_mfi_total_approvals__LPMZ3UP__count"       NUMERIC,

    -- MLAR figures (already x1,000,000 in silver)
    "mlar_gross_advances__MLAR_1_21_C_1__gbp"       NUMERIC,
    "mlar_net_advances__MLAR_1_21_C_2__gbp"         NUMERIC,
    "mlar_new_commitments__MLAR_1_21_C_3__gbp"      NUMERIC,
    "mlar_imp_repayment__MLAR_1_32_C_3__pct"        NUMERIC(6,3),
    "mlar_imp_interest_only__MLAR_1_32_C_4__pct"    NUMERIC(6,3),
    "mlar_new_house_purchase__MLAR_1_33_C_29__gbp"  NUMERIC,
    "mlar_new_remortgage__MLAR_1_33_C_30__gbp"      NUMERIC,

    -- Data quality flags
    "source_available_lr__flag"                      BOOLEAN,
    "source_available_boe__flag"                     BOOLEAN,
    "source_available_mlar__flag"                    BOOLEAN,

    loaded_at                                       TIMESTAMP NOT NULL DEFAULT NOW()
);


-- gold.column_dictionary
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

-- ═══════════════════════════════════════════════════════════════════════════
-- META — pipeline state tracking
-- ═══════════════════════════════════════════════════════════════════════════

-- meta.watermark
-- Tracks last processed value per source (date, month, or quarter granularity)
CREATE TABLE IF NOT EXISTS meta.watermark (
    source_name  TEXT        PRIMARY KEY,
    last_value   DATE        NOT NULL,
    updated_at   TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- meta.file_registry
-- Tracks file changes to avoid unnecessary re-COPY on scheduled runs
CREATE TABLE IF NOT EXISTS meta.file_registry (
    source_name    TEXT        PRIMARY KEY,
    filename       TEXT        NOT NULL,
    file_size      BIGINT      NOT NULL,
    last_modified  TIMESTAMP   NOT NULL,
    updated_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);