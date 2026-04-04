# Implementation Documentation

Detailed record of the pipeline implementation process, organized by phase.
Each phase includes what was done, key decisions made, and how correctness was verified.

## Table of Contents

- [Phase 1 — Repository and Environment Setup](#phase-1--repository-and-environment-setup)
- [Phase 2 — Docker Stack](#phase-2--docker-stack)
- [Phase 3 — Preprocessing (MLAR Parser)](#phase-3--preprocessing-mlar-parser)
- [Phase 4 — Bronze Layer (Validation)](#phase-4--bronze-layer-validation)
- [Phase 5 — Silver Layer](#phase-5--silver-layer)
- [Phase 6 — Gold Layer](#phase-6--gold-layer)
- [Phase 7 — Airflow DAG and Processing Modes](#phase-7--airflow-dag-and-processing-modes)
- [Phase 8 — Kafka Ingestion](#phase-8--kafka-ingestion)
- [Phase 9 — Final Verification](#phase-9--final-verification)
- [Appendix A — Things to Watch Out For](#appendix-a--things-to-watch-out-for)
- [Appendix B — Glossary](#appendix-b--glossary)

---

## Phase 1 — Repository and Environment Setup

**What was done:** created the GitHub repository with the full project folder structure, `.gitignore` configured to exclude data files (CSVs, XLSX) and Docker volumes while preserving empty directory structure via `.gitkeep` files. Downloaded all three source datasets and placed them in their respective `data/` subdirectories.

**Key decisions:**
- No `inbox/` or `bronze/` directory for file copying — raw files sit directly in `data/land_registry/`, `data/boe/`, `data/mlar/`. This simplifies the architecture compared to the initial design which planned an inbox-to-bronze copy step.
- `.gitignore` uses `!data/*/` and `!data/**/.gitkeep` to un-ignore directory structure without accidentally committing the 5 GB Land Registry file.

**Verification:**
```bash
git log --oneline          # confirms commits
ls -R data/                # confirms files in correct directories
```
Repository visible at: https://github.com/s24350/realestate-pipeline

---

## Phase 2 — Docker Stack

**What was done:** built and started the Docker stack consisting of PostgreSQL 16, Zookeeper, Kafka (Confluent 7.6), Airflow webserver, Airflow scheduler, and an Airflow init container. The Airflow image extends the official `apache/airflow:2.9.1-python3.11` with OpenJDK 17 (required by PySpark) and all Python dependencies.

**Key decisions:**
- Build context set to project root (`..`) so the Dockerfile can access `requirements.txt` from the top level.
- All project directories (`silver/`, `gold/`, `utils/`, etc.) mounted as volumes into the Airflow containers at `/opt/airflow/`, enabling live code editing without rebuilding.
- PostgreSQL hosts both the Airflow metadata database and the pipeline data database (`pipeline_db`) with separate users.

**Verification:**
```bash
docker ps                                          # 5+ containers running
docker exec -i <postgres-container> psql -U postgres -c "\l"   # airflow, pipeline_db, postgres databases
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "\dn"  # silver, gold, meta schemas
```

All containers started successfully. Airflow UI accessible at http://localhost:8080 (admin/admin). Kafka responded to topic listing without errors.

---

## Phase 3 — Preprocessing (MLAR Parser)

**What was done:** adapted the `mlar_parser.py` script from the v1 project. The script reads `mlar-longrun-detailed.XLSX` and converts three worksheets (1.21, 1.32, 1.33) into flat CSV files using pandas, with category labels resolved via mapping CSV files in `preprocessing/mappings/`.

**Key decisions:**
- Kept the mapping files from v1 rather than attempting to auto-detect category labels from the XLSX structure. The XLSX has nested sub-sections (e.g. "Business flows" → "Balances outstanding" within section A) that are too fragile to parse automatically.
- Added a `parse_all()` function callable by the Airflow DAG.
- Paths read from `utils.config` when running inside Docker, with fallback to `data/mlar/` for local execution.

**Issue encountered:** the year row in the XLSX is read as floats by pandas (e.g. `2007.0`), producing column headers like `2007.0Q1` instead of `2007Q1`. Fixed by casting years to int with NaN handling.

**Verification:**
```bash
ls -lh data/mlar/mlar_1_*.csv        # three non-zero CSV files
head -3 data/mlar/mlar_1_21.csv      # headers: category, 2007Q1, 2007Q2, ...
```

Output: 24 rows for sheet 1.21, correct category labels matching the mapping files, quarter columns from 2007Q1 to 2025Q3.

---

## Phase 4 — Bronze Layer (Validation)

**What was done:** the bronze layer in this project is simply raw files on disk. `bronze/ingest_bronze.py` validates that all expected source files exist in their directories and have non-zero size. No file copying occurs.

**Verification:**
```bash
docker exec -it <scheduler-container> bash -c "cd /opt/airflow && python -m bronze.ingest_bronze"
```

Output confirmed all 5 files present: Land Registry (5168.8 MB), BoE (0.04 MB), and three MLAR CSVs.

---

## Phase 5 — Silver Layer

The silver layer is where the bulk of data transformation happens. Each source has its own PySpark script that reads the raw CSV, applies transformations, and writes to PostgreSQL via a staging table + upsert pattern.

### 5.1 — Schema Initialization

Ran `init_schemas.sql` to create all silver and gold tables with primary keys and indexes. The script uses `CREATE TABLE IF NOT EXISTS` throughout — safe to re-run.

```bash
cat sql/init_schemas.sql | docker exec -i <postgres-container> psql -U pipeline -d pipeline_db
```

### 5.2 — Bank of England (Silver)

**Transformations applied:**
- Column renaming: BoE series codes (e.g. `LPMB3VA`) extracted from long CSV headers and mapped to friendly names (e.g. `mfi_house_purchase`)
- Numeric casting via `try_cast` — values like `n/a` become NULL automatically
- Date parsing with manual century correction (2-digit year issue — see Appendix A)
- Temporal columns derived: `quarter_start` via `date_trunc('quarter', month_start)`, `quarter_label` as `YYYYQn`

**Verification:**
```bash
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT COUNT(*) FROM silver.boe_monthly_clean;"
# Result: 394 rows (later 395 after incremental test)
```

Spot-checked: dates correct (2026-01-31 not 2099), numeric columns properly typed, `n/a` values converted to NULL (54 NULLs in `total_secured_lending` — the early 1990s rows).

### 5.3 — MLAR (Silver)

**Transformations applied:**
- Wide-to-long unpivot using PySpark `stack()`: one column per quarter → one row per (source, category, quarter)
- Numeric casting via `try_cast` — dashes (`-`) in the source become NULL
- Monetary values (sheets 1.21 and 1.33) multiplied by 1,000,000 to convert from £m to full GBP
- Quarter column detection uses plain Python string checks instead of regex

**Verification:**
```bash
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT src, COUNT(*) FROM silver.mlar_long GROUP BY src;"
# Result: 1.21 → 1800, 1.32 → 4500, 1.33 → 6000 (categories × quarters)
```

Spot-checked: source value `73139.02` (£m) → silver value `73,139,020,000` (full GBP). Correct.

### 5.4 — Land Registry (Silver)

**Transformations applied:**
- Transaction ID cleaned: curly braces stripped from `{GUID}`
- Price cast via `try_cast` to `DECIMAL(14,2)`
- Date parsed from `yyyy-MM-dd HH:mm` format (4-digit years, no century issue)
- Text columns: empty strings nullified via `nullif`
- Property type, old/new, duration normalized to uppercase single characters
- Temporal columns derived: `quarter_start`, `quarter_label`

**This is the largest task: 31,004,536 rows from a 5.3 GB CSV.** Initial attempts failed with Spark heartbeat timeouts. Resolved by increasing Spark timeouts, driver memory, and adding JDBC batch size (see Appendix A).

**Verification:**
```bash
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT COUNT(*) FROM silver.land_registry_clean;"
# Result: 31,004,536 rows

docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT MIN(quarter_start), MAX(quarter_start), COUNT(DISTINCT quarter_start) FROM silver.land_registry_clean;"
# Result: 1995-01-01 to 2026-01-01, 125 quarters

docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT transaction_id, COUNT(*) FROM silver.land_registry_clean GROUP BY transaction_id HAVING COUNT(*) > 1 LIMIT 5;"
# Result: 0 rows (no duplicates)
```

### 5.5 — Watermark Verification

After all silver tasks completed, the watermark table was checked:

```bash
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT * FROM meta.watermark ORDER BY source_name;"
```

Result: three rows — `boe → 2026-01-01`, `land_registry → 2026-01-01`, `mlar → 2025-07-01`. Each matches `MAX(quarter_start)` from the corresponding silver table.

---

## Phase 6 — Gold Layer

**What was done:** `gold_aggregations.py` reads from all three silver tables, aggregates to quarter granularity, and joins on `quarter_start` using a full outer join.

**Key design decisions:**
- Land Registry aggregation is pushed down to PostgreSQL as a JDBC subquery (Spark reads ~125 pre-aggregated rows instead of 31M). This was necessary because reading the full table into Spark caused JVM out-of-memory crashes.
- BoE monthly data is averaged up to quarter level.
- MLAR categories are pivoted from long format to wide columns with the naming convention `friendly_name__SOURCE_CODE__unit`.
- Gold column names with mixed case (e.g. `__LR__`) require double-quoting in PostgreSQL DDL and SQL statements.

**Verification:**
```bash
docker exec -i <postgres-container> psql -U pipeline -d pipeline_db -c "SELECT COUNT(*) FROM gold.housing_credit_summary;"
# Result: 132 rows (one per quarter)
```

Cross-checked quarter 2024Q3: gold `transactions_total = 248,225` matches silver `COUNT(*) WHERE quarter_label = '2024Q3' = 248,225`. MLAR values in billions range (e.g. 60.1 billion) — confirming ×1M conversion is applied correctly.

---

## Phase 7 — Airflow DAG and Processing Modes

### 7.1 — DAG Verification

The `realestate_pipeline` DAG loaded in the Airflow UI without import errors. The graph view confirmed the correct task dependency structure: sequential flow through publish → validate → init_schemas → preprocess, then three parallel silver tasks, then gold.

### 7.2 — Full DAG Run

Triggered with `{"mode": "full"}` from the Airflow UI. All 8 tasks completed successfully. The Land Registry silver task took approximately 49 minutes (17:53 → 18:42 UTC). Row counts after the run matched the manual execution results exactly — confirming idempotency.

### 7.3 — Incremental Processing Test

Prepared new data: BoE CSV with one additional row (Feb 2026), MLAR XLSX with Q4 2025 data, and the Land Registry monthly update file already in place.

Triggered with `{"mode": "incremental"}`. Results:

| Source | Before | After | Change |
|--------|--------|-------|--------|
| BoE silver rows | 394 | 395 | +1 (Feb 2026 row) |
| MLAR silver rows | 12,300 | 12,464 | +164 (new Q4 2025 quarter) |
| LR silver rows | 31,004,536 | 31,004,536 | unchanged |
| Gold rows | 132 | 132 | unchanged (Q1 2026 updated, no new quarter) |

The Land Registry count was unchanged because the monthly update file's newest data (Feb 2026) falls in Q1 2026, which equals the watermark — the strict `>` filter correctly skips it. This is the design limitation described in the README. The watermarks updated appropriately: MLAR moved from `2025-07-01` to `2025-10-01`.

---

## Phase 8 — Kafka Ingestion

**What was done:** ran the Kafka producer manually to publish file-available events for all source files.

```bash
docker exec -it <scheduler-container> bash -c "cd /opt/airflow && python -m ingestion.kafka_producer"
```

Published 7 events (pp-complete, monthly update, BoE CSV, MLAR XLSX, and 3 MLAR CSVs). Consumed one event from Kafka to verify:

```json
{
  "event_type": "file_available",
  "filename": "pp-complete.csv",
  "filepath": "/opt/airflow/data/land_registry/pp-complete.csv",
  "size_bytes": 5419881629,
  "published_at": "2026-04-04T17:53:17.426958"
}
```

All fields present with correct values. In a production setup, an Airflow KafkaSensor would detect these events and trigger the DAG automatically. In this project, the DAG is triggered manually from the Airflow UI.

---

## Phase 9 — Final Verification

### 9.1 — End-to-End Lineage

Traced a single transaction from raw CSV through silver to gold:

1. Raw CSV row 1: `{2A289E9F-6BB5-CDC8-E050-A8C063054829}`, price 36995, date 1995-03-24
2. Silver: braces stripped, price typed as 36995.00, `quarter_start = 1995-01-01`, `quarter_label = 1995Q1`
3. Gold: `transactions_total__LR__count = 172,668` for 1995Q1

Cross-check: `COUNT(*) FROM silver.land_registry_clean WHERE quarter_label = '1995Q1'` returns 172,668 — exact match. Data lineage verified end-to-end.

---

## Appendix A — Things to Watch Out For

### A.1 — BoE 2-digit year parsing

The Bank of England CSV uses dates like `31 Jan 26` (2-digit year). PySpark's `to_date` with format `dd MMM yy` interprets `26` as 2099 instead of 2026. The solution manually prepends the century: `19xx` for years 31–99, `20xx` for years 00–30, before parsing with `dd MMM yyyy`.

### A.2 — PySpark version and `try_cast`

`F.try_cast()` as a Python function was added in PySpark 4.0. This project uses PySpark 3.5.1, where `try_cast` is only available as a SQL expression. The workaround is `F.expr("try_cast(column_name as type)")` instead of `F.try_cast(F.col("column_name"), "type")`.

### A.3 — PostgreSQL case sensitivity with Spark-generated columns

Spark writes DataFrame column names preserving their original case (e.g. `transactions_total__LR__count`). PostgreSQL lowercases unquoted identifiers, so the DDL `CREATE TABLE` must use double quotes around mixed-case column names. The same applies to upsert SQL — all column references must be quoted. Without this, `INSERT ... ON CONFLICT` fails with "column does not exist."

### A.4 — JDBC write performance for large datasets

Writing 31 million rows via Spark's JDBC writer with default settings caused Spark heartbeat timeouts after ~10 minutes (`HeartbeatReceiver: Removing executor driver with no recent heartbeats: 652644 ms exceeds timeout 120000 ms`). The fix required four Spark configuration changes:

- `spark.driver.memory`: 2g → 4g
- `spark.network.timeout`: default → 800s
- `spark.executor.heartbeatInterval`: default → 120s
- JDBC `batchsize` option: default (1000) → 10000

### A.5 — Gold layer memory with large tables

Reading the full 31M-row Land Registry silver table into Spark for aggregation caused JVM out-of-memory crashes. The solution was to push the aggregation down to PostgreSQL by using a JDBC subquery: instead of `spark.read.jdbc(table="silver.land_registry_clean")`, the gold script reads `spark.read.jdbc(table="(SELECT ... GROUP BY quarter_start) AS lr_agg")`. PostgreSQL performs the aggregation, and Spark receives only ~125 rows.

### A.6 — Land Registry incremental design limitation

The watermark filter uses strict greater-than (`quarter_start > watermark`). When the monthly update file contains new transactions from the same quarter as the watermark (e.g. new Q1 2026 GUIDs when watermark is `2026-01-01`), these are not processed in incremental mode. Running full mode picks them up via the upsert pattern. This is by design — the watermark is conservative to prevent partial quarter reprocessing.

### A.7 — BoE column code mapping

The BoE CSV uses long descriptive headers ending with series codes like `LPMB3VA`. The silver transform maps these to friendly names (`mfi_house_purchase`). The codes carry business meaning on the Bank of England website and are preserved in gold column names for traceability (e.g. `boe_total_secured_lending__LPMB3C8__count`).

### A.8 — MLAR source codes in gold

Gold column names include MLAR source references like `MLAR_1_21_C_1`, which means sheet 1.21, section C, row 1 in the original Excel workbook. These codes allow anyone to trace a gold value back to the specific cell in the MLAR publication.

---

## Appendix B — Glossary

| Term | Meaning |
|------|---------|
| `<postgres-container>` | The PostgreSQL Docker container, e.g. `docker-postgres-1` |
| `<scheduler-container>` | The Airflow scheduler container, e.g. `docker-airflow-scheduler-1` |
| Bronze | Raw source files on disk, validated but not transformed |
| Silver | Cleaned, typed, normalized data in PostgreSQL (one table per source) |
| Gold | Aggregated analytical table joining all sources on `quarter_start` |
| Watermark | `meta.watermark` table tracking the last processed quarter per source |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` — idempotent write pattern |
| MLAR | Mortgage Lenders and Administrators Return (FCA/BoE quarterly publication) |
| BoE | Bank of England |
| LR | Land Registry (HM Land Registry Price Paid Data) |
| JDBC | Java Database Connectivity — used by PySpark to write to PostgreSQL |
| DAG | Directed Acyclic Graph — Airflow's workflow definition |
| `try_cast` | Spark SQL function that returns NULL instead of error for invalid casts |
