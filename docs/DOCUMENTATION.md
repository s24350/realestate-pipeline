# Implementation Documentation

Detailed record of the pipeline implementation process, organized by phase.
Each phase includes what was done, key decisions made, and how correctness was verified.

**Note:** Phases 1–9 cover the initial orchestration pipeline (tagged as `v1.0-orchestration`). Phases 10–13 document the Kafka integration, bronze-in-PostgreSQL migration, and scheduled trigger additions.

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
- [Phase 10 — Architecture Upgrade: Bronze in PostgreSQL](#phase-10--architecture-upgrade-bronze-in-postgresql)
- [Phase 11 — Kafka as Single Entry Point](#phase-11--kafka-as-single-entry-point)
- [Phase 12 — Watermark Refinement and Silver Reads from Bronze](#phase-12--watermark-refinement-and-silver-reads-from-bronze)
- [Phase 13 — End-to-End Testing](#phase-13--end-to-end-testing)
- [Appendix A — Things to Watch Out For](#appendix-a--things-to-watch-out-for)
- [Appendix B — Glossary](#appendix-b--glossary)

---

## Phase 1 — Repository and Environment Setup

**What was done:** created the GitHub repository with the full project folder structure, `.gitignore` configured to exclude data files (CSVs, XLSX) and Docker volumes while preserving empty directory structure via `.gitkeep` files. Downloaded all three source datasets and placed them in their respective `data/` subdirectories.

**Verification:**
```bash
git log --oneline          # confirms commits
ls -R data/                # confirms files in correct directories
```
Repository visible at: https://github.com/s24350/realestate-pipeline

---

## Phase 2 — Docker Stack

**What was done:** built and started the Docker stack consisting of PostgreSQL 16, Zookeeper, Kafka (Confluent 7.6), Airflow webserver, Airflow scheduler, and an Airflow init container. The Airflow image extends the official `apache/airflow:2.9.1-python3.11` with OpenJDK 17 (required by PySpark) and all Python dependencies.

**Verification:**
```bash
docker ps                  # 5+ containers running
docker exec -i <postgres-container> psql -U postgres -c "\l"  # databases listed
```

All containers started successfully. Airflow UI accessible at http://localhost:8080 (admin/admin). Kafka responded to topic listing without errors.

---

## Phase 3 — Preprocessing (MLAR Parser)

**What was done:** adapted the `mlar_parser.py` script from the v1 project. The script reads `mlar-longrun-detailed.XLSX` and converts three worksheets (1.21, 1.32, 1.33) into flat CSV files using pandas, with category labels resolved via mapping CSV files in `preprocessing/mappings/`.

**Issue encountered:** the year row in the XLSX is read as floats by pandas (e.g. `2007.0`). Fixed by casting years to int with NaN handling.

**Verification:**
```bash
head -3 data/mlar/mlar_long_raw.csv      # headers: src,category,quarter,value
wc -l data/mlar/mlar_long_raw.csv        # ~12,300 rows
```

---

## Phase 4 — Bronze Layer (Validation)

**What was done:** in the initial phase, the bronze layer was simply raw files on disk. `bronze/ingest_bronze.py` validated that all expected source files exist and have non-zero size. This was later replaced by `bronze/load_bronze.py` in Phase 10.

---

## Phase 5 — Silver Layer

The silver layer is where the bulk of data transformation happens. Each source has its own PySpark script that reads the source data, applies transformations, and writes to PostgreSQL via a staging table + upsert pattern.

### 5.1 — Bank of England (Silver)

**Transformations:** column renaming (BoE series codes to friendly names), numeric casting via `try_cast`, date parsing with manual century correction, temporal columns derived.

**Verification:** 394 rows in silver, dates correct (2026-01-31 not 2099), `n/a` values converted to NULL.

### 5.2 — MLAR (Silver)

**Transformations:** numeric casting via `try_cast`, monetary values (sheets 1.21, 1.33) multiplied by 1,000,000, temporal columns derived from quarter labels.

**Verification:** ~12,300 rows in silver, correct category distribution across sources.

### 5.3 — Land Registry (Silver)

**Transformations:** transaction ID cleaned (curly braces stripped), price cast, date parsed, text columns nullified, property type normalized, temporal columns derived.

**Verification:** 31,004,536 rows in silver, no duplicates, 125 quarters spanning 1995-2026.

---

## Phase 6 — Gold Layer

**What was done:** `gold_aggregations.py` reads from all three silver tables, aggregates to quarter granularity, and joins on `quarter_start` using a full outer join. Land Registry aggregation is pushed down to PostgreSQL as a JDBC subquery (Spark receives ~132 pre-aggregated rows instead of 31M).

**Verification:** 132 quarters in gold. Cross-checked: gold `transactions_total = 172,668` for 1995Q1 matches silver `COUNT(*)` exactly.

---

## Phase 7 — Airflow DAG and Processing Modes

**What was done:** the `realestate_pipeline` DAG loaded in Airflow UI. Triggered with `{"mode": "full"}`, all tasks completed successfully. Land Registry silver took approximately 49 minutes.

---

## Phase 8 — Kafka Ingestion

**What was done:** in the initial phase, Kafka was used as a file-event notification layer — the producer published JSON events to a single `file-events` topic. This was later upgraded to source-specific topics in Phase 11.

---

## Phase 9 — Final Verification

**What was done:** traced a single transaction from raw CSV through silver to gold, verifying data lineage end-to-end. Transaction `2A289E9F-6BB5-CDC8-E050-A8C063054829` confirmed: raw price 36995 → silver price_gbp 36995.00, quarter 1995Q1 → gold transactions_total 172,668 matching silver COUNT(*).

---

## Phase 10 — Architecture Upgrade: Bronze in PostgreSQL

**What was done:** the entire pipeline architecture was revised. The bronze layer moved from CSV files on disk to PostgreSQL tables. All existing schemas were dropped and recreated to start clean.

### 10.1 — Schema Reset

Dropped and recreated all schemas (bronze, silver, gold, meta):
```
bronze, silver, gold, meta — empty, no tables
```

### 10.2 — New DDL with Bronze Tables

Created 12 database objects: 3 bronze tables (all TEXT columns), 3 silver tables with indexes, 2 gold tables, 2 meta tables.

Bronze tables follow the "as-is" principle — raw data with no transformation:

| Table | Columns | Notes |
|-------|---------|-------|
| `bronze.land_registry_raw` | 16 TEXT columns (positional) | No header in CSV |
| `bronze.boe_raw` | 13 TEXT columns (BoE series codes) | Header skipped during COPY |
| `bronze.mlar_raw` | 4 TEXT columns (src, category, quarter, value) | Long format |

New meta tables added:
- `meta.watermark` with `last_value` column (renamed from `last_quarter`) for per-source granularity
- `meta.file_registry` for tracking file changes (source_name, filename, file_size, last_modified)

### 10.3 — MLAR Parser Upgrade

The parser was updated to output a single long-format CSV (`mlar_long_raw.csv`) instead of three wide CSVs. This means the bronze table has a fixed 4-column schema — new quarters add rows, not columns. No more need to DROP and recreate tables when new quarters arrive.

### 10.4 — Bronze Loading Script

Created `bronze/load_bronze.py` with source-specific loading strategies:

| Source | Full mode | Incremental mode |
|--------|-----------|-----------------|
| Land Registry | TRUNCATE + COPY pp-complete.csv (~8 min) | APPEND from monthly file via staging + INSERT WHERE NOT EXISTS |
| BoE | Staging + INSERT WHERE NOT EXISTS on date_col | Same |
| MLAR | Parser runs if XLSX changed, then staging + INSERT WHERE NOT EXISTS on (src, category, quarter) | Same |

The staging + INSERT WHERE NOT EXISTS pattern was chosen over TRUNCATE to comply with following requirement: never drop objects, loads should be idempotent and incremental.

### 10.5 — File Registry

Created `utils/file_registry.py` — checks file size and modification time against `meta.file_registry` before loading. If a file hasn't changed, COPY is skipped entirely. This prevents unnecessary 8-minute COPY operations on every scheduled run.

**Verification:**
```
bronze.land_registry_raw: 31,004,536 rows
bronze.boe_raw: 394 rows
bronze.mlar_raw: 12,300 rows
meta.file_registry: 4 entries with correct sizes and timestamps
```

---

## Phase 11 — Kafka as Single Entry Point

**What was done:** Kafka was upgraded from a simple audit log to the genuine single entry point for all data. Three source-specific topics were created, and the producer/consumer architecture was redesigned.

### 11.1 — Topic Creation

Three topics created with 1 partition and replication factor 1:
```
boe-data
land-registry-data
mlar-data
```

### 11.2 — Kafka Producer Redesign

The producer (`ingestion/kafka_producer.py`) was rewritten to:
- Scan `data/` directories for each source
- Check `meta.file_registry` for changes before publishing
- Publish to source-specific topics with different message formats:
  - `land-registry-data`: file path message (`{"source": "land_registry", "filepath": "...", "mode": "full/incremental"}`)
  - `boe-data`: actual row data as JSON (`{"source": "boe", "rows": [[...], ...], "row_count": 395}`)
  - `mlar-data`: file path message (after running parser if XLSX changed)

The BoE topic demonstrates real data-through-queue flow — 395 rows (~42KB) pass through Kafka as JSON payload. Land Registry and MLAR files are too large for row-level streaming.

### 11.3 — Kafka Consumer

Created `ingestion/kafka_consumer.py` with source-specific handlers:
- `land-registry-data` → reads file path, COPYs to bronze (TRUNCATE + COPY in full mode, staging + INSERT WHERE NOT EXISTS in incremental)
- `boe-data` → reads row data from Kafka message, inserts via staging + INSERT WHERE NOT EXISTS
- `mlar-data` → reads file path, COPYs to bronze via staging + INSERT WHERE NOT EXISTS

Key configuration: `max_poll_interval_ms=600000` (allows 10 minutes for Land Registry COPY), `consumer_timeout_ms=15000` (exits 15 seconds after last message), `auto_offset_reset="earliest"`.

### 11.4 — Verification

Producer published to all 3 topics. Consumer loaded all data through Kafka into bronze:
```
bronze.land_registry_raw: 31,004,536 rows
bronze.boe_raw: 394 rows
bronze.mlar_raw: 12,300 rows
```

Kafka topic details confirmed:
```
land-registry-data: 1 partition, offset > 0
boe-data: 1 partition, offset > 0 (contains actual row data)
mlar-data: 1 partition, offset > 0
```

---

## Phase 12 — Watermark Refinement and Silver Reads from Bronze

### 12.1 — Watermark Granularity

Updated `utils/watermark.py` to use `last_value` (renamed from `last_quarter`). Each source now tracks its watermark at the appropriate granularity:

| Source | Watermark value | Filter column |
|--------|----------------|---------------|
| Land Registry | `MAX(transfer_date)` | `transfer_date > watermark` |
| BoE | `MAX(month_start)` | `month_start > watermark` |
| MLAR | `MAX(quarter_start)` | `quarter_start > watermark` |

### 12.2 — Silver Scripts Read from Bronze PostgreSQL

All three silver scripts were rewritten to read from bronze PostgreSQL tables via JDBC instead of CSV files on disk.

**BoE and MLAR:** straightforward JDBC reads — 394 and 12,300 rows respectively, no memory issues.

**Land Registry — Partitioned JDBC Reads:**

Reading 31M rows from PostgreSQL via default single-partition JDBC caused out-of-memory crashes, even with `spark.driver.memory=8g`. The solution was partitioned JDBC reads:

1. Query bronze for MIN/MAX `transfer_date`, cast to epoch days (integer)
2. Read with `numPartitions=16`, `column="partition_days"`, `lowerBound=min_epoch`, `upperBound=max_epoch`
3. Each partition reads ~2M rows — well within memory limits

PostgreSQL casts the TEXT date to a real DATE and computes epoch days in a subquery. PySpark then does all transformations (type casting, NULL handling, quarter derivation) — keeping the silver logic in PySpark, not SQL.

**Verification after full mode:**
```
silver.land_registry_clean: 31,004,536 rows
silver.boe_monthly_clean: 394 rows
silver.mlar_long: 12,300 rows
Watermarks: boe → 2026-01-31, land_registry → 2026-01-30, mlar → 2025-07-01
```

---

## Phase 13 — End-to-End Testing

### 13.1 — Incremental Mode (Manual, Step by Step)

Replaced data files with newer versions: BoE with Feb 2026 row, MLAR XLSX with Q4 2025, Land Registry monthly update with data up to Feb 2026.

**Producer (incremental):**
- Detected Land Registry monthly file as new (no registry entry)
- Detected BoE file change (size 42996 → 43101)
- Detected MLAR XLSX change (size 1222641 → 1236283), ran parser, produced 12,464 rows
- Published to all 3 topics

**Consumer:**
- Land Registry: inserted 89,083 new rows (incremental append from monthly file)
- BoE: inserted 1 new row (Feb 2026)
- MLAR: inserted 164 new rows (Q4 2025 quarter)

**Silver (incremental):**
- BoE: watermark 2026-01-31 → processed 1 row → watermark updated to 2026-02-28
- MLAR: watermark 2025-07-01 → processed 164 rows → watermark updated to 2025-10-01
- Land Registry: watermark 2026-01-30 → processed 16,838 rows → watermark updated to 2026-02-27

**Gold (incremental):** wrote 1 updated quarter to gold.

**Results after incremental:**

| Layer | LR | BoE | MLAR | Gold |
|-------|-----|-----|------|------|
| Bronze | 31,093,619 | 395 | 12,464 | — |
| Silver | 31,021,374 | 395 | 12,464 | 132 |

Watermarks updated correctly per source granularity.

Note: The Bronze table contains all raw ingested rows (full historical + monthly updates). The Silver table only processes rows with `transfer_date` **greater than the watermark** (`2026-01-30`). Rows with older dates are ignored, resulting in a lower row count in Silver.

### 13.2 — Full Mode via Airflow

Truncated BoE and MLAR bronze/silver tables to test full mode recovery. Triggered DAG with `{"mode": "full"}`. All tasks completed successfully.

**Results after full mode:**
```
b_lr: 31,093,619 | b_boe: 394 | b_mlar: 12,300
s_lr: 31,093,619 | s_boe: 394 | s_mlar: 12,300 | gold: 132
```

Note: BoE shows 394 (not 395) because full mode used the original data files (without the Feb 2026 row).

### 13.3 — Scheduled Incremental Mode

Replaced data files with newer versions (including Feb 2026 BoE row and Q4 2025 MLAR). Set DAG `schedule_interval` and unpaused. The scheduled run triggered automatically and processed the new data:

```
Airflow run: scheduled__2026-04-16T21:50:00+00:00 → success
Duration: ~2.5 minutes (incremental, no Land Registry change)
```

**Results after scheduled run:**
```
b_lr: 31,093,619 | b_boe: 395 | b_mlar: 12,464
s_lr: 31,093,619 | s_boe: 395 | s_mlar: 12,464 | gold: 132
Watermarks: boe → 2026-02-28, land_registry → 2026-02-27, mlar → 2025-10-01
```

The pipeline detected file changes, processed only new data, and completed in minutes — demonstrating the scheduled trigger working as designed.

---

## Appendix A — Things to Watch Out For

### A.1 — BoE 2-digit year parsing

The Bank of England CSV uses dates like `31 Jan 26` (2-digit year). PySpark's `to_date` with format `dd MMM yy` interprets `26` as 2099. The solution manually prepends the century: `19xx` for years 31–99, `20xx` for years 00–30.

### A.2 — PySpark version and `try_cast`

`F.try_cast()` as a Python function was added in PySpark 4.0. This project uses PySpark 3.5.1, where `try_cast` is only available as a SQL expression: `F.expr("try_cast(column_name as type)")`.

### A.3 — PostgreSQL case sensitivity with Spark-generated columns

Spark writes DataFrame column names preserving original case. PostgreSQL lowercases unquoted identifiers. The DDL and upsert SQL must use double quotes around mixed-case column names.

### A.4 — JDBC write performance for large datasets

Writing 31M rows via Spark's JDBC writer caused heartbeat timeouts. Fix: `spark.driver.memory=8g`, `spark.network.timeout=800s`, `spark.executor.heartbeatInterval=120s`, JDBC `batchsize=50000`.

### A.5 — Gold layer memory with large tables

Reading the full 31M-row silver table into Spark caused OOM. Solution: SQL pushdown — PostgreSQL does GROUP BY, Spark receives ~132 rows.

### A.6 — Land Registry incremental design limitation

The watermark filter uses strict greater-than. New transactions from the same date as the watermark are skipped in incremental mode. Run full mode to pick them up.

### A.7 — BoE column code mapping

The BoE CSV uses series codes (LPMB3VA, etc.) mapped to friendly names in silver. Codes preserved in gold column names for traceability.

### A.8 — MLAR source codes in gold

Gold column names include MLAR source references (e.g. `MLAR_1_21_C_1` = sheet 1.21, section C, row 1).

### A.9 — Kafka consumer timeout management

Two separate timeouts: `max_poll_interval_ms=600000` (10 min) allows time for Land Registry COPY during processing. `consumer_timeout_ms=15000` (15 sec) controls how long the consumer waits for new messages after the last one. The DAG extends the consumer timeout to 600s in full mode. If the producer published nothing, the consumer is skipped entirely via XCom check.

### A.10 — JDBC partitioned reads for large tables

Reading 31M rows from bronze PostgreSQL via single-partition JDBC caused OOM even with 8g memory. Solution: partition the JDBC read into 16 parallel reads using `transfer_date` cast to epoch days as an integer partition column. `lowerBound` and `upperBound` must be numeric (PySpark requirement) — date strings cause `ValueError`.

### A.11 — Bronze idempotency without TRUNCATE

There was required not to drop/truncate tables. BoE and MLAR use staging table + INSERT WHERE NOT EXISTS. This is idempotent: running N times with the same data inserts 0 new rows after the first load.

### A.12 — XCom skip for no-change scheduled runs

If the Kafka producer detects no file changes, it publishes nothing and returns `{source: False}` for all sources. The `load_bronze` task reads this via XCom and skips the consumer entirely — preventing a 15-second idle wait on every scheduled run when nothing changed.

---

## Appendix B — Glossary

| Term | Meaning |
|------|---------|
| `<postgres-container>` | The PostgreSQL Docker container, e.g. `docker-postgres-1` |
| `<scheduler-container>` | The Airflow scheduler container, e.g. `docker-airflow-scheduler-1` |
| Bronze | Raw source data in PostgreSQL (all TEXT columns, no transformation) |
| Silver | Cleaned, typed, normalized data in PostgreSQL (one table per source) |
| Gold | Aggregated analytical table joining all sources on `quarter_start` |
| Watermark | `meta.watermark` table tracking last processed value per source (date/month/quarter) |
| File registry | `meta.file_registry` table tracking file changes to avoid unnecessary re-loading |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` — idempotent write pattern |
| INSERT WHERE NOT EXISTS | Append-only pattern for bronze — inserts only truly new rows |
| Staging table | Temporary table used during upsert — data is COPY'd in, then merged to target |
| MLAR | Mortgage Lenders and Administrators Return (FCA/BoE quarterly publication) |
| BoE | Bank of England |
| LR | Land Registry (HM Land Registry Price Paid Data) |
| JDBC | Java Database Connectivity — used by PySpark to read from / write to PostgreSQL |
| DAG | Directed Acyclic Graph — Airflow's workflow definition |
| `try_cast` | Spark SQL function that returns NULL instead of error for invalid casts |
| `partition_days` | Epoch days (days since 1970-01-01) used as numeric partition column for JDBC reads |
| `consumer_timeout_ms` | How long Kafka consumer waits for new messages before exiting |
| `max_poll_interval_ms` | How long Kafka tolerates silence between polls during message processing |
| XCom | Airflow's cross-task communication mechanism — used to pass producer results to consumer |
