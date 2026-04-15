"""
bronze/load_bronze.py
----------------------
Loads raw CSV data into bronze PostgreSQL tables.

Strategies per source:
  - Land Registry full:  TRUNCATE + COPY from pp-complete.csv (~5 GB, ~8 min)
  - Land Registry incr:  APPEND from pp-monthly-update-new-version.csv
  - BoE:                 Staging table + INSERT WHERE NOT EXISTS (on date_col)
  - MLAR:                Run parser if needed, then staging + INSERT WHERE NOT EXISTS
                         (on src, category, quarter)

File registry (meta.file_registry) prevents re-loading unchanged files
in incremental/scheduled mode.
"""

import argparse
import logging
from pathlib import Path

from utils.config import (
    LAND_REGISTRY_PATH, LAND_REGISTRY_FILENAME,
    LAND_REGISTRY_MONTHLY_FILENAME,
    BOE_PATH, BOE_FILENAME,
    MLAR_PATH, MLAR_XLSX_FILENAME,
)
from utils.db import get_conn
from utils.file_registry import has_file_changed, update_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Land Registry ─────────────────────────────────────────────────────────────

def load_land_registry(mode: str) -> None:
    """
    Full mode:  TRUNCATE + COPY from pp-complete.csv (no header)
    Incremental: APPEND from pp-monthly-update-new-version.csv (no header)
                 Only if file changed (file registry check)
    """
    if mode == "full":
        filepath = str(Path(LAND_REGISTRY_PATH) / LAND_REGISTRY_FILENAME)
        logger.info("Land Registry FULL: TRUNCATE + COPY from %s", filepath)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE bronze.land_registry_raw;")
                with open(filepath, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        "COPY bronze.land_registry_raw FROM STDIN WITH (FORMAT csv)",
                        f,
                    )
        update_registry("land_registry", filepath)

    else:  # incremental
        monthly_path = str(Path(LAND_REGISTRY_PATH) / LAND_REGISTRY_MONTHLY_FILENAME)
        if not Path(monthly_path).exists():
            logger.info("Land Registry: no monthly update file found — skipping.")
            return
        if not has_file_changed("land_registry_monthly", monthly_path):
            return

        logger.info("Land Registry INCREMENTAL: APPEND from %s", monthly_path)
        with get_conn() as conn:
            with conn.cursor() as cur:
                # COPY into a staging table, then INSERT WHERE NOT EXISTS
                cur.execute("CREATE TEMP TABLE _staging_lr (LIKE bronze.land_registry_raw);")
                with open(monthly_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        "COPY _staging_lr FROM STDIN WITH (FORMAT csv)",
                        f,
                    )
                cur.execute("""
                    INSERT INTO bronze.land_registry_raw
                    SELECT s.* FROM _staging_lr s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bronze.land_registry_raw b
                        WHERE b.transaction_id = s.transaction_id
                    );
                """)
                inserted = cur.rowcount
                cur.execute("DROP TABLE _staging_lr;")
        logger.info("Land Registry: inserted %d new rows from monthly update.", inserted)
        update_registry("land_registry_monthly", monthly_path)

    # Log final count
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.land_registry_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.land_registry_raw: %d total rows.", count)


# ── Bank of England ───────────────────────────────────────────────────────────

def load_boe(mode: str) -> None:
    """
    Staging table + INSERT WHERE NOT EXISTS on date_col.
    In full mode: always load. In incremental: only if file changed.
    """
    filepath = str(Path(BOE_PATH) / BOE_FILENAME)

    if mode == "incremental" and not has_file_changed("boe", filepath):
        return

    logger.info("BoE: loading from %s", filepath)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE _staging_boe (LIKE bronze.boe_raw);
            """)
            with open(filepath, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    "COPY _staging_boe FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
            cur.execute("""
                INSERT INTO bronze.boe_raw
                SELECT s.* FROM _staging_boe s
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.boe_raw b
                    WHERE b.date_col = s.date_col
                );
            """)
            inserted = cur.rowcount
            cur.execute("DROP TABLE _staging_boe;")
    logger.info("BoE: inserted %d new rows.", inserted)
    update_registry("boe", filepath)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.boe_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.boe_raw: %d total rows.", count)


# ── MLAR ──────────────────────────────────────────────────────────────────────

def load_mlar(mode: str) -> None:
    """
    Run mlar_parser.py if XLSX changed (or if output CSV doesn't exist),
    then staging + INSERT WHERE NOT EXISTS on (src, category, quarter).
    """
    xlsx_path = str(Path(MLAR_PATH) / MLAR_XLSX_FILENAME)
    csv_path = str(Path(MLAR_PATH) / "mlar_long_raw.csv")

    # Check if parser needs to run
    need_parse = not Path(csv_path).exists()
    if not need_parse and has_file_changed("mlar_xlsx", xlsx_path):
        need_parse = True

    if need_parse:
        logger.info("MLAR: running parser (XLSX → long CSV)...")
        from preprocessing.mlar_parser import parse_all
        parse_all()
        update_registry("mlar_xlsx", xlsx_path)

    # Now load the CSV
    if mode == "incremental" and not need_parse:
        if not has_file_changed("mlar_csv", csv_path):
            return

    if not Path(csv_path).exists():
        logger.warning("MLAR: no CSV file found at %s — skipping.", csv_path)
        return

    logger.info("MLAR: loading from %s", csv_path)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE _staging_mlar (LIKE bronze.mlar_raw);
            """)
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    "COPY _staging_mlar FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
            cur.execute("""
                INSERT INTO bronze.mlar_raw
                SELECT s.* FROM _staging_mlar s
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.mlar_raw b
                    WHERE b.src = s.src
                      AND b.category = s.category
                      AND b.quarter = s.quarter
                );
            """)
            inserted = cur.rowcount
            cur.execute("DROP TABLE _staging_mlar;")
    logger.info("MLAR: inserted %d new rows.", inserted)
    update_registry("mlar_csv", csv_path)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.mlar_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.mlar_raw: %d total rows.", count)


# ── Entry point ───────────────────────────────────────────────────────────────

def run(mode: str = "full") -> None:
    logger.info("Starting bronze load. mode=%s", mode)
    load_land_registry(mode)
    load_boe(mode)
    load_mlar(mode)
    logger.info("Bronze load complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run(mode=args.mode)
