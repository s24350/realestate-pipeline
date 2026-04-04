"""
watermark.py
------------
Manages the watermark table that tracks incremental processing state.

The meta.watermark table has one row per data source:
    source_name  TEXT PRIMARY KEY
    last_quarter DATE   -- first day of the last successfully processed quarter
    updated_at   TIMESTAMP

The DAG reads the watermark before processing and updates it after a
successful run. This makes incremental mode safe to re-run: if the DAG
fails mid-run, the watermark is not updated and the next run reprocesses
the same quarter (idempotent because of the UPSERT pattern in db.py).
"""

import logging
from datetime import date, datetime

from utils.db import execute_sql, get_conn

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.watermark (
    source_name  TEXT        PRIMARY KEY,
    last_quarter DATE        NOT NULL,
    updated_at   TIMESTAMP   NOT NULL DEFAULT NOW()
);
"""


def ensure_watermark_table() -> None:
    """Create meta.watermark if it does not exist yet."""
    execute_sql(CREATE_TABLE_SQL)
    logger.info("Watermark table ready.")


# ── Read ──────────────────────────────────────────────────────────────────────

def get_watermark(source_name: str) -> date | None:
    """
    Return the last successfully processed quarter_start for a source,
    or None if the source has never been processed (triggers full load).

    Parameters
    ----------
    source_name : str
        One of: 'land_registry', 'boe', 'mlar'
    """
    ensure_watermark_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_quarter FROM meta.watermark WHERE source_name = %s;",
                (source_name,),
            )
            row = cur.fetchone()

    if row is None:
        logger.info("No watermark found for '%s' — full load required.", source_name)
        return None

    logger.info("Watermark for '%s': %s", source_name, row[0])
    return row[0]


# ── Write ─────────────────────────────────────────────────────────────────────

def set_watermark(source_name: str, last_quarter: date) -> None:
    """
    Upsert the watermark for a source after a successful pipeline run.

    Parameters
    ----------
    source_name : str
        Source identifier.
    last_quarter : date
        The most recent quarter_start that was successfully processed
        (e.g. date(2025, 10, 1) for Q4 2025).
    """
    ensure_watermark_table()
    sql = """
        INSERT INTO meta.watermark (source_name, last_quarter, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET
            last_quarter = EXCLUDED.last_quarter,
            updated_at   = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_name, last_quarter))

    logger.info("Watermark updated: %s → %s", source_name, last_quarter)


# ── Convenience helper for the DAG ───────────────────────────────────────────

def get_filter_date(source_name: str, mode: str) -> date | None:
    """
    Return the date to use as the lower bound for incremental filtering.

    - mode='full'        → None  (no filter, process everything)
    - mode='incremental' → last_quarter from watermark (or None = full load
                           if source has never been processed before)

    Parameters
    ----------
    source_name : str
    mode : str  'full' or 'incremental'
    """
    if mode == "full":
        logger.info("Mode=full: no date filter for '%s'.", source_name)
        return None
    return get_watermark(source_name)
