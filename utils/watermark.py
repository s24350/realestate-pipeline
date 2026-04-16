"""
utils/watermark.py
------------------
Manages the watermark table that tracks incremental processing state.

The meta.watermark table has one row per data source:
    source_name  TEXT PRIMARY KEY
    last_value   DATE   -- last processed date/month/quarter depending on source
    updated_at   TIMESTAMP

Watermark granularity:
  - land_registry: MAX(transfer_date) — date-level
  - boe:           MAX(month_start)   — month-level
  - mlar:          MAX(quarter_start) — quarter-level

The DAG reads the watermark before processing and updates it after a
successful run. Idempotent: if the DAG fails mid-run, the watermark is
not updated and the next run reprocesses the same data.
"""

import logging
from datetime import date

from utils.db import get_conn

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.watermark (
    source_name  TEXT        PRIMARY KEY,
    last_value   DATE        NOT NULL,
    updated_at   TIMESTAMP   NOT NULL DEFAULT NOW()
);
"""


def ensure_watermark_table() -> None:
    """Create meta.watermark if it does not exist yet."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
    logger.info("Watermark table ready.")


def get_watermark(source_name: str) -> date | None:
    """
    Return the last processed value for a source,
    or None if the source has never been processed (triggers full load).
    """
    ensure_watermark_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_value FROM meta.watermark WHERE source_name = %s;",
                (source_name,),
            )
            row = cur.fetchone()

    if row is None:
        logger.info("No watermark found for '%s' — full load required.", source_name)
        return None

    logger.info("Watermark for '%s': %s", source_name, row[0])
    return row[0]


def set_watermark(source_name: str, last_value: date) -> None:
    """
    Upsert the watermark for a source after a successful pipeline run.
    """
    ensure_watermark_table()
    sql = """
        INSERT INTO meta.watermark (source_name, last_value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET
            last_value = EXCLUDED.last_value,
            updated_at = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_name, last_value))

    logger.info("Watermark updated: %s → %s", source_name, last_value)


def get_filter_date(source_name: str, mode: str) -> date | None:
    """
    Return the date to use as the lower bound for incremental filtering.

    - mode='full'        → None  (no filter, process everything)
    - mode='incremental' → last_value from watermark (or None = full load
                           if source has never been processed before)
    """
    if mode == "full":
        logger.info("Mode=full: no date filter for '%s'.", source_name)
        return None
    return get_watermark(source_name)