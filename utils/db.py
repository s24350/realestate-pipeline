"""
db.py
-----
Database helpers used by pipeline scripts and the watermark module.

We use two connection styles:
  - SQLAlchemy engine  →  used by PySpark JDBC writes and pandas reads
  - psycopg2 raw conn  →  used for DDL, MERGE statements, watermark updates
"""

import logging
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from utils.config import DB_URL, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


# ── SQLAlchemy engine (singleton) ─────────────────────────────────────────────

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a cached SQLAlchemy engine for pipeline_db."""
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_pre_ping=True)
    return _engine


# ── psycopg2 raw connection ───────────────────────────────────────────────────

@contextmanager
def get_conn():
    """
    Context manager that yields a psycopg2 connection.
    Commits on clean exit, rolls back on exception.

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Utility functions ─────────────────────────────────────────────────────────

def execute_sql(sql: str, params: tuple[Any, ...] | None = None) -> None:
    """Execute a single SQL statement (DDL or DML) via psycopg2."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    logger.debug("Executed SQL: %s", sql[:120])


def execute_sql_file(path: str) -> None:
    """Read a .sql file and execute its full contents."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("Executed SQL file: %s", path)


def upsert_from_staging(
    staging_table: str,
    target_table: str,
    merge_key: str | list[str],
    columns: list[str],
) -> int:
    """
    Merge rows from a staging table into the target table using
    PostgreSQL INSERT ... ON CONFLICT DO UPDATE.

    This is the idempotent load pattern required by the lecturer:
    running this N times with the same data produces the same result —
    no duplicates, no data loss.

    Parameters
    ----------
    staging_table : str
        Fully qualified staging table name, e.g. "silver._staging_land_registry"
    target_table : str
        Fully qualified target table name, e.g. "silver.land_registry_clean"
    merge_key : str | list[str]
        Column(s) that uniquely identify a row (conflict target).
    columns : list[str]
        All columns to insert/update (must exist in both tables).

    Returns
    -------
    int
        Number of rows affected (inserted + updated).
    """
    if isinstance(merge_key, str):
        merge_key = [merge_key]

    key_clause    = ", ".join(merge_key)
    col_clause    = ", ".join(columns)
    update_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in merge_key
    )

    sql = f"""
        INSERT INTO {target_table} ({col_clause})
        SELECT {col_clause} FROM {staging_table}
        ON CONFLICT ({key_clause}) DO UPDATE SET
            {update_clause};
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rowcount = cur.rowcount

    logger.info(
        "Upserted %d rows from %s → %s", rowcount, staging_table, target_table
    )
    return rowcount


def drop_staging_table(staging_table: str) -> None:
    """Drop a temporary staging table after the merge is complete."""
    execute_sql(f"DROP TABLE IF EXISTS {staging_table};")
