"""
utils/file_registry.py
-----------------------
Tracks file changes via meta.file_registry table.
Used by load_bronze to skip COPY when files haven't changed.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from utils.db import get_conn

logger = logging.getLogger(__name__)

ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta.file_registry (
    source_name    TEXT        PRIMARY KEY,
    filename       TEXT        NOT NULL,
    file_size      BIGINT      NOT NULL,
    last_modified  TIMESTAMP   NOT NULL,
    updated_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);
"""


def _ensure_table() -> None:
    """Create file_registry table if it doesn't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ENSURE_TABLE_SQL)


def has_file_changed(source_name: str, filepath: str) -> bool:
    """
    Check if a file has changed since the last registered load.
    Returns True if the file is new or has different size/mtime.
    Returns True if file_registry has no entry for this source (first run).
    """
    _ensure_table()
    p = Path(filepath)
    if not p.exists():
        logger.warning("File does not exist: %s", filepath)
        return False

    current_size = p.stat().st_size
    current_mtime = datetime.fromtimestamp(p.stat().st_mtime)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_size, last_modified FROM meta.file_registry "
                "WHERE source_name = %s;",
                (source_name,),
            )
            row = cur.fetchone()

    if row is None:
        logger.info("No registry entry for '%s' — file is new.", source_name)
        return True

    stored_size, stored_mtime = row
    if current_size != stored_size or abs((current_mtime - stored_mtime).total_seconds()) > 1:
        logger.info(
            "File changed for '%s': size %d→%d, mtime %s→%s",
            source_name, stored_size, current_size, stored_mtime, current_mtime,
        )
        return True

    logger.info("File unchanged for '%s' — skipping.", source_name)
    return False


def update_registry(source_name: str, filepath: str) -> None:
    """
    Update the file registry after a successful load.
    """
    _ensure_table()
    p = Path(filepath)
    current_size = p.stat().st_size
    current_mtime = datetime.fromtimestamp(p.stat().st_mtime)

    sql = """
        INSERT INTO meta.file_registry (source_name, filename, file_size, last_modified, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET
            filename      = EXCLUDED.filename,
            file_size     = EXCLUDED.file_size,
            last_modified = EXCLUDED.last_modified,
            updated_at    = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_name, p.name, current_size, current_mtime))

    logger.info("Registry updated: %s → %s (%d bytes)", source_name, p.name, current_size)
