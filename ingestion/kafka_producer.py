"""
ingestion/kafka_producer.py
----------------------------
Publishes data to source-specific Kafka topics.

- land-registry-data: file path message (full or monthly depending on mode)
- boe-data: actual CSV row data (demonstrates real data-through-queue)
- mlar-data: file path message (after parser runs if XLSX changed)

This script is called by the Airflow DAG's first task, or manually:
    python -m ingestion.kafka_producer --mode full
    python -m ingestion.kafka_producer --mode incremental
"""

import argparse
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from utils.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_LAND_REGISTRY,
    KAFKA_TOPIC_BOE,
    KAFKA_TOPIC_MLAR,
    LAND_REGISTRY_PATH, LAND_REGISTRY_FILENAME,
    LAND_REGISTRY_MONTHLY_FILENAME,
    BOE_PATH, BOE_FILENAME,
    MLAR_PATH, MLAR_XLSX_FILENAME, MLAR_LONG_RAW_FILENAME,
)
from utils.file_registry import has_file_changed, update_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_producer(retries: int = 5) -> KafkaProducer:
    """Return a KafkaProducer, retrying if broker is not yet ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in 5s...",
                attempt, retries,
            )
            time.sleep(5)
    raise RuntimeError(
        f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} "
        f"after {retries} attempts."
    )


def _publish(producer: KafkaProducer, topic: str, message: dict) -> None:
    """Publish a single message and wait for acknowledgment."""
    future = producer.send(topic, value=message)
    producer.flush()
    metadata = future.get(timeout=30)
    logger.info(
        "Published to %s (partition=%d, offset=%d)",
        metadata.topic, metadata.partition, metadata.offset,
    )


# ── Land Registry ─────────────────────────────────────────────────────────────

def publish_land_registry(producer: KafkaProducer, mode: str) -> bool:
    """
    Publish file path to land-registry-data topic.
    Full mode: pp-complete.csv
    Incremental: pp-monthly-update-new-version.csv (if changed)
    """
    if mode == "full":
        filepath = str(Path(LAND_REGISTRY_PATH) / LAND_REGISTRY_FILENAME)
    else:
        filepath = str(Path(LAND_REGISTRY_PATH) / LAND_REGISTRY_MONTHLY_FILENAME)
        if not Path(filepath).exists():
            logger.info("LR: no monthly update file — skipping.")
            return False
        if not has_file_changed("land_registry_monthly", filepath):
            return False

    message = {
        "source": "land_registry",
        "event_type": "file_available",
        "mode": mode,
        "filepath": filepath,
        "filename": Path(filepath).name,
        "size_bytes": Path(filepath).stat().st_size,
        "published_at": datetime.utcnow().isoformat(),
    }
    _publish(producer, KAFKA_TOPIC_LAND_REGISTRY, message)
    return True


# ── Bank of England ───────────────────────────────────────────────────────────

def publish_boe(producer: KafkaProducer, mode: str) -> bool:
    """
    Publish actual BoE CSV row data to boe-data topic.
    Demonstrates real data flowing through the queue.
    """
    filepath = str(Path(BOE_PATH) / BOE_FILENAME)
    if not Path(filepath).exists():
        logger.warning("BoE: file not found at %s — skipping.", filepath)
        return False

    if mode == "incremental" and not has_file_changed("boe", filepath):
        return False

    # Read CSV and publish rows as JSON
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header row
        for row in reader:
            rows.append(row)

    message = {
        "source": "boe",
        "event_type": "data_rows",
        "mode": mode,
        "filename": Path(filepath).name,
        "row_count": len(rows),
        "header": header,
        "rows": rows,
        "published_at": datetime.utcnow().isoformat(),
    }
    _publish(producer, KAFKA_TOPIC_BOE, message)
    logger.info("BoE: published %d rows to %s", len(rows), KAFKA_TOPIC_BOE)
    return True


# ── MLAR ──────────────────────────────────────────────────────────────────────

def publish_mlar(producer: KafkaProducer, mode: str) -> bool:
    """
    Check if MLAR XLSX changed, run parser if needed, then publish
    file path to mlar-data topic.
    """
    xlsx_path = str(Path(MLAR_PATH) / MLAR_XLSX_FILENAME)
    csv_path = str(Path(MLAR_PATH) / MLAR_LONG_RAW_FILENAME)

    if not Path(xlsx_path).exists():
        logger.warning("MLAR: XLSX not found at %s — skipping.", xlsx_path)
        return False

    # Check if parser needs to run
    need_parse = not Path(csv_path).exists()
    if not need_parse and has_file_changed("mlar_xlsx", xlsx_path):
        need_parse = True

    if mode == "incremental" and not need_parse:
        if not has_file_changed("mlar_csv", csv_path):
            return False

    if need_parse:
        logger.info("MLAR: running parser (XLSX → long CSV)...")
        from preprocessing.mlar_parser import parse_all
        parse_all()
        update_registry("mlar_xlsx", xlsx_path)

    message = {
        "source": "mlar",
        "event_type": "file_available",
        "mode": mode,
        "filepath": csv_path,
        "filename": Path(csv_path).name,
        "size_bytes": Path(csv_path).stat().st_size if Path(csv_path).exists() else -1,
        "published_at": datetime.utcnow().isoformat(),
    }
    _publish(producer, KAFKA_TOPIC_MLAR, message)
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def publish_all(mode: str = "full") -> dict:
    """
    Scan all data directories and publish to source-specific Kafka topics.
    Returns dict of {source: published (bool)}.
    """
    logger.info("Kafka producer starting. mode=%s", mode)
    producer = _get_producer()

    results = {
        "land_registry": publish_land_registry(producer, mode),
        "boe": publish_boe(producer, mode),
        "mlar": publish_mlar(producer, mode),
    }

    producer.close()
    published = [k for k, v in results.items() if v]
    logger.info("Published to %d topic(s): %s", len(published), published)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    publish_all(mode=args.mode)