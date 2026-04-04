"""
kafka_producer.py
-----------------
Publishes a JSON event to the Kafka 'file-events' topic for each
source data file found in the data directories.

This script is called by the Airflow DAG's first task, or can be run
manually to simulate a file arrival:
    python ingestion/kafka_producer.py --file data/boe/"Bank of England  Database.csv"

The Airflow DAG then uses a sensor to consume this event and trigger
the processing pipeline.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from utils.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    LAND_REGISTRY_PATH, BOE_PATH, MLAR_PATH,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_event(filepath: str) -> dict:
    """Build a JSON-serialisable event dict for a file."""
    p = Path(filepath)
    return {
        "event_type":   "file_available",
        "filename":     p.name,
        "filepath":     str(p.resolve()),
        "size_bytes":   p.stat().st_size if p.exists() else -1,
        "published_at": datetime.utcnow().isoformat(),
    }


def get_producer(retries: int = 5) -> KafkaProducer:
    """Return a KafkaProducer, retrying on startup if broker is not yet ready."""
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
                attempt,
                retries,
            )
            time.sleep(5)
    raise RuntimeError(
        f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} "
        f"after {retries} attempts."
    )


def publish_file_event(filepath: str) -> None:
    """Publish a single file-available event and close the producer."""
    producer = get_producer()
    event = build_event(filepath)
    future = producer.send(KAFKA_TOPIC, value=event)
    producer.flush()
    metadata = future.get(timeout=10)
    logger.info(
        "Published event for '%s' → topic=%s partition=%d offset=%d",
        event["filename"],
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
    producer.close()


def scan_and_publish_all() -> list[str]:
    """
    Scan all data directories and publish an event for every source file found.
    Returns the list of filenames published.
    """
    data_dirs = [LAND_REGISTRY_PATH, BOE_PATH, MLAR_PATH]
    published = []

    for dir_path in data_dirs:
        d = Path(dir_path)
        if not d.exists():
            logger.warning("Data directory does not exist: %s", d)
            continue

        for f in sorted(d.iterdir()):
            if f.suffix.lower() in {".csv", ".xlsx"} and f.name != ".gitkeep":
                publish_file_event(str(f))
                published.append(f.name)

    if not published:
        logger.info("No data files found in directories.")
    return published


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish file-available events to Kafka")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path to a specific file to publish. If omitted, scans all data directories.",
    )
    args = parser.parse_args()

    if args.file:
        publish_file_event(args.file)
    else:
        files = scan_and_publish_all()
        logger.info("Published %d file event(s).", len(files))
