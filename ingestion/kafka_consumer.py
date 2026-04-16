"""
ingestion/kafka_consumer.py
----------------------------
Consumes from source-specific Kafka topics and loads data into bronze PostgreSQL.

- land-registry-data: reads file path from message, COPYs to bronze
- boe-data: reads actual row data from message, inserts to bronze
- mlar-data: reads file path from message, COPYs to bronze

This script is called by the Airflow DAG's load_bronze task, or manually:
    python -m ingestion.kafka_consumer --mode full
    python -m ingestion.kafka_consumer --mode incremental
"""

import argparse
import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from utils.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_LAND_REGISTRY,
    KAFKA_TOPIC_BOE,
    KAFKA_TOPIC_MLAR,
)
from utils.db import get_conn
from utils.file_registry import update_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_consumer(topics: list, timeout_ms: int = 10000) -> KafkaConsumer:
    """Return a KafkaConsumer, retrying if broker is not yet ready."""
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=timeout_ms,
                max_poll_interval_ms=600000,
            )
            logger.info("Connected to Kafka, subscribed to: %s", topics)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not available (attempt %d/5). Retrying in 5s...", attempt)
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka after 5 attempts.")


# ── Land Registry consumer ────────────────────────────────────────────────────

def _consume_land_registry(message: dict) -> None:
    """
    Read file path from Kafka message, COPY to bronze.
    Full mode: TRUNCATE + COPY. Incremental: INSERT WHERE NOT EXISTS via staging.
    """
    filepath = message["filepath"]
    mode = message.get("mode", "full")

    logger.info("LR consumer: mode=%s, file=%s", mode, filepath)

    with get_conn() as conn:
        with conn.cursor() as cur:
            if mode == "full":
                cur.execute("TRUNCATE bronze.land_registry_raw;")
                with open(filepath, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        "COPY bronze.land_registry_raw FROM STDIN WITH (FORMAT csv)",
                        f,
                    )
                logger.info("LR: TRUNCATE + COPY complete.")
            else:
                cur.execute("CREATE TEMP TABLE _staging_lr (LIKE bronze.land_registry_raw);")
                with open(filepath, "r", encoding="utf-8") as f:
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
                logger.info("LR: inserted %d new rows (incremental).", inserted)

    update_registry("land_registry", filepath)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.land_registry_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.land_registry_raw: %d total rows.", count)


# ── BoE consumer ──────────────────────────────────────────────────────────────

def _consume_boe(message: dict) -> None:
    """
    Read actual row data from Kafka message, INSERT WHERE NOT EXISTS into bronze.
    Demonstrates real data flowing through the queue system.
    """
    rows = message["rows"]
    row_count = message.get("row_count", len(rows))
    logger.info("BoE consumer: received %d rows from Kafka.", row_count)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Create staging table, insert rows from Kafka message
            cur.execute("CREATE TEMP TABLE _staging_boe (LIKE bronze.boe_raw);")

            # Insert each row from the Kafka message into staging
            for row in rows:
                placeholders = ", ".join(["%s"] * len(row))
                cur.execute(
                    f"INSERT INTO _staging_boe VALUES ({placeholders});",
                    row,
                )

            # INSERT WHERE NOT EXISTS on date_col
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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.boe_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.boe_raw: %d total rows.", count)


# ── MLAR consumer ─────────────────────────────────────────────────────────────

def _consume_mlar(message: dict) -> None:
    """
    Read file path from Kafka message, COPY to bronze via staging +
    INSERT WHERE NOT EXISTS on (src, category, quarter).
    """
    filepath = message["filepath"]
    logger.info("MLAR consumer: file=%s", filepath)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _staging_mlar (LIKE bronze.mlar_raw);")
            with open(filepath, "r", encoding="utf-8") as f:
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
    update_registry("mlar_csv", filepath)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.mlar_raw;")
            count = cur.fetchone()[0]
    logger.info("bronze.mlar_raw: %d total rows.", count)


# ── Main consumer loop ────────────────────────────────────────────────────────

TOPIC_HANDLERS = {
    KAFKA_TOPIC_LAND_REGISTRY: _consume_land_registry,
    KAFKA_TOPIC_BOE: _consume_boe,
    KAFKA_TOPIC_MLAR: _consume_mlar,
}


def consume_all(timeout_ms: int = 15000) -> dict:
    """
    Consume all pending messages from all source topics.
    Returns dict of {topic: messages_processed}.
    """
    topics = list(TOPIC_HANDLERS.keys())
    consumer = _get_consumer(topics, timeout_ms=timeout_ms)

    counts = {t: 0 for t in topics}

    for msg in consumer:
        topic = msg.topic
        message = msg.value
        logger.info("Received message from topic '%s' (offset=%d)", topic, msg.offset)

        handler = TOPIC_HANDLERS.get(topic)
        if handler:
            handler(message)
            counts[topic] += 1
        else:
            logger.warning("No handler for topic '%s' — skipping.", topic)

    consumer.close()
    logger.info("Consumer finished. Processed: %s", counts)
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--timeout", type=int, default=15000,
        help="Consumer timeout in ms (how long to wait for messages)",
    )
    args = parser.parse_args()
    consume_all(timeout_ms=args.timeout)
