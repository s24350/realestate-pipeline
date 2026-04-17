"""
dags/realestate_dag.py
----------------------
Main Airflow DAG for the UK Real Estate pipeline.

Trigger modes
-------------
Scheduled incremental (default):
    Runs automatically via schedule_interval.
    → Kafka producer checks file registry, publishes changed files
    → Consumer loads to bronze
    → Silver processes only data newer than watermark
    → Gold recomputes affected quarters

Manual full:
    Trigger from Airflow UI with Param mode=full
    → Kafka producer publishes all files regardless of changes
    → Consumer does TRUNCATE + COPY for Land Registry, INSERT WHERE NOT EXISTS for others
    → Silver processes everything
    → Gold rebuilds all

Manual incremental:
    Trigger from Airflow UI with Param mode=incremental
    → Same as scheduled, but on-demand

DAG task order
--------------
publish_to_kafka → load_bronze → init_schemas
                                      ↓
                   silver_land_registry ←───┤
                   silver_boe           ←───┤  (parallel)
                   silver_mlar          ←───┘
                              ↓
                   gold_aggregations
"""

from __future__ import annotations

import sys
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Make project modules importable from the mounted volume
sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ── Task callables ─────────────────────────────────────────────────────────────

def task_publish_to_kafka(**context) -> None:
    """Scan data directories, publish to source-specific Kafka topics."""
    mode = context["params"].get("mode", "incremental")
    from ingestion.kafka_producer import publish_all
    results = publish_all(mode=mode)
    context["ti"].xcom_push(key="kafka_results", value=results)


def task_load_bronze(**context) -> None:
    kafka_results = context["ti"].xcom_pull(
        task_ids="publish_to_kafka", key="kafka_results"
    )
    if kafka_results and any(kafka_results.values()):
        mode = context["params"].get("mode", "incremental")
        timeout = 600000 if mode == "full" else 15000
        from ingestion.kafka_consumer import consume_all
        counts = consume_all(timeout_ms=timeout)
        context["ti"].xcom_push(key="bronze_counts", value=counts)
    else:
        import logging
        logging.getLogger(__name__).info(
            "No files changed — skipping Kafka consumer."
        )
        context["ti"].xcom_push(key="bronze_counts", value={})


def task_init_schemas(**context) -> None:
    """Run DDL to ensure all tables exist (idempotent)."""
    from utils.db import execute_sql_file
    execute_sql_file("/opt/airflow/sql/init_schemas.sql")


def task_silver_land_registry(**context) -> None:
    mode = context["params"].get("mode", "incremental")
    from silver.silver_land_registry import run
    run(mode=mode)


def task_silver_boe(**context) -> None:
    mode = context["params"].get("mode", "incremental")
    from silver.silver_boe import run
    run(mode=mode)


def task_silver_mlar(**context) -> None:
    mode = context["params"].get("mode", "incremental")
    from silver.silver_mlar import run
    run(mode=mode)


def task_gold_aggregations(**context) -> None:
    mode = context["params"].get("mode", "incremental")
    from gold.gold_aggregations import run
    run(mode=mode)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="realestate_pipeline",
    description="UK real estate medallion pipeline — Kafka → Bronze → Silver → Gold",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["realestate", "medallion", "pyspark", "kafka"],
    params={
        "mode": Param(
            default="incremental",
            enum=["full", "incremental"],
            description=(
                "full = TRUNCATE + COPY bronze, upsert all silver/gold (idempotent). "
                "incremental = only process new/changed data per watermark."
            ),
        )
    },
    doc_md=__doc__,
) as dag:

    publish = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=task_publish_to_kafka,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        execution_timeout=timedelta(hours=1),
    )

    init_schemas = PythonOperator(
        task_id="init_schemas",
        python_callable=task_init_schemas,
    )

    silver_lr = PythonOperator(
        task_id="silver_land_registry",
        python_callable=task_silver_land_registry,
        execution_timeout=timedelta(hours=2),
    )

    silver_boe = PythonOperator(
        task_id="silver_boe",
        python_callable=task_silver_boe,
    )

    silver_mlar = PythonOperator(
        task_id="silver_mlar",
        python_callable=task_silver_mlar,
    )

    gold = PythonOperator(
        task_id="gold_aggregations",
        python_callable=task_gold_aggregations,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    publish >> load_bronze >> init_schemas

    init_schemas >> [silver_lr, silver_boe, silver_mlar]

    [silver_lr, silver_boe, silver_mlar] >> gold