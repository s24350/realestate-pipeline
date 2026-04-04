"""
dags/realestate_dag.py
----------------------
Main Airflow DAG for the UK Real Estate pipeline.

Trigger modes
-------------
Full load (default):
    Trigger with Param  mode=full
    → Processes all source data, upserts everything.
    → Idempotent: run 100 times with same data = same result.

Incremental:
    Trigger with Param  mode=incremental
    → Only processes quarters newer than the watermark per source.
    → For Land Registry, uses monthly update file if available.
    → Falls back to full load automatically if no watermark exists yet.

How to trigger manually with a mode:
    Airflow UI → DAGs → realestate_pipeline → Trigger DAG w/ config
    JSON config: {"mode": "incremental"}

DAG task order
--------------
publish_file_events
        ↓
validate_bronze
        ↓
init_schemas           (idempotent DDL — safe to run every time)
        ↓
preprocess_mlar        (XLSX → CSV, skipped if CSVs already exist)
        ↓
silver_land_registry ──┐
silver_boe           ──┤  (run in parallel)
silver_mlar          ──┘
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

# ── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ── Task callables ─────────────────────────────────────────────────────────────
# Each callable is a thin wrapper that imports and calls the relevant module.
# Imports are deferred (inside the function) so Airflow can parse the DAG
# quickly without loading PySpark at import time.

def task_publish_file_events(**context) -> None:
    from ingestion.kafka_producer import scan_and_publish_all
    files = scan_and_publish_all()
    context["ti"].xcom_push(key="published_files", value=files)


def task_validate_bronze(**context) -> None:
    from bronze.ingest_bronze import validate_bronze
    summary = validate_bronze()
    context["ti"].xcom_push(key="bronze_summary", value=summary)


def task_init_schemas(**context) -> None:
    from utils.db import execute_sql_file
    execute_sql_file("/opt/airflow/sql/init_schemas.sql")


def task_preprocess_mlar(**context) -> None:
    """
    Run mlar_parser.py only if the output CSVs don't exist yet
    (or if a fresh XLSX was placed in the data directory).
    """
    from pathlib import Path
    from utils.config import MLAR_PATH
    expected = [
        f"{MLAR_PATH}/mlar_1_21.csv",
        f"{MLAR_PATH}/mlar_1_32.csv",
        f"{MLAR_PATH}/mlar_1_33.csv",
    ]
    if all(Path(p).exists() for p in expected):
        import logging
        logging.getLogger(__name__).info(
            "MLAR CSVs already exist — skipping preprocessing."
        )
        return
    from preprocessing.mlar_parser import parse_all
    parse_all()


def task_silver_land_registry(**context) -> None:
    mode = context["params"].get("mode", "full")
    from silver.silver_land_registry import run
    run(mode=mode)


def task_silver_boe(**context) -> None:
    mode = context["params"].get("mode", "full")
    from silver.silver_boe import run
    run(mode=mode)


def task_silver_mlar(**context) -> None:
    mode = context["params"].get("mode", "full")
    from silver.silver_mlar import run
    run(mode=mode)


def task_gold_aggregations(**context) -> None:
    mode = context["params"].get("mode", "full")
    from gold.gold_aggregations import run
    run(mode=mode)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="realestate_pipeline",
    description="UK real estate medallion pipeline — full & incremental",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    # Not scheduled automatically — triggered manually or via Kafka sensor.
    # Change to e.g. "@monthly" if you want automatic runs.
    schedule_interval=None,
    catchup=False,
    tags=["realestate", "medallion", "pyspark"],
    # Params appear in the Airflow UI "Trigger DAG" dialog
    params={
        "mode": Param(
            default="full",
            enum=["full", "incremental"],
            description=(
                "full = upsert all available data (idempotent). "
                "incremental = only process quarters newer than watermark."
            ),
        )
    },
    doc_md=__doc__,
) as dag:

    publish = PythonOperator(
        task_id="publish_file_events",
        python_callable=task_publish_file_events,
    )

    validate = PythonOperator(
        task_id="validate_bronze",
        python_callable=task_validate_bronze,
    )

    init_schemas = PythonOperator(
        task_id="init_schemas",
        python_callable=task_init_schemas,
    )

    preprocess = PythonOperator(
        task_id="preprocess_mlar",
        python_callable=task_preprocess_mlar,
    )

    silver_lr = PythonOperator(
        task_id="silver_land_registry",
        python_callable=task_silver_land_registry,
        execution_timeout=timedelta(hours=2),   # LR is 5 GB — allow time
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
    publish >> validate >> init_schemas >> preprocess

    # Silver tasks run in parallel after preprocessing
    preprocess >> [silver_lr, silver_boe, silver_mlar]

    # Gold runs after all silver tasks finish
    [silver_lr, silver_boe, silver_mlar] >> gold
