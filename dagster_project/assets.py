import dagster as dg

import os

from silver.silver_boe import run as run_silver_boe
from silver.silver_mlar import run as run_silver_mlar
from silver.silver_land_registry import run as run_silver_land_registry
from gold.gold_aggregations import run as run_gold_aggregations

# ── Config schema ──────────────────────────────────────────────────────────────
# Airflow used:  context["params"].get("mode", "incremental")
# Dagster uses:  a typed config class validated before the run starts.
class PipelineConfig(dg.Config):
    mode: str = "incremental"

# ── Bronze assets ──────────────────────────────────────────────────────────────
# Airflow's XCom push/pull becomes a typed return value flowing into a
# downstream asset's input parameter (see publish_to_kafka → load_bronze)
@dg.asset(
    group_name="bronze",
    description="Scan data dirs and publish file/data events to Kafka topics. Returns which sources were published.",
)
def publish_to_kafka(context: dg.AssetExecutionContext, config: PipelineConfig) -> dict:
    from ingestion.kafka_producer import publish_all
    results = publish_all(mode=config.mode)
    context.log.info(f"publish_to_kafka results: {results}")
    return results


@dg.asset(
    group_name="bronze",
    description="Consume Kafka events into bronze tables, but only if files actually changed.",
)
def load_bronze(
        context: dg.AssetExecutionContext,
        config: PipelineConfig,
        publish_to_kafka: dict,  # ← upstream asset's RETURN VALUE, injected by Dagster
) -> None:
    if publish_to_kafka and any(publish_to_kafka.values()):
        timeout = 600000 if config.mode == "full" else 15000
        from ingestion.kafka_consumer import consume_all
        counts = consume_all(timeout_ms=timeout)
        context.log.info(f"load_bronze counts: {counts}")
    else:
        context.log.info("No files changed — skipping Kafka consumer.")


@dg.asset(
    group_name="bronze",
    deps=[load_bronze],   # ordering only — init_schemas needs no data from load_bronze
    description="Run idempotent DDL (CREATE TABLE IF NOT EXISTS) for all layers.",
)
def init_schemas(context: dg.AssetExecutionContext) -> None:
    from utils.db import execute_sql_file
    sql_dir = os.environ.get("SQL_PATH", "/opt/airflow/sql")
    execute_sql_file(f"{sql_dir}/init_schemas.sql")
    context.log.info("init_schemas complete")

# ── Silver assets ──────────────────────────────────────────────────────────────

@dg.asset(
    group_name="silver",
    deps=[init_schemas],
    description="Clean BoE monthly interest rate data, loaded from bronze into silver.boe_monthly_clean."
)
def silver_boe(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_boe starting | mode={config.mode}")
    run_silver_boe(mode=config.mode)
    context.log.info("silver_boe complete")

@dg.asset(
    group_name="silver",
    deps=[init_schemas],
    description="Reshape MLAR mortgage lending data to long form → silver.mlar_long.",
)
def silver_mlar(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_mlar starting | mode={config.mode}")
    run_silver_mlar(mode=config.mode)
    context.log.info("silver_mlar complete")

@dg.asset(
    group_name="silver",
    deps=[init_schemas],
    description="Clean and standardize Land Registry price-paid data → silver.land_registry_clean.",
)
def silver_land_registry(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_land_registry starting | mode={config.mode}")
    run_silver_land_registry(mode=config.mode)
    context.log.info("silver_land_registry complete")

# ── Gold assets ──────────────────────────────────────────────────────────────
# Dagster reads list of dependencies deps=[...] and builds the lineage graph
# Airflow needed explicit graph edges declarations [...] >> gold
@dg.asset(
    group_name="gold",
    deps=[silver_land_registry, silver_boe, silver_mlar],
    description="Quarterly housing-credit aggregations → gold.housing_credit_summary.",
)
def gold_aggregations(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"gold_aggregations starting | mode={config.mode}")
    run_gold_aggregations(mode=config.mode)
    context.log.info("gold_aggregations complete")
