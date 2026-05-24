import dagster as dg

from silver.silver_boe import run as run_silver_boe
from silver.silver_mlar import run as run_silver_mlar
from silver.silver_land_registry import run as run_silver_land_registry
from gold.gold_aggregations import run as run_gold_aggregations

# ── Config schema ──────────────────────────────────────────────────────────────
# Airflow used:  context["params"].get("mode", "incremental")
# Dagster uses:  a typed config class validated before the run starts.
class PipelineConfig(dg.Config):
    mode: str = "incremental"

# ── Silver assets ──────────────────────────────────────────────────────────────

@dg.asset(
    group_name="silver",
    description="Clean BoE monthly interest rate data, loaded from bronze into silver.boe_monthly_clean."
)
def silver_boe(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_boe starting | mode={config.mode}")
    run_silver_boe(mode=config.mode)
    context.log.info("silver_boe complete")

@dg.asset(
    group_name="silver",
    description="Reshape MLAR mortgage lending data to long form → silver.mlar_long.",
)
def silver_mlar(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_mlar starting | mode={config.mode}")
    run_silver_mlar(mode=config.mode)
    context.log.info("silver_mlar complete")

@dg.asset(
    group_name="silver",
    description="Clean and standardize Land Registry price-paid data → silver.land_registry_clean.",
)
def silver_land_registry(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_land_registry starting | mode={config.mode}")
    run_silver_land_registry(mode=config.mode)
    context.log.info("silver_land_registry complete")

# ── Silver assets ──────────────────────────────────────────────────────────────
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
