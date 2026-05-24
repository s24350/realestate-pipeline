import dagster as dg

from silver.silver_boe import run as run_silver_boe

# ── Config schema ──────────────────────────────────────────────────────────────
# Airflow used:  context["params"].get("mode", "incremental")
# Dagster uses:  a typed config class validated before the run starts.
class PipelineConfig(dg.Config):
    mode: str = "incremental"

# ── Asset ──────────────────────────────────────────────────────────────────────

@dg.asset(
    description="Clean BoE monthly interest rate data, loaded from bronze into silver.boe_monthly_clean."
)
def silver_boe(context: dg.AssetExecutionContext, config: PipelineConfig):
    context.log.info(f"silver_boe starting | mode={config.mode}")
    run_silver_boe(mode=config.mode)
    context.log.info("silver_boe complete")