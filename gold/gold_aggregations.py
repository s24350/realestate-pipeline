"""
gold/gold_aggregations.py
--------------------------
Gold-layer aggregation.

Reads from Silver (PostgreSQL), joins all three sources on quarter_start,
aggregates to one row per quarter, and upserts into gold.housing_credit_summary.

Key design choices:
  - Uses FULL OUTER JOIN so quarters with partial source coverage are retained
  - Data quality flags (source_available_*) show which sources had data
  - Idempotent upsert on merge key: quarter_start
  - Incremental mode: only recomputes quarters newer than watermark
    (uses the minimum watermark across all three sources so no quarter
     is left half-updated)
  - Column naming: friendly_name__SOURCE_CODE__unit
  - No unit conversion here — MLAR ×1M already applied in silver
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.config import JDBC_URL, JDBC_PROPERTIES
from utils.spark_session import get_spark
from utils.db import get_conn, drop_staging_table
from utils.watermark import get_filter_date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_TABLE = "gold._staging_housing_credit"
TARGET_TABLE  = "gold.housing_credit_summary"

SILVER_LR_TABLE   = "silver.land_registry_clean"
SILVER_BOE_TABLE  = "silver.boe_monthly_clean"
SILVER_MLAR_TABLE = "silver.mlar_long"


# ── Read Silver ───────────────────────────────────────────────────────────────

def _read_table(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read
        .jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPERTIES)
    )


# ── Aggregate each source to quarter granularity ──────────────────────────────

def _agg_land_registry(spark: SparkSession, filter_date: date | None) -> DataFrame:
    where = f"WHERE quarter_start > '{filter_date}'" if filter_date else ""
    query = f"""
        (SELECT quarter_start, quarter_label,
                COUNT(transaction_id) AS "transactions_total__LR__count",
                AVG(price_gbp) AS "price_avg__LR__gbp",
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_gbp) AS "price_median__LR__gbp",
                MIN(price_gbp) AS "price_min__LR__gbp",
                MAX(price_gbp) AS "price_max__LR__gbp",
                TRUE AS "source_available_lr__flag"
         FROM silver.land_registry_clean
         {where}
         GROUP BY quarter_start, quarter_label) AS lr_agg
    """
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPERTIES)


def _agg_boe(df: DataFrame, filter_date: date | None) -> DataFrame:
    """Average monthly BoE figures up to quarter level."""
    if filter_date:
        df = df.filter(F.col("quarter_start") > F.lit(filter_date))
    return (
        df.groupBy("quarter_start")
        .agg(
            F.avg("total_house_purchase")    .alias("boe_house_purchase__LPMVTVX__count"),
            F.avg("total_remortgage")        .alias("boe_remortgage__LPMB4B3__count"),
            F.avg("total_secured_lending")   .alias("boe_total_secured_lending__LPMB3C8__count"),
            F.avg("mfi_total_approvals")     .alias("boe_mfi_total_approvals__LPMZ3UP__count"),
            F.lit(True)                      .alias("source_available_boe__flag"),
        )
    )


def _agg_mlar(df: DataFrame, filter_date: date | None) -> DataFrame:
    """
    Pivot specific MLAR categories from long format to wide quarter-level columns.
    Category names must match exactly what mlar_parser.py produces.
    """
    if filter_date:
        df = df.filter(F.col("quarter_start") > F.lit(filter_date))

    # Filter to the specific rows we need for gold
    # Category names come from the mapping CSVs used by mlar_parser.py
    categories = {
        "1.21": {
            "Regulated - Business flows - Gross advances":  "mlar_gross_advances__MLAR_1_21_C_1__gbp",
            "Regulated - Business flows - Net advances":    "mlar_net_advances__MLAR_1_21_C_2__gbp",
            "Regulated - Business flows - New commitments": "mlar_new_commitments__MLAR_1_21_C_3__gbp",
        },
        "1.32": {
            "Regulated - With Impaired credit history - Advances":  "mlar_imp_repayment__MLAR_1_32_C_3__pct",
            "Regulated - With Impaired credit history - Balances":  "mlar_imp_interest_only__MLAR_1_32_C_4__pct",
        },
        "1.33": {
            "Regulated - By purpose of loan - Advances - House purchase": "mlar_new_house_purchase__MLAR_1_33_C_29__gbp",
            "Regulated - By purpose of loan - Advances - Remortgage":     "mlar_new_remortgage__MLAR_1_33_C_30__gbp",
        },
    }

    # Build a mapping from (src, category) → alias
    flat_map = {
        (src, cat): alias
        for src, cats in categories.items()
        for cat, alias in cats.items()
    }

    # Filter DataFrame to only rows we care about
    filter_cond = F.lit(False)
    for (src, cat) in flat_map:
        filter_cond = filter_cond | (
            (F.col("src") == src) & (F.col("category") == cat)
        )
    df = df.filter(filter_cond)

    # Create a single composite key for pivoting
    df = df.withColumn(
        "metric_key",
        F.concat(F.col("src"), F.lit("__"), F.col("category"))
    )

    pivot_keys = [
        f"{src}__{cat}" for (src, cat) in flat_map
    ]

    pivoted = (
        df.groupBy("quarter_start")
        .pivot("metric_key", pivot_keys)
        .agg(F.first("value"))
    )

    # Rename pivot columns to friendly aliases
    for (src, cat), alias in flat_map.items():
        key = f"{src}__{cat}"
        if key in pivoted.columns:
            pivoted = pivoted.withColumnRenamed(key, alias)

    return pivoted.withColumn("source_available_mlar__flag", F.lit(True))


# ── Join and finalise ─────────────────────────────────────────────────────────

def build_gold(
    spark: SparkSession,
    filter_date: date | None,
) -> DataFrame:
    boe  = _read_table(spark, SILVER_BOE_TABLE)
    mlar = _read_table(spark, SILVER_MLAR_TABLE)

    lr_agg   = _agg_land_registry(spark, filter_date)
    boe_agg  = _agg_boe(boe,            filter_date)
    mlar_agg = _agg_mlar(mlar,          filter_date)

    # Full outer join on quarter_start so no quarter is silently dropped
    gold = (
        lr_agg
        .join(boe_agg,  on="quarter_start", how="full")
        .join(mlar_agg, on="quarter_start", how="full")
    )

    # Derive quarter_label from whichever source has it
    gold = gold.withColumn(
        "quarter_label",
        F.coalesce(
            F.col("quarter_label"),
            F.concat(
                F.year(F.col("quarter_start")).cast("string"),
                F.lit("Q"),
                F.quarter(F.col("quarter_start")).cast("string"),
            )
        )
    )

    # Fill missing availability flags with False
    gold = (
        gold
        .withColumn("source_available_lr__flag",   F.coalesce(F.col("source_available_lr__flag"),   F.lit(False)))
        .withColumn("source_available_boe__flag",  F.coalesce(F.col("source_available_boe__flag"),  F.lit(False)))
        .withColumn("source_available_mlar__flag", F.coalesce(F.col("source_available_mlar__flag"), F.lit(False)))
    )

    # Rename quarter_start to match gold table PK name
    gold = gold.withColumnRenamed("quarter_start", "quarter_start__date")

    return gold.orderBy("quarter_start__date")


# ── Write ─────────────────────────────────────────────────────────────────────

# All columns in the gold table (must match init_schemas.sql)
GOLD_COLUMNS = [
    "quarter_start__date",
    "quarter_label",
    "transactions_total__LR__count",
    "price_avg__LR__gbp",
    "price_median__LR__gbp",
    "price_min__LR__gbp",
    "price_max__LR__gbp",
    "boe_house_purchase__LPMVTVX__count",
    "boe_remortgage__LPMB4B3__count",
    "boe_total_secured_lending__LPMB3C8__count",
    "boe_mfi_total_approvals__LPMZ3UP__count",
    "mlar_gross_advances__MLAR_1_21_C_1__gbp",
    "mlar_net_advances__MLAR_1_21_C_2__gbp",
    "mlar_new_commitments__MLAR_1_21_C_3__gbp",
    "mlar_imp_repayment__MLAR_1_32_C_3__pct",
    "mlar_imp_interest_only__MLAR_1_32_C_4__pct",
    "mlar_new_house_purchase__MLAR_1_33_C_29__gbp",
    "mlar_new_remortgage__MLAR_1_33_C_30__gbp",
    "source_available_lr__flag",
    "source_available_boe__flag",
    "source_available_mlar__flag",
]


def write_gold(df: DataFrame) -> None:
    row_count = df.count()
    if row_count == 0:
        logger.info("No new gold rows to write.")
        return

    logger.info("Writing %d quarters to staging %s", row_count, STAGING_TABLE)

    df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=JDBC_URL, table=STAGING_TABLE, properties=JDBC_PROPERTIES
    )

    non_key_cols = [c for c in GOLD_COLUMNS if c != "quarter_start__date"]
    col_list = ", ".join(f'"{c}"' for c in GOLD_COLUMNS)
    update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols)

    upsert_sql = f"""
            INSERT INTO {TARGET_TABLE} ({col_list}, loaded_at)
            SELECT {col_list}, NOW()
            FROM {STAGING_TABLE}
            ON CONFLICT (quarter_start__date) DO UPDATE SET
                {update_clause},
                loaded_at = NOW();
        """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
    logger.info("Upsert complete → %s", TARGET_TABLE)
    drop_staging_table(STAGING_TABLE)


# ── Entry point ───────────────────────────────────────────────────────────────

def run(mode: str = "full") -> None:
    logger.info("Starting gold aggregation. mode=%s", mode)

    # Use the most conservative watermark across all three sources
    # so we never compute gold for a quarter where silver is still partial
    from utils.watermark import get_watermark
    if mode == "incremental":
        wm_lr   = get_watermark("land_registry")
        wm_boe  = get_watermark("boe")
        wm_mlar = get_watermark("mlar")
        valid   = [d for d in [wm_lr, wm_boe, wm_mlar] if d is not None]
        filter_date = min(valid) if valid else None
        logger.info("Incremental gold filter date (min watermark): %s", filter_date)
    else:
        filter_date = None

    spark = get_spark("gold-aggregations")
    try:
        df = build_gold(spark, filter_date)
        write_gold(df)
    finally:
        spark.stop()
    logger.info("Gold aggregation complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run(mode=args.mode)
