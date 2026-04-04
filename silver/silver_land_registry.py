"""
silver/silver_land_registry.py
-------------------------------
Silver-layer transformation for the HM Land Registry Price Paid dataset.

Changes vs. v1 (pure SQL):
  - All type casting and NULL normalisation done in PySpark (not SQL)
  - quarter_start and quarter_label derived here in Silver
  - Idempotent upsert via staging table + INSERT ON CONFLICT
  - Incremental mode: reads monthly update file if available
  - Full mode: reads pp-complete.csv
  - Uses try_cast instead of regex for numeric conversion

Merge key: transaction_id  (the {GUID} in column A of pp-complete.csv)
"""

import logging
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType
)

from utils.config import (
    LAND_REGISTRY_PATH, LAND_REGISTRY_FILENAME,
    LAND_REGISTRY_MONTHLY_FILENAME,
    JDBC_URL, JDBC_PROPERTIES,
)
from utils.spark_session import get_spark
from utils.db import get_conn, drop_staging_table
from utils.watermark import get_filter_date, set_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────
# Land Registry pp-complete.csv has no header row — column names are positional
LR_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  True),
    StructField("price_raw",        StringType(),  True),
    StructField("transfer_date_raw",StringType(),  True),
    StructField("postcode",         StringType(),  True),
    StructField("property_type",    StringType(),  True),
    StructField("old_new",          StringType(),  True),
    StructField("duration",         StringType(),  True),
    StructField("paon",             StringType(),  True),
    StructField("saon",             StringType(),  True),
    StructField("street",           StringType(),  True),
    StructField("locality",         StringType(),  True),
    StructField("town_city",        StringType(),  True),
    StructField("district",         StringType(),  True),
    StructField("county",           StringType(),  True),
    StructField("ppd_category",     StringType(),  True),
    StructField("record_status",    StringType(),  True),
])

# Columns that must be present after transformation to write to Silver
SILVER_COLUMNS = [
    "transaction_id", "transfer_date", "price_gbp", "postcode",
    "property_type", "old_new", "duration", "paon", "saon", "street",
    "locality", "town_city", "district", "county", "ppd_category",
    "record_status", "quarter_start", "quarter_label",
]

STAGING_TABLE = "silver._staging_land_registry"
TARGET_TABLE  = "silver.land_registry_clean"


# ── Source file selection ─────────────────────────────────────────────────────

def _resolve_source_file(mode: str) -> str:
    """
    Decide which CSV to read based on mode:
      - full:        always pp-complete.csv (full history ~5 GB)
      - incremental: prefer pp-monthly-update-new-version.csv if it exists,
                     otherwise fall back to pp-complete.csv (watermark will
                     filter to only new rows anyway)
    """
    if mode == "incremental":
        monthly = Path(LAND_REGISTRY_PATH) / LAND_REGISTRY_MONTHLY_FILENAME
        if monthly.exists() and monthly.stat().st_size > 0:
            logger.info("Incremental mode: using monthly update file %s", monthly)
            return str(monthly)
        logger.info(
            "Incremental mode: monthly file not found, "
            "falling back to full file with watermark filter."
        )
    return f"{LAND_REGISTRY_PATH}/{LAND_REGISTRY_FILENAME}"


# ── Transform ─────────────────────────────────────────────────────────────────

def transform(
    spark: SparkSession,
    filter_date: date | None,
    mode: str = "full",
) -> DataFrame:
    src = _resolve_source_file(mode)
    logger.info("Reading Land Registry source: %s", src)

    df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(LR_SCHEMA)
        .csv(src)
    )

    df = (
        df
        # 1. Clean transaction_id: strip curly braces from {GUID}
        .withColumn(
            "transaction_id",
            F.regexp_replace(F.col("transaction_id"), r"[{}]", "")
        )

        # 2. Cast price to numeric using try_cast (NULL for non-numeric)
        .withColumn(
            "price_gbp",
            F.expr("try_cast(price_raw as decimal(14,2))")
        )

        # 3. Parse transfer_date (format: "2024-01-15 00:00")
        .withColumn(
            "transfer_date",
            F.to_date(F.col("transfer_date_raw"), "yyyy-MM-dd HH:mm")
        )

        # 4. Normalise property_type, old_new, duration to uppercase single char
        .withColumn("property_type", F.upper(F.trim(F.col("property_type"))))
        .withColumn("old_new",       F.upper(F.trim(F.col("old_new"))))
        .withColumn("duration",      F.upper(F.trim(F.col("duration"))))

        # 5. Nullify empty strings in text columns
        .withColumn("postcode",  F.nullif(F.trim(F.col("postcode")),  F.lit("")))
        .withColumn("paon",      F.nullif(F.trim(F.col("paon")),      F.lit("")))
        .withColumn("saon",      F.nullif(F.trim(F.col("saon")),      F.lit("")))
        .withColumn("street",    F.nullif(F.trim(F.col("street")),    F.lit("")))
        .withColumn("locality",  F.nullif(F.trim(F.col("locality")),  F.lit("")))
        .withColumn("town_city", F.nullif(F.trim(F.col("town_city")), F.lit("")))
        .withColumn("district",  F.nullif(F.trim(F.col("district")),  F.lit("")))
        .withColumn("county",    F.nullif(F.trim(F.col("county")),    F.lit("")))

        # 6. Drop rows with no transaction_id or no valid date
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("transfer_date").isNotNull())

        # 7. Derive quarter_start = first day of the quarter
        .withColumn(
            "quarter_start",
            F.date_trunc("quarter", F.col("transfer_date")).cast(DateType())
        )

        # 8. Derive quarter_label e.g. "2025Q4"
        .withColumn(
            "quarter_label",
            F.concat(
                F.year(F.col("quarter_start")).cast(StringType()),
                F.lit("Q"),
                F.quarter(F.col("quarter_start")).cast(StringType())
            )
        )

        # 9. Drop raw helper columns
        .drop("price_raw", "transfer_date_raw")
    )

    # 10. Incremental filter
    if filter_date is not None:
        logger.info("Incremental filter: quarter_start > %s", filter_date)
        df = df.filter(F.col("quarter_start") > F.lit(filter_date))

    return df.select(SILVER_COLUMNS)


# ── Write ─────────────────────────────────────────────────────────────────────

def write_silver(df: DataFrame) -> None:
    row_count = df.count()
    if row_count == 0:
        logger.info("No new rows to write for Land Registry silver.")
        return

    logger.info("Writing %d rows to staging table %s", row_count, STAGING_TABLE)

    (
        df.write
        .mode("overwrite")
        .option("truncate", "true")
        .option("batchsize", "10000")
        .jdbc(
            url=JDBC_URL,
            table=STAGING_TABLE,
            properties=JDBC_PROPERTIES,
        )
    )

    # UPSERT from staging → target
    upsert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(SILVER_COLUMNS)}, loaded_at)
        SELECT {', '.join(SILVER_COLUMNS)}, NOW()
        FROM {STAGING_TABLE}
        ON CONFLICT (transaction_id) DO UPDATE SET
            price_gbp       = EXCLUDED.price_gbp,
            transfer_date   = EXCLUDED.transfer_date,
            quarter_start   = EXCLUDED.quarter_start,
            quarter_label   = EXCLUDED.quarter_label,
            postcode        = EXCLUDED.postcode,
            property_type   = EXCLUDED.property_type,
            old_new         = EXCLUDED.old_new,
            duration        = EXCLUDED.duration,
            loaded_at       = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
        logger.info("Upsert complete → %s", TARGET_TABLE)

    drop_staging_table(STAGING_TABLE)


def update_watermark_from_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(quarter_start) FROM {TARGET_TABLE};"
            )
            result = cur.fetchone()

    if result and result[0]:
        set_watermark("land_registry", result[0])


def run(mode: str = "full") -> None:
    logger.info("Starting Land Registry silver transform. mode=%s", mode)
    filter_date = get_filter_date("land_registry", mode)

    spark = get_spark("silver-land-registry")
    try:
        df = transform(spark, filter_date, mode=mode)
        write_silver(df)
        update_watermark_from_db()
    finally:
        spark.stop()

    logger.info("Land Registry silver transform complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run(mode=args.mode)
