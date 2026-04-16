"""
silver/silver_land_registry.py
-------------------------------
Silver-layer transformation for the HM Land Registry Price Paid dataset.

Reads from bronze.land_registry_raw (PostgreSQL) via PySpark JDBC with
partitioned reads to avoid OOM on 31M+ rows. Partitioning is done on
transfer_date (cast to DATE in a subquery) with 16 parallel reads.

All type casting and NULL normalisation done in PySpark.
quarter_start and quarter_label derived here in Silver.
Idempotent upsert via staging table + INSERT ON CONFLICT.

Watermark: MAX(transfer_date) — date-level granularity.
Incremental filter: transfer_date > watermark.
Merge key: transaction_id (the {GUID} stripped of braces).
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

from utils.config import JDBC_URL, JDBC_PROPERTIES
from utils.spark_session import get_spark
from utils.db import get_conn, drop_staging_table
from utils.watermark import get_filter_date, set_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_COLUMNS = [
    "transaction_id", "transfer_date", "price_gbp", "postcode",
    "property_type", "old_new", "duration", "paon", "saon", "street",
    "locality", "town_city", "district", "county", "ppd_category",
    "record_status", "quarter_start", "quarter_label",
]

STAGING_TABLE = "silver._staging_land_registry"
TARGET_TABLE  = "silver.land_registry_clean"
BRONZE_TABLE  = "bronze.land_registry_raw"


def _get_date_bounds(spark: SparkSession) -> tuple[str, str]:
    """
    Query bronze for min/max transfer_date to set JDBC partition bounds.
    Returns (min_date, max_date) as strings like '1995-01-01'.
    """
    bounds_query = """
        (SELECT
            MIN(TO_DATE(transfer_date, 'YYYY-MM-DD HH24:MI'))::TEXT AS min_dt,
            MAX(TO_DATE(transfer_date, 'YYYY-MM-DD HH24:MI'))::TEXT AS max_dt
         FROM bronze.land_registry_raw
         WHERE transaction_id IS NOT NULL AND transfer_date IS NOT NULL
        ) AS bounds
    """
    bounds_df = spark.read.jdbc(
        url=JDBC_URL, table=bounds_query, properties=JDBC_PROPERTIES
    )
    row = bounds_df.collect()[0]
    logger.info("Date bounds: min=%s, max=%s", row["min_dt"], row["max_dt"])
    return row["min_dt"], row["max_dt"]


def transform(spark: SparkSession, filter_date: date | None) -> DataFrame:
    """
    Read bronze LR table with partitioned JDBC reads (16 partitions by date).
    Each partition reads ~2M rows — well within driver memory.
    PySpark does all transformations (type casting, NULL handling, quarter derivation).
    """
    logger.info("Reading Land Registry from bronze with partitioned JDBC reads")

    # Get date range for partition bounds
    min_dt, max_dt = _get_date_bounds(spark)
    # Convert to epoch days for numeric partitioning
    from datetime import datetime
    min_epoch = (datetime.strptime(min_dt, "%Y-%m-%d") - datetime(1970, 1, 1)).days
    max_epoch = (datetime.strptime(max_dt, "%Y-%m-%d") - datetime(1970, 1, 1)).days

    # Build subquery that adds a DATE column for partitioning
    # PostgreSQL casts transfer_date TEXT → DATE for the partition column
    where_clause = ""
    if filter_date is not None:
        where_clause = f"AND TO_DATE(transfer_date, 'YYYY-MM-DD HH24:MI') > '{filter_date}'"
        logger.info("Incremental filter: transfer_date > %s", filter_date)

    bronze_query = f"""
            (SELECT *,
                TO_DATE(transfer_date, 'YYYY-MM-DD HH24:MI') AS partition_date,
                (TO_DATE(transfer_date, 'YYYY-MM-DD HH24:MI') - DATE '1970-01-01') AS partition_days
             FROM bronze.land_registry_raw
             WHERE transaction_id IS NOT NULL
               AND transfer_date IS NOT NULL
               {where_clause}
            ) AS lr_bronze
        """

    df = (
        spark.read
        .option("fetchsize", "10000")
        .jdbc(
            url=JDBC_URL,
            table=bronze_query,
            properties=JDBC_PROPERTIES,
            column="partition_days",
            lowerBound=min_epoch,
            upperBound=max_epoch,
            numPartitions=16,
        )
    )

    df = (
        df
        # 1. Clean transaction_id: strip curly braces from {GUID}
        .withColumn(
            "transaction_id",
            F.regexp_replace(F.col("transaction_id"), r"[{}]", "")
        )

        # 2. Cast price to numeric using try_cast
        .withColumn(
            "price_gbp",
            F.expr("try_cast(price as decimal(14,2))")
        )

        # 3. Use the already-parsed partition_date as transfer_date
        .withColumn("transfer_date", F.col("partition_date").cast(DateType()))

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

        # 6. Derive quarter_start = first day of the quarter
        .withColumn(
            "quarter_start",
            F.date_trunc("quarter", F.col("transfer_date")).cast(DateType())
        )

        # 7. Derive quarter_label e.g. "2025Q4"
        .withColumn(
            "quarter_label",
            F.concat(
                F.year(F.col("quarter_start")).cast(StringType()),
                F.lit("Q"),
                F.quarter(F.col("quarter_start")).cast(StringType())
            )
        )

        # 8. Drop helper columns
        .drop("price", "partition_date")
    )

    return df.select(SILVER_COLUMNS)


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
        .option("batchsize", "50000")
        .jdbc(
            url=JDBC_URL,
            table=STAGING_TABLE,
            properties=JDBC_PROPERTIES,
        )
    )

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
    """Store MAX(transfer_date) as watermark — date-level granularity."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(transfer_date) FROM {TARGET_TABLE};")
            result = cur.fetchone()

    if result and result[0]:
        set_watermark("land_registry", result[0])


def run(mode: str = "full") -> None:
    logger.info("Starting Land Registry silver transform. mode=%s", mode)
    filter_date = get_filter_date("land_registry", mode)

    spark = get_spark("silver-land-registry")
    try:
        df = transform(spark, filter_date)
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