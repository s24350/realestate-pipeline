"""
silver/silver_mlar.py
---------------------
Silver-layer transformation for the MLAR dataset.

Reads from bronze.mlar_raw (PostgreSQL) via PySpark JDBC.
Bronze data is already in long format (src, category, quarter, value)
thanks to the transposing mlar_parser.py. No unpivot/stack needed here.

Transformations:
  - Type casting: value TEXT → NUMERIC via try_cast
  - Monetary values (sheets 1.21, 1.33) multiplied by 1,000,000
  - Temporal columns derived from quarter label (e.g. "2007Q1" → quarter_start DATE)
  - Idempotent upsert on merge key: (src, category, quarter_start)

Watermark: MAX(quarter_start) — quarter-level granularity.
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

STAGING_TABLE = "silver._staging_mlar"
TARGET_TABLE  = "silver.mlar_long"
BRONZE_TABLE  = "bronze.mlar_raw"

# Sheets 1.21 and 1.33 contain monetary values in £m → multiply by 1e6
MONETARY_SOURCES = {"1.21", "1.33"}

SILVER_COLUMNS = [
    "src", "category", "quarter_start",
    "quarter_label", "quarter_num", "year", "value",
]


def transform(spark: SparkSession, filter_date: date | None) -> DataFrame:
    logger.info("Reading MLAR from bronze: %s", BRONZE_TABLE)

    df = spark.read.jdbc(url=JDBC_URL, table=BRONZE_TABLE, properties=JDBC_PROPERTIES)

    # Cast value using try_cast (returns NULL for '-', 'n/a', empty, etc.)
    df = df.withColumn(
        "value_numeric",
        F.expr("try_cast(value as double)")
    )

    # Multiply monetary values by 1,000,000 (source is £m)
    df = df.withColumn(
        "value",
        F.when(
            F.col("src").isin(list(MONETARY_SOURCES)),
            F.col("value_numeric") * F.lit(1_000_000.0)
        ).otherwise(F.col("value_numeric"))
    )

    # Derive temporal columns from quarter column (e.g. "2007Q1")
    df = (
        df
        .withColumnRenamed("quarter", "quarter_label")
        .withColumn("year",        F.col("quarter_label").substr(1, 4).cast("int"))
        .withColumn("quarter_num", F.col("quarter_label").substr(6, 1).cast("int"))
        .withColumn(
            "quarter_start",
            F.to_date(
                F.concat(
                    F.col("quarter_label").substr(1, 4),
                    F.lit("-"),
                    ((F.col("quarter_num") - 1) * 3 + 1).cast(StringType()),
                    F.lit("-01")
                ),
                "yyyy-M-dd"
            ).cast(DateType())
        )
    )

    # Drop rows with no category or no valid date
    df = (
        df
        .filter(F.col("category").isNotNull())
        .filter(F.col("quarter_start").isNotNull())
    )

    # Incremental filter
    if filter_date is not None:
        logger.info("Incremental filter: quarter_start > %s", filter_date)
        df = df.filter(F.col("quarter_start") > F.lit(filter_date))

    row_count = df.count()
    logger.info("MLAR rows to process: %d", row_count)
    return df.select(SILVER_COLUMNS)


def write_silver(df: DataFrame) -> None:
    row_count = df.count()
    if row_count == 0:
        logger.info("No new rows to write for MLAR silver.")
        return

    logger.info("Writing %d rows to staging %s", row_count, STAGING_TABLE)
    df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=JDBC_URL, table=STAGING_TABLE, properties=JDBC_PROPERTIES
    )

    upsert_sql = f"""
        INSERT INTO {TARGET_TABLE}
            (src, category, quarter_start,
             quarter_label, quarter_num, year, value, loaded_at)
        SELECT
            src, category, quarter_start,
            quarter_label, quarter_num, year, value, NOW()
        FROM {STAGING_TABLE}
        ON CONFLICT (src, category, quarter_start) DO UPDATE SET
            value         = EXCLUDED.value,
            quarter_label = EXCLUDED.quarter_label,
            quarter_num   = EXCLUDED.quarter_num,
            year          = EXCLUDED.year,
            loaded_at     = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
    logger.info("Upsert complete → %s", TARGET_TABLE)
    drop_staging_table(STAGING_TABLE)


def update_watermark_from_db() -> None:
    """Store MAX(quarter_start) as watermark — quarter-level granularity."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(quarter_start) FROM {TARGET_TABLE};")
            result = cur.fetchone()
    if result and result[0]:
        set_watermark("mlar", result[0])


def run(mode: str = "full") -> None:
    logger.info("Starting MLAR silver transform. mode=%s", mode)
    filter_date = get_filter_date("mlar", mode)
    spark = get_spark("silver-mlar")
    try:
        df = transform(spark, filter_date)
        write_silver(df)
        update_watermark_from_db()
    finally:
        spark.stop()
    logger.info("MLAR silver transform complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run(mode=args.mode)