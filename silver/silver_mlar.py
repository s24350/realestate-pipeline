"""
silver/silver_mlar.py
---------------------
Silver-layer transformation for the MLAR dataset (all three sheets:
1.21, 1.32, 1.33), which arrive as CSV files after mlar_parser.py
has converted them from the original Excel.

The CSVs are in wide format: one column per quarter (e.g. "2007Q1").
This script unpivots them into the long format used in Silver:
    (src, category, quarter_start, value)

Changes vs. v1:
  - Unpivot / melt done in PySpark (not SQL UNNEST)
  - Type casting via try_cast (no regex)
  - quarter_start and quarter_label derived in Silver
  - Idempotent upsert on merge key: (src, category, quarter_start)
  - Incremental mode: filter to quarter_start > filter_date
  - MLAR monetary values multiplied by 1,000,000 (source is £m)
    — moved here from Gold so Silver already holds full GBP values
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

from utils.config import MLAR_PATH, JDBC_URL, JDBC_PROPERTIES
from utils.spark_session import get_spark
from utils.db import get_conn, drop_staging_table
from utils.watermark import get_filter_date, set_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_TABLE = "silver._staging_mlar"
TARGET_TABLE  = "silver.mlar_long"

# Sheets and their source CSV filenames
MLAR_SOURCES = {
    "1.21": "mlar_1_21.csv",
    "1.32": "mlar_1_32.csv",
    "1.33": "mlar_1_33.csv",
}

# Sheets 1.21 and 1.33 contain monetary values in £m → multiply by 1e6
MONETARY_SOURCES = {"1.21", "1.33"}

SILVER_COLUMNS = [
    "src", "category", "quarter_start",
    "quarter_label", "quarter_num", "year", "value",
]


def _is_quarter_column(col_name: str) -> bool:
    """Check if a column name matches the pattern ####Q# (e.g. 2007Q1)."""
    return (
        len(col_name) == 6
        and col_name[:4].isdigit()
        and col_name[4] == "Q"
        and col_name[5] in "1234"
    )


def _transform_one_source(
    spark: SparkSession,
    src_name: str,
    filename: str,
    filter_date: date | None,
) -> DataFrame:
    path = f"{MLAR_PATH}/{filename}"
    logger.info("Reading MLAR source %s from: %s", src_name, path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
    )

    # Identify which columns are quarter columns vs. the category column
    all_cols = df.columns
    quarter_cols = [c for c in all_cols if _is_quarter_column(c)]
    category_col = all_cols[0]  # first column is always the category label

    if not quarter_cols:
        logger.warning("No quarter columns found in %s — skipping.", filename)
        return spark.createDataFrame([], schema=None)

    # Unpivot (melt): stack all quarter columns into (quarter_label, value_raw)
    stack_expr = f"stack({len(quarter_cols)}, " + \
        ", ".join(f"'{q}', `{q}`" for q in quarter_cols) + \
        ") as (quarter_label, value_raw)"

    df_long = df.select(
        F.col(category_col).alias("category"),
        F.expr(stack_expr),
    )

    # Cast value using try_cast (returns NULL for '-', 'n/a', empty, etc.)
    df_long = df_long.withColumn(
        "value_numeric",
        F.expr("try_cast(value_raw as double)")
    )

    # Multiply monetary values by 1,000,000 (source is £m)
    if src_name in MONETARY_SOURCES:
        df_long = df_long.withColumn(
            "value",
            F.col("value_numeric") * F.lit(1_000_000.0)
        )
    else:
        df_long = df_long.withColumn("value", F.col("value_numeric"))

    # Derive temporal columns from quarter_label
    df_long = (
        df_long
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
        .withColumn("src", F.lit(src_name))
    )

    # Drop rows with no category or no valid date
    df_long = (
        df_long
        .filter(F.col("category").isNotNull())
        .filter(F.col("quarter_start").isNotNull())
    )

    # Incremental filter
    if filter_date is not None:
        df_long = df_long.filter(F.col("quarter_start") > F.lit(filter_date))

    return df_long.select(SILVER_COLUMNS)


def transform(spark: SparkSession, filter_date: date | None) -> DataFrame:
    """Combine all three MLAR sources into one long DataFrame."""
    frames = []
    for src_name, filename in MLAR_SOURCES.items():
        df = _transform_one_source(spark, src_name, filename, filter_date)
        frames.append(df)

    from functools import reduce
    from pyspark.sql import DataFrame as DF
    combined = reduce(DF.union, frames)
    logger.info("MLAR combined row count (before write): %d", combined.count())
    return combined


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
