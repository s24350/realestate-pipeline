"""
silver/silver_boe.py
--------------------
Silver-layer transformation for the Bank of England
Lending to Individuals (monthly) dataset.

Changes vs. v1:
  - BoE series codes (LPMB3VA etc.) mapped to friendly column names
  - Type casting via try_cast (no regex)
  - quarter_start and quarter_label derived here in Silver
  - Idempotent upsert on merge key: month_start
  - Incremental mode: filter to month_start > filter_date
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

from utils.config import BOE_PATH, BOE_FILENAME, JDBC_URL, JDBC_PROPERTIES
from utils.spark_session import get_spark
from utils.db import get_conn, drop_staging_table
from utils.watermark import get_filter_date, set_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_TABLE = "silver._staging_boe"
TARGET_TABLE  = "silver.boe_monthly_clean"

# ── BoE series code → friendly column name mapping ───────────────────────────
# The CSV column headers end with series codes like LPMB3VA.
# We extract the code from the header and map to our silver column names.
BOE_CODE_TO_NAME = {
    "LPMB3VA": "mfi_house_purchase",
    "LPMB3SI": "mfi_remortgage",
    "LPMB3TI": "mfi_other_lending",
    "LPMZ3UP": "mfi_total_approvals",
    "LPMVYVA": "other_spec_house_purchase",
    "LPMB23A": "other_spec_remortgage",
    "LPMB26A": "other_spec_other_lending",
    "LPMZ3UR": "other_spec_total_approvals",
    "LPMVTVX": "total_house_purchase",
    "LPMB4B3": "total_remortgage",
    "LPMB4B4": "total_other_lending",
    "LPMB3C8": "total_secured_lending",
}

NUMERIC_COLS = list(BOE_CODE_TO_NAME.values())

SILVER_COLUMNS = [
    "month_start", "year", "month",
    "quarter_start", "quarter_label",
] + NUMERIC_COLS


def _rename_boe_columns(df: DataFrame) -> DataFrame:
    """
    Rename BoE CSV columns from long descriptions to friendly names.
    The series code (e.g. LPMB3VA) appears at the end of each column header.
    We find it by matching against our known codes.
    """
    for original_col in df.columns:
        for code, friendly_name in BOE_CODE_TO_NAME.items():
            if code in original_col:
                df = df.withColumnRenamed(original_col, friendly_name)
                break
    return df


def transform(spark: SparkSession, filter_date: date | None) -> DataFrame:
    src = f"{BOE_PATH}/{BOE_FILENAME}"
    logger.info("Reading BoE source: %s", src)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(src)
    )

    # Rename columns from long BoE descriptions to friendly names
    df = _rename_boe_columns(df)

    # The BoE export has a 'Date' column in format '31 Jan 26' or 'Jan 1993'
    # Normalise to first-of-month date
    date_col = df.columns[0]  # first column is always the date
    df = df.withColumnRenamed(date_col, "date_raw")
    # All BoE dates are "dd MMM yy" (2-digit year).
    # Spark's default century pivot misinterprets "26" as 2099.
    # Prepend "20" for years 00-30, "19" for years 31-99.
    df = df.withColumn(
        "year_2d",
        F.regexp_extract(F.col("date_raw"), r"(\d{2})$", 1)
    )
    df = df.withColumn(
        "date_raw_4d",
        F.concat(
            F.regexp_extract(F.col("date_raw"), r"^(\d{1,2}\s\w{3}\s)", 1),
            F.when(F.col("year_2d").cast("int") <= 90, F.lit("20"))
            .otherwise(F.lit("19")),
            F.col("year_2d"),
        )
    )
    df = df.withColumn(
        "month_start",
        F.to_date(F.col("date_raw_4d"), "dd MMM yyyy").cast(DateType())
    )

    # Cast all numeric columns using try_cast (returns NULL for non-numeric)
    for col_name in NUMERIC_COLS:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` as double)")
            )
        else:
            logger.warning("Column %s not found in BoE data — filling with NULL", col_name)
            df = df.withColumn(col_name, F.lit(None).cast("double"))

    df = (
        df
        .filter(F.col("month_start").isNotNull())
        .withColumn("year",  F.year(F.col("month_start")))
        .withColumn("month", F.month(F.col("month_start")))
        .withColumn(
            "quarter_start",
            F.date_trunc("quarter", F.col("month_start")).cast(DateType())
        )
        .withColumn(
            "quarter_label",
            F.concat(
                F.year(F.col("quarter_start")).cast(StringType()),
                F.lit("Q"),
                F.quarter(F.col("quarter_start")).cast(StringType())
            )
        )
    )

    if filter_date is not None:
        logger.info("Incremental filter: month_start > %s", filter_date)
        df = df.filter(F.col("month_start") > F.lit(filter_date))

    return df.select(SILVER_COLUMNS)


def write_silver(df: DataFrame) -> None:
    row_count = df.count()
    if row_count == 0:
        logger.info("No new rows to write for BoE silver.")
        return

    logger.info("Writing %d rows to staging %s", row_count, STAGING_TABLE)

    df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=JDBC_URL, table=STAGING_TABLE, properties=JDBC_PROPERTIES
    )

    update_cols = [c for c in SILVER_COLUMNS if c != "month_start"]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    upsert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(SILVER_COLUMNS)}, loaded_at)
        SELECT {', '.join(SILVER_COLUMNS)}, NOW()
        FROM {STAGING_TABLE}
        ON CONFLICT (month_start) DO UPDATE SET
            {update_clause},
            loaded_at = NOW();
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
        set_watermark("boe", result[0])


def run(mode: str = "full") -> None:
    logger.info("Starting BoE silver transform. mode=%s", mode)
    filter_date = get_filter_date("boe", mode)
    spark = get_spark("silver-boe")
    try:
        df = transform(spark, filter_date)
        write_silver(df)
        update_watermark_from_db()
    finally:
        spark.stop()
    logger.info("BoE silver transform complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run(mode=args.mode)
