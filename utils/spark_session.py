"""
spark_session.py
----------------
Factory for a shared SparkSession.
Import get_spark() from any PySpark script — it returns the same session
if one already exists (SparkSession is a singleton per JVM process).
"""

from pyspark.sql import SparkSession
from utils.config import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    JDBC_JAR_PATH,
)


def get_spark(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """
    Return a SparkSession configured for this pipeline.

    Parameters
    ----------
    app_name : str
        Shown in the Spark UI. Defaults to the global app name from config.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # Postgres JDBC driver jar
        .config("spark.jars", JDBC_JAR_PATH)
        # Keep Spark logs quieter — only WARN and above
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
