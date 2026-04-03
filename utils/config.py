"""
config.py
---------
Central configuration for the pipeline.
All values are read from environment variables so the same code runs
both inside Docker (env set in docker-compose) and locally (set in .env or shell).
"""

import os

# ── Database ──────────────────────────────────────────────────────────────────
DB_URL: str = os.environ.get(
    "PIPELINE_DB_URL",
    "postgresql+psycopg2://pipeline:pipeline@localhost:5432/pipeline_db",
)
DB_HOST: str = os.environ.get("PIPELINE_DB_HOST", "localhost")
DB_PORT: int = int(os.environ.get("PIPELINE_DB_PORT", "5432"))
DB_NAME: str = os.environ.get("PIPELINE_DB_NAME", "pipeline_db")
DB_USER: str = os.environ.get("PIPELINE_DB_USER", "pipeline")
DB_PASSWORD: str = os.environ.get("PIPELINE_DB_PASSWORD", "pipeline")

# JDBC URL used by PySpark to write to PostgreSQL
JDBC_URL: str = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
JDBC_DRIVER_CLASS: str = "org.postgresql.Driver"
# Path to the Postgres JDBC driver jar (mounted into the Airflow container)
JDBC_JAR_PATH: str = os.environ.get(
    "JDBC_JAR_PATH",
    "/opt/airflow/jars/postgresql-42.7.3.jar",
)

JDBC_PROPERTIES: dict = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": JDBC_DRIVER_CLASS,
}

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "file-events")
KAFKA_GROUP_ID: str = "realestate-pipeline"

# ── Data paths ────────────────────────────────────────────────────────────────
# Base data dir — inside container it's /opt/airflow/data, locally ./data
_BASE_DATA: str = os.environ.get("BASE_DATA_PATH", "data")

# Each source has its own subdirectory under data/
LAND_REGISTRY_PATH: str = os.environ.get(
    "LAND_REGISTRY_PATH", f"{_BASE_DATA}/land_registry"
)
BOE_PATH: str = os.environ.get(
    "BOE_PATH", f"{_BASE_DATA}/boe"
)
MLAR_PATH: str = os.environ.get(
    "MLAR_PATH", f"{_BASE_DATA}/mlar"
)

# ── Source file names ─────────────────────────────────────────────────────────
# Full history file (used in full mode)
LAND_REGISTRY_FILENAME: str      = "pp-complete.csv"
# Monthly update file (used in incremental mode for Land Registry)
LAND_REGISTRY_MONTHLY_FILENAME: str = "pp-monthly-update-new-version.csv"
BOE_FILENAME: str                = "Bank of England  Database.csv"
MLAR_XLSX_FILENAME: str          = "mlar-longrun-detailed.XLSX"
MLAR_1_21_FILENAME: str          = "mlar_1_21.csv"
MLAR_1_32_FILENAME: str          = "mlar_1_32.csv"
MLAR_1_33_FILENAME: str          = "mlar_1_33.csv"

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME: str = "realestate-pipeline"
SPARK_MASTER: str   = "local[*]"
