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
KAFKA_GROUP_ID: str = "realestate-pipeline"

# Source-specific topics
KAFKA_TOPIC_LAND_REGISTRY: str = "land-registry-data"
KAFKA_TOPIC_BOE: str = "boe-data"
KAFKA_TOPIC_MLAR: str = "mlar-data"

# Legacy topic (kept for backward compatibility / audit log)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "file-events")

# ── Data paths ────────────────────────────────────────────────────────────────
_BASE_DATA: str = os.environ.get("BASE_DATA_PATH", "data")

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
LAND_REGISTRY_FILENAME: str      = "pp-complete.csv"
LAND_REGISTRY_MONTHLY_FILENAME: str = "pp-monthly-update-new-version.csv"
BOE_FILENAME: str                = "Bank of England  Database.csv"
MLAR_XLSX_FILENAME: str          = "mlar-longrun-detailed.XLSX"
MLAR_LONG_RAW_FILENAME: str      = "mlar_long_raw.csv"

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME: str = "realestate-pipeline"
SPARK_MASTER: str   = "local[*]"