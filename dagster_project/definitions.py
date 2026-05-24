"""
dagster_project/definitions.py
------------------------------
Dagster entry point. Lists everything the webserver and daemon need to know about.
The Dagster entry point — the equivalent of Airflow finding dags/ folder.
"""

from dagster import Definitions
from dagster_project.assets import silver_boe

defs = Definitions(
    assets=[silver_boe],
)