"""
dagster_project/definitions.py
------------------------------
Dagster entry point. Lists everything the webserver and daemon need to know about.
The Dagster entry point — the equivalent of Airflow finding dags/ folder.
"""

import dagster as dg
from dagster_project.assets import (
    publish_to_kafka,
    load_bronze,
    init_schemas,
    silver_boe,
    silver_mlar,
    silver_land_registry,
    gold_aggregations,
)

defs = dg.Definitions(
    assets=[
        publish_to_kafka,
        load_bronze,
        init_schemas,
        silver_boe,
        silver_mlar,
        silver_land_registry,
        gold_aggregations,
    ],
)