"""
dagster_project/definitions.py
------------------------------
The Dagster entry point — the equivalent of Airflow finding dags/ folder.

Airflow autodiscovers any DAG object in the dags/ folder. Dagster is explicit
instead: you hand it a single `Definitions` object listing assets, schedules,
resources, etc. The webserver loads this module (see `-m dagster_project.definitions`
in docker-compose.dagster.yml).

STEP 1: this is intentionally almost empty — zero assets. Success criterion is
that the code location loads GREEN in the UI with no errors. We add the first
real asset (silver_boe) in Step 2, in dagster_project/assets.py.
"""

from dagster import Definitions

# No assets yet. An empty Definitions is valid and loads cleanly — it proves the
# image, mounts, imports, and webserver wiring all work before we add logic.
defs = Definitions(
    assets=[],
)