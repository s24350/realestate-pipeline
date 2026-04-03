-- Creates airflow metadata database and pipeline users/databases
-- This runs automatically on first postgres container startup

-- Airflow metadata DB user
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Pipeline data user
CREATE USER pipeline WITH PASSWORD 'pipeline';
CREATE DATABASE pipeline_db OWNER pipeline;

-- Connect to pipeline_db and create schemas
\connect pipeline_db

CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION pipeline;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION pipeline;
CREATE SCHEMA IF NOT EXISTS meta   AUTHORIZATION pipeline;

GRANT ALL ON SCHEMA silver TO pipeline;
GRANT ALL ON SCHEMA gold   TO pipeline;
GRANT ALL ON SCHEMA meta   TO pipeline;
