# V2 Architecture

## Goal

Migrate the project from a local-storage + Postgres architecture to a cloud-native lakehouse architecture using GCS, Databricks, Delta Lake, and dbt.

## Why

The current version works well for learning, but local file storage does not scale well for large historical ingestion. The new version should:
- not store raw data in local
- preserve raw data for replay and debugging
- use Databricks bronze/silver/gold layers
- keep dbt as the transformation layer

## Target Stack

- Source API: Open-Meteo Historical API
- Raw storage: Google Cloud Storage (GCS)
- Bronze storage: Databricks Delta tables
- Transformations: dbt
- Orchestration: Databricks Workflows
- Legacy orchestration during migration: Airflow
- Governance/storage pattern: Unity Catalog + external cloud storage

## Target Data Flow

1. Read active locations from `seeds/dim_locations.csv`
2. Fetch historical weather data from Open-Meteo
3. Write raw JSON/JSONL payloads to GCS
4. Ingest raw files into Databricks bronze Delta tables
5. Run dbt staging, intermediate, and marts on Databricks
6. Run freshness and quality tests


## Medallion Mapping

- Raw: immutable API payloads in GCS
- Bronze: source-aligned Delta tables in Databricks
- Silver: cleaned and enriched dbt models
- Gold: marts and quality models for analytics and reporting

## Proposed Naming

### Cloud Storage
- Bucket: `weather-historical-raw`
- Raw prefix: `weather/raw/open_meteo`

### Databricks
- Catalog: `weather_dev`
- Bronze schema: `bronze`
- Silver schema: `silver`
- Gold schema: `gold`

## Key Design Decisions

### Keep raw files
Reason:
- supports replay, debugging, and backfills

### Use GCS instead of local storage
Reason:
- object storage is a better long-term home for growing raw datasets

### Use Databricks Delta for bronze
Reason:
- Delta tables are a more production-like replacement for local Parquet + Postgres bronze loading

### Keep dbt for transformation logic
Reason:
- dbt is still the clearest way to demonstrate analytics engineering skills in this project

### Keep Airflow only during migration
Reason:
- the final architecture should have one clear primary orchestrator

## Open Questions

- Will the final orchestrator be Databricks Workflows only, or will Airflow stay for portfolio reasons?
- Will local fallback mode remain supported after migration?
- What raw-data retention window should be used in GCS for development?

## Out of Scope for Phase 1

- streaming ingestion
- Terraform or full IaC
- production IAM hardening
- multi-environment deployment