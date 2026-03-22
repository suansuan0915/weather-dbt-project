# Migration Checklist

## Phase 1: Planning and Setup
- [x] Freeze current local/Postgres version as `v1-local-postgres`
- [x] Define target GCS bucket and raw path structure
- [x] Define Databricks catalog and schema names
- [x] Create `.env.example`
- [x] Update root README with migration story

## Phase 2: Cloud Resource Setup
- [ ] Create GCS bucket
- [ ] Add lifecycle rule for raw-data retention
- [ ] Create Databricks SQL warehouse
- [ ] Record Databricks connection details
- [ ] Create catalog `weather_dev`
- [ ] Create schemas `bronze`, `silver`, `gold`

## Phase 3: Raw Ingestion Migration
- [ ] Add GCS-backed raw ingestion script
- [ ] Keep existing local ingestion script during migration
- [ ] Validate files are written to the expected GCS prefix

## Phase 4: Bronze Migration
- [ ] Build Databricks bronze ingestion job
- [ ] Create `weather_hourly` Delta table
- [ ] Create `weather_daily` Delta table
- [ ] Implement idempotent merge/upsert logic

## Phase 5: dbt Migration
- [ ] Switch from `dbt-postgres` to `dbt-databricks`
- [ ] Update profile configuration
- [ ] Point dbt sources to Databricks bronze tables
- [ ] Run `dbt seed`
- [ ] Run `dbt source freshness`
- [ ] Run `dbt run`
- [ ] Run `dbt test`

## Phase 6: Orchestration
- [ ] Add Databricks Workflow definition
- [ ] Decide whether Airflow remains as fallback
- [ ] Validate full end-to-end run

## Phase 7: Cleanup and Positioning
- [ ] Mark Postgres path as deprecated in docs
- [ ] Keep v1 accessible through tag/history
- [ ] Update architecture diagram
- [ ] Add resume-ready project bullets