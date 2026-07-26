# NYC Taxi Lakehouse Pipeline

## Overview
A production-style batch ELT pipeline implementing Medallion Architecture
(Bronze/Silver/Gold) on NYC TLC trip data using PySpark, Delta Lake, and
Databricks Workflows, with a Power BI dashboard on top.

## Architecture
[architecture_diagram.png]
Source → Landing → Bronze → Silver → Gold → Warehouse → Visualization → Monitoring → Alerts

## Tech Stack
PySpark · Delta Lake · Databricks Workflows · Python · SQL · Power BI

## Data Model
[star_schema.png]
fact_trips ← dim_date, dim_location, dim_vendor

## Key Engineering Decisions
- Bronze is append-only for full auditability and reprocessing capability
- Silver writes use MERGE for idempotency (safe to rerun)
- Gold fact table partitioned by pickup_date to match query patterns
- Custom DQ framework runs post-Silver and blocks downstream writes on failure
(full reasoning in docs/decisions.md)

## Results
- ~2.9 million rows processed across Bronze → Gold
- 3% of raw rows failed validation and were excluded (documented, not silently dropped)
- 1.1-second average pipeline runtime end-to-end
[powerbi_dashboard.png]

## How to Run
1. Import notebooks into Databricks Community Edition
2. Update configs/pipeline_config.yaml with your workspace paths
3. Run notebooks 01→04 in order (or trigger the Databricks Workflow)
4. Run `pytest tests/ -v` to validate transformation logic

## Future Improvements
See docs/decisions.md — Airflow orchestration, Snowflake Gold target, CI/CD via GitHub Actions
