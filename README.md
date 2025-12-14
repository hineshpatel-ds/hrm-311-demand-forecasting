# HRM 311 Demand & Staffing Forecasting

A production-style data science project that forecasts Halifax Regional Municipality (HRM) 311 service demand to help operations teams plan staffing and reduce wait times.

## Business Problem
311 call volume is highly seasonal and affected by time-of-day, day-of-week, weather, and events. Under-staffing increases abandoned calls and customer frustration; over-staffing increases cost. This project builds an end-to-end forecasting system to predict demand surges and explain drivers.

## What This Repo Demonstrates (Industry Standard)
- Automated ingestion of open civic datasets
- Data warehousing in PostgreSQL (raw → cleaned → marts)
- Data quality checks (schema, nulls, freshness)
- Forecasting models + baseline comparisons
- Experiment tracking with MLflow
- Model served via FastAPI and visualized via Streamlit
- CI-ready structure and reproducible local environment via Docker

## Local Stack (Free)
- PostgreSQL (warehouse)
- pgAdmin (DB UI)
- MLflow (experiment tracking)

## Quickstart

### 1 Start services
```bash
docker compose up -d
