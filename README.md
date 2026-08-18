# HRM 311 Demand & Staffing Forecasting

A production-style data science project that forecasts Halifax Regional Municipality (HRM) 311 service demand to help operations teams plan staffing and reduce wait times.

## Business Problem
311 call volume is highly seasonal and affected by time-of-day and day-of-week. Under-staffing increases abandoned calls and customer frustration; over-staffing increases cost. This project builds an end-to-end forecasting system: ingest → warehouse → validate → model → serve → visualize.

## Architecture
```
open data (ArcGIS Hub)
      │  Prefect flow
      ▼
Postgres: raw.raw_311_call_volumes
      │  dbt
      ▼
Postgres: staging.stg_311_call_volumes  →  mart.mart_311_call_volume_30m
      │  Great Expectations                    │
      ▼                                         ▼
data quality report                 baseline (seasonal-naive) + XGBoost
                                     models, tracked & registered in MLflow
                                                 │
                                                 ▼
                                   FastAPI /forecast  →  Streamlit dashboard
```

## What's Implemented
- **Ingestion**: Prefect flow downloads HRM 311 call volume data and loads it into Postgres (`src/ingestion/`)
- **Warehouse**: dbt models for raw → staging → mart, producing a clean 30-minute-grain forecasting-ready series (`dbt/models/`)
- **Data quality**: Great Expectations suite validating nulls, uniqueness, and value ranges on the mart (`gx/`, `src/quality/`)
- **Models**: a seasonal-naive baseline and an XGBoost model with lag/time-of-day features, both tracked in MLflow with a common MAE comparison (`src/modeling/`)
- **Serving**: a FastAPI `/forecast` endpoint that loads the latest registered model from the MLflow registry and forecasts forward recursively (`src/serving/`)
- **Dashboard**: a Streamlit app plotting recent actuals against the live forecast (`dashboard/`)
- **Tests + CI**: pytest unit tests for the feature engineering and forecasting logic, run on every push via GitHub Actions (`tests/`, `.github/workflows/ci.yml`)

## Known Limitations
- No weather or civic-event features yet — the models only use lag and calendar (hour/day-of-week) features
- Model training/registration is manual (`python -m src.modeling.xgboost_forecast`), not scheduled
- Great Expectations validation isn't wired into the dbt run as an automated gate yet

## Local Stack (Free)
- PostgreSQL (warehouse)
- pgAdmin (DB UI)
- MLflow (experiment tracking + model registry)

## Quickstart

### 1. Environment
```bash
cp .env.example .env
pip install -r requirements.txt
```

### 2. Start services
```bash
docker compose up -d
```
This starts Postgres (`localhost:5432`), pgAdmin (`localhost:5050`), and MLflow (`localhost:5000`).

### 3. Bootstrap the warehouse schemas
```bash
python scripts/bootstrap_db.py
```

### 4. Ingest raw data
```bash
python -m src.ingestion.flows
```

### 5. Build the warehouse (dbt)
```bash
cp dbt/profiles.yml.example dbt/profiles.yml   # first time only
cd dbt && dbt run && cd ..
```

### 6. Data quality checks (Great Expectations)
```bash
python setup_gx.py
python -m src.quality.ge_init_datasource
python -m src.quality.ge_suite_mart_311
python -m src.quality.run_validation
```

### 7. Train models
```bash
python -m src.modeling.baseline_forecast     # seasonal-naive, logs to MLflow
python -m src.modeling.xgboost_forecast       # XGBoost, logs + registers to MLflow
```
Compare runs at http://localhost:5000.

### 8. Serve forecasts
```bash
uvicorn src.serving.app:app --reload
```
`GET http://localhost:8000/forecast?horizon_steps=48` returns the next 24 hours of 30-minute forecasts.

### 9. View the dashboard
```bash
streamlit run dashboard/app.py
```

### 10. Run tests
```bash
pytest tests/
```

## Repo Layout
```
src/
  ingestion/   Prefect flow + downloader for the open dataset
  utils/       DB engine + shared mart-loading helper
  features/    Feature engineering shared by training and serving
  modeling/    Baseline + XGBoost training scripts
  serving/     FastAPI app
  quality/     Great Expectations setup/validation scripts
dashboard/     Streamlit app
dbt/           raw → staging → mart models
gx/            Great Expectations project config + expectation suites
tests/         pytest unit tests (no DB required)
```
