import os

import mlflow
import mlflow.sklearn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from mlflow.tracking import MlflowClient
from pydantic import BaseModel

from src.modeling.xgboost_forecast import REGISTERED_MODEL_NAME, forecast_recursive
from src.utils.db import load_mart_series

load_dotenv()
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))

_model = None


def get_model():
    """Lazily load the latest registered model version on first request,
    rather than at import time, so the API can start before a model exists.
    """
    global _model
    if _model is None:
        client = MlflowClient()
        versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
        if not versions:
            raise RuntimeError(
                f"No registered versions of '{REGISTERED_MODEL_NAME}' found in MLflow. "
                "Run `python -m src.modeling.xgboost_forecast` to train and register one."
            )
        latest_version = max(int(v.version) for v in versions)
        _model = mlflow.sklearn.load_model(f"models:/{REGISTERED_MODEL_NAME}/{latest_version}")
    return _model


app = FastAPI(title="HRM 311 Demand Forecast API")


class ForecastPoint(BaseModel):
    bucket_ts: str
    predicted_offered: float


class ForecastResponse(BaseModel):
    horizon_steps: int
    points: list[ForecastPoint]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/forecast", response_model=ForecastResponse)
def forecast(horizon_steps: int = 48):
    if horizon_steps < 1 or horizon_steps > 336:
        raise HTTPException(400, "horizon_steps must be between 1 and 336 (one week)")

    try:
        model = get_model()
    except RuntimeError as e:
        raise HTTPException(503, str(e))

    history = load_mart_series()
    preds = forecast_recursive(model, history, horizon_steps=horizon_steps)

    return ForecastResponse(
        horizon_steps=horizon_steps,
        points=[
            ForecastPoint(bucket_ts=ts.isoformat(), predicted_offered=round(float(val), 2))
            for ts, val in preds.items()
        ],
    )
