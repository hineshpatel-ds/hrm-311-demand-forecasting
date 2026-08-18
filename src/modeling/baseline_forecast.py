import os
from dataclasses import dataclass

import mlflow
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error

from src.utils.db import load_mart_series


@dataclass
class Metrics:
    mae: float

def seasonal_naive_forecast(df: pd.DataFrame, horizon_steps: int = 48, season_steps: int = 7 * 48):
    """
    30-min data => 48 steps/day.
    season_steps default = 7 days.
    """
    y = df["offered"].astype(float)
    train = y.iloc[:-horizon_steps]
    test = y.iloc[-horizon_steps:]

    # Seasonal naive: prediction = value from 7 days ago at same time
    preds = []
    for i in range(len(test)):
        idx_in_train = len(train) - season_steps + i
        if idx_in_train >= 0:
            preds.append(train.iloc[idx_in_train])
        else:
            preds.append(train.mean())
    pred = pd.Series(preds, index=test.index)
    return train, test, pred

def evaluate(test, pred) -> Metrics:
    mae = mean_absolute_error(test, pred)
    return Metrics(mae=mae)

def main():
    load_dotenv()
    # 1) Load
    df = load_mart_series()

    # 2) Forecast next day (48 half-hours)
    horizon_steps = 48
    train, test, pred = seasonal_naive_forecast(df, horizon_steps=horizon_steps)

    # 3) Eval
    metrics = evaluate(test, pred)

    # 4) MLflow logging
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("hrm311_offered_forecasting")

    with mlflow.start_run(run_name="baseline_seasonal_naive"):
        mlflow.log_param("model_type", "seasonal_naive")
        mlflow.log_param("horizon_steps", horizon_steps)
        mlflow.log_param("season_steps", 7 * 48)
        mlflow.log_metric("mae", metrics.mae)

        # Save predictions artifact
        out = pd.DataFrame({"actual": test.values, "pred": pred.values}, index=test.index)
        out.to_csv("baseline_predictions.csv")
        mlflow.log_artifact("baseline_predictions.csv")

    print(f"✅ Baseline complete. MAE={metrics.mae:.3f}")
    print("Open MLflow UI to see the run: http://localhost:5000")

if __name__ == "__main__":
    main()
