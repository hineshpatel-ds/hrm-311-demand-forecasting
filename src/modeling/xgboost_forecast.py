import os
from dataclasses import dataclass

import mlflow
import mlflow.sklearn
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

from src.features.build_features import FEATURE_COLUMNS, build_features, next_bucket_row
from src.modeling.baseline_forecast import seasonal_naive_forecast
from src.utils.db import load_mart_series

REGISTERED_MODEL_NAME = "hrm311_xgboost_forecast"


@dataclass
class Metrics:
    mae: float


def train_test_split(df: pd.DataFrame, horizon_steps: int = 48):
    featured = build_features(df).dropna(subset=FEATURE_COLUMNS + ["offered"])
    train = featured.iloc[:-horizon_steps]
    test = featured.iloc[-horizon_steps:]
    return train, test


def fit(train: pd.DataFrame) -> XGBRegressor:
    model = XGBRegressor(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
    )
    model.fit(train[FEATURE_COLUMNS], train["offered"])
    return model


def evaluate(model: XGBRegressor, test: pd.DataFrame) -> Metrics:
    pred = model.predict(test[FEATURE_COLUMNS])
    return Metrics(mae=mean_absolute_error(test["offered"], pred))


def forecast_recursive(
    model: XGBRegressor, history: pd.DataFrame, horizon_steps: int, step: pd.Timedelta = pd.Timedelta(minutes=30)
) -> pd.Series:
    """Forecast forward from the end of `history` (real future, not a backtest):
    each predicted value is fed back in as the lag input for the next step,
    since real lag/rolling features aren't available yet that far out.
    """
    history = history[["offered"]].copy()

    preds = []
    timestamps = []
    cursor = history.index[-1]
    for _ in range(horizon_steps):
        cursor = cursor + step
        row = next_bucket_row(history, cursor)
        pred = float(model.predict(row)[0])
        history.loc[cursor] = pred
        preds.append(pred)
        timestamps.append(cursor)

    return pd.Series(preds, index=pd.DatetimeIndex(timestamps))


def main():
    load_dotenv()
    horizon_steps = 48

    df = load_mart_series()
    train, test = train_test_split(df, horizon_steps=horizon_steps)

    model = fit(train)
    xgb_metrics = evaluate(model, test)

    # Same held-out window, scored with the seasonal-naive baseline, so the
    # comparison is apples-to-apples.
    _, naive_test, naive_pred = seasonal_naive_forecast(df, horizon_steps=horizon_steps)
    naive_mae = mean_absolute_error(naive_test, naive_pred)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("hrm311_offered_forecasting")

    with mlflow.start_run(run_name="xgboost_forecast"):
        mlflow.log_param("model_type", "xgboost")
        mlflow.log_param("horizon_steps", horizon_steps)
        mlflow.log_param("features", FEATURE_COLUMNS)
        mlflow.log_metric("mae", xgb_metrics.mae)
        mlflow.log_metric("baseline_seasonal_naive_mae", naive_mae)
        mlflow.log_metric("mae_improvement_pct", 100 * (naive_mae - xgb_metrics.mae) / naive_mae)

        mlflow.sklearn.log_model(
            model, "model", registered_model_name=REGISTERED_MODEL_NAME
        )

        out = pd.DataFrame(
            {"actual": test["offered"].values, "pred": model.predict(test[FEATURE_COLUMNS])},
            index=test.index,
        )
        out.to_csv("xgboost_predictions.csv")
        mlflow.log_artifact("xgboost_predictions.csv")

    print(f"XGBoost MAE={xgb_metrics.mae:.3f} vs seasonal-naive baseline MAE={naive_mae:.3f}")


if __name__ == "__main__":
    main()
