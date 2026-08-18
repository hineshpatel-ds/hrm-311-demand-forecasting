import pandas as pd

from src.features.build_features import STEPS_PER_WEEK
from src.modeling.xgboost_forecast import forecast_recursive


class ConstantModel:
    """Stand-in for a trained model: always predicts a fixed value, so we can
    test the recursive-forecast plumbing without training a real XGBoost model.
    """

    def __init__(self, value: float):
        self.value = value

    def predict(self, X):
        return [self.value] * len(X)


def _history(n_steps: int) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n_steps, freq="30min", tz="UTC")
    return pd.DataFrame({"offered": range(n_steps)}, index=idx)


def test_forecast_recursive_returns_expected_horizon_and_timestamps():
    history = _history(STEPS_PER_WEEK + 10)
    model = ConstantModel(value=42.0)

    preds = forecast_recursive(model, history, horizon_steps=5)

    assert len(preds) == 5
    assert (preds.values == 42.0).all()

    expected_start = history.index[-1] + pd.Timedelta(minutes=30)
    assert preds.index[0] == expected_start
    assert list(preds.index.to_series().diff().dropna().unique()) == [pd.Timedelta(minutes=30)]


def test_forecast_recursive_feeds_predictions_back_as_history():
    # A model that just echoes lag_1 should propagate the very first
    # prediction forward unchanged, proving predictions get fed back in.
    class EchoLag1:
        def predict(self, X):
            return X["lag_1"].to_numpy()

    history = _history(STEPS_PER_WEEK + 10)
    preds = forecast_recursive(EchoLag1(), history, horizon_steps=3)

    assert preds.iloc[0] == history["offered"].iloc[-1]
    assert (preds.values == preds.iloc[0]).all()
