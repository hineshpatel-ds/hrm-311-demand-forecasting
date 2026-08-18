import pandas as pd

from src.modeling.baseline_forecast import evaluate, seasonal_naive_forecast


def _weekly_pattern_series(n_weeks: int) -> pd.DataFrame:
    steps_per_week = 7 * 48
    idx = pd.date_range("2024-01-01", periods=n_weeks * steps_per_week, freq="30min", tz="UTC")
    pattern = [i % steps_per_week for i in range(len(idx))]
    return pd.DataFrame({"offered": pattern}, index=idx)


def test_seasonal_naive_is_exact_on_a_perfectly_repeating_series():
    df = _weekly_pattern_series(n_weeks=3)
    _, test, pred = seasonal_naive_forecast(df, horizon_steps=48, season_steps=7 * 48)

    assert len(test) == len(pred) == 48
    assert (pred.values == test.values).all()

    metrics = evaluate(test, pred)
    assert metrics.mae == 0.0


def test_seasonal_naive_falls_back_to_train_mean_without_enough_history():
    steps_per_week = 7 * 48
    df = _weekly_pattern_series(n_weeks=1)
    _, _, pred = seasonal_naive_forecast(df, horizon_steps=48, season_steps=steps_per_week)

    train_mean = df["offered"].iloc[:-48].mean()
    assert (pred.values == train_mean).all()
