import pandas as pd

from src.features.build_features import (
    FEATURE_COLUMNS,
    STEPS_PER_DAY,
    STEPS_PER_WEEK,
    build_features,
    next_bucket_row,
)


def _synthetic_series(n_steps: int) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n_steps, freq="30min", tz="UTC")
    return pd.DataFrame({"offered": range(n_steps)}, index=idx)


def test_build_features_has_all_columns():
    df = _synthetic_series(STEPS_PER_WEEK + 10)
    featured = build_features(df)
    for col in FEATURE_COLUMNS:
        assert col in featured.columns


def test_lag_features_reference_correct_offsets():
    df = _synthetic_series(STEPS_PER_WEEK + 10)
    featured = build_features(df)

    row = featured.iloc[-1]
    assert row["lag_1"] == df["offered"].iloc[-2]
    assert row["lag_1_day"] == df["offered"].iloc[-1 - STEPS_PER_DAY]
    assert row["lag_1_week"] == df["offered"].iloc[-1 - STEPS_PER_WEEK]


def test_early_rows_have_nan_lags():
    df = _synthetic_series(5)
    featured = build_features(df)
    assert featured["lag_1_week"].isna().all()


def test_next_bucket_row_matches_build_features():
    df = _synthetic_series(STEPS_PER_WEEK + 10)
    next_ts = df.index[-1] + pd.Timedelta(minutes=30)

    row = next_bucket_row(df, next_ts)

    assert list(row.columns) == FEATURE_COLUMNS
    assert row.index[0] == next_ts
    # The new row's lag_1 should be the last known observed value.
    assert row["lag_1"].iloc[0] == df["offered"].iloc[-1]
