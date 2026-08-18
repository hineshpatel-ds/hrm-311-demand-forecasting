"""Feature engineering shared by model training and serving.

Kept separate from the modeling code so the exact same feature logic used to
train the model is also used to score it — the classic cause of train/serve
skew is these two paths quietly drifting apart.
"""

import pandas as pd

STEPS_PER_DAY = 48  # 30-min buckets
STEPS_PER_WEEK = 7 * STEPS_PER_DAY

FEATURE_COLUMNS = [
    "hour",
    "day_of_week",
    "is_weekend",
    "lag_1",
    "lag_1_day",
    "lag_1_week",
    "rolling_mean_1_day",
]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Given a DataFrame indexed by bucket_ts with an 'offered' column,
    return it with model features added. Rows without enough history for a
    given lag/rolling feature come back with NaNs in that column.
    """
    out = df.copy()
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["is_weekend"] = out["day_of_week"].isin([5, 6]).astype(int)

    out["lag_1"] = out["offered"].shift(1)
    out["lag_1_day"] = out["offered"].shift(STEPS_PER_DAY)
    out["lag_1_week"] = out["offered"].shift(STEPS_PER_WEEK)
    out["rolling_mean_1_day"] = out["offered"].shift(1).rolling(STEPS_PER_DAY).mean()

    return out


def next_bucket_row(history: pd.DataFrame, next_ts: pd.Timestamp) -> pd.DataFrame:
    """Build the feature row for a single future timestamp from known history.

    `history` must be indexed by bucket_ts and contain an 'offered' column
    covering at least the prior week so lag features can be computed.
    """
    future_row = pd.DataFrame({"offered": [float("nan")]}, index=[next_ts])
    padded = pd.concat([history[["offered"]].astype(float), future_row])
    featured = build_features(padded)
    return featured.loc[[next_ts], FEATURE_COLUMNS]
