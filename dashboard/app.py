import os

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from src.utils.db import load_mart_series

load_dotenv()
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="HRM 311 Demand Forecast", layout="wide")
st.title("HRM 311 Call Volume — Demand Forecast")

with st.sidebar:
    lookback_days = st.slider("History to show (days)", 1, 30, 7)
    horizon_steps = st.slider("Forecast horizon (30-min steps)", 4, 336, 48)

history = load_mart_series()
recent = history.tail(lookback_days * 48)

st.subheader("Recent actual call volume")
st.line_chart(recent["offered"])

st.subheader("Forecast")
try:
    resp = requests.get(f"{API_URL}/forecast", params={"horizon_steps": horizon_steps}, timeout=30)
    resp.raise_for_status()
    points = resp.json()["points"]
    forecast_df = pd.DataFrame(points)
    forecast_df["bucket_ts"] = pd.to_datetime(forecast_df["bucket_ts"])
    forecast_df = forecast_df.set_index("bucket_ts")

    combined = pd.DataFrame(
        {
            "actual": recent["offered"],
            "forecast": forecast_df["predicted_offered"],
        }
    )
    st.line_chart(combined)
except requests.RequestException as e:
    st.error(
        f"Couldn't reach the forecast API at {API_URL} ({e}). "
        "Start it with `uvicorn src.serving.app:app --reload` and make sure a model has been trained."
    )
