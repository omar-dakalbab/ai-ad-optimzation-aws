"""Model performance tracking over time."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Model Accuracy")

with get_db() as db:
    model_df = pd.read_sql(
        text("""
            SELECT * FROM model_performance
            ORDER BY trained_date DESC
            LIMIT 30
        """),
        db.bind,
    )

if not model_df.empty:
    fig = px.line(
        model_df, x="trained_date", y="metric_value",
        color="model_name", symbol="metric_name",
        title="Model Performance Over Time",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No model performance data yet.")
