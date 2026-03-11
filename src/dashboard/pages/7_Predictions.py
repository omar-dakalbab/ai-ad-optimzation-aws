"""Top keyword predictions from the latest model run."""

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Predictions")

with get_db() as db:
    pred_df = pd.read_sql(
        text("""
            SELECT
                k.keyword_text, k.match_type,
                p.predicted_cvr, p.predicted_acos,
                p.expected_profit_per_click,
                p.recommended_bid, k.bid as current_bid,
                p.prediction_confidence
            FROM predictions p
            JOIN keywords k ON k.id = p.keyword_id
            WHERE p.prediction_date = (SELECT MAX(prediction_date) FROM predictions)
            ORDER BY p.expected_profit_per_click DESC
            LIMIT 50
        """),
        db.bind,
    )

if not pred_df.empty:
    st.dataframe(pred_df, use_container_width=True, hide_index=True)
else:
    st.info("No predictions available yet.")
