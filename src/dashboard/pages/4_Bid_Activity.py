"""Recent bid change history."""

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Bid Activity")

with get_db() as db:
    bid_df = pd.read_sql(
        text("""
            SELECT
                bh.created_at, k.keyword_text, k.match_type,
                bh.old_bid, bh.new_bid, bh.change_pct,
                bh.reason, bh.predicted_acos, bh.applied
            FROM bid_history bh
            JOIN keywords k ON k.id = bh.keyword_id
            WHERE bh.created_at >= CURRENT_DATE - 7
            ORDER BY bh.created_at DESC
            LIMIT 100
        """),
        db.bind,
    )

if not bid_df.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Bid Increases", len(bid_df[bid_df["change_pct"] > 0]))
    col2.metric("Bid Decreases", len(bid_df[bid_df["change_pct"] < 0]))
    col3.metric("Applied", len(bid_df[bid_df["applied"]]))

    st.dataframe(
        bid_df[["keyword_text", "match_type", "old_bid", "new_bid",
                "change_pct", "reason", "applied"]],
        use_container_width=True, hide_index=True,
    )
else:
    st.info("No bid changes in the last 7 days.")
