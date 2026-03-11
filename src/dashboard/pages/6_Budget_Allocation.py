"""Budget allocation recommendations."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Budget Allocation")

with get_db() as db:
    budget_df = pd.read_sql(
        text("""
            SELECT
                c.name as campaign_name,
                c.daily_budget as current_budget,
                ba.new_budget as recommended_budget,
                ba.change_pct,
                ba.predicted_roas,
                ba.reason,
                ba.applied
            FROM budget_allocations ba
            JOIN campaigns c ON c.id = ba.campaign_id
            WHERE ba.allocation_date >= CURRENT_DATE - 7
            ORDER BY ba.predicted_roas DESC
        """),
        db.bind,
    )

if not budget_df.empty:
    fig = px.bar(
        budget_df, x="campaign_name", y=["current_budget", "recommended_budget"],
        barmode="group", title="Current vs Recommended Budget",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(budget_df, use_container_width=True, hide_index=True)
else:
    st.info("No budget allocation data yet.")
