"""Account-level performance metrics and trends."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Performance")

date_range = st.session_state.get("date_range", 30)
target_acos = st.session_state.get("target_acos", 25)

with get_db() as db:
    perf_df = pd.read_sql(
        text("""
            SELECT
                report_date,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(spend) as spend,
                SUM(orders) as orders,
                SUM(ad_sales) as ad_sales
            FROM daily_keyword_metrics
            WHERE report_date >= CURRENT_DATE - :days
            GROUP BY report_date
            ORDER BY report_date
        """),
        db.bind,
        params={"days": date_range},
    )

if not perf_df.empty:
    safe_sales = perf_df["ad_sales"].replace(0, float("nan"))
    safe_spend = perf_df["spend"].replace(0, float("nan"))
    safe_impressions = perf_df["impressions"].replace(0, float("nan"))
    safe_clicks = perf_df["clicks"].replace(0, float("nan"))
    perf_df["acos"] = (perf_df["spend"] / safe_sales) * 100
    perf_df["roas"] = perf_df["ad_sales"] / safe_spend
    perf_df["ctr"] = (perf_df["clicks"] / safe_impressions) * 100
    perf_df["cvr"] = (perf_df["orders"] / safe_clicks) * 100

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Spend", f"${perf_df['spend'].sum():,.2f}")
    col2.metric("Total Sales", f"${perf_df['ad_sales'].sum():,.2f}")
    col3.metric("Avg ACoS", f"{perf_df['acos'].mean():.1f}%")
    col4.metric("Avg ROAS", f"{perf_df['roas'].mean():.1f}x")
    col5.metric("Total Orders", f"{perf_df['orders'].sum():,}")

    col_left, col_right = st.columns(2)

    with col_left:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=perf_df["report_date"], y=perf_df["spend"],
            name="Spend", line=dict(color="red"),
        ))
        fig.add_trace(go.Scatter(
            x=perf_df["report_date"], y=perf_df["ad_sales"],
            name="Sales", line=dict(color="green"),
        ))
        fig.update_layout(title="Spend vs Sales", height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=perf_df["report_date"], y=perf_df["acos"],
            name="ACoS %", line=dict(color="orange"),
        ))
        fig.add_hline(y=target_acos, line_dash="dash", line_color="red",
                      annotation_text=f"Target: {target_acos}%")
        fig.update_layout(title="ACoS Trend", height=350)
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No performance data available yet.")
