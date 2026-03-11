"""
Streamlit Dashboard for Amazon Ads AI Optimizer

Run: streamlit run src/dashboard/app.py

Panels:
  1. Account Performance Overview (spend, sales, ACoS, ROAS over time)
  2. Bid Change Activity (what the AI changed and why)
  3. Model Accuracy Tracking (are predictions improving?)
  4. Budget Allocation Map (where money is flowing)
  5. Keyword Management Log (harvested, negated, paused)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from sqlalchemy import text

from src.database.connection import get_db

st.set_page_config(page_title="Amazon Ads AI", layout="wide")
st.title("Amazon Ads AI Optimizer")

# ------------------------------------------------------------------
# Sidebar Controls
# ------------------------------------------------------------------
st.sidebar.header("Controls")
date_range = st.sidebar.slider(
    "Days of history",
    min_value=7, max_value=90, value=30,
)
target_acos = st.sidebar.number_input("Target ACoS %", value=25, min_value=5, max_value=100)

# ------------------------------------------------------------------
# 1. Account Performance Overview
# ------------------------------------------------------------------
st.header("Account Performance")

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

    # KPI Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Spend", f"${perf_df['spend'].sum():,.2f}")
    col2.metric("Total Sales", f"${perf_df['ad_sales'].sum():,.2f}")
    col3.metric("Avg ACoS", f"{perf_df['acos'].mean():.1f}%")
    col4.metric("Avg ROAS", f"{perf_df['roas'].mean():.1f}x")
    col5.metric("Total Orders", f"{perf_df['orders'].sum():,}")

    # Charts
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

# ------------------------------------------------------------------
# 2. Bid Change Activity
# ------------------------------------------------------------------
st.header("Recent Bid Changes")

with get_db() as db:
    bid_df = pd.read_sql(
        text("""
            SELECT
                bh.created_at,
                k.keyword_text,
                k.match_type,
                bh.old_bid,
                bh.new_bid,
                bh.change_pct,
                bh.reason,
                bh.predicted_acos,
                bh.applied
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
        use_container_width=True,
    )
else:
    st.info("No bid changes in the last 7 days.")

# ------------------------------------------------------------------
# 3. Model Performance
# ------------------------------------------------------------------
st.header("Model Accuracy")

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

# ------------------------------------------------------------------
# 4. Budget Allocation
# ------------------------------------------------------------------
st.header("Budget Allocation")

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
    st.dataframe(budget_df, use_container_width=True)
else:
    st.info("No budget allocation data yet.")

# ------------------------------------------------------------------
# 5. Top Predictions
# ------------------------------------------------------------------
st.header("Top Keyword Predictions")

with get_db() as db:
    pred_df = pd.read_sql(
        text("""
            SELECT
                k.keyword_text,
                k.match_type,
                p.predicted_cvr,
                p.predicted_acos,
                p.expected_profit_per_click,
                p.recommended_bid,
                k.bid as current_bid,
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
    st.dataframe(pred_df, use_container_width=True)
else:
    st.info("No predictions available yet.")
