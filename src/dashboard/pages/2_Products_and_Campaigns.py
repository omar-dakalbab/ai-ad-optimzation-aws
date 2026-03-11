"""Products catalog and campaign overview."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Products & Campaigns")

# ------------------------------------------------------------------
# Products
# ------------------------------------------------------------------
st.header("Products")

with get_db() as db:
    products_df = pd.read_sql(
        text("""
            SELECT p.asin, p.title, p.price, p.margin, p.rating,
                   p.review_count, p.inventory_level, p.status
            FROM products p
            ORDER BY p.created_at DESC
        """),
        db.bind,
    )

if not products_df.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Products", len(products_df))
    col2.metric("Avg Margin", f"${products_df['margin'].mean():.2f}")
    col3.metric("Avg Rating", f"{products_df['rating'].mean():.1f}")
    st.dataframe(products_df, use_container_width=True, hide_index=True)
else:
    st.info("No products yet. Head to **Data & Pipeline** to generate some!")

# ------------------------------------------------------------------
# Campaigns
# ------------------------------------------------------------------
st.divider()
st.header("Campaigns")

with get_db() as db:
    camp_df = pd.read_sql(
        text("""
            SELECT
                c.name as campaign_name, c.targeting_type, c.state, c.daily_budget,
                c.start_date,
                COUNT(DISTINCT k.id) as keywords,
                COALESCE(SUM(m.impressions), 0) as total_impressions,
                COALESCE(SUM(m.clicks), 0) as total_clicks,
                COALESCE(SUM(m.spend), 0) as total_spend,
                COALESCE(SUM(m.orders), 0) as total_orders,
                COALESCE(SUM(m.ad_sales), 0) as total_sales
            FROM campaigns c
            LEFT JOIN keywords k ON k.campaign_id = c.id
            LEFT JOIN daily_keyword_metrics m ON m.campaign_id = c.id
                AND m.report_date >= CURRENT_DATE - 30
            GROUP BY c.id, c.name, c.targeting_type, c.state, c.daily_budget, c.start_date
            ORDER BY c.created_at DESC
        """),
        db.bind,
    )

if not camp_df.empty:
    camp_df["acos"] = camp_df.apply(
        lambda r: round((r["total_spend"] / r["total_sales"]) * 100, 1) if r["total_sales"] > 0 else None, axis=1
    )
    camp_df["roas"] = camp_df.apply(
        lambda r: round(r["total_sales"] / r["total_spend"], 1) if r["total_spend"] > 0 else None, axis=1
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Campaigns", len(camp_df))
    col2.metric("Auto", len(camp_df[camp_df["targeting_type"] == "auto"]))
    col3.metric("Manual", len(camp_df[camp_df["targeting_type"] == "manual"]))
    col4.metric("Total Daily Budget", f"${camp_df['daily_budget'].sum():,.2f}")

    st.dataframe(
        camp_df[["campaign_name", "targeting_type", "state", "daily_budget",
                 "keywords", "total_spend", "total_sales", "acos", "roas"]],
        use_container_width=True, hide_index=True,
    )

    fig = px.bar(
        camp_df, x="campaign_name", y="daily_budget", color="targeting_type",
        title="Campaign Daily Budgets",
        color_discrete_map={"auto": "#636EFA", "manual": "#EF553B"},
    )
    fig.update_layout(height=350, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No campaigns yet.")
