"""
Streamlit Dashboard for Amazon Ads AI Optimizer – Home Page

Run: streamlit run src/dashboard/app.py

Pages (via sidebar navigation):
  1. Data & Pipeline – generate demo data, run ML pipeline
  2. Products & Campaigns – product catalog and campaign overview
  3. Performance – account-level spend, sales, ACoS, ROAS over time
  4. Bid Activity – what the AI changed and why
  5. Model Accuracy – are predictions improving?
  6. Budget Allocation – where money is flowing
  7. Predictions – top keyword predictions
"""

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Amazon Ads AI Optimizer", is_main=True)

# ------------------------------------------------------------------
# Quick account summary
# ------------------------------------------------------------------
st.markdown("Use the **sidebar** to navigate between pages.")
st.divider()

with get_db() as db:
    product_count = db.execute(text("SELECT COUNT(*) FROM products")).scalar()
    campaign_count = db.execute(text("SELECT COUNT(*) FROM campaigns")).scalar()
    keyword_count = db.execute(text("SELECT COUNT(*) FROM keywords")).scalar()

    totals = db.execute(text("""
        SELECT
            COALESCE(SUM(spend), 0) as spend,
            COALESCE(SUM(ad_sales), 0) as sales,
            COALESCE(SUM(orders), 0) as orders
        FROM daily_keyword_metrics
        WHERE report_date >= CURRENT_DATE - 30
    """)).mappings().first()

col1, col2, col3 = st.columns(3)
col1.metric("Products", product_count)
col2.metric("Campaigns", campaign_count)
col3.metric("Keywords", keyword_count)

col4, col5, col6 = st.columns(3)
col4.metric("30-Day Spend", f"${totals['spend']:,.2f}")
col5.metric("30-Day Sales", f"${totals['sales']:,.2f}")
col6.metric("30-Day Orders", f"{totals['orders']:,}")

acos_val = (totals["spend"] / totals["sales"] * 100) if totals["sales"] > 0 else 0
roas_val = (totals["sales"] / totals["spend"]) if totals["spend"] > 0 else 0

col7, col8 = st.columns(2)
col7.metric("30-Day ACoS", f"{acos_val:.1f}%")
col8.metric("30-Day ROAS", f"{roas_val:.1f}x")

if product_count == 0:
    st.info("No data yet — head to **Data & Pipeline** in the sidebar to generate demo data.")
