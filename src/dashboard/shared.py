"""Shared sidebar controls and helpers for all dashboard pages."""

import streamlit as st
from sqlalchemy import text

from src.database.connection import get_db


def setup_page(title: str, is_main: bool = False):
    """Common page setup: config, title, and sidebar controls.

    Only the main app.py should pass is_main=True (calls set_page_config).
    Sub-pages must NOT call set_page_config — Streamlit handles it automatically.
    """
    if is_main:
        st.set_page_config(page_title="Amazon Ads AI Optimizer", layout="wide")
    st.title(title)
    render_sidebar()


def render_sidebar():
    """Render shared sidebar controls and persist values in session_state."""
    st.sidebar.header("Controls")
    st.session_state["date_range"] = st.sidebar.slider(
        "Days of history",
        min_value=7, max_value=90,
        value=st.session_state.get("date_range", 30),
    )
    st.session_state["target_acos"] = st.sidebar.number_input(
        "Target ACoS %",
        value=st.session_state.get("target_acos", 25),
        min_value=5, max_value=100,
    )

    st.sidebar.divider()
    st.sidebar.header("Danger Zone")
    if st.sidebar.button("Clear All Data", type="primary"):
        # Close any other open dialogs first
        st.session_state.pop("show_predictions_popup", None)
        st.session_state.pop("show_pipeline_popup", None)
        st.session_state["confirm_clear"] = True

    if st.session_state.get("confirm_clear"):
        _confirm_clear_dialog()


def _confirm_clear_dialog():
    @st.dialog("Clear Database")
    def confirm_clear_db():
        st.warning("This will delete ALL data from every table. This cannot be undone.")

        with get_db() as db:
            counts = {}
            for table in ["products", "campaigns", "keywords", "daily_keyword_metrics",
                          "predictions", "keyword_features", "bid_history",
                          "budget_allocations", "search_terms", "negative_keywords"]:
                counts[table] = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

        st.write("**Data that will be deleted:**")
        for table, count in counts.items():
            if count > 0:
                st.write(f"- {table}: **{count:,}** rows")

        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("Yes, delete everything", type="primary"):
                with get_db() as db:
                    for table in [
                        "predictions", "keyword_features", "bid_history",
                        "budget_allocations", "daily_keyword_metrics",
                        "daily_campaign_metrics", "daily_product_sales",
                        "search_terms", "negative_keywords", "model_performance",
                        "keywords", "ad_groups", "campaigns", "products",
                    ]:
                        db.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                st.session_state["confirm_clear"] = False
                st.rerun()
        with col_no:
            if st.button("Cancel"):
                st.session_state["confirm_clear"] = False
                st.rerun()

    confirm_clear_db()
