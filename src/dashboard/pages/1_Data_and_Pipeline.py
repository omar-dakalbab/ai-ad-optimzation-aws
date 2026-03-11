"""Demo data generation and ML pipeline execution."""

import random
import re
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.dashboard.shared import setup_page
from src.database.connection import get_db

setup_page("Data & Pipeline")

# ------------------------------------------------------------------
# Demo Data Generation Helpers
# ------------------------------------------------------------------

DEMO_PRODUCTS = [
    ("Screen Protector Tempered Glass 3-Pack", "Electronics", "Phone Accessories", 9.99, 2.50, 2.50, 1.50, 4.2, 4500),
    ("Wireless Charging Pad 15W Fast Charge", "Electronics", "Chargers", 19.99, 5.00, 3.22, 3.00, 4.5, 2100),
    ("Laptop Stand Adjustable Aluminum", "Electronics", "Computer Accessories", 34.99, 10.00, 4.75, 5.25, 4.7, 870),
    ("Webcam 1080p HD with Microphone", "Electronics", "Computer Accessories", 29.99, 8.00, 4.22, 4.50, 4.3, 1650),
    ("Portable Power Bank 20000mAh", "Electronics", "Chargers", 27.99, 7.00, 3.92, 4.20, 4.4, 3200),
    ("Noise Cancelling Headphones Over-Ear", "Electronics", "Audio", 49.99, 14.00, 5.50, 7.50, 4.6, 920),
    ("Smart Watch Band Silicone Replacement", "Electronics", "Wearables", 8.99, 1.50, 2.22, 1.35, 4.1, 6100),
    ("Ring Light 10 inch with Tripod Stand", "Electronics", "Camera Accessories", 24.99, 6.00, 3.72, 3.75, 4.3, 1400),
    ("Mechanical Keyboard RGB Backlit", "Electronics", "Computer Accessories", 44.99, 12.00, 5.25, 6.75, 4.5, 780),
    ("Car Phone Mount Magnetic Dashboard", "Electronics", "Car Accessories", 15.99, 3.00, 2.72, 2.40, 4.4, 5300),
]

STOP_WORDS = {
    "a", "an", "the", "and", "or", "for", "with", "in", "on", "to", "of",
    "by", "is", "it", "at", "as", "from", "that", "this", "be", "are",
    "set", "new", "pack", "pcs", "piece",
}


def generate_seed_keywords(title: str) -> list[str]:
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", title.lower())
    words = [w for w in cleaned.split() if w not in STOP_WORDS and len(w) > 1]
    keywords = set()
    if len(words) >= 2:
        keywords.add(" ".join(words))
    for i in range(len(words) - 1):
        keywords.add(f"{words[i]} {words[i+1]}")
    for i in range(len(words) - 2):
        keywords.add(f"{words[i]} {words[i+1]} {words[i+2]}")
    for w in words:
        if len(w) >= 4:
            keywords.add(w)
    return sorted(keywords, key=len, reverse=True)[:20]


def add_product_and_campaigns(db, product_tuple, product_num):
    title, category, subcategory, price, cost, fba_fee, referral_fee, rating, reviews = product_tuple
    asin = f"B0GEN{product_num:05d}"
    margin = round(price - cost - fba_fee - referral_fee, 2)
    margin_pct = round((margin / price) * 100, 2)
    inventory = random.randint(100, 800)

    exists = db.execute(text("SELECT 1 FROM products WHERE asin = :a"), {"a": asin}).first()
    if exists:
        return None

    db.execute(text("""
        INSERT INTO products (asin, sku, title, category, subcategory, price, cost,
            fba_fee, referral_fee, margin, margin_pct, rating, review_count, inventory_level, status)
        VALUES (:asin, :sku, :title, :cat, :subcat, :price, :cost,
            :fba, :ref, :margin, :mpct, :rating, :reviews, :inv, 'active')
    """), {
        "asin": asin, "sku": f"SKU-GEN-{product_num:03d}", "title": title,
        "cat": category, "subcat": subcategory, "price": price, "cost": cost,
        "fba": fba_fee, "ref": referral_fee, "margin": margin, "mpct": margin_pct,
        "rating": rating, "reviews": reviews, "inv": inventory,
    })

    short_title = " ".join(title.split()[:4])
    campaign_configs = [
        (f"SP | {short_title} | Auto", "auto", 0.40, 0.80),
        (f"SP | {short_title} | Manual Exact", "manual", 0.35, 1.20),
        (f"SP | {short_title} | Manual Broad", "manual", 0.25, 0.90),
    ]

    base_bid = round(margin * 0.25 * 0.10, 2)
    base_bid = max(base_bid, 0.15)
    total_budget = 50.0
    seed_kws = generate_seed_keywords(title)
    campaigns_created = []

    for camp_name, targeting, budget_share, bid_mult in campaign_configs:
        budget = round(max(total_budget * budget_share, 10.0), 2)
        bid = round(max(base_bid * bid_mult, 0.10), 2)
        camp_amazon_id = random.randint(100000000, 999999999)

        camp_id = db.execute(text("""
            INSERT INTO campaigns (amazon_campaign_id, name, campaign_type, targeting_type,
                state, daily_budget, start_date)
            VALUES (:aid, :name, 'sponsoredProducts', :targeting, 'enabled', :budget, CURRENT_DATE)
            RETURNING id
        """), {
            "aid": camp_amazon_id, "name": camp_name,
            "targeting": targeting, "budget": budget,
        }).scalar()

        ag_amazon_id = random.randint(100000000, 999999999)
        ag_id = db.execute(text("""
            INSERT INTO ad_groups (amazon_ad_group_id, campaign_id, name, default_bid, state)
            VALUES (:aid, :cid, :name, :bid, 'enabled')
            RETURNING id
        """), {
            "aid": ag_amazon_id, "cid": camp_id,
            "name": camp_name.replace("SP |", "AG |"), "bid": bid,
        }).scalar()

        kw_count = 0
        if targeting == "manual":
            match_type = "exact" if "Exact" in camp_name else "broad"
            for kw in seed_kws:
                kw_amazon_id = random.randint(100000000, 999999999)
                kw_id = db.execute(text("""
                    INSERT INTO keywords (amazon_keyword_id, ad_group_id, campaign_id,
                        keyword_text, match_type, bid, state, asin)
                    VALUES (:akid, :agid, :cid, :text, :match, :bid, 'enabled', :asin)
                    RETURNING id
                """), {
                    "akid": kw_amazon_id, "agid": ag_id, "cid": camp_id,
                    "text": kw, "match": match_type, "bid": bid, "asin": asin,
                }).scalar()
                kw_count += 1

                for day_offset in range(30):
                    report_date = date.today() - timedelta(days=day_offset)
                    impressions = random.randint(50, 800)
                    ctr = random.uniform(0.005, 0.04)
                    clicks = max(1, int(impressions * ctr))
                    cpc = round(random.uniform(0.20, bid * 1.5), 2)
                    spend = round(clicks * cpc, 2)
                    cvr = random.uniform(0.03, 0.18)
                    orders = max(0, int(clicks * cvr))
                    avg_order_value = price * random.uniform(0.9, 1.1)
                    ad_sales = round(orders * avg_order_value, 2)

                    db.execute(text("""
                        INSERT INTO daily_keyword_metrics
                            (keyword_id, campaign_id, asin, report_date,
                             impressions, clicks, spend, orders, ad_sales, units_sold,
                             ctr, cpc, cvr, acos, roas, bid_at_time)
                        VALUES (:kid, :cid, :asin, :rd,
                                :imp, :clicks, :spend, :orders, :sales, :orders,
                                :ctr, :cpc, :cvr, :acos, :roas, :bid)
                        ON CONFLICT (keyword_id, report_date) DO NOTHING
                    """), {
                        "kid": kw_id, "cid": camp_id, "asin": asin, "rd": report_date,
                        "imp": impressions, "clicks": clicks, "spend": spend,
                        "orders": orders, "sales": ad_sales,
                        "ctr": round(clicks / impressions, 6) if impressions else 0,
                        "cpc": cpc,
                        "cvr": round(orders / clicks, 6) if clicks else 0,
                        "acos": round(spend / ad_sales, 4) if ad_sales > 0 else None,
                        "roas": round(ad_sales / spend, 2) if spend > 0 else None,
                        "bid": bid,
                    })

        campaigns_created.append({
            "campaign": camp_name,
            "type": targeting,
            "budget": budget,
            "bid": bid,
            "keywords": kw_count,
        })

    return {"asin": asin, "title": title, "margin": margin, "campaigns": campaigns_created}


# ------------------------------------------------------------------
# Generate Demo Data
# ------------------------------------------------------------------
st.header("Generate Demo Data")
st.caption("Add new products with auto-created campaigns and simulated performance data.")

col_btn1, col_btn2 = st.columns(2)

with col_btn1:
    if st.button("Add 1 Random Product + Campaigns", type="primary"):
        with get_db() as db:
            max_num = db.execute(text(
                "SELECT COUNT(*) FROM products WHERE asin LIKE 'B0GEN%'"
            )).scalar() or 0
            product = random.choice(DEMO_PRODUCTS)
            result = add_product_and_campaigns(db, product, max_num + 1)
        if result:
            st.success(f"Created **{result['title']}** ({result['asin']}) with {len(result['campaigns'])} campaigns and 30 days of metrics!")
            for c in result["campaigns"]:
                st.write(f"  - {c['campaign']} | Budget: ${c['budget']} | Bid: ${c['bid']} | Keywords: {c['keywords']}")
        else:
            st.warning("Product already exists. Try again.")
        st.rerun()

with col_btn2:
    if st.button("Add 5 Products + Campaigns"):
        added = 0
        with get_db() as db:
            max_num = db.execute(text(
                "SELECT COUNT(*) FROM products WHERE asin LIKE 'B0GEN%'"
            )).scalar() or 0
            for i, product in enumerate(random.sample(DEMO_PRODUCTS, min(5, len(DEMO_PRODUCTS)))):
                result = add_product_and_campaigns(db, product, max_num + i + 1)
                if result:
                    added += 1
        if added:
            st.success(f"Added {added} products with campaigns and metrics!")
        else:
            st.warning("All products already exist.")
        st.rerun()

# ------------------------------------------------------------------
# Run ML Pipeline
# ------------------------------------------------------------------
st.divider()
st.header("Run ML Pipeline")
st.caption("Compute features from metrics data, train the models, and generate bid/budget predictions.")

col_ml1, col_ml2, col_ml3 = st.columns(3)

with col_ml1:
    if st.button("1. Compute Features", type="secondary"):
        with st.spinner("Computing features for all keywords..."):
            try:
                from src.features.feature_engineering import FeatureEngineer
                engineer = FeatureEngineer()
                engineer.compute_features(compute_date=date.today())
                with get_db() as db:
                    count = db.execute(text("SELECT COUNT(*) FROM keyword_features")).scalar()
                st.success(f"Features computed! {count} total feature rows.")
            except Exception as e:
                st.error(f"Feature computation failed: {e}")
        st.rerun()

with col_ml2:
    if st.button("2. Train Models", type="secondary"):
        with st.spinner("Training click, conversion, and revenue models..."):
            try:
                from src.models.training import ModelTrainer
                trainer = ModelTrainer()
                results = trainer.train_all_models(training_date=date.today())
                st.success(f"Models trained! Click AUC: {results.get('click_model', {}).get('auc', 'N/A'):.3f}")
            except Exception as e:
                st.error(f"Training failed: {e}")
        st.rerun()

with col_ml3:
    if st.button("3. Run Predictions", type="secondary"):
        with st.spinner("Generating predictions for all keywords..."):
            try:
                from src.models.inference import ModelInference
                inference = ModelInference()
                inference.predict_all_keywords(prediction_date=date.today())
                st.session_state["show_results_dialog"] = "predictions"
                st.session_state.pop("confirm_clear", None)
            except Exception as e:
                st.error(f"Prediction failed: {e}")

st.divider()

if st.button("Run Full Pipeline (Features + Train + Predict + Optimize)", type="primary"):
    progress = st.progress(0, text="Starting pipeline...")
    try:
        progress.progress(10, text="Computing features...")
        from src.features.feature_engineering import FeatureEngineer
        FeatureEngineer().compute_features(compute_date=date.today())

        progress.progress(30, text="Training models...")
        from src.models.training import ModelTrainer
        ModelTrainer().train_all_models(training_date=date.today())

        progress.progress(55, text="Running predictions...")
        from src.models.inference import ModelInference
        ModelInference().predict_all_keywords(prediction_date=date.today())

        progress.progress(70, text="Optimizing bids...")
        from src.optimization.bid_optimizer import BidOptimizer
        bid_recs = BidOptimizer().generate_bid_recommendations(prediction_date=date.today())

        progress.progress(85, text="Optimizing budgets...")
        from src.optimization.budget_allocator import BudgetAllocator
        budget_recs = BudgetAllocator().generate_budget_recommendations(prediction_date=date.today())

        progress.progress(95, text="Managing keywords...")
        from src.optimization.keyword_manager import KeywordManager
        kw_actions = KeywordManager().run_keyword_management(eval_date=date.today())

        progress.progress(100, text="Done!")
        st.session_state["show_results_dialog"] = "pipeline"
        st.session_state.pop("confirm_clear", None)
        st.session_state["pipeline_results"] = {
            "bid_recs": len(bid_recs),
            "budget_recs": len(budget_recs),
            "kw_actions": len(kw_actions),
        }
    except Exception as e:
        st.error(f"Pipeline failed: {e}")

# ------------------------------------------------------------------
# Single unified results dialog (Streamlit only allows one per page)
# ------------------------------------------------------------------
_dialog_mode = st.session_state.get("show_results_dialog")

if _dialog_mode and not st.session_state.get("confirm_clear"):
    @st.dialog("Results", width="large")
    def show_results_dialog():
        mode = st.session_state.get("show_results_dialog")

        # Load predictions (used by both modes)
        with get_db() as db:
            pred_df = pd.read_sql(
                text("""
                    SELECT
                        k.keyword_text, k.match_type, c.name as campaign,
                        p.predicted_ctr, p.predicted_cvr, p.predicted_acos,
                        p.recommended_bid, k.bid as current_bid,
                        p.expected_profit_per_click, p.prediction_confidence
                    FROM predictions p
                    JOIN keywords k ON k.id = p.keyword_id
                    JOIN campaigns c ON c.id = k.campaign_id
                    WHERE p.prediction_date = CURRENT_DATE
                    ORDER BY p.expected_profit_per_click DESC
                """),
                db.bind,
            )

        # Pipeline-specific header
        if mode == "pipeline":
            results = st.session_state.get("pipeline_results", {})
            st.subheader("Pipeline Complete!")
            col1, col2, col3 = st.columns(3)
            col1.metric("Bid Changes", results.get("bid_recs", 0))
            col2.metric("Budget Changes", results.get("budget_recs", 0))
            col3.metric("Keyword Actions", results.get("kw_actions", 0))
            st.divider()

        # Predictions table (both modes)
        if pred_df.empty:
            st.warning("No predictions generated for today.")
        else:
            if mode == "predictions":
                st.metric("Keywords Predicted", len(pred_df))
                st.subheader("Top 5 Most Profitable Keywords")
                for _, row in pred_df.head(5).iterrows():
                    col_a, col_b, col_c = st.columns(3)
                    col_a.write(f"**{row['keyword_text']}** ({row['match_type']})")
                    col_b.write(f"Predicted ACoS: **{row['predicted_acos']:.1%}** | CVR: **{row['predicted_cvr']:.1%}**")
                    col_c.write(f"Bid: ${row['current_bid']:.2f} → **${row['recommended_bid']:.2f}** | Confidence: {row['prediction_confidence']:.0%}")
                st.divider()

            header = f"Predictions ({len(pred_df)} keywords)" if mode == "pipeline" else "All Predictions"
            st.subheader(header)
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Avg Predicted ACoS", f"{pred_df['predicted_acos'].mean():.1%}")
            col2.metric("Avg Predicted CVR", f"{pred_df['predicted_cvr'].mean():.1%}")
            col3.metric("Avg Recommended Bid", f"${pred_df['recommended_bid'].mean():.2f}")
            col4.metric("Avg Confidence", f"{pred_df['prediction_confidence'].mean():.0%}")
            st.dataframe(pred_df, use_container_width=True, hide_index=True)

        # Bid changes (pipeline mode only)
        if mode == "pipeline":
            with get_db() as db:
                bid_df = pd.read_sql(
                    text("""
                        SELECT k.keyword_text, k.match_type,
                               bh.old_bid, bh.new_bid, bh.change_pct, bh.reason
                        FROM bid_history bh
                        JOIN keywords k ON k.id = bh.keyword_id
                        WHERE bh.created_at >= CURRENT_DATE
                        ORDER BY ABS(bh.change_pct) DESC
                        LIMIT 50
                    """),
                    db.bind,
                )
            if not bid_df.empty:
                st.subheader(f"Bid Changes ({len(bid_df)})")
                st.dataframe(bid_df, use_container_width=True, hide_index=True)

        if st.button("Close"):
            st.session_state.pop("show_results_dialog", None)
            st.session_state.pop("pipeline_results", None)
            st.rerun()

    show_results_dialog()
