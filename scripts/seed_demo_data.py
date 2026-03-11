"""
Seed the database with realistic demo data for testing.

Run: python scripts/seed_demo_data.py

This creates:
  - 5 products with realistic margins
  - 4 campaigns (2 manual, 2 auto)
  - 8 ad groups
  - 50 keywords
  - 60 days of daily keyword metrics
  - search term data
  - So you can test the full pipeline without Amazon API credentials
"""

import sys
import os
import random
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from sqlalchemy import text

from src.database.connection import get_db

random.seed(42)
np.random.seed(42)

# ---- Products ----
PRODUCTS = [
    {"asin": "B0DEMO00001", "sku": "WE-001", "title": "Wireless Bluetooth Earbuds Pro",
     "category": "Electronics", "subcategory": "Headphones",
     "price": 29.99, "cost": 8.50, "fba_fee": 5.20, "referral_fee": 4.50,
     "rating": 4.3, "review_count": 1247, "inventory_level": 342},
    {"asin": "B0DEMO00002", "sku": "PC-002", "title": "USB-C Fast Charger 65W",
     "category": "Electronics", "subcategory": "Chargers",
     "price": 24.99, "cost": 6.00, "fba_fee": 4.80, "referral_fee": 3.75,
     "rating": 4.5, "review_count": 892, "inventory_level": 518},
    {"asin": "B0DEMO00003", "sku": "PH-003", "title": "Phone Case Slim Fit Clear",
     "category": "Cell Phone Accessories", "subcategory": "Cases",
     "price": 12.99, "cost": 1.80, "fba_fee": 3.50, "referral_fee": 1.95,
     "rating": 4.1, "review_count": 3421, "inventory_level": 1205},
    {"asin": "B0DEMO00004", "sku": "SP-004", "title": "Bluetooth Portable Speaker Waterproof",
     "category": "Electronics", "subcategory": "Speakers",
     "price": 39.99, "cost": 12.00, "fba_fee": 6.50, "referral_fee": 6.00,
     "rating": 4.4, "review_count": 567, "inventory_level": 189},
    {"asin": "B0DEMO00005", "sku": "CB-005", "title": "Lightning to USB-C Cable 6ft 2-Pack",
     "category": "Electronics", "subcategory": "Cables",
     "price": 14.99, "cost": 2.50, "fba_fee": 3.80, "referral_fee": 2.25,
     "rating": 4.2, "review_count": 2103, "inventory_level": 876},
]

# ---- Campaigns ----
CAMPAIGNS = [
    {"amazon_campaign_id": 100001, "name": "SP - Earbuds - Exact", "campaign_type": "sponsoredProducts",
     "targeting_type": "manual", "state": "enabled", "daily_budget": 50.00},
    {"amazon_campaign_id": 100002, "name": "SP - Earbuds - Auto", "campaign_type": "sponsoredProducts",
     "targeting_type": "auto", "state": "enabled", "daily_budget": 30.00},
    {"amazon_campaign_id": 100003, "name": "SP - Charger - Exact", "campaign_type": "sponsoredProducts",
     "targeting_type": "manual", "state": "enabled", "daily_budget": 40.00},
    {"amazon_campaign_id": 100004, "name": "SP - Accessories - Broad", "campaign_type": "sponsoredProducts",
     "targeting_type": "manual", "state": "enabled", "daily_budget": 80.00},
]

# ---- Keywords per campaign ----
KEYWORDS = [
    # Campaign 1: Earbuds Exact (high performers)
    {"campaign_idx": 0, "text": "wireless bluetooth earbuds", "match": "exact", "bid": 0.85, "asin_idx": 0},
    {"campaign_idx": 0, "text": "bluetooth earbuds", "match": "exact", "bid": 0.75, "asin_idx": 0},
    {"campaign_idx": 0, "text": "wireless earbuds", "match": "exact", "bid": 0.90, "asin_idx": 0},
    {"campaign_idx": 0, "text": "earbuds bluetooth wireless", "match": "exact", "bid": 0.65, "asin_idx": 0},
    {"campaign_idx": 0, "text": "best wireless earbuds", "match": "exact", "bid": 1.10, "asin_idx": 0},
    {"campaign_idx": 0, "text": "earbuds for running", "match": "exact", "bid": 0.70, "asin_idx": 0},
    {"campaign_idx": 0, "text": "noise cancelling earbuds", "match": "exact", "bid": 1.20, "asin_idx": 0},
    {"campaign_idx": 0, "text": "earbuds with microphone", "match": "exact", "bid": 0.55, "asin_idx": 0},
    # Campaign 2: Earbuds Auto
    {"campaign_idx": 1, "text": "auto_targeting", "match": "broad", "bid": 0.50, "asin_idx": 0},
    # Campaign 3: Charger Exact
    {"campaign_idx": 2, "text": "usb c charger", "match": "exact", "bid": 0.60, "asin_idx": 1},
    {"campaign_idx": 2, "text": "65w charger", "match": "exact", "bid": 0.55, "asin_idx": 1},
    {"campaign_idx": 2, "text": "fast charger usb c", "match": "exact", "bid": 0.70, "asin_idx": 1},
    {"campaign_idx": 2, "text": "laptop charger usb c", "match": "exact", "bid": 0.80, "asin_idx": 1},
    {"campaign_idx": 2, "text": "macbook charger", "match": "exact", "bid": 0.90, "asin_idx": 1},
    {"campaign_idx": 2, "text": "usb c wall charger", "match": "exact", "bid": 0.50, "asin_idx": 1},
    # Campaign 4: Accessories Broad (mixed performance)
    {"campaign_idx": 3, "text": "phone case clear", "match": "broad", "bid": 0.40, "asin_idx": 2},
    {"campaign_idx": 3, "text": "iphone case", "match": "broad", "bid": 0.55, "asin_idx": 2},
    {"campaign_idx": 3, "text": "bluetooth speaker waterproof", "match": "broad", "bid": 0.75, "asin_idx": 3},
    {"campaign_idx": 3, "text": "portable speaker", "match": "broad", "bid": 0.65, "asin_idx": 3},
    {"campaign_idx": 3, "text": "lightning cable", "match": "broad", "bid": 0.35, "asin_idx": 4},
    {"campaign_idx": 3, "text": "usb c cable", "match": "broad", "bid": 0.30, "asin_idx": 4},
    # Some underperformers for the optimizer to catch
    {"campaign_idx": 3, "text": "cheap headphones", "match": "broad", "bid": 0.45, "asin_idx": 0},
    {"campaign_idx": 3, "text": "free earbuds", "match": "broad", "bid": 0.40, "asin_idx": 0},
    {"campaign_idx": 3, "text": "speaker repair", "match": "broad", "bid": 0.50, "asin_idx": 3},
    {"campaign_idx": 3, "text": "cable organizer", "match": "broad", "bid": 0.35, "asin_idx": 4},
]


def seed():
    today = date.today()
    start_date = today - timedelta(days=60)

    with get_db() as db:
        print("Seeding products...")
        product_ids = []
        for p in PRODUCTS:
            margin = p["price"] - p["cost"] - p["fba_fee"] - p["referral_fee"]
            margin_pct = (margin / p["price"]) * 100
            result = db.execute(
                text("""
                    INSERT INTO products (asin, sku, title, category, subcategory,
                        price, cost, fba_fee, referral_fee, margin, margin_pct,
                        rating, review_count, inventory_level, status)
                    VALUES (:asin, :sku, :title, :cat, :subcat,
                        :price, :cost, :fba, :ref, :margin, :margin_pct,
                        :rating, :reviews, :inv, 'active')
                    ON CONFLICT (asin) DO UPDATE SET price = EXCLUDED.price
                    RETURNING id
                """),
                {"asin": p["asin"], "sku": p["sku"], "title": p["title"],
                 "cat": p["category"], "subcat": p["subcategory"],
                 "price": p["price"], "cost": p["cost"], "fba": p["fba_fee"],
                 "ref": p["referral_fee"], "margin": round(margin, 2),
                 "margin_pct": round(margin_pct, 2),
                 "rating": p["rating"], "reviews": p["review_count"], "inv": p["inventory_level"]},
            )
            product_ids.append(result.fetchone()[0])

        print(f"  Created {len(product_ids)} products")

        print("Seeding campaigns...")
        campaign_ids = []
        for c in CAMPAIGNS:
            result = db.execute(
                text("""
                    INSERT INTO campaigns (amazon_campaign_id, name, campaign_type,
                        targeting_type, state, daily_budget)
                    VALUES (:acid, :name, :ctype, :ttype, :state, :budget)
                    ON CONFLICT (amazon_campaign_id) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                """),
                {"acid": c["amazon_campaign_id"], "name": c["name"],
                 "ctype": c["campaign_type"], "ttype": c["targeting_type"],
                 "state": c["state"], "budget": c["daily_budget"]},
            )
            campaign_ids.append(result.fetchone()[0])

        print(f"  Created {len(campaign_ids)} campaigns")

        print("Seeding ad groups...")
        ad_group_ids = []
        for i, c in enumerate(CAMPAIGNS):
            result = db.execute(
                text("""
                    INSERT INTO ad_groups (amazon_ad_group_id, campaign_id, name,
                        default_bid, state)
                    VALUES (:agid, :cid, :name, :bid, 'enabled')
                    ON CONFLICT (amazon_ad_group_id) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                """),
                {"agid": 200000 + i, "cid": campaign_ids[i],
                 "name": f"AG - {c['name']}", "bid": 0.50},
            )
            ad_group_ids.append(result.fetchone()[0])

        print(f"  Created {len(ad_group_ids)} ad groups")

        print("Seeding keywords...")
        keyword_ids = []
        keyword_meta = []  # store campaign_idx and asin_idx for metrics generation
        for j, kw in enumerate(KEYWORDS):
            cid = campaign_ids[kw["campaign_idx"]]
            agid = ad_group_ids[kw["campaign_idx"]]
            asin = PRODUCTS[kw["asin_idx"]]["asin"]
            result = db.execute(
                text("""
                    INSERT INTO keywords (amazon_keyword_id, ad_group_id, campaign_id,
                        keyword_text, match_type, bid, state, asin)
                    VALUES (:akid, :agid, :cid, :text, :match, :bid, 'enabled', :asin)
                    ON CONFLICT (amazon_keyword_id) DO UPDATE SET bid = EXCLUDED.bid
                    RETURNING id
                """),
                {"akid": 300000 + j, "agid": agid, "cid": cid,
                 "text": kw["text"], "match": kw["match"], "bid": kw["bid"], "asin": asin},
            )
            kid = result.fetchone()[0]
            keyword_ids.append(kid)
            keyword_meta.append({"campaign_idx": kw["campaign_idx"], "asin_idx": kw["asin_idx"], "bid": kw["bid"]})

        print(f"  Created {len(keyword_ids)} keywords")

        # ---- Generate 60 days of daily metrics ----
        print("Generating 60 days of keyword metrics...")

        # Define keyword performance profiles
        # Good keywords: high CTR, decent CVR
        # Bad keywords: low CTR, zero CVR (the ones optimizer should catch)
        bad_keywords = {"cheap headphones", "free earbuds", "speaker repair", "cable organizer"}

        total_rows = 0
        for day_offset in range(60):
            report_day = start_date + timedelta(days=day_offset)
            dow = report_day.weekday()
            # Weekend multiplier (less traffic on weekends)
            weekend_mult = 0.7 if dow >= 5 else 1.0
            # Gradual improvement trend (simulate optimization over time)
            time_mult = 1.0 + (day_offset / 60) * 0.15

            for idx, kid in enumerate(keyword_ids):
                kw = KEYWORDS[idx]
                product = PRODUCTS[kw["asin_idx"]]
                is_bad = kw["text"] in bad_keywords

                # Base impressions vary by keyword
                if kw["match"] == "exact":
                    base_impressions = random.randint(80, 400)
                elif kw["match"] == "broad":
                    base_impressions = random.randint(150, 800)
                else:
                    base_impressions = random.randint(100, 500)

                impressions = int(base_impressions * weekend_mult * random.uniform(0.6, 1.4))

                # CTR depends on keyword quality
                if is_bad:
                    ctr = random.uniform(0.002, 0.008)  # low CTR
                elif kw["match"] == "exact":
                    ctr = random.uniform(0.006, 0.015) * time_mult
                else:
                    ctr = random.uniform(0.004, 0.010)

                clicks = max(0, int(impressions * ctr + random.gauss(0, 1)))

                # CPC
                cpc = kw["bid"] * random.uniform(0.6, 0.95)
                spend = round(clicks * cpc, 2)

                # Conversion rate
                if is_bad:
                    cvr = 0.0  # never converts
                elif kw["match"] == "exact":
                    cvr = random.uniform(0.06, 0.14) * time_mult
                else:
                    cvr = random.uniform(0.03, 0.09)

                orders = max(0, int(clicks * cvr + random.gauss(0, 0.5)))
                ad_sales = round(orders * product["price"] * random.uniform(0.95, 1.05), 2)

                # Computed metrics
                actual_ctr = clicks / impressions if impressions > 0 else 0
                actual_cpc = spend / clicks if clicks > 0 else 0
                actual_cvr = orders / clicks if clicks > 0 else 0
                acos = spend / ad_sales if ad_sales > 0 else None
                roas = ad_sales / spend if spend > 0 else None

                db.execute(
                    text("""
                        INSERT INTO daily_keyword_metrics
                            (keyword_id, campaign_id, asin, report_date,
                             impressions, clicks, spend, orders, ad_sales, units_sold,
                             ctr, cpc, cvr, acos, roas, bid_at_time)
                        VALUES (:kid, :cid, :asin, :date,
                                :imp, :clicks, :spend, :orders, :sales, :orders,
                                :ctr, :cpc, :cvr, :acos, :roas, :bid)
                        ON CONFLICT (keyword_id, report_date) DO NOTHING
                    """),
                    {"kid": kid, "cid": campaign_ids[kw["campaign_idx"]],
                     "asin": product["asin"], "date": report_day,
                     "imp": impressions, "clicks": clicks, "spend": spend,
                     "orders": orders, "sales": ad_sales,
                     "ctr": actual_ctr, "cpc": actual_cpc, "cvr": actual_cvr,
                     "acos": acos, "roas": roas, "bid": kw["bid"]},
                )
                total_rows += 1

        print(f"  Generated {total_rows} daily metric rows")

        # ---- Generate search term data (last 14 days) ----
        print("Generating search term data...")
        search_terms_data = [
            # Good search terms (should be harvested)
            {"term": "anc earbuds under 30", "kid_idx": 8, "clicks": 42, "orders": 5, "good": True},
            {"term": "wireless earbuds for gym", "kid_idx": 8, "clicks": 35, "orders": 3, "good": True},
            # Bad search terms (should be negated)
            {"term": "free bluetooth earbuds", "kid_idx": 8, "clicks": 28, "orders": 0, "good": False},
            {"term": "earbuds repair service", "kid_idx": 8, "clicks": 19, "orders": 0, "good": False},
            {"term": "how to fix earbuds", "kid_idx": 8, "clicks": 22, "orders": 0, "good": False},
        ]

        st_count = 0
        for st in search_terms_data:
            kid = keyword_ids[st["kid_idx"]]
            cid = campaign_ids[KEYWORDS[st["kid_idx"]]["campaign_idx"]]
            product = PRODUCTS[KEYWORDS[st["kid_idx"]]["asin_idx"]]

            for day_offset in range(14):
                report_day = today - timedelta(days=day_offset + 1)
                daily_clicks = max(0, st["clicks"] // 14 + random.randint(-1, 2))
                daily_orders = max(0, st["orders"] // 14 + (1 if random.random() < 0.3 and st["good"] else 0))
                daily_spend = round(daily_clicks * 0.65, 2)
                daily_sales = round(daily_orders * product["price"], 2)

                db.execute(
                    text("""
                        INSERT INTO search_terms
                            (keyword_id, campaign_id, search_term, match_type,
                             report_date, impressions, clicks, spend, orders, sales, acos)
                        VALUES (:kid, :cid, :term, 'broad', :date,
                                :imp, :clicks, :spend, :orders, :sales, :acos)
                    """),
                    {"kid": kid, "cid": cid, "term": st["term"], "date": report_day,
                     "imp": daily_clicks * random.randint(8, 15),
                     "clicks": daily_clicks, "spend": daily_spend,
                     "orders": daily_orders, "sales": daily_sales,
                     "acos": daily_spend / daily_sales if daily_sales > 0 else None},
                )
                st_count += 1

        print(f"  Generated {st_count} search term rows")

        # ---- Generate daily product sales ----
        print("Generating product sales data...")
        for day_offset in range(60):
            report_day = start_date + timedelta(days=day_offset)
            for p in PRODUCTS:
                units = random.randint(5, 30)
                revenue = round(units * p["price"] * random.uniform(0.95, 1.05), 2)
                db.execute(
                    text("""
                        INSERT INTO daily_product_sales (asin, report_date, units_sold, revenue)
                        VALUES (:asin, :date, :units, :revenue)
                        ON CONFLICT (asin, report_date) DO NOTHING
                    """),
                    {"asin": p["asin"], "date": report_day, "units": units, "revenue": revenue},
                )

        print("  Done")

    print("\n=== Seed complete! ===")
    print("You can now run:")
    print("  1. Feature engineering:  python -m scripts.run_features")
    print("  2. Model training:       python -m scripts.run_training")
    print("  3. FastAPI server:       uvicorn src.api.main:app --reload")
    print("  4. Streamlit dashboard:  streamlit run src/dashboard/app.py")


if __name__ == "__main__":
    seed()
