"""
Data Loader - Orchestrates pulling data from Amazon APIs and storing in PostgreSQL.

This is called by the Airflow DAG daily.

Flow:
  1. Pull keyword report from Ads API → daily_keyword_metrics
  2. Pull search term report from Ads API → search_terms
  3. Pull campaign data from Ads API → campaigns, ad_groups
  4. Pull orders from SP-API → daily_product_sales
  5. Pull inventory from SP-API → products.inventory_level
"""

from datetime import date, timedelta

import pandas as pd
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from src.data_ingestion.ads_api_client import AmazonAdsClient
from src.data_ingestion.sp_api_client import SellingPartnerClient

logger = structlog.get_logger()


class DataLoader:
    """Loads data from Amazon APIs into the database."""

    def __init__(self):
        self.ads_client = AmazonAdsClient()
        self.sp_client = SellingPartnerClient()

    def run_daily_load(self, report_date: date = None):
        """
        Execute the full daily data load pipeline.

        Called by Airflow DAG at ~6:00 AM daily.
        Amazon Ads data has a 2-day lag, so we pull data for (today - 2).
        """
        if report_date is None:
            report_date = date.today() - timedelta(days=2)

        logger.info("daily_load_start", report_date=str(report_date))

        # Step 1: Sync campaign structure
        self._sync_campaigns()

        # Step 2: Load keyword performance report
        self._load_keyword_report(report_date)

        # Step 3: Load search term report
        self._load_search_term_report(report_date)

        # Step 4: Load sales data from SP-API
        self._load_sales_data(report_date)

        # Step 5: Update inventory levels
        self._update_inventory()

        # Step 6: Update campaign daily metrics
        self._aggregate_campaign_metrics(report_date)

        logger.info("daily_load_complete", report_date=str(report_date))

    def _sync_campaigns(self):
        """Sync campaign and ad group structure from Ads API."""
        campaigns = self.ads_client.get_campaigns()

        with get_db() as db:
            for c in campaigns:
                db.execute(
                    text("""
                        INSERT INTO campaigns (amazon_campaign_id, name, campaign_type,
                                               targeting_type, state, daily_budget)
                        VALUES (:cid, :name, :ctype, :ttype, :state, :budget)
                        ON CONFLICT (amazon_campaign_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            state = EXCLUDED.state,
                            daily_budget = EXCLUDED.daily_budget,
                            updated_at = NOW()
                    """),
                    {
                        "cid": c["campaignId"],
                        "name": c.get("name", ""),
                        "ctype": c.get("campaignType", "sponsoredProducts"),
                        "ttype": c.get("targetingType", "manual"),
                        "state": c.get("state", "enabled"),
                        "budget": c.get("dailyBudget", 0),
                    },
                )
        logger.info("campaigns_synced", count=len(campaigns))

    def _load_keyword_report(self, report_date: date):
        """Pull keyword-level performance and store in daily_keyword_metrics."""
        report_id = self.ads_client.request_sp_keyword_report(report_date)
        rows = self.ads_client.poll_report(report_id)

        with get_db() as db:
            for row in rows:
                try:
                    impressions = int(float(row.get("impressions", 0) or 0))
                    clicks = int(float(row.get("clicks", 0) or 0))
                    spend = float(row.get("cost", 0) or 0)
                    orders = int(float(row.get("purchases1d", 0) or 0))
                    sales = float(row.get("sales1d", 0) or 0)
                except (ValueError, TypeError) as e:
                    logger.warning("skipping_malformed_row", row=row, error=str(e))
                    continue

                ctr = clicks / impressions if impressions > 0 else 0
                cpc = spend / clicks if clicks > 0 else 0
                cvr = orders / clicks if clicks > 0 else 0
                acos = spend / sales if sales > 0 else None
                roas = sales / spend if spend > 0 else None

                db.execute(
                    text("""
                        INSERT INTO daily_keyword_metrics
                            (keyword_id, campaign_id, asin, report_date,
                             impressions, clicks, spend, orders, ad_sales,
                             ctr, cpc, cvr, acos, roas)
                        SELECT k.id, k.campaign_id, k.asin, :report_date,
                               :impressions, :clicks, :spend, :orders, :sales,
                               :ctr, :cpc, :cvr, :acos, :roas
                        FROM keywords k
                        WHERE k.amazon_keyword_id = :keyword_id
                        ON CONFLICT (keyword_id, report_date) DO UPDATE SET
                            impressions = EXCLUDED.impressions,
                            clicks = EXCLUDED.clicks,
                            spend = EXCLUDED.spend,
                            orders = EXCLUDED.orders,
                            ad_sales = EXCLUDED.ad_sales,
                            ctr = EXCLUDED.ctr,
                            cpc = EXCLUDED.cpc,
                            cvr = EXCLUDED.cvr,
                            acos = EXCLUDED.acos,
                            roas = EXCLUDED.roas
                    """),
                    {
                        "keyword_id": row.get("keywordId"),
                        "report_date": report_date,
                        "impressions": impressions,
                        "clicks": clicks,
                        "spend": spend,
                        "orders": orders,
                        "sales": sales,
                        "ctr": ctr,
                        "cpc": cpc,
                        "cvr": cvr,
                        "acos": acos,
                        "roas": roas,
                    },
                )

        logger.info("keyword_report_loaded", rows=len(rows), date=str(report_date))

    def _load_search_term_report(self, report_date: date):
        """Pull search term data and store in search_terms table."""
        report_id = self.ads_client.request_search_term_report(report_date)
        rows = self.ads_client.poll_report(report_id)

        with get_db() as db:
            for row in rows:
                spend = float(row.get("cost", 0))
                sales = float(row.get("sales1d", 0))

                db.execute(
                    text("""
                        INSERT INTO search_terms
                            (keyword_id, campaign_id, search_term, match_type,
                             report_date, impressions, clicks, spend, orders, sales, acos)
                        SELECT k.id, k.campaign_id, :search_term, :match_type,
                               :report_date, :impressions, :clicks, :spend,
                               :orders, :sales, :acos
                        FROM keywords k
                        WHERE k.amazon_keyword_id = :keyword_id
                    """),
                    {
                        "keyword_id": row.get("keywordId"),
                        "search_term": row.get("query", ""),
                        "match_type": row.get("matchType", ""),
                        "report_date": report_date,
                        "impressions": int(row.get("impressions", 0)),
                        "clicks": int(row.get("clicks", 0)),
                        "spend": spend,
                        "orders": int(row.get("purchases1d", 0)),
                        "sales": sales,
                        "acos": spend / sales if sales > 0 else None,
                    },
                )

        logger.info("search_term_report_loaded", rows=len(rows), date=str(report_date))

    def _load_sales_data(self, report_date: date):
        """Pull order/sales data from SP-API."""
        orders = self.sp_client.get_orders(report_date, report_date + timedelta(days=1))

        # Aggregate by ASIN
        asin_sales = {}
        for order in orders:
            items = self.sp_client.get_order_items(order["AmazonOrderId"])
            for item in items:
                asin = item.get("ASIN", "")
                if asin not in asin_sales:
                    asin_sales[asin] = {"units": 0, "revenue": 0}
                asin_sales[asin]["units"] += int(item.get("QuantityOrdered", 0))
                price = float(item.get("ItemPrice", {}).get("Amount", 0))
                asin_sales[asin]["revenue"] += price

        with get_db() as db:
            for asin, data in asin_sales.items():
                db.execute(
                    text("""
                        INSERT INTO daily_product_sales (asin, report_date, units_sold, revenue)
                        VALUES (:asin, :date, :units, :revenue)
                        ON CONFLICT (asin, report_date) DO UPDATE SET
                            units_sold = EXCLUDED.units_sold,
                            revenue = EXCLUDED.revenue
                    """),
                    {
                        "asin": asin,
                        "date": report_date,
                        "units": data["units"],
                        "revenue": data["revenue"],
                    },
                )

        logger.info("sales_data_loaded", asins=len(asin_sales), date=str(report_date))

    def _update_inventory(self):
        """Update current inventory levels."""
        inventories = self.sp_client.get_inventory_levels()

        with get_db() as db:
            for inv in inventories:
                asin = inv.get("asin", "")
                qty = inv.get("totalQuantity", 0)
                db.execute(
                    text("""
                        UPDATE products SET inventory_level = :qty, updated_at = NOW()
                        WHERE asin = :asin
                    """),
                    {"asin": asin, "qty": qty},
                )

        logger.info("inventory_updated", count=len(inventories))

    def _aggregate_campaign_metrics(self, report_date: date):
        """Roll up keyword metrics to campaign level."""
        with get_db() as db:
            db.execute(
                text("""
                    INSERT INTO daily_campaign_metrics
                        (campaign_id, report_date, impressions, clicks, spend,
                         orders, ad_sales, acos, roas)
                    SELECT
                        campaign_id,
                        report_date,
                        SUM(impressions),
                        SUM(clicks),
                        SUM(spend),
                        SUM(orders),
                        SUM(ad_sales),
                        CASE WHEN SUM(ad_sales) > 0
                             THEN SUM(spend) / SUM(ad_sales) ELSE NULL END,
                        CASE WHEN SUM(spend) > 0
                             THEN SUM(ad_sales) / SUM(spend) ELSE NULL END
                    FROM daily_keyword_metrics
                    WHERE report_date = :date
                    GROUP BY campaign_id, report_date
                    ON CONFLICT (campaign_id, report_date) DO UPDATE SET
                        impressions = EXCLUDED.impressions,
                        clicks = EXCLUDED.clicks,
                        spend = EXCLUDED.spend,
                        orders = EXCLUDED.orders,
                        ad_sales = EXCLUDED.ad_sales,
                        acos = EXCLUDED.acos,
                        roas = EXCLUDED.roas
                """),
                {"date": report_date},
            )

        logger.info("campaign_metrics_aggregated", date=str(report_date))
