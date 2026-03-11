"""
Feature Engineering Pipeline
Computes ML features from raw performance data and stores in keyword_features table.

Feature Categories:
  1. Rolling performance metrics (7d, 14d, 30d windows)
  2. Trend features (direction & magnitude of change)
  3. Product features (price, margin, rating, inventory)
  4. Keyword metadata features (length, match type, age)
  5. Seasonality features (day of week, month, holidays)
  6. Competition proxy features (impression share estimates)

Run daily after data ingestion completes.
"""

from datetime import date, timedelta

import pandas as pd
import numpy as np
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()

# Holiday / event dates (update annually)
PRIME_DAY_DATES = {
    date(2025, 7, 15), date(2025, 7, 16),
    date(2026, 7, 14), date(2026, 7, 15),
}
HOLIDAY_SEASON_START = 11  # November
HOLIDAY_SEASON_END = 12    # December


class FeatureEngineer:
    """Computes and stores ML features for every active keyword."""

    def compute_features(self, compute_date: date = None):
        """
        Main entry point. Computes features for all active keywords.

        Steps:
          1. Pull raw daily metrics for each keyword (last 90 days)
          2. Compute rolling aggregates
          3. Compute trend slopes
          4. Join product data
          5. Add seasonality features
          6. Store in keyword_features table
        """
        if compute_date is None:
            compute_date = date.today()

        logger.info("feature_computation_start", date=str(compute_date))

        with get_db() as db:
            # Get all active keywords with their daily metrics
            df = pd.read_sql(
                text("""
                    SELECT
                        k.id as keyword_id,
                        k.keyword_text,
                        k.match_type,
                        k.asin,
                        k.created_at as keyword_created_at,
                        k.bid as current_bid,
                        m.report_date,
                        m.impressions,
                        m.clicks,
                        m.spend,
                        m.orders,
                        m.ad_sales,
                        m.ctr,
                        m.cpc,
                        m.cvr,
                        m.acos,
                        m.roas,
                        p.price as product_price,
                        p.margin as product_margin,
                        p.margin_pct as product_margin_pct,
                        p.rating as product_rating,
                        p.review_count as product_review_count,
                        p.inventory_level
                    FROM keywords k
                    JOIN daily_keyword_metrics m ON m.keyword_id = k.id
                    LEFT JOIN products p ON p.asin = k.asin
                    WHERE k.state = 'enabled'
                      AND m.report_date >= :start_date
                      AND m.report_date <= :end_date
                    ORDER BY k.id, m.report_date
                """),
                db.bind,
                params={
                    "start_date": compute_date - timedelta(days=settings.prediction_lookback_days),
                    "end_date": compute_date,
                },
            )

        if df.empty:
            logger.warning("no_data_for_features", date=str(compute_date))
            return

        # Process each keyword
        features_list = []
        for keyword_id, kw_df in df.groupby("keyword_id"):
            kw_df = kw_df.sort_values("report_date")
            features = self._compute_keyword_features(keyword_id, kw_df, compute_date)
            if features:
                features_list.append(features)

        # Bulk insert features
        if features_list:
            self._store_features(features_list, compute_date)

        logger.info("feature_computation_complete", keywords=len(features_list))

    def _compute_keyword_features(
        self, keyword_id: int, df: pd.DataFrame, compute_date: date
    ) -> dict | None:
        """Compute all features for a single keyword."""

        if len(df) < 3:
            return None  # Not enough data

        # ----- Rolling Performance Metrics -----
        features = {"keyword_id": keyword_id, "computed_date": compute_date}

        for window in [7, 14, 30]:
            window_df = df[df["report_date"] >= compute_date - timedelta(days=window)]

            total_impressions = window_df["impressions"].sum()
            total_clicks = window_df["clicks"].sum()
            total_spend = window_df["spend"].sum()
            total_orders = window_df["orders"].sum()
            total_sales = window_df["ad_sales"].sum()

            features[f"ctr_{window}d"] = (
                total_clicks / total_impressions if total_impressions > 0 else 0
            )
            features[f"cvr_{window}d"] = (
                total_orders / total_clicks if total_clicks > 0 else 0
            )
            features[f"acos_{window}d"] = (
                total_spend / total_sales if total_sales > 0 else None
            )
            features[f"cpc_{window}d"] = (
                total_spend / total_clicks if total_clicks > 0 else 0
            )
            features[f"roas_{window}d"] = (
                total_sales / total_spend if total_spend > 0 else None
            )

            if window in [7, 30]:
                features[f"impressions_{window}d"] = int(total_impressions)
                features[f"clicks_{window}d"] = int(total_clicks)
                features[f"spend_{window}d"] = float(total_spend)
                features[f"orders_{window}d"] = int(total_orders)

        # ----- Trend Features (linear regression slope over 14 days) -----
        recent = df[df["report_date"] >= compute_date - timedelta(days=14)].copy()
        if len(recent) >= 3:
            recent["day_num"] = range(len(recent))

            features["ctr_trend"] = self._compute_slope(recent["day_num"], recent["ctr"])
            features["cvr_trend"] = self._compute_slope(recent["day_num"], recent["cvr"])
            features["spend_trend"] = self._compute_slope(recent["day_num"], recent["spend"])
            features["sales_trend"] = self._compute_slope(recent["day_num"], recent["ad_sales"])
            features["impression_trend"] = self._compute_slope(
                recent["day_num"], recent["impressions"]
            )
        else:
            features["ctr_trend"] = 0
            features["cvr_trend"] = 0
            features["spend_trend"] = 0
            features["sales_trend"] = 0
            features["impression_trend"] = 0

        # ----- Product Features (from latest row) -----
        latest = df.iloc[-1]
        features["product_price"] = latest.get("product_price")
        features["product_margin"] = latest.get("product_margin")
        features["product_margin_pct"] = latest.get("product_margin_pct")
        features["product_rating"] = latest.get("product_rating")
        features["product_review_count"] = latest.get("product_review_count")
        features["inventory_level"] = latest.get("inventory_level")

        # Price change over 7 days
        # (We'd need historical price data; approximate with product table)
        features["price_change_7d"] = 0  # placeholder - enhance with price history

        # ----- Keyword Metadata -----
        features["keyword_length"] = len(latest["keyword_text"].split())
        features["match_type_encoded"] = {"exact": 0, "phrase": 1, "broad": 2}.get(
            latest["match_type"], 2
        )
        keyword_age = (compute_date - latest["keyword_created_at"].date()).days
        features["keyword_age_days"] = keyword_age

        # ----- Seasonality Features -----
        features["day_of_week"] = compute_date.weekday()
        features["is_weekend"] = compute_date.weekday() >= 5
        features["month"] = compute_date.month
        features["is_prime_day"] = compute_date in PRIME_DAY_DATES
        features["is_holiday_season"] = compute_date.month in (
            HOLIDAY_SEASON_START,
            HOLIDAY_SEASON_END,
        )

        # ----- Competition Proxy -----
        # Impression share estimate: compare current impressions to 30d average
        avg_daily_impressions = features.get("impressions_30d", 0) / 30
        recent_daily_impressions = features.get("impressions_7d", 0) / 7
        if avg_daily_impressions > 0:
            features["impression_share_est"] = min(
                recent_daily_impressions / avg_daily_impressions, 2.0
            )
        else:
            features["impression_share_est"] = 0

        # Position estimate from CTR (higher CTR ≈ better position)
        ctr_30 = features.get("ctr_30d", 0)
        if ctr_30 > 0.01:
            features["avg_position_est"] = 1.0  # top of search
        elif ctr_30 > 0.005:
            features["avg_position_est"] = 2.0
        elif ctr_30 > 0.002:
            features["avg_position_est"] = 3.0
        else:
            features["avg_position_est"] = 4.0

        return features

    @staticmethod
    def _compute_slope(x: pd.Series, y: pd.Series) -> float:
        """Compute linear regression slope. Returns 0 if not enough data."""
        if len(x) < 2 or y.std() == 0:
            return 0.0
        try:
            slope = np.polyfit(x.values, y.fillna(0).values, 1)[0]
            return float(slope)
        except (np.linalg.LinAlgError, ValueError):
            return 0.0

    @staticmethod
    def _convert_numpy_types(d: dict) -> dict:
        """Convert numpy types to Python natives for psycopg2 compatibility."""
        converted = {}
        for k, v in d.items():
            if isinstance(v, (np.integer,)):
                converted[k] = int(v)
            elif isinstance(v, (np.floating,)):
                converted[k] = float(v) if not np.isnan(v) else None
            elif isinstance(v, np.bool_):
                converted[k] = bool(v)
            else:
                converted[k] = v
        return converted

    def _store_features(self, features_list: list[dict], compute_date: date):
        """Bulk upsert features into keyword_features table."""
        with get_db() as db:
            for f in [self._convert_numpy_types(feat) for feat in features_list]:
                db.execute(
                    text("""
                        INSERT INTO keyword_features (
                            keyword_id, computed_date,
                            ctr_7d, ctr_14d, ctr_30d,
                            cvr_7d, cvr_14d, cvr_30d,
                            acos_7d, acos_14d, acos_30d,
                            cpc_7d, cpc_14d, cpc_30d,
                            roas_7d, roas_14d, roas_30d,
                            impressions_7d, impressions_30d,
                            clicks_7d, clicks_30d,
                            spend_7d, spend_30d,
                            orders_7d, orders_30d,
                            ctr_trend, cvr_trend, spend_trend, sales_trend, impression_trend,
                            product_price, product_margin, product_margin_pct,
                            product_rating, product_review_count, inventory_level,
                            price_change_7d,
                            keyword_length, match_type_encoded, keyword_age_days,
                            day_of_week, is_weekend, month, is_prime_day, is_holiday_season,
                            impression_share_est, avg_position_est
                        ) VALUES (
                            :keyword_id, :computed_date,
                            :ctr_7d, :ctr_14d, :ctr_30d,
                            :cvr_7d, :cvr_14d, :cvr_30d,
                            :acos_7d, :acos_14d, :acos_30d,
                            :cpc_7d, :cpc_14d, :cpc_30d,
                            :roas_7d, :roas_14d, :roas_30d,
                            :impressions_7d, :impressions_30d,
                            :clicks_7d, :clicks_30d,
                            :spend_7d, :spend_30d,
                            :orders_7d, :orders_30d,
                            :ctr_trend, :cvr_trend, :spend_trend, :sales_trend, :impression_trend,
                            :product_price, :product_margin, :product_margin_pct,
                            :product_rating, :product_review_count, :inventory_level,
                            :price_change_7d,
                            :keyword_length, :match_type_encoded, :keyword_age_days,
                            :day_of_week, :is_weekend, :month, :is_prime_day, :is_holiday_season,
                            :impression_share_est, :avg_position_est
                        )
                        ON CONFLICT (keyword_id, computed_date) DO UPDATE SET
                            ctr_7d = EXCLUDED.ctr_7d, ctr_14d = EXCLUDED.ctr_14d,
                            ctr_30d = EXCLUDED.ctr_30d,
                            cvr_7d = EXCLUDED.cvr_7d, cvr_14d = EXCLUDED.cvr_14d,
                            cvr_30d = EXCLUDED.cvr_30d,
                            acos_7d = EXCLUDED.acos_7d, acos_14d = EXCLUDED.acos_14d,
                            acos_30d = EXCLUDED.acos_30d,
                            cpc_7d = EXCLUDED.cpc_7d, cpc_14d = EXCLUDED.cpc_14d,
                            cpc_30d = EXCLUDED.cpc_30d,
                            roas_7d = EXCLUDED.roas_7d, roas_14d = EXCLUDED.roas_14d,
                            roas_30d = EXCLUDED.roas_30d,
                            impressions_7d = EXCLUDED.impressions_7d,
                            impressions_30d = EXCLUDED.impressions_30d,
                            clicks_7d = EXCLUDED.clicks_7d, clicks_30d = EXCLUDED.clicks_30d,
                            spend_7d = EXCLUDED.spend_7d, spend_30d = EXCLUDED.spend_30d,
                            orders_7d = EXCLUDED.orders_7d, orders_30d = EXCLUDED.orders_30d,
                            ctr_trend = EXCLUDED.ctr_trend, cvr_trend = EXCLUDED.cvr_trend,
                            spend_trend = EXCLUDED.spend_trend,
                            sales_trend = EXCLUDED.sales_trend,
                            impression_trend = EXCLUDED.impression_trend,
                            product_price = EXCLUDED.product_price,
                            product_margin = EXCLUDED.product_margin,
                            product_margin_pct = EXCLUDED.product_margin_pct,
                            product_rating = EXCLUDED.product_rating,
                            product_review_count = EXCLUDED.product_review_count,
                            inventory_level = EXCLUDED.inventory_level,
                            impression_share_est = EXCLUDED.impression_share_est,
                            avg_position_est = EXCLUDED.avg_position_est
                    """),
                    f,
                )

        logger.info("features_stored", count=len(features_list))


# ----- FEATURE VECTOR FOR ML MODELS -----
# These are the columns the models expect as input

CLICK_MODEL_FEATURES = [
    "ctr_7d", "ctr_14d", "ctr_30d",
    "impressions_7d", "impressions_30d",
    "ctr_trend", "impression_trend",
    "product_price", "product_rating", "product_review_count",
    "keyword_length", "match_type_encoded", "keyword_age_days",
    "day_of_week", "is_weekend", "month",
    "is_prime_day", "is_holiday_season",
    "impression_share_est", "avg_position_est",
    "cpc_7d",
]

CONVERSION_MODEL_FEATURES = [
    "cvr_7d", "cvr_14d", "cvr_30d",
    "ctr_7d", "ctr_30d",
    "clicks_7d", "clicks_30d",
    "orders_7d", "orders_30d",
    "cvr_trend", "sales_trend",
    "product_price", "product_margin_pct",
    "product_rating", "product_review_count",
    "inventory_level",
    "keyword_length", "match_type_encoded", "keyword_age_days",
    "day_of_week", "is_weekend", "month",
    "is_prime_day", "is_holiday_season",
    "acos_7d", "roas_7d",
]

REVENUE_MODEL_FEATURES = [
    "cvr_7d", "cvr_30d",
    "acos_7d", "acos_30d",
    "roas_7d", "roas_30d",
    "spend_7d", "spend_30d",
    "orders_7d", "orders_30d",
    "sales_trend", "spend_trend",
    "product_price", "product_margin",
    "product_margin_pct",
    "inventory_level",
    "keyword_length", "match_type_encoded",
    "month", "is_holiday_season",
]
