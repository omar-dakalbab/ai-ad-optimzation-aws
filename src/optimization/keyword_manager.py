"""
Keyword Management Service

Automates three key keyword operations:

1. HARVEST: Find high-performing search terms from auto/broad campaigns
   and add them as exact-match keywords in manual campaigns.

2. NEGATE: Find wasteful search terms (high spend, zero conversions)
   and add them as negative keywords.

3. PAUSE: Pause keywords that consistently underperform despite bid optimization.

Search Term Harvesting Flow:
  ┌──────────────────────┐    ┌───────────────────────┐
  │  Auto Campaign       │    │  Manual Campaign       │
  │  (broad discovery)   │    │  (exact targeting)     │
  │                      │    │                        │
  │  search term "xyz"   │    │  keyword "xyz" (exact) │
  │  3 orders, $45 sales │──► │  bid optimized by ML   │
  │  ACoS: 15%          │    │                        │
  └──────────────────────┘    └───────────────────────┘

Negative Keyword Flow:
  search term "free samples" → 50 clicks, $25 spend, 0 orders
  → Add as negative exact in campaign
  → Stop wasting money on irrelevant traffic
"""

from datetime import date, timedelta
from dataclasses import dataclass

import pandas as pd
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()


@dataclass
class KeywordAction:
    """A keyword management action to execute."""
    action: str           # 'add_keyword', 'add_negative', 'pause_keyword'
    campaign_id: int
    ad_group_id: int | None
    keyword_text: str
    match_type: str
    suggested_bid: float | None
    reason: str
    metrics: dict         # supporting data for the decision


class KeywordManager:
    """Manages keyword harvesting, negation, and pausing."""

    # --- Thresholds ---
    HARVEST_MIN_ORDERS = 2          # minimum orders to consider harvesting
    HARVEST_MIN_CVR = 0.05          # minimum 5% CVR
    HARVEST_MAX_ACOS = 0.30         # must be below 30% ACoS
    NEGATE_MIN_CLICKS = 15          # minimum clicks before negating
    NEGATE_MIN_SPEND = 20.0         # minimum $20 spend
    PAUSE_MIN_CLICKS = 50           # minimum clicks before pausing
    PAUSE_LOOKBACK_DAYS = 30        # evaluation window

    def run_keyword_management(self, eval_date: date = None) -> list[KeywordAction]:
        """Execute all keyword management operations."""
        if eval_date is None:
            eval_date = date.today()

        actions = []
        actions.extend(self._harvest_search_terms(eval_date))
        actions.extend(self._find_negative_candidates(eval_date))
        actions.extend(self._find_pause_candidates(eval_date))

        logger.info(
            "keyword_management_complete",
            harvested=sum(1 for a in actions if a.action == "add_keyword"),
            negated=sum(1 for a in actions if a.action == "add_negative"),
            paused=sum(1 for a in actions if a.action == "pause_keyword"),
        )
        return actions

    def _harvest_search_terms(self, eval_date: date) -> list[KeywordAction]:
        """
        Find converting search terms from auto/broad campaigns
        that should be added as exact keywords.

        Criteria:
          - At least 2 orders in last 14 days
          - CVR >= 5%
          - ACoS <= 30%
          - Not already an exact keyword in the account
        """
        with get_db() as db:
            candidates = pd.read_sql(
                text("""
                    WITH search_term_perf AS (
                        SELECT
                            st.search_term,
                            st.campaign_id,
                            k.ad_group_id,
                            SUM(st.clicks) as total_clicks,
                            SUM(st.orders) as total_orders,
                            SUM(st.spend) as total_spend,
                            SUM(st.sales) as total_sales
                        FROM search_terms st
                        JOIN keywords k ON k.id = st.keyword_id
                        WHERE st.report_date >= :start_date
                          AND st.report_date <= :end_date
                        GROUP BY st.search_term, st.campaign_id, k.ad_group_id
                    ),
                    existing_exact AS (
                        SELECT LOWER(keyword_text) as keyword_text
                        FROM keywords
                        WHERE match_type = 'exact' AND state = 'enabled'
                    )
                    SELECT stp.*
                    FROM search_term_perf stp
                    WHERE stp.total_orders >= :min_orders
                      AND stp.total_clicks > 0
                      AND (stp.total_orders::float / stp.total_clicks) >= :min_cvr
                      AND stp.total_spend > 0
                      AND (stp.total_spend / stp.total_sales) <= :max_acos
                      AND LOWER(stp.search_term) NOT IN (SELECT keyword_text FROM existing_exact)
                    ORDER BY stp.total_orders DESC
                """),
                db.bind,
                params={
                    "start_date": eval_date - timedelta(days=14),
                    "end_date": eval_date,
                    "min_orders": self.HARVEST_MIN_ORDERS,
                    "min_cvr": self.HARVEST_MIN_CVR,
                    "max_acos": self.HARVEST_MAX_ACOS,
                },
            )

        actions = []
        for _, row in candidates.iterrows():
            cvr = row["total_orders"] / row["total_clicks"]
            acos = row["total_spend"] / row["total_sales"] if row["total_sales"] > 0 else 999

            # Suggest initial bid based on historical CPC * CVR multiplier
            avg_cpc = row["total_spend"] / row["total_clicks"]
            suggested_bid = round(avg_cpc * 1.1, 2)  # 10% premium for exact match

            actions.append(KeywordAction(
                action="add_keyword",
                campaign_id=int(row["campaign_id"]),
                ad_group_id=int(row["ad_group_id"]),
                keyword_text=row["search_term"],
                match_type="exact",
                suggested_bid=suggested_bid,
                reason=f"harvest: {row['total_orders']} orders, CVR={cvr:.1%}, ACoS={acos:.1%}",
                metrics={
                    "orders": int(row["total_orders"]),
                    "clicks": int(row["total_clicks"]),
                    "spend": float(row["total_spend"]),
                    "sales": float(row["total_sales"]),
                    "cvr": cvr,
                    "acos": acos,
                },
            ))

        logger.info("search_terms_harvested", count=len(actions))
        return actions

    def _find_negative_candidates(self, eval_date: date) -> list[KeywordAction]:
        """
        Find search terms wasting ad spend with zero conversions.

        Criteria:
          - At least 15 clicks in last 14 days
          - Zero orders
          - At least $20 spent
        """
        with get_db() as db:
            candidates = pd.read_sql(
                text("""
                    SELECT
                        st.search_term,
                        st.campaign_id,
                        SUM(st.clicks) as total_clicks,
                        SUM(st.spend) as total_spend,
                        SUM(st.orders) as total_orders
                    FROM search_terms st
                    WHERE st.report_date >= :start_date
                      AND st.report_date <= :end_date
                    GROUP BY st.search_term, st.campaign_id
                    HAVING SUM(st.orders) = 0
                       AND SUM(st.clicks) >= :min_clicks
                       AND SUM(st.spend) >= :min_spend
                    ORDER BY SUM(st.spend) DESC
                """),
                db.bind,
                params={
                    "start_date": eval_date - timedelta(days=14),
                    "end_date": eval_date,
                    "min_clicks": self.NEGATE_MIN_CLICKS,
                    "min_spend": self.NEGATE_MIN_SPEND,
                },
            )

        actions = []
        for _, row in candidates.iterrows():
            actions.append(KeywordAction(
                action="add_negative",
                campaign_id=int(row["campaign_id"]),
                ad_group_id=None,  # campaign-level negative
                keyword_text=row["search_term"],
                match_type="negativeExact",
                suggested_bid=None,
                reason=f"negate: {row['total_clicks']} clicks, ${row['total_spend']:.2f} spent, 0 orders",
                metrics={
                    "clicks": int(row["total_clicks"]),
                    "spend": float(row["total_spend"]),
                },
            ))

        logger.info("negative_candidates_found", count=len(actions))
        return actions

    def _find_pause_candidates(self, eval_date: date) -> list[KeywordAction]:
        """
        Find keywords that should be paused due to persistent underperformance.

        Criteria:
          - At least 50 clicks in last 30 days
          - Zero orders
          - OR: ACoS consistently > 2x target ACoS for 30 days
        """
        with get_db() as db:
            candidates = pd.read_sql(
                text("""
                    SELECT
                        k.id as keyword_id,
                        k.keyword_text,
                        k.match_type,
                        k.campaign_id,
                        k.ad_group_id,
                        SUM(m.clicks) as total_clicks,
                        SUM(m.spend) as total_spend,
                        SUM(m.orders) as total_orders,
                        SUM(m.ad_sales) as total_sales
                    FROM keywords k
                    JOIN daily_keyword_metrics m ON m.keyword_id = k.id
                    WHERE k.state = 'enabled'
                      AND m.report_date >= :start_date
                      AND m.report_date <= :end_date
                    GROUP BY k.id, k.keyword_text, k.match_type, k.campaign_id, k.ad_group_id
                    HAVING (
                        -- Case 1: lots of clicks, zero conversions
                        (SUM(m.clicks) >= :min_clicks AND SUM(m.orders) = 0)
                        OR
                        -- Case 2: ACoS > 2x target for significant spend
                        (SUM(m.ad_sales) > 0
                         AND SUM(m.spend) / SUM(m.ad_sales) > :max_acos
                         AND SUM(m.spend) >= :min_spend)
                    )
                    ORDER BY SUM(m.spend) DESC
                """),
                db.bind,
                params={
                    "start_date": eval_date - timedelta(days=self.PAUSE_LOOKBACK_DAYS),
                    "end_date": eval_date,
                    "min_clicks": self.PAUSE_MIN_CLICKS,
                    "max_acos": settings.target_acos * 2,
                    "min_spend": self.NEGATE_MIN_SPEND,
                },
            )

        actions = []
        for _, row in candidates.iterrows():
            acos = (
                row["total_spend"] / row["total_sales"]
                if row["total_sales"] > 0 else float("inf")
            )
            actions.append(KeywordAction(
                action="pause_keyword",
                campaign_id=int(row["campaign_id"]),
                ad_group_id=int(row["ad_group_id"]) if row["ad_group_id"] else None,
                keyword_text=row["keyword_text"],
                match_type=row["match_type"],
                suggested_bid=None,
                reason=f"pause: {row['total_clicks']} clicks, {row['total_orders']} orders, ACoS={acos:.1%}",
                metrics={
                    "clicks": int(row["total_clicks"]),
                    "spend": float(row["total_spend"]),
                    "orders": int(row["total_orders"]),
                    "acos": acos,
                },
            ))

        logger.info("pause_candidates_found", count=len(actions))
        return actions
