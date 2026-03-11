"""
Bid Optimization Engine

Converts ML predictions into concrete bid adjustments.

Core Formula:
  optimal_bid = P(conversion) * profit_per_order * target_efficiency_factor

  Where:
    P(conversion)           = predicted CVR from ML model
    profit_per_order        = product price - COGS - FBA fees - referral fee
    target_efficiency_factor = (1 - target_ACoS)

Example:
  P(conversion) = 0.12 (12% of clicks convert)
  profit_per_order = $8.00
  target_ACoS = 0.25

  optimal_bid = 0.12 * $8.00 * 0.75 = $0.72

Guardrails:
  - Max bid increase per cycle: 30%
  - Max bid decrease per cycle: 40%
  - Absolute bid floor: $0.10
  - Absolute bid ceiling: $15.00
  - Bids above $8.00 require human approval
  - Keywords with < 50 impressions get conservative bids
"""

from datetime import date
from dataclasses import dataclass

import pandas as pd
import numpy as np
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()


@dataclass
class BidRecommendation:
    """A single bid change recommendation."""
    keyword_id: int
    amazon_keyword_id: int
    campaign_id: int
    current_bid: float
    recommended_bid: float
    change_pct: float
    reason: str
    predicted_cvr: float
    predicted_acos: float
    confidence: float
    requires_approval: bool


class BidOptimizer:
    """
    Generates bid recommendations from ML predictions.

    Strategy tiers:
      1. High-confidence profitable keywords  → bid up toward optimal
      2. Moderate keywords                    → small adjustments
      3. Low-performing keywords              → bid down
      4. Zero-conversion keywords             → pause or minimum bid
    """

    def generate_bid_recommendations(self, prediction_date: date = None) -> list[BidRecommendation]:
        """
        Main method: load predictions, compute optimal bids, apply guardrails.

        Returns a list of BidRecommendation objects.
        """
        if prediction_date is None:
            prediction_date = date.today()

        # Load predictions + current bids
        with get_db() as db:
            df = pd.read_sql(
                text("""
                    SELECT
                        p.keyword_id,
                        k.amazon_keyword_id,
                        k.campaign_id,
                        k.bid as current_bid,
                        p.predicted_ctr,
                        p.predicted_cvr,
                        p.predicted_acos,
                        p.predicted_revenue,
                        p.expected_profit_per_click,
                        p.recommended_bid as ml_recommended_bid,
                        p.prediction_confidence,
                        p.data_points_used,
                        pr.margin as product_margin,
                        kf.clicks_30d,
                        kf.orders_30d,
                        kf.impressions_30d,
                        kf.acos_30d
                    FROM predictions p
                    JOIN keywords k ON k.id = p.keyword_id
                    LEFT JOIN products pr ON pr.asin = k.asin
                    LEFT JOIN keyword_features kf
                        ON kf.keyword_id = p.keyword_id
                        AND kf.computed_date = p.prediction_date
                    WHERE p.prediction_date = :date
                      AND k.state = 'enabled'
                """),
                db.bind,
                params={"date": prediction_date},
            )

        if df.empty:
            logger.warning("no_predictions_for_bid_optimization")
            return []

        recommendations = []
        for _, row in df.iterrows():
            rec = self._compute_bid(row)
            if rec:
                recommendations.append(rec)

        # Store recommendations in bid_history
        self._store_recommendations(recommendations)

        # Summary stats
        increases = sum(1 for r in recommendations if r.change_pct > 0)
        decreases = sum(1 for r in recommendations if r.change_pct < 0)
        logger.info(
            "bid_recommendations_generated",
            total=len(recommendations),
            increases=increases,
            decreases=decreases,
        )

        return recommendations

    def _compute_bid(self, row: pd.Series) -> BidRecommendation | None:
        """
        Compute the optimal bid for a single keyword.

        Decision tree:

        ┌─────────────────────────────────────────────────┐
        │ Has enough data? (impressions_30d >= 50)        │
        │                                                  │
        │ YES ──► Use ML-recommended bid                   │
        │   │                                              │
        │   ├── predicted_acos < target? ──► bid UP        │
        │   │   (keyword is profitable, capture more)      │
        │   │                                              │
        │   ├── predicted_acos > target * 1.5? ──► bid DOWN│
        │   │   (keyword is unprofitable)                  │
        │   │                                              │
        │   └── else ──► small adjustment toward optimal   │
        │                                                  │
        │ NO ──► Conservative: keep current or slight down │
        └─────────────────────────────────────────────────┘
        """
        current_bid = float(row["current_bid"] or 0)
        ml_bid = float(row["ml_recommended_bid"] or current_bid)
        predicted_cvr = float(row["predicted_cvr"] or 0)
        predicted_acos = float(row["predicted_acos"] or 999)
        confidence = float(row["prediction_confidence"] or 0)
        product_margin = float(row["product_margin"] or 0)
        impressions_30d = int(row["impressions_30d"] or 0)
        clicks_30d = int(row["clicks_30d"] or 0)
        orders_30d = int(row["orders_30d"] or 0)

        if current_bid <= 0:
            return None

        # ----- Strategy Selection -----

        if impressions_30d < settings.min_impressions_for_prediction:
            # Not enough data: keep current bid, wait for data
            return None

        if clicks_30d >= settings.pause_keyword_threshold_clicks and orders_30d == 0:
            # Lots of clicks, zero conversions → pause candidate
            new_bid = settings.min_bid
            reason = "pause_candidate_zero_conversions"
        elif predicted_acos < settings.target_acos and confidence > 0.5:
            # Profitable keyword with good confidence → bid toward ML recommendation
            # Scale adjustment by confidence
            new_bid = current_bid + (ml_bid - current_bid) * confidence
            reason = "profitable_bid_up"
        elif predicted_acos > settings.target_acos * 1.5:
            # Significantly unprofitable → bid down aggressively
            new_bid = current_bid * (1 - settings.max_bid_decrease_pct * confidence)
            reason = "unprofitable_bid_down"
        elif predicted_acos > settings.target_acos:
            # Slightly unprofitable → bid down conservatively
            new_bid = current_bid * (1 - 0.10 * confidence)
            reason = "marginal_bid_down"
        else:
            # Moderately profitable → nudge toward ML bid
            new_bid = current_bid + (ml_bid - current_bid) * 0.3 * confidence
            reason = "moderate_adjustment"

        # ----- Apply Guardrails -----

        # Cap maximum increase/decrease per cycle
        max_increase = current_bid * (1 + settings.max_bid_increase_pct)
        max_decrease = current_bid * (1 - settings.max_bid_decrease_pct)
        new_bid = np.clip(new_bid, max_decrease, max_increase)

        # Absolute floor and ceiling
        new_bid = np.clip(new_bid, settings.min_bid, settings.max_bid)

        # Round to 2 decimal places
        new_bid = round(float(new_bid), 2)

        # Skip if change is trivial (< 1 cent)
        if abs(new_bid - current_bid) < 0.01:
            return None

        change_pct = (new_bid - current_bid) / current_bid

        return BidRecommendation(
            keyword_id=int(row["keyword_id"]),
            amazon_keyword_id=int(row["amazon_keyword_id"]),
            campaign_id=int(row["campaign_id"]),
            current_bid=current_bid,
            recommended_bid=new_bid,
            change_pct=round(change_pct, 4),
            reason=reason,
            predicted_cvr=predicted_cvr,
            predicted_acos=predicted_acos,
            confidence=confidence,
            requires_approval=new_bid > settings.require_approval_above_bid,
        )

    def _store_recommendations(self, recommendations: list[BidRecommendation]):
        """Store bid recommendations in bid_history for audit trail."""
        with get_db() as db:
            for rec in recommendations:
                db.execute(
                    text("""
                        INSERT INTO bid_history
                            (keyword_id, campaign_id, old_bid, new_bid, change_pct,
                             reason, predicted_acos, applied)
                        VALUES (:keyword_id, :campaign_id, :old_bid, :new_bid,
                                :change_pct, :reason, :predicted_acos, FALSE)
                    """),
                    {
                        "keyword_id": rec.keyword_id,
                        "campaign_id": rec.campaign_id,
                        "old_bid": rec.current_bid,
                        "new_bid": rec.recommended_bid,
                        "change_pct": rec.change_pct,
                        "reason": rec.reason,
                        "predicted_acos": rec.predicted_acos,
                    },
                )
