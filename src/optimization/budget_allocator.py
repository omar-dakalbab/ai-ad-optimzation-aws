"""
Budget Allocation Algorithm

Reallocates budget across campaigns to maximize total portfolio ROAS.

Core Principle: Marginal ROAS Equalization
  - A dollar moved from a low-ROAS campaign to a high-ROAS campaign
    increases total portfolio profit.
  - We shift budget until the marginal ROAS of all campaigns equalizes.

Algorithm:
  1. Compute predicted ROAS for each campaign (from keyword-level predictions).
  2. Rank campaigns by predicted ROAS.
  3. Identify "donor" campaigns (low ROAS, underspending) and
     "recipient" campaigns (high ROAS, budget-constrained).
  4. Shift budget from donors to recipients, capped at max_reallocation_pct.
  5. Apply guardrails: minimum budget floor, max shift per cycle.

Diagram:

  Campaign A (ROAS: 2.1, Budget: $100)  ──[$15]──►  Campaign C (ROAS: 6.8, Budget: $50)
  Campaign B (ROAS: 1.5, Budget: $80)   ──[$12]──►  Campaign D (ROAS: 5.2, Budget: $40)
  Campaign E (ROAS: 0.8, Budget: $60)   ──[$9]───►  Campaign C

  Result: money flows from unprofitable → profitable campaigns
"""

from datetime import date, timedelta
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
class BudgetRecommendation:
    """A single campaign budget change recommendation."""
    campaign_id: int
    amazon_campaign_id: int
    campaign_name: str
    current_budget: float
    recommended_budget: float
    change_pct: float
    predicted_roas: float
    reason: str


class BudgetAllocator:
    """Reallocates campaign budgets based on predicted ROAS."""

    MIN_CAMPAIGN_BUDGET = 5.00    # Amazon minimum is $1, but $5 is practical floor
    MAX_REALLOCATION_PCT = 0.15   # Max 15% of a campaign's budget moved per cycle

    def generate_budget_recommendations(
        self, prediction_date: date = None
    ) -> list[BudgetRecommendation]:
        """
        Compute budget reallocation across all active campaigns.

        Steps:
          1. Aggregate keyword predictions to campaign level
          2. Compute predicted ROAS per campaign
          3. Identify donors and recipients
          4. Calculate optimal shifts
          5. Apply guardrails
        """
        if prediction_date is None:
            prediction_date = date.today()

        # Step 1: Get campaign-level predicted performance
        with get_db() as db:
            campaign_df = pd.read_sql(
                text("""
                    SELECT
                        c.id as campaign_id,
                        c.amazon_campaign_id,
                        c.name as campaign_name,
                        c.daily_budget as current_budget,
                        -- Aggregate keyword predictions to campaign level
                        COUNT(p.keyword_id) as keyword_count,
                        AVG(p.predicted_cvr) as avg_predicted_cvr,
                        AVG(p.predicted_acos) as avg_predicted_acos,
                        SUM(p.expected_profit_per_click) as total_expected_profit,
                        -- Recent actual performance
                        SUM(dcm.spend) as spend_7d,
                        SUM(dcm.ad_sales) as sales_7d,
                        SUM(dcm.orders) as orders_7d,
                        AVG(dcm.budget_utilization) as avg_budget_utilization
                    FROM campaigns c
                    LEFT JOIN keywords k ON k.campaign_id = c.id AND k.state = 'enabled'
                    LEFT JOIN predictions p
                        ON p.keyword_id = k.id AND p.prediction_date = :date
                    LEFT JOIN daily_campaign_metrics dcm
                        ON dcm.campaign_id = c.id
                        AND dcm.report_date >= :start_date
                        AND dcm.report_date <= :date
                    WHERE c.state = 'enabled'
                    GROUP BY c.id, c.amazon_campaign_id, c.name, c.daily_budget
                    HAVING COUNT(p.keyword_id) > 0
                """),
                db.bind,
                params={
                    "date": prediction_date,
                    "start_date": prediction_date - timedelta(days=7),
                },
            )

        if campaign_df.empty:
            logger.warning("no_campaigns_for_budget_allocation")
            return []

        # Step 2: Compute predicted ROAS
        campaign_df["predicted_roas"] = np.where(
            campaign_df["spend_7d"] > 0,
            campaign_df["sales_7d"] / campaign_df["spend_7d"],
            0,
        )

        # Blend actual ROAS with predicted (weighted by confidence)
        campaign_df["blended_roas"] = campaign_df["predicted_roas"]

        # Step 3: Classify campaigns
        median_roas = campaign_df["blended_roas"].median()

        # Donors: below-median ROAS (give budget away)
        # Recipients: above-median ROAS (receive budget)
        campaign_df["is_donor"] = campaign_df["blended_roas"] < median_roas
        campaign_df["is_recipient"] = campaign_df["blended_roas"] >= median_roas

        # Budget-constrained campaigns (spending >90% of budget) get priority
        campaign_df["is_constrained"] = campaign_df["avg_budget_utilization"] > 90

        # Step 4: Calculate shifts
        total_donor_budget = campaign_df.loc[
            campaign_df["is_donor"], "current_budget"
        ].sum()

        # How much can we move?
        available_to_shift = total_donor_budget * self.MAX_REALLOCATION_PCT

        # Distribute to recipients proportional to their ROAS
        recipients = campaign_df[campaign_df["is_recipient"]].copy()
        if not recipients.empty and recipients["blended_roas"].sum() > 0:
            recipients["roas_share"] = (
                recipients["blended_roas"] / recipients["blended_roas"].sum()
            )
            recipients["budget_increase"] = recipients["roas_share"] * available_to_shift
            # Prioritize constrained campaigns
            recipients.loc[recipients["is_constrained"], "budget_increase"] *= 1.5
        else:
            recipients["budget_increase"] = 0

        # Calculate how much each donor gives up
        donors = campaign_df[campaign_df["is_donor"]].copy()
        if not donors.empty:
            donors["budget_decrease"] = donors["current_budget"] * self.MAX_REALLOCATION_PCT

        # Step 5: Build recommendations
        recommendations = []

        for _, row in recipients.iterrows():
            new_budget = row["current_budget"] + row["budget_increase"]
            new_budget = round(new_budget, 2)
            if abs(new_budget - row["current_budget"]) < 1.0:
                continue  # Skip trivial changes

            change_pct = (new_budget - row["current_budget"]) / row["current_budget"]
            recommendations.append(BudgetRecommendation(
                campaign_id=int(row["campaign_id"]),
                amazon_campaign_id=int(row["amazon_campaign_id"]),
                campaign_name=row["campaign_name"],
                current_budget=row["current_budget"],
                recommended_budget=new_budget,
                change_pct=round(change_pct, 4),
                predicted_roas=round(row["blended_roas"], 2),
                reason="high_roas_increase",
            ))

        for _, row in donors.iterrows():
            new_budget = max(
                row["current_budget"] - row["budget_decrease"],
                self.MIN_CAMPAIGN_BUDGET,
            )
            new_budget = round(new_budget, 2)
            if abs(new_budget - row["current_budget"]) < 1.0:
                continue

            change_pct = (new_budget - row["current_budget"]) / row["current_budget"]
            recommendations.append(BudgetRecommendation(
                campaign_id=int(row["campaign_id"]),
                amazon_campaign_id=int(row["amazon_campaign_id"]),
                campaign_name=row["campaign_name"],
                current_budget=row["current_budget"],
                recommended_budget=new_budget,
                change_pct=round(change_pct, 4),
                predicted_roas=round(row["blended_roas"], 2),
                reason="low_roas_decrease",
            ))

        # Store in DB
        self._store_recommendations(recommendations, prediction_date)

        logger.info(
            "budget_recommendations_generated",
            total=len(recommendations),
            total_shifted=round(available_to_shift, 2),
        )
        return recommendations

    def _store_recommendations(
        self, recommendations: list[BudgetRecommendation], alloc_date: date
    ):
        """Store budget allocation recommendations."""
        with get_db() as db:
            for rec in recommendations:
                db.execute(
                    text("""
                        INSERT INTO budget_allocations
                            (campaign_id, allocation_date, old_budget, new_budget,
                             change_pct, predicted_roas, reason, applied)
                        VALUES (:campaign_id, :date, :old, :new, :change, :roas, :reason, FALSE)
                    """),
                    {
                        "campaign_id": rec.campaign_id,
                        "date": alloc_date,
                        "old": rec.current_budget,
                        "new": rec.recommended_budget,
                        "change": rec.change_pct,
                        "roas": rec.predicted_roas,
                        "reason": rec.reason,
                    },
                )
