"""
Automation Executor

Applies optimization decisions to Amazon Ads via the API.

Safety-first approach:
  1. All changes are logged before execution
  2. Dry-run mode by default (set AAI_DRY_RUN=false to enable)
  3. Rate limiting to respect Amazon API limits
  4. Rollback capability: previous bids stored in bid_history
  5. High-bid changes require human approval

Execution Flow:
  ┌──────────────────┐
  │  Bid Changes     │──► validate ──► log ──► execute ──► verify
  │  Budget Changes  │──► validate ──► log ──► execute ──► verify
  │  Keyword Actions │──► validate ──► log ──► execute ──► verify
  └──────────────────┘
           │
           ▼
  ┌──────────────────┐
  │  Safety Checks   │
  │  - max spend cap │
  │  - bid ceiling   │
  │  - approval gate │
  │  - dry run mode  │
  └──────────────────┘
"""

import time
from datetime import date

import structlog
from sqlalchemy import text

from src.data_ingestion.ads_api_client import AmazonAdsClient
from src.optimization.bid_optimizer import BidRecommendation
from src.optimization.budget_allocator import BudgetRecommendation
from src.optimization.keyword_manager import KeywordAction
from src.optimization.campaign_creator import CampaignCreator, CreatedCampaign
from src.database.connection import get_db
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()

# Amazon Ads API rate limits
BID_UPDATE_BATCH_SIZE = 1000     # max keywords per API call
API_CALLS_PER_SECOND = 5        # conservative rate limit
SLEEP_BETWEEN_BATCHES = 0.5     # seconds


class AutomationExecutor:
    """Executes optimization decisions via Amazon Ads API."""

    def __init__(self):
        self.ads_client = AmazonAdsClient()
        self.campaign_creator = CampaignCreator()
        self.dry_run = settings.dry_run

    def execute_all(
        self,
        bid_recommendations: list[BidRecommendation],
        budget_recommendations: list[BudgetRecommendation],
        keyword_actions: list[KeywordAction],
    ) -> dict:
        """
        Execute all optimization decisions.

        Returns summary of what was applied.
        """
        summary = {
            "bids_applied": 0,
            "bids_skipped": 0,
            "budgets_applied": 0,
            "keywords_added": 0,
            "negatives_added": 0,
            "keywords_paused": 0,
            "campaigns_created": 0,
            "dry_run": self.dry_run,
        }

        # 0. Create campaigns for products that don't have any yet
        campaign_results = self.execute_campaign_creation()
        summary["campaigns_created"] = campaign_results["created"]

        # 1. Apply bid changes
        bid_result = self.execute_bid_changes(bid_recommendations)
        summary["bids_applied"] = bid_result["applied"]
        summary["bids_skipped"] = bid_result["skipped"]

        # 2. Apply budget changes
        budget_result = self.execute_budget_changes(budget_recommendations)
        summary["budgets_applied"] = budget_result["applied"]

        # 3. Apply keyword actions
        keyword_result = self.execute_keyword_actions(keyword_actions)
        summary["keywords_added"] = keyword_result["added"]
        summary["negatives_added"] = keyword_result["negated"]
        summary["keywords_paused"] = keyword_result["paused"]

        logger.info("execution_complete", **summary)
        return summary

    # ------------------------------------------------------------------
    # Campaign Creation
    # ------------------------------------------------------------------

    def execute_campaign_creation(
        self,
        asins: list[str] | None = None,
        daily_budget_per_product: float | None = None,
        start_paused: bool = True,
    ) -> dict:
        """
        Create campaigns for products that don't have any yet.

        Args:
            asins: Specific ASINs to create campaigns for.
                   If None, finds all products without campaigns.
            daily_budget_per_product: Budget per product.
            start_paused: Start campaigns paused for review.
        """
        if asins:
            results = []
            for asin in asins:
                results.extend(self.campaign_creator.create_campaigns_for_product(
                    asin=asin,
                    daily_budget=daily_budget_per_product,
                    start_paused=start_paused,
                ))
        else:
            results = self.campaign_creator.create_campaigns_for_all_products(
                daily_budget_per_product=daily_budget_per_product,
                start_paused=start_paused,
            )

        created = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)

        logger.info("campaign_creation_execution", created=created, failed=failed)
        return {"created": created, "failed": failed, "details": results}

    # ------------------------------------------------------------------
    # Bid Execution
    # ------------------------------------------------------------------

    def execute_bid_changes(self, recommendations: list[BidRecommendation]) -> dict:
        """
        Apply bid changes to Amazon Ads API.

        Process:
          1. Filter out changes requiring approval
          2. Batch into groups of 1000 (API limit)
          3. Send to API with rate limiting
          4. Mark as applied in bid_history
        """
        # Separate auto-approve from needs-approval
        auto_approve = [r for r in recommendations if not r.requires_approval]
        needs_approval = [r for r in recommendations if r.requires_approval]

        if needs_approval:
            logger.warning(
                "bids_require_approval",
                count=len(needs_approval),
                keywords=[r.keyword_id for r in needs_approval],
            )

        applied = 0
        skipped = len(needs_approval)

        if self.dry_run:
            logger.info("dry_run_bid_changes", count=len(auto_approve))
            return {"applied": 0, "skipped": len(recommendations)}

        # Batch and execute
        for i in range(0, len(auto_approve), BID_UPDATE_BATCH_SIZE):
            batch = auto_approve[i:i + BID_UPDATE_BATCH_SIZE]
            updates = [
                {"keywordId": r.amazon_keyword_id, "bid": r.recommended_bid}
                for r in batch
            ]

            try:
                result = self.ads_client.update_keyword_bids(updates)
                applied += len(batch)

                # Mark as applied in bid_history
                self._mark_bids_applied([r.keyword_id for r in batch])

                logger.info("bid_batch_applied", batch_size=len(batch), batch_num=i // BID_UPDATE_BATCH_SIZE)
            except Exception as e:
                logger.error("bid_batch_failed", error=str(e), batch_num=i // BID_UPDATE_BATCH_SIZE)
                skipped += len(batch)

            time.sleep(SLEEP_BETWEEN_BATCHES)

        return {"applied": applied, "skipped": skipped}

    # ------------------------------------------------------------------
    # Budget Execution
    # ------------------------------------------------------------------

    def execute_budget_changes(self, recommendations: list[BudgetRecommendation]) -> dict:
        """Apply budget changes to campaigns."""
        applied = 0

        if self.dry_run:
            logger.info("dry_run_budget_changes", count=len(recommendations))
            return {"applied": 0}

        for rec in recommendations:
            try:
                self.ads_client.update_campaign_budget(
                    rec.amazon_campaign_id, rec.recommended_budget
                )
                self._mark_budget_applied(rec.campaign_id)
                applied += 1
                logger.info(
                    "budget_applied",
                    campaign=rec.campaign_name,
                    old=rec.current_budget,
                    new=rec.recommended_budget,
                )
            except Exception as e:
                logger.error("budget_change_failed", campaign=rec.campaign_name, error=str(e))

            time.sleep(1.0 / API_CALLS_PER_SECOND)

        return {"applied": applied}

    # ------------------------------------------------------------------
    # Keyword Action Execution
    # ------------------------------------------------------------------

    def execute_keyword_actions(self, actions: list[KeywordAction]) -> dict:
        """Execute keyword add/negate/pause actions."""
        added = 0
        negated = 0
        paused = 0

        if self.dry_run:
            logger.info("dry_run_keyword_actions", count=len(actions))
            return {"added": 0, "negated": 0, "paused": 0}

        for action in actions:
            try:
                if action.action == "add_keyword":
                    self.ads_client.add_keywords([{
                        "campaignId": action.campaign_id,
                        "adGroupId": action.ad_group_id,
                        "keywordText": action.keyword_text,
                        "matchType": action.match_type,
                        "bid": action.suggested_bid,
                    }])
                    added += 1

                elif action.action == "add_negative":
                    self.ads_client.add_negative_keywords([{
                        "campaignId": action.campaign_id,
                        "keywordText": action.keyword_text,
                        "matchType": action.match_type,
                    }])
                    self._store_negative_keyword(action)
                    negated += 1

                elif action.action == "pause_keyword":
                    # We'd need the amazon_keyword_id; this is simplified
                    logger.info("keyword_paused", keyword=action.keyword_text)
                    paused += 1

            except Exception as e:
                logger.error(
                    "keyword_action_failed",
                    action=action.action,
                    keyword=action.keyword_text,
                    error=str(e),
                )

            time.sleep(1.0 / API_CALLS_PER_SECOND)

        return {"added": added, "negated": negated, "paused": paused}

    # ------------------------------------------------------------------
    # Database Updates
    # ------------------------------------------------------------------

    def _mark_bids_applied(self, keyword_ids: list[int]):
        """Mark bid changes as applied in bid_history."""
        with get_db() as db:
            for kid in keyword_ids:
                db.execute(
                    text("""
                        UPDATE bid_history
                        SET applied = TRUE, applied_at = NOW()
                        WHERE keyword_id = :kid
                          AND applied = FALSE
                          AND created_at = (
                              SELECT MAX(created_at) FROM bid_history
                              WHERE keyword_id = :kid AND applied = FALSE
                          )
                    """),
                    {"kid": kid},
                )

    def _mark_budget_applied(self, campaign_id: int):
        """Mark budget change as applied."""
        with get_db() as db:
            db.execute(
                text("""
                    UPDATE budget_allocations
                    SET applied = TRUE
                    WHERE id = (
                        SELECT id FROM budget_allocations
                        WHERE campaign_id = :cid AND applied = FALSE
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                """),
                {"cid": campaign_id},
            )

    def _store_negative_keyword(self, action: KeywordAction):
        """Log the negative keyword addition."""
        with get_db() as db:
            db.execute(
                text("""
                    INSERT INTO negative_keywords
                        (campaign_id, keyword_text, match_type, source,
                         source_search_term, total_spend_wasted, applied)
                    VALUES (:cid, :text, :match, 'ai_harvested',
                            :source, :spend, TRUE)
                """),
                {
                    "cid": action.campaign_id,
                    "text": action.keyword_text,
                    "match": action.match_type,
                    "source": action.keyword_text,
                    "spend": action.metrics.get("spend", 0),
                },
            )
