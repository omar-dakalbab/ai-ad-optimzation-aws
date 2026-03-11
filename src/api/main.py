"""
FastAPI Application Server

Provides REST endpoints for:
  - Triggering optimization runs
  - Viewing predictions and recommendations
  - Dashboard data
  - Manual overrides
"""

from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from src.models.inference import ModelInference
from src.optimization.bid_optimizer import BidOptimizer
from src.optimization.budget_allocator import BudgetAllocator
from src.optimization.keyword_manager import KeywordManager
from src.automation.executor import AutomationExecutor
from src.optimization.campaign_creator import CampaignCreator

logger = structlog.get_logger()

app = FastAPI(
    title="Amazon Ads AI Optimizer",
    description="ML-powered Amazon advertising optimization platform",
    version="0.1.0",
)


# ------------------------------------------------------------------
# Health Check
# ------------------------------------------------------------------

@app.get("/health")
def health_check():
    return {"status": "healthy", "version": "0.1.0"}


# ------------------------------------------------------------------
# Optimization Endpoints
# ------------------------------------------------------------------

class OptimizationResponse(BaseModel):
    bids_applied: int
    bids_skipped: int
    budgets_applied: int
    keywords_added: int
    negatives_added: int
    keywords_paused: int
    campaigns_created: int
    dry_run: bool


@app.post("/optimize/run", response_model=OptimizationResponse)
def run_full_optimization(target_date: Optional[date] = None):
    """
    Run the complete optimization pipeline:
      1. Generate ML predictions
      2. Compute bid recommendations
      3. Compute budget allocations
      4. Run keyword management
      5. Execute changes (or dry-run)
    """
    if target_date is None:
        target_date = date.today()

    try:
        # Step 1: Generate predictions
        inference = ModelInference()
        predictions = inference.predict_all_keywords(target_date)

        # Step 2: Bid optimization
        bid_optimizer = BidOptimizer()
        bid_recs = bid_optimizer.generate_bid_recommendations(target_date)

        # Step 3: Budget allocation
        budget_allocator = BudgetAllocator()
        budget_recs = budget_allocator.generate_budget_recommendations(target_date)

        # Step 4: Keyword management
        keyword_mgr = KeywordManager()
        keyword_actions = keyword_mgr.run_keyword_management(target_date)

        # Step 5: Execute
        executor = AutomationExecutor()
        result = executor.execute_all(bid_recs, budget_recs, keyword_actions)

        return OptimizationResponse(**result)

    except Exception as e:
        logger.error("optimization_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/optimize/bids")
def optimize_bids_only(target_date: Optional[date] = None):
    """Run bid optimization only."""
    if target_date is None:
        target_date = date.today()

    bid_optimizer = BidOptimizer()
    recs = bid_optimizer.generate_bid_recommendations(target_date)

    return {
        "total_recommendations": len(recs),
        "increases": sum(1 for r in recs if r.change_pct > 0),
        "decreases": sum(1 for r in recs if r.change_pct < 0),
        "avg_change_pct": sum(r.change_pct for r in recs) / max(len(recs), 1),
        "recommendations": [
            {
                "keyword_id": r.keyword_id,
                "current_bid": r.current_bid,
                "recommended_bid": r.recommended_bid,
                "change_pct": r.change_pct,
                "reason": r.reason,
                "predicted_acos": r.predicted_acos,
                "requires_approval": r.requires_approval,
            }
            for r in recs[:50]  # return top 50
        ],
    }


class CampaignCreateRequest(BaseModel):
    asin: str
    daily_budget: Optional[float] = None
    seed_keywords: Optional[list[str]] = None
    start_paused: bool = True


@app.post("/campaigns/create")
def create_campaigns_for_product(request: CampaignCreateRequest):
    """
    Create a full SP campaign structure (auto + exact + broad) for a product.

    By default, campaigns start paused so you can review before enabling.
    """
    creator = CampaignCreator()
    results = creator.create_campaigns_for_product(
        asin=request.asin,
        daily_budget=request.daily_budget,
        seed_keywords=request.seed_keywords,
        start_paused=request.start_paused,
    )

    return {
        "asin": request.asin,
        "campaigns_created": sum(1 for r in results if r.success),
        "campaigns_failed": sum(1 for r in results if not r.success),
        "details": [
            {
                "campaign_name": r.campaign_name,
                "campaign_type": r.campaign_type,
                "daily_budget": r.daily_budget,
                "default_bid": r.default_bid,
                "keywords_added": r.keywords_added,
                "success": r.success,
                "error": r.error,
            }
            for r in results
        ],
    }


@app.post("/campaigns/create-all")
def create_campaigns_for_all_products(
    daily_budget_per_product: Optional[float] = None,
    start_paused: bool = True,
):
    """Create campaigns for all active products that don't have campaigns yet."""
    creator = CampaignCreator()
    results = creator.create_campaigns_for_all_products(
        daily_budget_per_product=daily_budget_per_product,
        start_paused=start_paused,
    )

    return {
        "total_campaigns": len(results),
        "created": sum(1 for r in results if r.success),
        "failed": sum(1 for r in results if not r.success),
        "by_product": _group_results_by_asin(results),
    }


def _group_results_by_asin(results):
    grouped = {}
    for r in results:
        if r.asin not in grouped:
            grouped[r.asin] = []
        grouped[r.asin].append({
            "campaign_name": r.campaign_name,
            "campaign_type": r.campaign_type,
            "success": r.success,
            "error": r.error,
        })
    return grouped


@app.post("/optimize/budgets")
def optimize_budgets_only(target_date: Optional[date] = None):
    """Run budget allocation only."""
    if target_date is None:
        target_date = date.today()

    allocator = BudgetAllocator()
    recs = allocator.generate_budget_recommendations(target_date)

    return {
        "total_recommendations": len(recs),
        "recommendations": [
            {
                "campaign_name": r.campaign_name,
                "current_budget": r.current_budget,
                "recommended_budget": r.recommended_budget,
                "change_pct": r.change_pct,
                "predicted_roas": r.predicted_roas,
                "reason": r.reason,
            }
            for r in recs
        ],
    }


# ------------------------------------------------------------------
# Prediction Endpoints
# ------------------------------------------------------------------

@app.get("/predictions/{keyword_id}")
def get_keyword_prediction(keyword_id: int):
    """Get the latest prediction for a specific keyword."""
    with get_db() as db:
        result = db.execute(
            text("""
                SELECT * FROM predictions
                WHERE keyword_id = :kid
                ORDER BY prediction_date DESC
                LIMIT 1
            """),
            {"kid": keyword_id},
        )
        row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="No prediction found")

    return dict(row)


@app.get("/predictions")
def get_all_predictions(
    target_date: Optional[date] = None,
    limit: int = Query(default=100, le=1000),
    min_confidence: float = Query(default=0.0, ge=0, le=1),
):
    """Get predictions for all keywords, sorted by expected profit."""
    if target_date is None:
        target_date = date.today()

    with get_db() as db:
        result = db.execute(
            text("""
                SELECT p.*, k.keyword_text, k.match_type, k.bid as current_bid
                FROM predictions p
                JOIN keywords k ON k.id = p.keyword_id
                WHERE p.prediction_date = :date
                  AND p.prediction_confidence >= :min_conf
                ORDER BY p.expected_profit_per_click DESC
                LIMIT :limit
            """),
            {"date": target_date, "min_conf": min_confidence, "limit": limit},
        )
        rows = [dict(r) for r in result.mappings()]

    return {"date": str(target_date), "count": len(rows), "predictions": rows}


# ------------------------------------------------------------------
# Dashboard Data Endpoints
# ------------------------------------------------------------------

@app.get("/dashboard/performance")
def get_performance_summary(days: int = Query(default=30, le=90)):
    """Get account-level performance summary for the dashboard."""
    with get_db() as db:
        result = db.execute(
            text("""
                SELECT
                    report_date,
                    SUM(impressions) as impressions,
                    SUM(clicks) as clicks,
                    SUM(spend) as spend,
                    SUM(orders) as orders,
                    SUM(ad_sales) as ad_sales,
                    CASE WHEN SUM(clicks) > 0
                         THEN SUM(clicks)::float / SUM(impressions) ELSE 0 END as ctr,
                    CASE WHEN SUM(clicks) > 0
                         THEN SUM(spend) / SUM(clicks) ELSE 0 END as avg_cpc,
                    CASE WHEN SUM(clicks) > 0
                         THEN SUM(orders)::float / SUM(clicks) ELSE 0 END as cvr,
                    CASE WHEN SUM(ad_sales) > 0
                         THEN SUM(spend) / SUM(ad_sales) ELSE 0 END as acos,
                    CASE WHEN SUM(spend) > 0
                         THEN SUM(ad_sales) / SUM(spend) ELSE 0 END as roas
                FROM daily_keyword_metrics
                WHERE report_date >= CURRENT_DATE - :days
                GROUP BY report_date
                ORDER BY report_date
            """),
            {"days": days},
        )
        rows = [dict(r) for r in result.mappings()]

    return {"days": days, "data": rows}


@app.get("/dashboard/bid-history")
def get_bid_history(days: int = Query(default=7, le=30)):
    """Get recent bid change history."""
    with get_db() as db:
        result = db.execute(
            text("""
                SELECT
                    bh.*, k.keyword_text, k.match_type
                FROM bid_history bh
                JOIN keywords k ON k.id = bh.keyword_id
                WHERE bh.created_at >= CURRENT_DATE - :days
                ORDER BY bh.created_at DESC
                LIMIT 200
            """),
            {"days": days},
        )
        rows = [dict(r) for r in result.mappings()]

    return {"count": len(rows), "changes": rows}


@app.get("/dashboard/model-performance")
def get_model_performance():
    """Get model accuracy metrics over time."""
    with get_db() as db:
        result = db.execute(
            text("""
                SELECT * FROM model_performance
                ORDER BY trained_date DESC
                LIMIT 50
            """)
        )
        rows = [dict(r) for r in result.mappings()]

    return {"metrics": rows}
