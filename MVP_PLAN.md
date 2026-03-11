# MVP Plan - Build in 4-6 Weeks

## Week 1: Foundation

### Days 1-2: Infrastructure Setup
- [ ] Set up PostgreSQL + Redis via Docker Compose
- [ ] Run `migrations/001_schema.sql` to create all tables
- [ ] Set up Python project with virtual env
- [ ] Configure `.env` with Amazon API credentials
- [ ] Verify API connectivity (test Ads API auth + SP-API auth)

### Days 3-5: Data Pipeline (MVP)
- [ ] Implement `ads_api_client.py` - keyword report pulling
- [ ] Implement `sp_api_client.py` - order/sales data
- [ ] Implement `data_loader.py` - daily load orchestration
- [ ] Run first successful data pull and verify data in PostgreSQL
- [ ] Backfill 30-60 days of historical data

**Deliverable:** Data flowing from Amazon into PostgreSQL daily.

---

## Week 2: Feature Engineering + First Model

### Days 1-3: Feature Engineering
- [ ] Implement rolling metrics (7d, 14d, 30d CTR, CVR, ACoS)
- [ ] Implement trend features (slopes)
- [ ] Join product data (price, margin, rating)
- [ ] Add seasonality features
- [ ] Verify feature table is populating correctly

### Days 4-5: First ML Model
- [ ] Train conversion model (XGBoost) on historical data
- [ ] Evaluate with time-series cross-validation
- [ ] Track in MLflow
- [ ] Target: AUC > 0.70 on validation set

**Deliverable:** Feature pipeline + working conversion prediction model.

---

## Week 3: Bid Optimizer + Budget Allocator

### Days 1-2: Train Remaining Models
- [ ] Train click model (LightGBM)
- [ ] Train revenue model (GBM Regressor)
- [ ] Evaluate all models, log to MLflow

### Days 3-4: Bid Optimization Logic
- [ ] Implement `bid_optimizer.py`
- [ ] Test bid formula: `optimal_bid = P(conv) * margin * efficiency`
- [ ] Implement guardrails (max change %, floor, ceiling)
- [ ] Generate first batch of bid recommendations
- [ ] Manual review: do the recommendations make sense?

### Day 5: Budget Allocation
- [ ] Implement `budget_allocator.py`
- [ ] Test marginal ROAS equalization logic
- [ ] Generate first budget reallocation plan

**Deliverable:** Working optimization engine producing sensible recommendations.

---

## Week 4: Automation + API

### Days 1-2: Automation Executor
- [ ] Implement `executor.py` with dry-run mode
- [ ] Test API calls to Amazon Ads in dry-run
- [ ] Implement bid_history logging
- [ ] Run first dry-run end-to-end pipeline

### Days 3-4: FastAPI Server
- [ ] Implement all API endpoints
- [ ] Test `/optimize/run` endpoint end-to-end
- [ ] Add error handling and logging

### Day 5: Airflow DAGs
- [ ] Set up Airflow (Docker or managed)
- [ ] Deploy daily optimization DAG
- [ ] Deploy weekly retraining DAG
- [ ] Test DAG execution

**Deliverable:** Complete automated pipeline running in dry-run mode.

---

## Week 5: Dashboard + Validation

### Days 1-2: Streamlit Dashboard
- [ ] Build performance overview panel
- [ ] Build bid change history panel
- [ ] Build model accuracy panel
- [ ] Build budget allocation view

### Days 3-5: Validation Phase
- [ ] Run in dry-run mode for 5+ days
- [ ] Compare AI recommendations vs actual performance
- [ ] Manually review top 50 bid recommendations daily
- [ ] Tune thresholds (target ACoS, min data requirements)
- [ ] Fix any data quality issues discovered

**Deliverable:** Dashboard live, system validated in dry-run.

---

## Week 6: Go Live

### Days 1-2: Gradual Rollout
- [ ] Enable live execution for 1-2 campaigns (lowest risk)
- [ ] Monitor closely: check ACoS, spend, conversions
- [ ] Verify bid changes are applied correctly in Amazon

### Days 3-4: Expand
- [ ] If metrics improve, expand to 50% of campaigns
- [ ] Add keyword harvesting (search term → exact match)
- [ ] Add negative keyword automation

### Day 5: Full Deployment
- [ ] Enable for all campaigns
- [ ] Set up alerting (spend spike, ACoS spike, model degradation)
- [ ] Document runbook for operations

**Deliverable:** System live and optimizing all campaigns.

---

## Post-MVP Enhancements (Weeks 7+)
- Demand forecasting with Prophet for budget planning
- Dayparting: adjust bids by hour of day
- Placement optimization: top-of-search vs rest-of-search bid modifiers
- Multi-marketplace support (US + EU + etc.)
- A/B testing framework for bid strategies
- Anomaly detection for sudden performance drops
- Integration with inventory management (pause ads for low stock)


# ============================================================
# Example Daily Workflow
# ============================================================

## 6:00 AM - Data Sync (Automated via Airflow)

```
Airflow DAG triggers → task_sync_data()

  1. Ads API: Pull keyword report for 2 days ago
     → 15,000 keyword-day rows loaded into daily_keyword_metrics

  2. Ads API: Pull search term report
     → 45,000 search term rows loaded into search_terms

  3. SP-API: Pull orders and sales
     → 500 ASIN-day rows loaded into daily_product_sales

  4. SP-API: Update inventory levels
     → 200 ASINs updated in products table
```

## 7:30 AM - Feature Engineering (Automated)

```
Airflow DAG triggers → task_compute_features()

For each of 5,000 active keywords:

  Keyword: "wireless bluetooth earbuds" (exact match)

  Rolling metrics computed:
    ctr_7d:    0.0082  (0.82%)
    ctr_14d:   0.0075  (0.75%)
    cvr_7d:    0.089   (8.9%)
    cvr_14d:   0.076   (7.6%)
    acos_7d:   0.18    (18%)
    roas_7d:   5.56

  Trends:
    ctr_trend:    +0.0003  (CTR improving)
    cvr_trend:    +0.002   (CVR improving)

  Product data:
    price:       $29.99
    margin:      $8.50
    rating:      4.3
    reviews:     1,247
    inventory:   342 units

  → Stored in keyword_features table
```

## 8:00 AM - ML Predictions (Automated)

```
Airflow DAG triggers → task_run_inference()

For "wireless bluetooth earbuds" (exact match):

  Click Model (LightGBM):
    P(click) = 0.0085  →  "likely to get clicks"

  Conversion Model (XGBoost):
    P(conversion | click) = 0.092  →  "9.2% of clicks will convert"

  Revenue Model (GBM):
    E[revenue per conversion] = $29.99  →  "expect ~$30 per order"

  Derived:
    Expected profit per click = 0.092 * $8.50 - $0.85 = -$0.07
    Predicted ACoS = $0.85 / (0.092 * $29.99) = 30.8%
    Recommended bid = 0.092 * $8.50 * 0.75 = $0.59

  Current bid: $0.85
  Recommended bid: $0.59  (30% decrease)
  Reason: predicted ACoS (30.8%) > target (25%)

  → Stored in predictions table
```

## 8:30 AM - Optimization Decisions (Automated)

```
Bid Optimizer output for this keyword:

  ┌────────────────────────────────────────┐
  │  Keyword: wireless bluetooth earbuds   │
  │  Match: exact                          │
  │                                        │
  │  Current bid:     $0.85                │
  │  Recommended bid: $0.68                │
  │  Change:          -20%                 │
  │                                        │
  │  Reason: marginal_bid_down             │
  │  Predicted ACoS: 30.8% (target: 25%)  │
  │  Confidence: 0.85 (high)              │
  │  Requires approval: No                 │
  └────────────────────────────────────────┘

Budget Allocator:
  Campaign "SP - Earbuds - Exact" (ROAS: 4.2)
    → Increase budget $50 → $55 (+10%)
    → Reason: high ROAS, budget constrained (92% utilization)

  Campaign "SP - Chargers - Broad" (ROAS: 1.8)
    → Decrease budget $80 → $72 (-10%)
    → Reason: below-median ROAS, donor campaign

Keyword Manager:
  HARVEST: "anc earbuds under 30" → add as exact match
    (5 orders, 42 clicks, 11.9% CVR, 14% ACoS in last 14 days)

  NEGATE: "free bluetooth earbuds" → add as negative exact
    (28 clicks, $19.60 spent, 0 orders)

  PAUSE: "cheap wireless headphones" (broad)
    (62 clicks, $44.50 spent, 0 orders in 30 days)
```

## 9:00 AM - Execution (Automated)

```
Executor applies changes via Amazon Ads API:

  Bid changes:    312 keywords updated (avg change: -8%)
  Budget changes: 5 campaigns adjusted
  Keywords added: 3 new exact match keywords
  Negatives added: 7 negative keywords
  Keywords paused: 2

  All changes logged to bid_history and budget_allocations tables.
```

## 9:30 AM - Dashboard Updated

```
Dashboard shows:
  - Today's ACoS: 22.3% (↓ from 26.1% last week)
  - ROAS: 4.5x (↑ from 3.8x)
  - 312 bid changes applied
  - Model accuracy: AUC 0.78 (stable)
  - $47.20 saved from negative keywords
```
