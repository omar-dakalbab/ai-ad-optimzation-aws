# Amazon Ads AI Optimization Platform - System Architecture

## 1. High-Level System Diagram

```
+----------------------------------------------------------+
|                    DATA INGESTION LAYER                    |
|                                                            |
|  Amazon Ads API ──┐    Amazon SP-API ──┐                  |
|  (campaigns,       │    (orders, sales,  │                 |
|   keywords,        │     inventory,       │                |
|   search terms)    │     products)        │                |
|                    ▼                      ▼                |
|              ┌──────────────────────────────┐              |
|              │   Apache Airflow Scheduler    │              |
|              │   (Daily + Hourly DAGs)       │              |
|              └──────────┬───────────────────┘              |
+-------------------------|----------------------------------+
                          │
+-------------------------|----------------------------------+
|                    STORAGE LAYER                           |
|                          ▼                                 |
|  ┌─────────────────────────────────────────┐              |
|  │         PostgreSQL (Primary DB)          │              |
|  │                                          │              |
|  │  - products        - campaigns           │              |
|  │  - keywords        - search_terms        │              |
|  │  - daily_metrics   - predictions         │              |
|  │  - bid_history     - budget_allocations  │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │         Redis (Cache + Queues)           │              |
|  │  - current bids    - rate limits         │              |
|  │  - feature cache   - job queues          │              |
|  └─────────────────────────────────────────┘              |
+----------------------------------------------------------+
                          │
+-------------------------|----------------------------------+
|                 ML PIPELINE LAYER                          |
|                          ▼                                 |
|  ┌─────────────────────────────────────────┐              |
|  │        Feature Engineering Service       │              |
|  │  - rolling CTR (7d, 14d, 30d)           │              |
|  │  - conversion trends                     │              |
|  │  - price elasticity signals              │              |
|  │  - seasonality features                  │              |
|  │  - competitive position features         │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │         Model Training Pipeline          │              |
|  │                                          │              |
|  │  Model 1: Click Probability (LightGBM)  │              |
|  │  Model 2: Conversion Prob (XGBoost)     │              |
|  │  Model 3: Revenue Prediction (GBM)      │              |
|  │  Model 4: Demand Forecast (Prophet)     │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │         Model Registry (MLflow)          │              |
|  │  - versioned models                      │              |
|  │  - experiment tracking                   │              |
|  │  - A/B test configs                      │              |
|  └─────────────────────────────────────────┘              |
+----------------------------------------------------------+
                          │
+-------------------------|----------------------------------+
|              OPTIMIZATION ENGINE LAYER                     |
|                          ▼                                 |
|  ┌─────────────────────────────────────────┐              |
|  │         Bid Optimization Service         │              |
|  │                                          │              |
|  │  Input:  ML predictions per keyword      │              |
|  │  Logic:  EV/click = P(conv) * margin     │              |
|  │  Output: optimal_bid per keyword         │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │       Budget Allocation Service          │              |
|  │                                          │              |
|  │  Input:  campaign-level predicted ROAS   │              |
|  │  Logic:  marginal ROAS equalization      │              |
|  │  Output: budget reallocation plan        │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │       Keyword Management Service         │              |
|  │                                          │              |
|  │  - harvest converting search terms       │              |
|  │  - negate wasteful search terms          │              |
|  │  - pause underperforming keywords        │              |
|  └─────────────────────────────────────────┘              |
+----------------------------------------------------------+
                          │
+-------------------------|----------------------------------+
|               EXECUTION + API LAYER                        |
|                          ▼                                 |
|  ┌─────────────────────────────────────────┐              |
|  │       FastAPI Application Server         │              |
|  │                                          │              |
|  │  POST /optimize/bids                     │              |
|  │  POST /optimize/budgets                  │              |
|  │  POST /keywords/harvest                  │              |
|  │  GET  /predictions/{keyword_id}          │              |
|  │  GET  /dashboard/performance             │              |
|  └──────────────┬──────────────────────────┘              |
|                  │                                         |
|  ┌───────────────▼─────────────────────────┐              |
|  │     Amazon Ads API Execution Layer       │              |
|  │                                          │              |
|  │  - apply bid changes (with rate limits)  │              |
|  │  - update budgets                        │              |
|  │  - add/pause keywords                    │              |
|  │  - safety guardrails + rollback          │              |
|  └─────────────────────────────────────────┘              |
+----------------------------------------------------------+
                          │
+-------------------------|----------------------------------+
|                MONITORING + DASHBOARD                      |
|                          ▼                                 |
|  ┌─────────────────────────────────────────┐              |
|  │          Streamlit Dashboard             │              |
|  │                                          │              |
|  │  - real-time performance metrics         │              |
|  │  - bid change history + impact           │              |
|  │  - model accuracy tracking               │              |
|  │  - budget allocation visualization       │              |
|  │  - alerts + anomaly detection            │              |
|  └─────────────────────────────────────────┘              |
+----------------------------------------------------------+
```

## 2. Data Flow - Daily Optimization Cycle

```
 6:00 AM  ──► Airflow triggers data pull DAGs
              │
              ├── Pull Ads API: sponsored products report
              ├── Pull Ads API: search term report
              ├── Pull SP-API: orders, revenue, inventory
              │
 7:00 AM  ──► Raw data lands in PostgreSQL staging tables
              │
              ├── Data validation + deduplication
              ├── Merge ads data with sales data (ASIN join)
              │
 7:30 AM  ──► Feature engineering pipeline runs
              │
              ├── Compute rolling metrics (7d, 14d, 30d)
              ├── Compute trend features (slope of CTR, CVR)
              ├── Compute seasonality + day-of-week features
              ├── Compute competitive features (impression share)
              │
 8:00 AM  ──► ML inference pipeline runs
              │
              ├── Model 1: predict P(click) per keyword
              ├── Model 2: predict P(conversion) per keyword
              ├── Model 3: predict revenue per conversion
              ├── Combine: expected_profit = P(click) * P(conv) * margin
              │
 8:30 AM  ──► Optimization engine runs
              │
              ├── Bid optimizer: compute optimal bid per keyword
              ├── Budget allocator: rebalance campaign budgets
              ├── Keyword manager: harvest/negate search terms
              │
 9:00 AM  ──► Execution layer applies changes
              │
              ├── Apply bid changes via Ads API (rate-limited)
              ├── Apply budget changes via Ads API
              ├── Add/pause keywords via Ads API
              ├── Log all changes to bid_history table
              │
 9:30 AM  ──► Dashboard updates with new predictions + actions
```

## 3. Tech Stack Summary

| Layer              | Technology                        |
|--------------------|-----------------------------------|
| Language           | Python 3.11+                      |
| API Framework      | FastAPI                           |
| Orchestration      | Apache Airflow                    |
| Primary Database   | PostgreSQL 15                     |
| Cache / Queue      | Redis                             |
| ML Training        | XGBoost, LightGBM, scikit-learn   |
| Time Series        | Prophet / statsmodels             |
| Experiment Track   | MLflow                            |
| Dashboard          | Streamlit                         |
| Containerization   | Docker + Docker Compose           |
| Cloud (optional)   | AWS (natural fit for Amazon APIs) |
