-- ============================================================
-- Amazon Ads AI - Database Schema
-- ============================================================
-- Run: psql -d amazon_ads_ai -f migrations/001_schema.sql
-- ============================================================

-- ----------------------------
-- 1. PRODUCTS TABLE
-- ----------------------------
-- Core product catalog synced from SP-API
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    asin            VARCHAR(20) NOT NULL UNIQUE,
    sku             VARCHAR(50),
    title           TEXT,
    category        VARCHAR(200),
    subcategory     VARCHAR(200),
    price           NUMERIC(10,2),
    cost            NUMERIC(10,2),          -- landed cost (COGS)
    fba_fee         NUMERIC(10,2),          -- FBA fulfillment fee
    referral_fee    NUMERIC(10,2),          -- Amazon referral fee
    margin          NUMERIC(10,2),          -- price - cost - fba_fee - referral_fee
    margin_pct      NUMERIC(5,2),           -- margin / price * 100
    rating          NUMERIC(3,2),
    review_count    INTEGER DEFAULT 0,
    inventory_level INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'active',  -- active, out_of_stock, suppressed
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_products_asin ON products(asin);
CREATE INDEX idx_products_status ON products(status);

-- ----------------------------
-- 2. CAMPAIGNS TABLE
-- ----------------------------
CREATE TABLE IF NOT EXISTS campaigns (
    id                  SERIAL PRIMARY KEY,
    amazon_campaign_id  BIGINT NOT NULL UNIQUE,
    name                VARCHAR(500),
    campaign_type       VARCHAR(50),   -- sponsoredProducts, sponsoredBrands, sponsoredDisplay
    targeting_type      VARCHAR(50),   -- manual, auto
    state               VARCHAR(20),   -- enabled, paused, archived
    daily_budget        NUMERIC(10,2),
    start_date          DATE,
    end_date            DATE,
    portfolio_id        BIGINT,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_campaigns_amazon_id ON campaigns(amazon_campaign_id);
CREATE INDEX idx_campaigns_state ON campaigns(state);

-- ----------------------------
-- 3. AD GROUPS TABLE
-- ----------------------------
CREATE TABLE IF NOT EXISTS ad_groups (
    id                  SERIAL PRIMARY KEY,
    amazon_ad_group_id  BIGINT NOT NULL UNIQUE,
    campaign_id         INTEGER REFERENCES campaigns(id),
    name                VARCHAR(500),
    default_bid         NUMERIC(10,2),
    state               VARCHAR(20),
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ad_groups_campaign ON ad_groups(campaign_id);

-- ----------------------------
-- 4. KEYWORDS TABLE
-- ----------------------------
-- Every targeted keyword in the account
CREATE TABLE IF NOT EXISTS keywords (
    id                  SERIAL PRIMARY KEY,
    amazon_keyword_id   BIGINT NOT NULL UNIQUE,
    ad_group_id         INTEGER REFERENCES ad_groups(id),
    campaign_id         INTEGER REFERENCES campaigns(id),
    keyword_text        VARCHAR(500) NOT NULL,
    match_type          VARCHAR(20) NOT NULL,  -- exact, phrase, broad
    bid                 NUMERIC(10,2),
    state               VARCHAR(20),           -- enabled, paused, archived
    asin                VARCHAR(20),           -- the advertised ASIN
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_keywords_text ON keywords(keyword_text);
CREATE INDEX idx_keywords_asin ON keywords(asin);
CREATE INDEX idx_keywords_campaign ON keywords(campaign_id);
CREATE INDEX idx_keywords_state ON keywords(state);

-- ----------------------------
-- 5. SEARCH TERMS TABLE
-- ----------------------------
-- Raw search term report data (what customers actually typed)
CREATE TABLE IF NOT EXISTS search_terms (
    id              SERIAL PRIMARY KEY,
    keyword_id      INTEGER REFERENCES keywords(id),
    campaign_id     INTEGER REFERENCES campaigns(id),
    search_term     VARCHAR(500) NOT NULL,
    match_type      VARCHAR(20),
    report_date     DATE NOT NULL,
    impressions     INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    spend           NUMERIC(10,2) DEFAULT 0,
    orders          INTEGER DEFAULT 0,
    sales           NUMERIC(10,2) DEFAULT 0,
    acos            NUMERIC(8,4),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_search_terms_date ON search_terms(report_date);
CREATE INDEX idx_search_terms_keyword ON search_terms(keyword_id);
CREATE INDEX idx_search_terms_term ON search_terms(search_term);

-- ----------------------------
-- 6. DAILY KEYWORD METRICS
-- ----------------------------
-- Aggregated daily performance per keyword (core analytics table)
CREATE TABLE IF NOT EXISTS daily_keyword_metrics (
    id              SERIAL PRIMARY KEY,
    keyword_id      INTEGER REFERENCES keywords(id),
    campaign_id     INTEGER REFERENCES campaigns(id),
    asin            VARCHAR(20),
    report_date     DATE NOT NULL,
    impressions     INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    spend           NUMERIC(10,2) DEFAULT 0,
    orders          INTEGER DEFAULT 0,
    ad_sales        NUMERIC(10,2) DEFAULT 0,
    units_sold      INTEGER DEFAULT 0,
    ctr             NUMERIC(8,6),           -- clicks / impressions
    cpc             NUMERIC(10,2),          -- spend / clicks
    cvr             NUMERIC(8,6),           -- orders / clicks
    acos            NUMERIC(8,4),           -- spend / ad_sales
    roas            NUMERIC(8,2),           -- ad_sales / spend
    bid_at_time     NUMERIC(10,2),          -- what the bid was on this day
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(keyword_id, report_date)
);

CREATE INDEX idx_dkm_date ON daily_keyword_metrics(report_date);
CREATE INDEX idx_dkm_keyword ON daily_keyword_metrics(keyword_id);
CREATE INDEX idx_dkm_keyword_date ON daily_keyword_metrics(keyword_id, report_date);
CREATE INDEX idx_dkm_asin ON daily_keyword_metrics(asin);

-- ----------------------------
-- 7. DAILY CAMPAIGN METRICS
-- ----------------------------
CREATE TABLE IF NOT EXISTS daily_campaign_metrics (
    id              SERIAL PRIMARY KEY,
    campaign_id     INTEGER REFERENCES campaigns(id),
    report_date     DATE NOT NULL,
    impressions     INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    spend           NUMERIC(10,2) DEFAULT 0,
    orders          INTEGER DEFAULT 0,
    ad_sales        NUMERIC(10,2) DEFAULT 0,
    budget          NUMERIC(10,2),
    budget_utilization NUMERIC(5,2),  -- spend / budget * 100
    acos            NUMERIC(8,4),
    roas            NUMERIC(8,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(campaign_id, report_date)
);

CREATE INDEX idx_dcm_date ON daily_campaign_metrics(report_date);
CREATE INDEX idx_dcm_campaign ON daily_campaign_metrics(campaign_id);

-- ----------------------------
-- 8. DAILY PRODUCT SALES
-- ----------------------------
-- Organic + ad sales from SP-API
CREATE TABLE IF NOT EXISTS daily_product_sales (
    id              SERIAL PRIMARY KEY,
    asin            VARCHAR(20) NOT NULL,
    report_date     DATE NOT NULL,
    units_sold      INTEGER DEFAULT 0,
    revenue         NUMERIC(10,2) DEFAULT 0,
    organic_units   INTEGER DEFAULT 0,
    organic_revenue NUMERIC(10,2) DEFAULT 0,
    ad_units        INTEGER DEFAULT 0,
    ad_revenue      NUMERIC(10,2) DEFAULT 0,
    refunds         INTEGER DEFAULT 0,
    refund_amount   NUMERIC(10,2) DEFAULT 0,
    price           NUMERIC(10,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(asin, report_date)
);

CREATE INDEX idx_dps_asin_date ON daily_product_sales(asin, report_date);

-- ----------------------------
-- 9. KEYWORD FEATURES (ML)
-- ----------------------------
-- Pre-computed features for ML models, updated daily
CREATE TABLE IF NOT EXISTS keyword_features (
    id              SERIAL PRIMARY KEY,
    keyword_id      INTEGER REFERENCES keywords(id),
    computed_date   DATE NOT NULL,

    -- Rolling performance metrics
    ctr_7d          NUMERIC(8,6),
    ctr_14d         NUMERIC(8,6),
    ctr_30d         NUMERIC(8,6),
    cvr_7d          NUMERIC(8,6),
    cvr_14d         NUMERIC(8,6),
    cvr_30d         NUMERIC(8,6),
    acos_7d         NUMERIC(8,4),
    acos_14d        NUMERIC(8,4),
    acos_30d        NUMERIC(8,4),
    cpc_7d          NUMERIC(10,2),
    cpc_14d         NUMERIC(10,2),
    cpc_30d         NUMERIC(10,2),
    roas_7d         NUMERIC(8,2),
    roas_14d        NUMERIC(8,2),
    roas_30d        NUMERIC(8,2),

    -- Volume metrics
    impressions_7d  INTEGER,
    impressions_30d INTEGER,
    clicks_7d       INTEGER,
    clicks_30d      INTEGER,
    spend_7d        NUMERIC(10,2),
    spend_30d       NUMERIC(10,2),
    orders_7d       INTEGER,
    orders_30d      INTEGER,

    -- Trend features (slope of rolling metric)
    ctr_trend       NUMERIC(10,6),    -- positive = improving CTR
    cvr_trend       NUMERIC(10,6),    -- positive = improving CVR
    spend_trend     NUMERIC(10,2),
    sales_trend     NUMERIC(10,2),
    impression_trend NUMERIC(10,2),

    -- Product features (denormalized for speed)
    product_price   NUMERIC(10,2),
    product_margin  NUMERIC(10,2),
    product_margin_pct NUMERIC(5,2),
    product_rating  NUMERIC(3,2),
    product_review_count INTEGER,
    inventory_level INTEGER,
    price_change_7d NUMERIC(5,2),     -- % price change over 7 days

    -- Keyword features
    keyword_length  INTEGER,          -- number of words
    match_type_encoded INTEGER,       -- 0=exact, 1=phrase, 2=broad
    keyword_age_days INTEGER,

    -- Seasonality features
    day_of_week     INTEGER,          -- 0=Monday
    is_weekend      BOOLEAN,
    month           INTEGER,
    is_prime_day    BOOLEAN DEFAULT FALSE,
    is_holiday_season BOOLEAN DEFAULT FALSE,

    -- Competition proxy
    impression_share_est NUMERIC(5,2),  -- estimated from impression volume changes
    avg_position_est NUMERIC(5,2),      -- estimated from CTR patterns

    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(keyword_id, computed_date)
);

CREATE INDEX idx_kf_keyword_date ON keyword_features(keyword_id, computed_date);

-- ----------------------------
-- 10. PREDICTIONS TABLE
-- ----------------------------
-- ML model output: predicted metrics per keyword
CREATE TABLE IF NOT EXISTS predictions (
    id                      SERIAL PRIMARY KEY,
    keyword_id              INTEGER REFERENCES keywords(id),
    prediction_date         DATE NOT NULL,
    model_version           VARCHAR(50),

    -- Core predictions
    predicted_ctr           NUMERIC(8,6),
    predicted_cvr           NUMERIC(8,6),
    predicted_cpc           NUMERIC(10,2),
    predicted_revenue       NUMERIC(10,2),     -- expected revenue per conversion
    predicted_acos          NUMERIC(8,4),

    -- Derived calculations
    expected_profit_per_click NUMERIC(10,4),   -- P(conv) * margin - CPC
    expected_value_per_impression NUMERIC(10,6),
    recommended_bid         NUMERIC(10,2),

    -- Confidence
    prediction_confidence   NUMERIC(5,2),      -- 0-1 confidence score
    data_points_used        INTEGER,           -- how many days of data

    created_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE(keyword_id, prediction_date)
);

CREATE INDEX idx_pred_keyword_date ON predictions(keyword_id, prediction_date);

-- ----------------------------
-- 11. BID HISTORY TABLE
-- ----------------------------
-- Audit log of every bid change (crucial for analysis)
CREATE TABLE IF NOT EXISTS bid_history (
    id              SERIAL PRIMARY KEY,
    keyword_id      INTEGER REFERENCES keywords(id),
    campaign_id     INTEGER REFERENCES campaigns(id),
    old_bid         NUMERIC(10,2),
    new_bid         NUMERIC(10,2),
    change_pct      NUMERIC(8,2),
    reason          VARCHAR(200),      -- 'ml_optimization', 'pause_low_cvr', 'manual'
    predicted_acos  NUMERIC(8,4),
    predicted_roas  NUMERIC(8,2),
    applied         BOOLEAN DEFAULT FALSE,  -- was it actually sent to Amazon
    applied_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_bh_keyword ON bid_history(keyword_id);
CREATE INDEX idx_bh_date ON bid_history(created_at);

-- ----------------------------
-- 12. BUDGET ALLOCATIONS
-- ----------------------------
CREATE TABLE IF NOT EXISTS budget_allocations (
    id              SERIAL PRIMARY KEY,
    campaign_id     INTEGER REFERENCES campaigns(id),
    allocation_date DATE NOT NULL,
    old_budget      NUMERIC(10,2),
    new_budget      NUMERIC(10,2),
    change_pct      NUMERIC(8,2),
    predicted_roas  NUMERIC(8,2),
    reason          VARCHAR(200),
    applied         BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ba_date ON budget_allocations(allocation_date);

-- ----------------------------
-- 13. NEGATIVE KEYWORDS
-- ----------------------------
CREATE TABLE IF NOT EXISTS negative_keywords (
    id                  SERIAL PRIMARY KEY,
    campaign_id         INTEGER REFERENCES campaigns(id),
    ad_group_id         INTEGER REFERENCES ad_groups(id),
    keyword_text        VARCHAR(500) NOT NULL,
    match_type          VARCHAR(20),        -- negativeExact, negativePhrase
    source              VARCHAR(50),        -- 'ai_harvested', 'manual'
    source_search_term  VARCHAR(500),       -- the search term that triggered this
    total_spend_wasted  NUMERIC(10,2),      -- spend on this term before negation
    applied             BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_nk_campaign ON negative_keywords(campaign_id);

-- ----------------------------
-- 14. MODEL PERFORMANCE LOG
-- ----------------------------
CREATE TABLE IF NOT EXISTS model_performance (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(100),       -- 'click_model', 'conversion_model', 'revenue_model'
    model_version   VARCHAR(50),
    trained_date    DATE,
    metric_name     VARCHAR(50),        -- 'auc_roc', 'rmse', 'precision', 'recall'
    metric_value    NUMERIC(10,6),
    test_set_size   INTEGER,
    feature_count   INTEGER,
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
