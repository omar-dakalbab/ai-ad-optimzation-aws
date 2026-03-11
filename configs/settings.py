"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Database ---
    database_url: str = "postgresql://postgres:postgres@localhost:5432/amazon_ads_ai"
    redis_url: str = "redis://localhost:6379/0"

    # --- Amazon Ads API ---
    amazon_ads_client_id: str = ""
    amazon_ads_client_secret: str = ""
    amazon_ads_refresh_token: str = ""
    amazon_ads_profile_id: str = ""
    amazon_ads_region: str = "NA"  # NA, EU, FE

    # --- Amazon SP-API ---
    sp_api_refresh_token: str = ""
    sp_api_lwa_app_id: str = ""
    sp_api_lwa_client_secret: str = ""
    sp_api_aws_access_key: str = ""
    sp_api_aws_secret_key: str = ""
    sp_api_role_arn: str = ""
    marketplace_id: str = "ATVPDKIKX0DER"  # US marketplace

    # --- ML Settings ---
    model_retrain_days: int = 7
    prediction_lookback_days: int = 90
    min_clicks_for_training: int = 10
    min_impressions_for_prediction: int = 50

    # --- Optimization Settings ---
    max_bid_increase_pct: float = 0.30       # max 30% bid increase per cycle
    max_bid_decrease_pct: float = 0.40       # max 40% bid decrease per cycle
    min_bid: float = 0.10                    # floor bid $0.10
    max_bid: float = 15.00                   # ceiling bid $15.00
    target_acos: float = 0.25               # 25% target ACoS
    min_roas: float = 3.0                   # minimum acceptable ROAS
    budget_reallocation_pct: float = 0.15   # max 15% budget shift per cycle
    pause_keyword_threshold_clicks: int = 50  # pause after 50 clicks, 0 orders
    negative_keyword_threshold_spend: float = 20.0  # negate after $20 spend, 0 orders

    # --- Safety Guardrails ---
    max_daily_spend_increase_pct: float = 0.20  # max 20% daily spend increase
    require_approval_above_bid: float = 8.00     # human approval for bids > $8
    dry_run: bool = True                          # default: don't execute changes

    # --- MLflow ---
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment_name: str = "amazon-ads-optimization"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="AAI_")
