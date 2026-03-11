"""
ML Model Inference Pipeline

Loads trained models and generates predictions for all active keywords.
Predictions are stored in the predictions table and used by the optimizer.

Pseudocode for the prediction flow:
  for each keyword:
    P(click)      = click_model.predict_proba(features)
    P(conversion) = conversion_model.predict_proba(features)
    E[revenue]    = revenue_model.predict(features)
    E[profit/click] = P(conversion) * (margin - CPC)
    recommended_bid = P(conversion) * margin * bid_multiplier
"""

from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np
import joblib
import structlog
from sqlalchemy import text

from src.database.connection import get_db
from src.features.feature_engineering import (
    CLICK_MODEL_FEATURES,
    CONVERSION_MODEL_FEATURES,
    REVENUE_MODEL_FEATURES,
)
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()

MODEL_DIR = Path("models/artifacts")


class ModelInference:
    """Generate predictions for all active keywords."""

    def __init__(self):
        self.click_model = None
        self.conversion_model = None
        self.revenue_model = None
        self.model_version = None
        self._load_models()

    def _load_models(self):
        """Load the latest trained models from disk."""
        try:
            self.click_model = joblib.load(MODEL_DIR / "click_model.joblib")
            self.conversion_model = joblib.load(MODEL_DIR / "conversion_model.joblib")
            self.revenue_model = joblib.load(MODEL_DIR / "revenue_model.joblib")
            # Derive version from model file modification time
            mtime = (MODEL_DIR / "conversion_model.joblib").stat().st_mtime
            self.model_version = f"v_{int(mtime)}"
            logger.info("models_loaded", version=self.model_version)
        except FileNotFoundError as e:
            logger.error("model_not_found", error=str(e))
            raise

    def predict_all_keywords(self, prediction_date: date = None) -> pd.DataFrame:
        """
        Generate predictions for every active keyword.

        Returns DataFrame with columns:
          - keyword_id
          - predicted_ctr (P(click))
          - predicted_cvr (P(conversion | click))
          - predicted_revenue (E[revenue per conversion])
          - predicted_acos
          - expected_profit_per_click
          - recommended_bid
          - prediction_confidence
        """
        if prediction_date is None:
            prediction_date = date.today()

        logger.info("inference_start", date=str(prediction_date))

        # Load latest features
        with get_db() as db:
            features_df = pd.read_sql(
                text("""
                    SELECT kf.*, k.bid as current_bid, p.margin as product_margin_raw
                    FROM keyword_features kf
                    JOIN keywords k ON k.id = kf.keyword_id
                    LEFT JOIN products p ON p.asin = k.asin
                    WHERE kf.computed_date = :date
                      AND k.state = 'enabled'
                """),
                db.bind,
                params={"date": prediction_date},
            )

        if features_df.empty:
            logger.warning("no_features_for_prediction")
            return pd.DataFrame()

        # ----- Model 1: Click Probability -----
        X_click = features_df[CLICK_MODEL_FEATURES].fillna(0)
        predicted_ctr = self.click_model.predict_proba(X_click)[:, 1]
        predicted_ctr = np.clip(predicted_ctr, 0.0, 1.0)

        # ----- Model 2: Conversion Probability -----
        X_conv = features_df[CONVERSION_MODEL_FEATURES].fillna(0)
        predicted_cvr = self.conversion_model.predict_proba(X_conv)[:, 1]
        predicted_cvr = np.clip(predicted_cvr, 0.0, 1.0)

        # ----- Model 3: Revenue Prediction -----
        X_rev = features_df[REVENUE_MODEL_FEATURES].fillna(0)
        predicted_revenue = self.revenue_model.predict(X_rev)
        predicted_revenue = np.clip(predicted_revenue, 0, 500)  # cap at $500

        # ----- Derived Metrics -----
        margin = features_df["product_margin_raw"].fillna(0).values
        current_bid = features_df["current_bid"].fillna(0).values

        # Expected profit per click = P(conv) * margin - expected CPC
        # We approximate expected CPC as the current bid (conservative estimate)
        expected_profit_per_click = (predicted_cvr * margin) - current_bid

        # Predicted ACoS = expected CPC / (P(conv) * expected revenue)
        expected_revenue_per_click = predicted_cvr * predicted_revenue
        predicted_acos = np.where(
            expected_revenue_per_click > 0,
            current_bid / expected_revenue_per_click,
            999.0,  # infinite ACoS if no expected revenue
        )

        # ----- Recommended Bid -----
        # bid = P(conversion) * profit_per_order * target_efficiency
        # target_efficiency < 1 to maintain profitability (e.g., 0.7 = keep 30% margin)
        target_efficiency = 1.0 - settings.target_acos  # e.g., 0.75
        recommended_bid = predicted_cvr * margin * target_efficiency

        # Apply bid guardrails
        recommended_bid = np.clip(recommended_bid, settings.min_bid, settings.max_bid)

        # ----- Confidence Score -----
        # Based on data volume: more historical data = higher confidence
        data_points = features_df["clicks_30d"].fillna(0).values
        confidence = np.clip(data_points / 100, 0.1, 1.0)  # 100 clicks = full confidence

        # ----- Build results DataFrame -----
        results = pd.DataFrame({
            "keyword_id": features_df["keyword_id"],
            "prediction_date": prediction_date,
            "predicted_ctr": predicted_ctr,
            "predicted_cvr": predicted_cvr,
            "predicted_cpc": current_bid,
            "predicted_revenue": predicted_revenue,
            "predicted_acos": predicted_acos,
            "expected_profit_per_click": expected_profit_per_click,
            "expected_value_per_impression": predicted_ctr * expected_profit_per_click,
            "recommended_bid": recommended_bid,
            "prediction_confidence": confidence,
            "data_points_used": data_points,
            "model_version": self.model_version,
        })

        # Store predictions
        self._store_predictions(results)

        logger.info(
            "inference_complete",
            keywords=len(results),
            avg_recommended_bid=results["recommended_bid"].mean(),
            avg_predicted_cvr=results["predicted_cvr"].mean(),
        )

        return results

    def _store_predictions(self, predictions: pd.DataFrame):
        """Store predictions in the database."""
        with get_db() as db:
            for _, row in predictions.iterrows():
                db.execute(
                    text("""
                        INSERT INTO predictions (
                            keyword_id, prediction_date, model_version,
                            predicted_ctr, predicted_cvr, predicted_cpc,
                            predicted_revenue, predicted_acos,
                            expected_profit_per_click,
                            expected_value_per_impression,
                            recommended_bid,
                            prediction_confidence, data_points_used
                        ) VALUES (
                            :keyword_id, :prediction_date, :model_version,
                            :predicted_ctr, :predicted_cvr, :predicted_cpc,
                            :predicted_revenue, :predicted_acos,
                            :expected_profit_per_click,
                            :expected_value_per_impression,
                            :recommended_bid,
                            :prediction_confidence, :data_points_used
                        )
                        ON CONFLICT (keyword_id, prediction_date) DO UPDATE SET
                            predicted_ctr = EXCLUDED.predicted_ctr,
                            predicted_cvr = EXCLUDED.predicted_cvr,
                            predicted_revenue = EXCLUDED.predicted_revenue,
                            predicted_acos = EXCLUDED.predicted_acos,
                            recommended_bid = EXCLUDED.recommended_bid,
                            prediction_confidence = EXCLUDED.prediction_confidence
                    """),
                    row.to_dict(),
                )

        logger.info("predictions_stored", count=len(predictions))
