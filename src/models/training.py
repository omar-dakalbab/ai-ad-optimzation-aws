"""
ML Model Training Pipeline

Trains three core models:

  Model 1 - Click Model (LightGBM Classifier)
    Input:  keyword features
    Output: P(click | impression)
    Use:    predict which keywords will get clicks

  Model 2 - Conversion Model (XGBoost Classifier)
    Input:  keyword features
    Output: P(conversion | click)
    Use:    predict which clicks will convert to orders

  Model 3 - Revenue Model (Gradient Boosting Regressor)
    Input:  keyword features
    Output: expected revenue per conversion ($)
    Use:    predict how much revenue each conversion generates

  Model 4 - Demand Forecast (Prophet - optional)
    Input:  daily campaign spend/sales time series
    Output: forecasted sales next 7-30 days
    Use:    budget planning

All models are tracked with MLflow for versioning and experiment comparison.
"""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import joblib
import structlog
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import mlflow.lightgbm

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    mean_squared_error, mean_absolute_error, r2_score,
)
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sqlalchemy import text

from src.database.connection import get_db
from src.features.feature_engineering import (
    CLICK_MODEL_FEATURES,
    CONVERSION_MODEL_FEATURES,
    REVENUE_MODEL_FEATURES,
)
from contextlib import contextmanager

from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()

MODEL_DIR = Path("models/artifacts")
MODEL_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def _optional_mlflow_run(active: bool, **kwargs):
    """Wrap mlflow.start_run; yields a no-op when MLflow is unavailable."""
    if active:
        with mlflow.start_run(**kwargs) as run:
            yield run
    else:
        yield None


class ModelTrainer:
    """Trains and evaluates all ML models."""

    def __init__(self):
        try:
            mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
            mlflow.set_experiment(settings.mlflow_experiment_name)
            self._mlflow_available = True
        except Exception:
            logger.warning("mlflow_unavailable, training will proceed without tracking")
            self._mlflow_available = False

    def train_all_models(self, training_date: date = None):
        """Train all models. Called weekly by Airflow."""
        if training_date is None:
            training_date = date.today()

        logger.info("model_training_start", date=str(training_date))

        click_model = self.train_click_model(training_date)
        conversion_model = self.train_conversion_model(training_date)
        revenue_model = self.train_revenue_model(training_date)

        logger.info("model_training_complete")
        return {
            "click_model": click_model,
            "conversion_model": conversion_model,
            "revenue_model": revenue_model,
        }

    # ------------------------------------------------------------------
    # Model 1: Click Probability (LightGBM)
    # ------------------------------------------------------------------

    def train_click_model(self, training_date: date) -> dict:
        """
        Train a LightGBM classifier to predict P(click | impression).

        Target variable: had_click (1 if clicks > 0 on that day, 0 otherwise)

        Why LightGBM:
        - Handles categorical features natively
        - Fast training on large keyword datasets
        - Good with imbalanced classes (most impressions don't get clicks)
        """
        logger.info("training_click_model")

        df = self._load_training_data(training_date)
        if df.empty:
            logger.warning("no_training_data_click_model")
            return {}

        # Target: did this keyword get at least 1 click today?
        df["target"] = (df["clicks_7d"] > 0).astype(int)

        X = df[CLICK_MODEL_FEATURES].fillna(0)
        y = df["target"]

        # Time-series aware split (don't leak future data)
        n_splits = min(3, max(2, len(X) // 5))  # adapt to dataset size
        tscv = TimeSeriesSplit(n_splits=n_splits)
        metrics_list = []

        with _optional_mlflow_run(self._mlflow_available, run_name=f"click_model_{training_date}"):
            for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

                # Skip fold if only one class present
                if y_train.nunique() < 2 or y_val.nunique() < 2:
                    continue

                n_neg = len(y_train[y_train == 0])
                n_pos = len(y_train[y_train == 1])
                spw = max(n_neg / max(n_pos, 1), 0.1)

                model = LGBMClassifier(
                    n_estimators=300,
                    learning_rate=0.05,
                    max_depth=4,
                    num_leaves=15,
                    min_child_samples=max(3, len(X_train) // 10),
                    subsample=0.8,
                    colsample_bytree=0.8,
                    scale_pos_weight=spw,
                    random_state=42,
                    verbose=-1,
                )
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    callbacks=[],
                )

                y_pred_proba = model.predict_proba(X_val)[:, 1]
                auc = roc_auc_score(y_val, y_pred_proba)
                metrics_list.append({"fold": fold, "auc": auc})

            # Train final model on all data
            n_neg = len(y[y == 0])
            n_pos = len(y[y == 1])
            final_model = LGBMClassifier(
                n_estimators=300, learning_rate=0.05, max_depth=4,
                num_leaves=15, min_child_samples=max(3, len(X) // 10),
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=max(n_neg / max(n_pos, 1), 0.1),
                random_state=42, verbose=-1,
            )
            final_model.fit(X, y)

            avg_auc = np.mean([m["auc"] for m in metrics_list]) if metrics_list else 0.5

            if self._mlflow_available:
                mlflow.log_metric("avg_auc", avg_auc)
                mlflow.log_param("n_features", len(CLICK_MODEL_FEATURES))
                mlflow.log_param("n_samples", len(X))
                mlflow.lightgbm.log_model(final_model, "click_model")
                importance = dict(zip(CLICK_MODEL_FEATURES, final_model.feature_importances_))
                mlflow.log_dict(importance, "feature_importance.json")

            # Save locally
            model_path = MODEL_DIR / "click_model.joblib"
            joblib.dump(final_model, model_path)

        self._log_model_performance("click_model", training_date, "auc_roc", avg_auc, len(X))
        logger.info("click_model_trained", auc=avg_auc)
        return {"auc": avg_auc, "model_path": str(model_path)}

    # ------------------------------------------------------------------
    # Model 2: Conversion Probability (XGBoost)
    # ------------------------------------------------------------------

    def train_conversion_model(self, training_date: date) -> dict:
        """
        Train XGBoost classifier to predict P(conversion | click).

        Target variable: had_order (1 if orders > 0 for keywords that got clicks)

        Why XGBoost:
        - Excellent for tabular data with mixed feature types
        - Built-in handling of missing values
        - Strong regularization prevents overfitting on sparse conversion data
        """
        logger.info("training_conversion_model")

        df = self._load_training_data(training_date)
        if df.empty:
            return {}

        # Filter to keywords that had clicks (we predict conversion given click)
        df = df[df["clicks_7d"] > 0].copy()
        df["target"] = (df["orders_7d"] > 0).astype(int)

        X = df[CONVERSION_MODEL_FEATURES].fillna(0)
        y = df["target"]

        if len(X) < 5 or y.nunique() < 2:
            logger.warning("insufficient_data_conversion_model", samples=len(X))
            return {}

        n_splits = min(3, max(2, len(X) // 5))
        tscv = TimeSeriesSplit(n_splits=n_splits)
        metrics_list = []

        with _optional_mlflow_run(self._mlflow_available, run_name=f"conversion_model_{training_date}"):
            for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

                if y_train.nunique() < 2 or y_val.nunique() < 2:
                    continue

                n_neg = len(y_train[y_train == 0])
                n_pos = len(y_train[y_train == 1])

                model = XGBClassifier(
                    n_estimators=300,
                    learning_rate=0.03,
                    max_depth=4,
                    min_child_weight=max(3, len(X_train) // 10),
                    subsample=0.8,
                    colsample_bytree=0.8,
                    scale_pos_weight=max(n_neg / max(n_pos, 1), 0.1),
                    eval_metric="auc",
                    random_state=42,
                    verbosity=0,
                )
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    verbose=False,
                )

                y_pred_proba = model.predict_proba(X_val)[:, 1]
                auc = roc_auc_score(y_val, y_pred_proba)
                precision = precision_score(y_val, (y_pred_proba > 0.5).astype(int), zero_division=0)
                recall = recall_score(y_val, (y_pred_proba > 0.5).astype(int), zero_division=0)
                metrics_list.append({"fold": fold, "auc": auc, "precision": precision, "recall": recall})

            # Final model
            n_neg = len(y[y == 0])
            n_pos = len(y[y == 1])
            final_model = XGBClassifier(
                n_estimators=300, learning_rate=0.03, max_depth=4,
                min_child_weight=max(3, len(X) // 10), subsample=0.8,
                colsample_bytree=0.8,
                scale_pos_weight=max(n_neg / max(n_pos, 1), 0.1),
                random_state=42, verbosity=0,
            )
            final_model.fit(X, y)

            avg_auc = np.mean([m["auc"] for m in metrics_list]) if metrics_list else 0.5

            if self._mlflow_available:
                mlflow.log_metric("avg_auc", avg_auc)
                if metrics_list:
                    mlflow.log_metric("avg_precision", np.mean([m["precision"] for m in metrics_list]))
                    mlflow.log_metric("avg_recall", np.mean([m["recall"] for m in metrics_list]))
                mlflow.xgboost.log_model(final_model, "conversion_model")

            model_path = MODEL_DIR / "conversion_model.joblib"
            joblib.dump(final_model, model_path)

        self._log_model_performance("conversion_model", training_date, "auc_roc", avg_auc, len(X))
        logger.info("conversion_model_trained", auc=avg_auc)
        return {"auc": avg_auc, "model_path": str(model_path)}

    # ------------------------------------------------------------------
    # Model 3: Revenue Prediction (Gradient Boosting Regressor)
    # ------------------------------------------------------------------

    def train_revenue_model(self, training_date: date) -> dict:
        """
        Train a GBM regressor to predict expected revenue per conversion.

        Target variable: revenue_per_order (ad_sales / orders for converting keywords)

        Why GBM Regressor:
        - Handles non-linear revenue patterns
        - Robust to outliers with Huber loss
        - Captures complex interactions (price × category × season)
        """
        logger.info("training_revenue_model")

        df = self._load_training_data(training_date)
        if df.empty:
            return {}

        # Filter to keywords with conversions
        df = df[df["orders_7d"] > 0].copy()
        df["target"] = df["spend_7d"] / df["orders_7d"]  # avg revenue per order

        # Remove extreme outliers
        q99 = df["target"].quantile(0.99)
        df = df[df["target"] <= q99]

        X = df[REVENUE_MODEL_FEATURES].fillna(0)
        y = df["target"]

        if len(X) < 5:
            logger.warning("insufficient_data_revenue_model", samples=len(X))
            return {}

        n_splits = min(3, max(2, len(X) // 3))
        tscv = TimeSeriesSplit(n_splits=n_splits)
        metrics_list = []

        with _optional_mlflow_run(self._mlflow_available, run_name=f"revenue_model_{training_date}"):
            for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

                if len(X_train) < 3 or len(X_val) < 2:
                    continue

                model = GradientBoostingRegressor(
                    n_estimators=200,
                    learning_rate=0.05,
                    max_depth=3,
                    min_samples_leaf=max(2, len(X_train) // 5),
                    subsample=0.8,
                    loss="huber",
                    random_state=42,
                )
                model.fit(X_train, y_train)

                y_pred = model.predict(X_val)
                rmse = np.sqrt(mean_squared_error(y_val, y_pred))
                mae = mean_absolute_error(y_val, y_pred)
                r2 = r2_score(y_val, y_pred)
                metrics_list.append({"fold": fold, "rmse": rmse, "mae": mae, "r2": r2})

            # Final model
            final_model = GradientBoostingRegressor(
                n_estimators=200, learning_rate=0.05, max_depth=3,
                min_samples_leaf=max(2, len(X) // 5),
                subsample=0.8, loss="huber", random_state=42,
            )
            final_model.fit(X, y)

            avg_rmse = np.mean([m["rmse"] for m in metrics_list]) if metrics_list else 0
            avg_r2 = np.mean([m["r2"] for m in metrics_list]) if metrics_list else 0

            if self._mlflow_available:
                mlflow.log_metric("avg_rmse", avg_rmse)
                mlflow.log_metric("avg_r2", avg_r2)
                mlflow.sklearn.log_model(final_model, "revenue_model")

            model_path = MODEL_DIR / "revenue_model.joblib"
            joblib.dump(final_model, model_path)

        self._log_model_performance("revenue_model", training_date, "rmse", avg_rmse, len(X))
        logger.info("revenue_model_trained", rmse=avg_rmse, r2=avg_r2)
        return {"rmse": avg_rmse, "r2": avg_r2, "model_path": str(model_path)}

    # ------------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------------

    def _load_training_data(self, training_date: date) -> pd.DataFrame:
        """Load feature data for model training."""
        with get_db() as db:
            df = pd.read_sql(
                text("""
                    SELECT * FROM keyword_features
                    WHERE computed_date >= :start_date
                      AND computed_date <= :end_date
                    ORDER BY computed_date
                """),
                db.bind,
                params={
                    "start_date": training_date - timedelta(days=settings.prediction_lookback_days),
                    "end_date": training_date,
                },
            )
        return df

    def _log_model_performance(
        self, model_name: str, trained_date: date, metric_name: str,
        metric_value: float, test_set_size: int
    ):
        """Log model performance to the database for tracking."""
        with get_db() as db:
            db.execute(
                text("""
                    INSERT INTO model_performance
                        (model_name, trained_date, metric_name, metric_value, test_set_size)
                    VALUES (:name, :date, :metric, :value, :size)
                """),
                {
                    "name": model_name,
                    "date": trained_date,
                    "metric": metric_name,
                    "value": float(metric_value),
                    "size": int(test_set_size),
                },
            )
