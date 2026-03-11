"""Run feature engineering pipeline on existing data."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.features.feature_engineering import FeatureEngineer

if __name__ == "__main__":
    engineer = FeatureEngineer()
    engineer.compute_features()
    print("Feature engineering complete.")
