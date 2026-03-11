"""Run model training pipeline."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Disable MLflow remote tracking for local testing
os.environ["AAI_MLFLOW_TRACKING_URI"] = ""

import mlflow
mlflow.set_tracking_uri("")  # use local ./mlruns directory

from src.models.training import ModelTrainer

if __name__ == "__main__":
    trainer = ModelTrainer()
    results = trainer.train_all_models()
    print("\nTraining results:")
    for model_name, metrics in results.items():
        print(f"  {model_name}: {metrics}")
