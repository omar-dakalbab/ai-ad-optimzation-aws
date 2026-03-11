"""
Airflow DAG: Daily Optimization Pipeline

Schedule: Every day at 6:00 AM UTC
Runs the complete data → features → predictions → optimization → execution pipeline.

DAG Graph:
  sync_data ──► create_campaigns ──► compute_features ──► run_inference ──► optimize_bids ──► execute_changes
                                                                         ──► optimize_budgets ──┘
                                                                         ──► manage_keywords ──┘
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "amazon-ads-ai",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_sync_data(**context):
    """Pull data from Amazon APIs."""
    from src.data_ingestion.data_loader import DataLoader
    loader = DataLoader()
    report_date = context["ds"]  # Airflow execution date
    loader.run_daily_load(report_date=datetime.strptime(report_date, "%Y-%m-%d").date())


def task_create_campaigns(**context):
    """Create campaigns for new products that don't have any yet."""
    from src.optimization.campaign_creator import CampaignCreator
    creator = CampaignCreator()
    results = creator.create_campaigns_for_all_products(start_paused=True)
    created = sum(1 for r in results if r.success)
    context["ti"].xcom_push(key="campaigns_created", value=created)


def task_compute_features(**context):
    """Compute ML features for all keywords."""
    from src.features.feature_engineering import FeatureEngineer
    engineer = FeatureEngineer()
    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    engineer.compute_features(compute_date=report_date)


def task_run_inference(**context):
    """Generate ML predictions."""
    from src.models.inference import ModelInference
    inference = ModelInference()
    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    inference.predict_all_keywords(prediction_date=report_date)


def task_optimize_bids(**context):
    """Generate bid recommendations."""
    from src.optimization.bid_optimizer import BidOptimizer
    optimizer = BidOptimizer()
    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    recs = optimizer.generate_bid_recommendations(prediction_date=report_date)
    # Pass to execution task via XCom
    context["ti"].xcom_push(key="bid_recs_count", value=len(recs))


def task_optimize_budgets(**context):
    """Generate budget allocation recommendations."""
    from src.optimization.budget_allocator import BudgetAllocator
    allocator = BudgetAllocator()
    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    recs = allocator.generate_budget_recommendations(prediction_date=report_date)
    context["ti"].xcom_push(key="budget_recs_count", value=len(recs))


def task_manage_keywords(**context):
    """Run keyword harvesting, negation, and pausing."""
    from src.optimization.keyword_manager import KeywordManager
    manager = KeywordManager()
    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    actions = manager.run_keyword_management(eval_date=report_date)
    context["ti"].xcom_push(key="keyword_actions_count", value=len(actions))


def task_execute_changes(**context):
    """Apply all optimizations via Amazon Ads API."""
    from src.optimization.bid_optimizer import BidOptimizer
    from src.optimization.budget_allocator import BudgetAllocator
    from src.optimization.keyword_manager import KeywordManager
    from src.automation.executor import AutomationExecutor

    report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    # Re-generate recommendations (they're stored in DB)
    bid_recs = BidOptimizer().generate_bid_recommendations(report_date)
    budget_recs = BudgetAllocator().generate_budget_recommendations(report_date)
    keyword_actions = KeywordManager().run_keyword_management(report_date)

    executor = AutomationExecutor()
    result = executor.execute_all(bid_recs, budget_recs, keyword_actions)

    context["ti"].xcom_push(key="execution_result", value=result)


with DAG(
    dag_id="daily_optimization",
    default_args=default_args,
    description="Daily Amazon Ads AI optimization pipeline",
    schedule_interval="0 6 * * *",  # 6 AM UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["amazon-ads", "ml", "optimization"],
) as dag:

    sync_data = PythonOperator(
        task_id="sync_data",
        python_callable=task_sync_data,
    )

    create_campaigns = PythonOperator(
        task_id="create_campaigns",
        python_callable=task_create_campaigns,
    )

    compute_features = PythonOperator(
        task_id="compute_features",
        python_callable=task_compute_features,
    )

    run_inference = PythonOperator(
        task_id="run_inference",
        python_callable=task_run_inference,
    )

    optimize_bids = PythonOperator(
        task_id="optimize_bids",
        python_callable=task_optimize_bids,
    )

    optimize_budgets = PythonOperator(
        task_id="optimize_budgets",
        python_callable=task_optimize_budgets,
    )

    manage_keywords = PythonOperator(
        task_id="manage_keywords",
        python_callable=task_manage_keywords,
    )

    execute_changes = PythonOperator(
        task_id="execute_changes",
        python_callable=task_execute_changes,
    )

    # DAG dependencies
    sync_data >> create_campaigns >> compute_features >> run_inference
    run_inference >> [optimize_bids, optimize_budgets, manage_keywords]
    [optimize_bids, optimize_budgets, manage_keywords] >> execute_changes


# ------------------------------------------------------------------
# Weekly Model Retraining DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="weekly_model_retrain",
    default_args=default_args,
    description="Weekly ML model retraining",
    schedule_interval="0 2 * * 0",  # 2 AM UTC every Sunday
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["amazon-ads", "ml", "training"],
) as retrain_dag:

    def task_retrain_models(**context):
        from src.models.training import ModelTrainer
        trainer = ModelTrainer()
        report_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
        results = trainer.train_all_models(training_date=report_date)
        context["ti"].xcom_push(key="training_results", value=str(results))

    retrain = PythonOperator(
        task_id="retrain_all_models",
        python_callable=task_retrain_models,
    )
