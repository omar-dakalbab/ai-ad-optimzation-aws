"""Run the full optimization pipeline (predictions + bids + budgets + keywords)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.inference import ModelInference
from src.optimization.bid_optimizer import BidOptimizer
from src.optimization.budget_allocator import BudgetAllocator
from src.optimization.keyword_manager import KeywordManager

if __name__ == "__main__":
    print("=== Step 1: Generating predictions ===")
    inference = ModelInference()
    predictions = inference.predict_all_keywords()
    print(f"  Predictions generated for {len(predictions)} keywords\n")

    print("=== Step 2: Bid optimization ===")
    bid_optimizer = BidOptimizer()
    bid_recs = bid_optimizer.generate_bid_recommendations()
    increases = sum(1 for r in bid_recs if r.change_pct > 0)
    decreases = sum(1 for r in bid_recs if r.change_pct < 0)
    print(f"  {len(bid_recs)} bid recommendations ({increases} up, {decreases} down)")
    for r in bid_recs[:5]:
        direction = "UP" if r.change_pct > 0 else "DOWN"
        print(f"    [{direction}] keyword={r.keyword_id} ${r.current_bid} -> ${r.recommended_bid} ({r.change_pct:+.1%}) reason={r.reason}")
    print()

    print("=== Step 3: Budget allocation ===")
    budget_allocator = BudgetAllocator()
    budget_recs = budget_allocator.generate_budget_recommendations()
    print(f"  {len(budget_recs)} budget recommendations")
    for r in budget_recs:
        print(f"    {r.campaign_name}: ${r.current_budget} -> ${r.recommended_budget} ({r.change_pct:+.1%}) ROAS={r.predicted_roas}")
    print()

    print("=== Step 4: Keyword management ===")
    keyword_mgr = KeywordManager()
    actions = keyword_mgr.run_keyword_management()
    for a in actions:
        print(f"    [{a.action}] '{a.keyword_text}' - {a.reason}")
    print()

    print("=== Done (dry run - no changes applied to Amazon) ===")
