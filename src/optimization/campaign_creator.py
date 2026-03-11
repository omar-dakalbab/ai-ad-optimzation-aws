"""
Automatic Campaign Creator

Creates a complete Sponsored Products campaign structure for a product (ASIN).

Campaign Structure (proven Amazon PPC framework):

  Product: "Wireless Earbuds" (B0XXXXXXXXX)
  │
  ├── SP | Wireless Earbuds | Auto
  │   └── AG | Auto
  │       └── Product Ad (ASIN)
  │       └── Amazon auto-targets keywords
  │
  ├── SP | Wireless Earbuds | Manual Exact
  │   └── AG | Exact
  │       └── Product Ad (ASIN)
  │       └── seed keywords (exact match)
  │
  └── SP | Wireless Earbuds | Manual Broad
      └── AG | Broad
          └── Product Ad (ASIN)
          └── seed keywords (broad match)

How it works:
  1. Look up the product in the DB (title, category, price, margin)
  2. Generate seed keywords from the product title
  3. Compute starting bids based on product margin
  4. Create campaign → ad group → product ad → keywords via API
  5. Store everything in the DB for the optimizer to manage going forward

Starting Bid Formula:
  base_bid = margin * target_acos * estimated_cvr
  - Auto campaign: base_bid * 0.8  (cheaper discovery)
  - Exact match:   base_bid * 1.2  (premium for high intent)
  - Broad match:   base_bid * 0.9  (mid-range discovery)
"""

import re
from datetime import date
from dataclasses import dataclass, field

import structlog
from sqlalchemy import text

from src.data_ingestion.ads_api_client import AmazonAdsClient
from src.database.connection import get_db
from configs.settings import Settings

logger = structlog.get_logger()
settings = Settings()

# Common Amazon PPC stop words to exclude from seed keywords
STOP_WORDS = {
    "a", "an", "the", "and", "or", "for", "with", "in", "on", "to", "of",
    "by", "is", "it", "at", "as", "from", "that", "this", "be", "are",
    "was", "were", "not", "but", "if", "no", "so", "up", "out", "do",
    "set", "new", "pack", "pcs", "piece",
}

# Default estimated CVR for new products with no data
DEFAULT_ESTIMATED_CVR = 0.10


@dataclass
class CampaignPlan:
    """A planned campaign structure before creation."""
    asin: str
    product_title: str
    product_margin: float
    campaigns: list[dict] = field(default_factory=list)


@dataclass
class CreatedCampaign:
    """Result of a campaign creation."""
    asin: str
    campaign_name: str
    campaign_type: str  # "auto", "manual_exact", "manual_broad"
    amazon_campaign_id: int | None
    amazon_ad_group_id: int | None
    keywords_added: int
    daily_budget: float
    default_bid: float
    success: bool
    error: str | None = None


class CampaignCreator:
    """Creates complete SP campaign structures for products."""

    # Budget allocation for new campaigns
    AUTO_BUDGET_SHARE = 0.40       # 40% of budget to auto
    EXACT_BUDGET_SHARE = 0.35      # 35% to exact
    BROAD_BUDGET_SHARE = 0.25      # 25% to broad

    MIN_DAILY_BUDGET = 10.00       # minimum $10/day per campaign
    DEFAULT_TOTAL_BUDGET = 50.00   # default daily budget if not specified

    # Bid multipliers relative to base_bid
    AUTO_BID_MULTIPLIER = 0.80
    EXACT_BID_MULTIPLIER = 1.20
    BROAD_BID_MULTIPLIER = 0.90

    MAX_SEED_KEYWORDS = 25         # max keywords per ad group

    def __init__(self):
        self.ads_client = AmazonAdsClient()
        self.dry_run = settings.dry_run

    def create_campaigns_for_product(
        self,
        asin: str,
        daily_budget: float | None = None,
        seed_keywords: list[str] | None = None,
        start_paused: bool = False,
    ) -> list[CreatedCampaign]:
        """
        Create the full campaign structure for a product.

        Args:
            asin: The product ASIN to advertise.
            daily_budget: Total daily budget across all campaigns.
                          Split across auto/exact/broad.
            seed_keywords: Optional list of seed keywords. If not provided,
                           auto-generated from product title.
            start_paused: If True, create campaigns in paused state (for review).

        Returns:
            List of CreatedCampaign results.
        """
        # 1. Load product data
        product = self._get_product(asin)
        if not product:
            logger.error("product_not_found", asin=asin)
            return [CreatedCampaign(
                asin=asin, campaign_name="", campaign_type="",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=0, daily_budget=0, default_bid=0,
                success=False, error=f"Product {asin} not found in database",
            )]

        # 2. Check if campaigns already exist for this ASIN
        if self._campaigns_exist(asin):
            logger.warning("campaigns_already_exist", asin=asin)
            return [CreatedCampaign(
                asin=asin, campaign_name="", campaign_type="",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=0, daily_budget=0, default_bid=0,
                success=False, error=f"Campaigns already exist for {asin}",
            )]

        # 3. Plan the campaigns
        total_budget = daily_budget or self.DEFAULT_TOTAL_BUDGET
        product_margin = float(product["margin"] or 0)
        product_title = product["title"] or asin
        short_title = self._shorten_title(product_title)

        # Compute base bid from margin
        base_bid = self._compute_base_bid(product_margin)

        # Generate seed keywords if not provided
        if not seed_keywords:
            seed_keywords = self._generate_seed_keywords(product_title)

        state = "paused" if start_paused else "enabled"
        start_date = date.today().strftime("%Y%m%d")

        logger.info(
            "creating_campaigns",
            asin=asin,
            title=short_title,
            margin=product_margin,
            base_bid=base_bid,
            budget=total_budget,
            seed_keywords=len(seed_keywords),
            dry_run=self.dry_run,
        )

        results = []

        # 4. Create Auto Campaign
        results.append(self._create_auto_campaign(
            asin=asin,
            short_title=short_title,
            budget=round(total_budget * self.AUTO_BUDGET_SHARE, 2),
            bid=round(base_bid * self.AUTO_BID_MULTIPLIER, 2),
            state=state,
            start_date=start_date,
        ))

        # 5. Create Manual Exact Campaign
        results.append(self._create_manual_campaign(
            asin=asin,
            short_title=short_title,
            match_type="exact",
            budget=round(total_budget * self.EXACT_BUDGET_SHARE, 2),
            bid=round(base_bid * self.EXACT_BID_MULTIPLIER, 2),
            keywords=seed_keywords,
            state=state,
            start_date=start_date,
        ))

        # 6. Create Manual Broad Campaign
        results.append(self._create_manual_campaign(
            asin=asin,
            short_title=short_title,
            match_type="broad",
            budget=round(total_budget * self.BROAD_BUDGET_SHARE, 2),
            bid=round(base_bid * self.BROAD_BID_MULTIPLIER, 2),
            keywords=seed_keywords,
            state=state,
            start_date=start_date,
        ))

        # Summary
        successes = sum(1 for r in results if r.success)
        logger.info(
            "campaign_creation_complete",
            asin=asin,
            total=len(results),
            successes=successes,
        )

        return results

    def create_campaigns_for_all_products(
        self,
        daily_budget_per_product: float | None = None,
        start_paused: bool = True,
    ) -> list[CreatedCampaign]:
        """
        Create campaigns for all active products that don't have campaigns yet.

        Args:
            daily_budget_per_product: Budget per product. Defaults to DEFAULT_TOTAL_BUDGET.
            start_paused: Start campaigns paused for review.

        Returns:
            Flat list of all CreatedCampaign results.
        """
        products = self._get_products_without_campaigns()
        if not products:
            logger.info("all_products_have_campaigns")
            return []

        logger.info("creating_campaigns_for_products", count=len(products))

        all_results = []
        for product in products:
            results = self.create_campaigns_for_product(
                asin=product["asin"],
                daily_budget=daily_budget_per_product,
                start_paused=start_paused,
            )
            all_results.extend(results)

        return all_results

    # ------------------------------------------------------------------
    # Campaign Creation Helpers
    # ------------------------------------------------------------------

    def _create_auto_campaign(
        self, asin: str, short_title: str, budget: float,
        bid: float, state: str, start_date: str,
    ) -> CreatedCampaign:
        """Create an auto-targeting campaign."""
        budget = max(budget, self.MIN_DAILY_BUDGET)
        bid = max(bid, settings.min_bid)
        campaign_name = f"SP | {short_title} | Auto"
        ag_name = f"AG | {short_title} | Auto"

        if self.dry_run:
            logger.info("dry_run_create_auto", campaign=campaign_name, budget=budget, bid=bid)
            self._store_campaign_locally(asin, campaign_name, "auto", budget, bid, state)
            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name, campaign_type="auto",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=0, daily_budget=budget, default_bid=bid, success=True,
            )

        try:
            # Create campaign
            camp_result = self.ads_client.create_campaign({
                "name": campaign_name,
                "campaignType": "sponsoredProducts",
                "targetingType": "auto",
                "dailyBudget": budget,
                "startDate": start_date,
                "state": state,
            })
            campaign_id = camp_result[0]["campaignId"]

            # Create ad group
            ag_result = self.ads_client.create_ad_group({
                "campaignId": campaign_id,
                "name": ag_name,
                "defaultBid": bid,
                "state": state,
            })
            ad_group_id = ag_result[0]["adGroupId"]

            # Create product ad
            self.ads_client.create_product_ad({
                "campaignId": campaign_id,
                "adGroupId": ad_group_id,
                "asin": asin,
                "state": state,
            })

            # Store in DB
            self._store_campaign_in_db(
                asin, campaign_name, "auto", campaign_id, ad_group_id,
                budget, bid, state,
            )

            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name, campaign_type="auto",
                amazon_campaign_id=campaign_id, amazon_ad_group_id=ad_group_id,
                keywords_added=0, daily_budget=budget, default_bid=bid, success=True,
            )
        except Exception as e:
            logger.error("auto_campaign_creation_failed", asin=asin, error=str(e))
            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name, campaign_type="auto",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=0, daily_budget=budget, default_bid=bid,
                success=False, error=str(e),
            )

    def _create_manual_campaign(
        self, asin: str, short_title: str, match_type: str,
        budget: float, bid: float, keywords: list[str],
        state: str, start_date: str,
    ) -> CreatedCampaign:
        """Create a manual campaign with keywords."""
        budget = max(budget, self.MIN_DAILY_BUDGET)
        bid = max(bid, settings.min_bid)
        campaign_name = f"SP | {short_title} | Manual {match_type.title()}"
        ag_name = f"AG | {short_title} | {match_type.title()}"

        if self.dry_run:
            logger.info(
                "dry_run_create_manual",
                campaign=campaign_name, budget=budget, bid=bid,
                keywords=len(keywords), match_type=match_type,
            )
            self._store_campaign_locally(asin, campaign_name, f"manual_{match_type}", budget, bid, state)
            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name,
                campaign_type=f"manual_{match_type}",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=len(keywords), daily_budget=budget,
                default_bid=bid, success=True,
            )

        try:
            # Create campaign
            camp_result = self.ads_client.create_campaign({
                "name": campaign_name,
                "campaignType": "sponsoredProducts",
                "targetingType": "manual",
                "dailyBudget": budget,
                "startDate": start_date,
                "state": state,
            })
            campaign_id = camp_result[0]["campaignId"]

            # Create ad group
            ag_result = self.ads_client.create_ad_group({
                "campaignId": campaign_id,
                "name": ag_name,
                "defaultBid": bid,
                "state": state,
            })
            ad_group_id = ag_result[0]["adGroupId"]

            # Create product ad
            self.ads_client.create_product_ad({
                "campaignId": campaign_id,
                "adGroupId": ad_group_id,
                "asin": asin,
                "state": state,
            })

            # Add keywords
            keyword_payloads = [
                {
                    "campaignId": campaign_id,
                    "adGroupId": ad_group_id,
                    "keywordText": kw,
                    "matchType": match_type,
                    "bid": bid,
                }
                for kw in keywords[:self.MAX_SEED_KEYWORDS]
            ]
            if keyword_payloads:
                self.ads_client.add_keywords(keyword_payloads)

            # Store in DB
            db_campaign_id = self._store_campaign_in_db(
                asin, campaign_name, f"manual_{match_type}", campaign_id,
                ad_group_id, budget, bid, state,
            )
            self._store_keywords_in_db(
                db_campaign_id, ad_group_id, asin, keywords[:self.MAX_SEED_KEYWORDS],
                match_type, bid,
            )

            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name,
                campaign_type=f"manual_{match_type}",
                amazon_campaign_id=campaign_id, amazon_ad_group_id=ad_group_id,
                keywords_added=len(keywords[:self.MAX_SEED_KEYWORDS]),
                daily_budget=budget, default_bid=bid, success=True,
            )
        except Exception as e:
            logger.error(
                "manual_campaign_creation_failed",
                asin=asin, match_type=match_type, error=str(e),
            )
            return CreatedCampaign(
                asin=asin, campaign_name=campaign_name,
                campaign_type=f"manual_{match_type}",
                amazon_campaign_id=None, amazon_ad_group_id=None,
                keywords_added=0, daily_budget=budget, default_bid=bid,
                success=False, error=str(e),
            )

    # ------------------------------------------------------------------
    # Keyword Generation
    # ------------------------------------------------------------------

    def _generate_seed_keywords(self, title: str) -> list[str]:
        """
        Generate seed keywords from a product title.

        Strategy:
          - Full title (cleaned) as a phrase
          - 2-word and 3-word combinations from title
          - Individual significant words (4+ chars)

        Example: "Wireless Bluetooth Earbuds with Charging Case"
          → ["wireless bluetooth earbuds", "wireless earbuds",
             "bluetooth earbuds", "earbuds charging case",
             "wireless bluetooth earbuds charging case"]
        """
        # Clean title: remove special chars, lowercase
        cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", title.lower())
        words = [w for w in cleaned.split() if w not in STOP_WORDS and len(w) > 1]

        keywords = set()

        # Full cleaned title
        if len(words) >= 2:
            keywords.add(" ".join(words))

        # 2-word combinations (adjacent)
        for i in range(len(words) - 1):
            keywords.add(f"{words[i]} {words[i+1]}")

        # 3-word combinations (adjacent)
        for i in range(len(words) - 2):
            keywords.add(f"{words[i]} {words[i+1]} {words[i+2]}")

        # Individual significant words (4+ chars, likely product-relevant)
        for w in words:
            if len(w) >= 4:
                keywords.add(w)

        # Sort by length (longer = more specific = higher intent)
        sorted_keywords = sorted(keywords, key=len, reverse=True)
        return sorted_keywords[:self.MAX_SEED_KEYWORDS]

    # ------------------------------------------------------------------
    # Bid Computation
    # ------------------------------------------------------------------

    def _compute_base_bid(self, product_margin: float) -> float:
        """
        Compute starting bid from product margin.

        base_bid = margin * target_acos * estimated_cvr

        Example:
          margin=$8, target_acos=0.25, est_cvr=0.10
          base_bid = $8 * 0.25 * 0.10 = $0.20
          (We can afford $0.20/click if 10% convert at 25% ACoS)

        For products with no margin data, use a conservative default.
        """
        if product_margin <= 0:
            return round(settings.min_bid * 2, 2)  # $0.20 conservative default

        base_bid = product_margin * settings.target_acos * DEFAULT_ESTIMATED_CVR
        # Clamp to min/max
        base_bid = max(base_bid, settings.min_bid)
        base_bid = min(base_bid, settings.max_bid * 0.5)  # start at most 50% of ceiling
        return round(base_bid, 2)

    # ------------------------------------------------------------------
    # Database Operations
    # ------------------------------------------------------------------

    def _get_product(self, asin: str) -> dict | None:
        """Look up a product by ASIN."""
        with get_db() as db:
            result = db.execute(
                text("SELECT * FROM products WHERE asin = :asin AND status = 'active'"),
                {"asin": asin},
            ).mappings().first()
            return dict(result) if result else None

    def _campaigns_exist(self, asin: str) -> bool:
        """Check if any active campaigns already target this ASIN."""
        with get_db() as db:
            count = db.execute(
                text("""
                    SELECT COUNT(*) FROM keywords k
                    JOIN campaigns c ON c.id = k.campaign_id
                    WHERE k.asin = :asin AND c.state != 'archived'
                """),
                {"asin": asin},
            ).scalar()
            return count > 0

    def _get_products_without_campaigns(self) -> list[dict]:
        """Find active products with no campaigns."""
        with get_db() as db:
            results = db.execute(
                text("""
                    SELECT p.* FROM products p
                    WHERE p.status = 'active'
                      AND NOT EXISTS (
                          SELECT 1 FROM keywords k
                          JOIN campaigns c ON c.id = k.campaign_id
                          WHERE k.asin = p.asin AND c.state != 'archived'
                      )
                    ORDER BY p.margin DESC
                """),
            ).mappings().all()
            return [dict(r) for r in results]

    def _store_campaign_locally(
        self, asin: str, name: str, campaign_type: str,
        budget: float, bid: float, state: str,
    ):
        """Store a dry-run campaign plan in the DB (no amazon IDs)."""
        with get_db() as db:
            db.execute(
                text("""
                    INSERT INTO campaigns
                        (amazon_campaign_id, name, campaign_type, targeting_type,
                         state, daily_budget)
                    VALUES (:amazon_id, :name, 'sponsoredProducts', :targeting,
                            :state, :budget)
                """),
                {
                    "amazon_id": 0,  # placeholder for dry run
                    "name": name,
                    "targeting": "auto" if campaign_type == "auto" else "manual",
                    "state": state,
                    "budget": budget,
                },
            )

    def _store_campaign_in_db(
        self, asin: str, name: str, campaign_type: str,
        amazon_campaign_id: int, amazon_ad_group_id: int,
        budget: float, bid: float, state: str,
    ) -> int:
        """Store the created campaign and ad group in the DB. Returns campaign DB id."""
        with get_db() as db:
            campaign_id = db.execute(
                text("""
                    INSERT INTO campaigns
                        (amazon_campaign_id, name, campaign_type, targeting_type,
                         state, daily_budget, start_date)
                    VALUES (:amazon_id, :name, 'sponsoredProducts', :targeting,
                            :state, :budget, CURRENT_DATE)
                    RETURNING id
                """),
                {
                    "amazon_id": amazon_campaign_id,
                    "name": name,
                    "targeting": "auto" if campaign_type == "auto" else "manual",
                    "state": state,
                    "budget": budget,
                },
            ).scalar()

            db.execute(
                text("""
                    INSERT INTO ad_groups
                        (amazon_ad_group_id, campaign_id, name, default_bid, state)
                    VALUES (:ag_id, :campaign_id, :name, :bid, :state)
                """),
                {
                    "ag_id": amazon_ad_group_id,
                    "campaign_id": campaign_id,
                    "name": name.replace("SP |", "AG |"),
                    "bid": bid,
                    "state": state,
                },
            )
            return campaign_id

    def _store_keywords_in_db(
        self, campaign_id: int, amazon_ad_group_id: int, asin: str,
        keywords: list[str], match_type: str, bid: float,
    ):
        """Store seed keywords in the DB."""
        with get_db() as db:
            # Get the ad_group DB id
            ag_id = db.execute(
                text("SELECT id FROM ad_groups WHERE amazon_ad_group_id = :ag_id"),
                {"ag_id": amazon_ad_group_id},
            ).scalar()

            for i, kw in enumerate(keywords):
                db.execute(
                    text("""
                        INSERT INTO keywords
                            (amazon_keyword_id, ad_group_id, campaign_id,
                             keyword_text, match_type, bid, state, asin)
                        VALUES (:akid, :ag_id, :campaign_id,
                                :text, :match, :bid, 'enabled', :asin)
                    """),
                    {
                        "akid": 0,  # will be updated after API returns IDs
                        "ag_id": ag_id,
                        "campaign_id": campaign_id,
                        "text": kw,
                        "match": match_type,
                        "bid": bid,
                        "asin": asin,
                    },
                )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _shorten_title(self, title: str, max_words: int = 4) -> str:
        """Shorten a product title for campaign naming."""
        words = title.split()[:max_words]
        short = " ".join(words)
        if len(short) > 50:
            short = short[:47] + "..."
        return short
