"""
Amazon Ads API Client
Handles authentication and data retrieval from the Amazon Advertising API.

API Docs: https://advertising.amazon.com/API/docs/en-us

Key endpoints used:
- Sponsored Products Reports (v3) - campaign, keyword, search term reports
- Sponsored Products Campaigns - CRUD operations
- Sponsored Products Keywords - bid updates, keyword management
"""

import gzip
import time
import json
from datetime import date, timedelta
from typing import Optional

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from configs.settings import Settings

logger = structlog.get_logger()


class AmazonAdsClient:
    """Client for Amazon Advertising API v3."""

    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    BASE_URLS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com",
    }

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.base_url = self.BASE_URLS[self.settings.amazon_ads_region]
        self.access_token: Optional[str] = None
        self.token_expiry: float = 0
        self.client = httpx.Client(timeout=60.0)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _refresh_access_token(self):
        """Exchange refresh token for a new access token."""
        resp = self.client.post(
            self.TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.settings.amazon_ads_refresh_token,
                "client_id": self.settings.amazon_ads_client_id,
                "client_secret": self.settings.amazon_ads_client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.token_expiry = time.time() + data["expires_in"] - 60  # 60s buffer
        logger.info("ads_api_token_refreshed")

    def _get_headers(self) -> dict:
        if time.time() >= self.token_expiry:
            self._refresh_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.settings.amazon_ads_client_id,
            "Amazon-Advertising-API-Scope": self.settings.amazon_ads_profile_id,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Report Requests (Async Report API v3)
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def request_sp_keyword_report(self, report_date: date) -> str:
        """
        Request a Sponsored Products keyword-level performance report.
        Returns the report_id to poll for completion.
        """
        payload = {
            "reportDate": report_date.isoformat(),
            "metrics": [
                "impressions", "clicks", "cost", "purchases1d",
                "sales1d", "unitsSold1d", "kindleEditionNormalizedPagesRoyalties14d"
            ],
            "segment": "query",  # gives search term breakout
            "creativeType": "all",
        }
        resp = self.client.post(
            f"{self.base_url}/sp/keywords/report",
            headers=self._get_headers(),
            json=payload,
        )
        resp.raise_for_status()
        report_id = resp.json()["reportId"]
        logger.info("report_requested", report_id=report_id, date=str(report_date))
        return report_id

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def request_search_term_report(self, report_date: date) -> str:
        """Request a search term report for the given date."""
        payload = {
            "reportDate": report_date.isoformat(),
            "metrics": [
                "impressions", "clicks", "cost", "purchases1d", "sales1d"
            ],
        }
        resp = self.client.post(
            f"{self.base_url}/sp/searchTerms/report",
            headers=self._get_headers(),
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()["reportId"]

    def poll_report(self, report_id: str, max_wait: int = 300) -> dict:
        """
        Poll until report is ready, then download and return parsed data.
        Amazon reports typically take 1-5 minutes to generate.
        """
        start = time.time()
        while time.time() - start < max_wait:
            resp = self.client.get(
                f"{self.base_url}/v2/reports/{report_id}",
                headers=self._get_headers(),
            )
            resp.raise_for_status()
            status = resp.json()

            if status["status"] == "SUCCESS":
                return self._download_report(status["location"])
            elif status["status"] == "FAILURE":
                raise RuntimeError(f"Report {report_id} failed: {status}")

            logger.info("report_polling", report_id=report_id, status=status["status"])
            time.sleep(15)

        raise TimeoutError(f"Report {report_id} not ready after {max_wait}s")

    def _download_report(self, url: str) -> list[dict]:
        """Download and decompress the report file."""
        resp = self.client.get(url, headers=self._get_headers())
        resp.raise_for_status()
        # Reports are gzipped JSON
        data = gzip.decompress(resp.content)
        return json.loads(data)

    # ------------------------------------------------------------------
    # Campaign & Keyword CRUD
    # ------------------------------------------------------------------

    def get_campaigns(self) -> list[dict]:
        """Fetch all SP campaigns."""
        resp = self.client.get(
            f"{self.base_url}/sp/campaigns",
            headers=self._get_headers(),
            params={"stateFilter": "enabled,paused"},
        )
        resp.raise_for_status()
        return resp.json()

    def get_keywords(self, campaign_id: int) -> list[dict]:
        """Fetch all keywords for a campaign."""
        resp = self.client.get(
            f"{self.base_url}/sp/keywords",
            headers=self._get_headers(),
            params={"campaignIdFilter": campaign_id},
        )
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def update_keyword_bids(self, updates: list[dict]) -> dict:
        """
        Batch update keyword bids.

        updates: [{"keywordId": 123, "bid": 1.50}, ...]
        Amazon allows up to 1000 keywords per request.
        """
        resp = self.client.put(
            f"{self.base_url}/sp/keywords",
            headers=self._get_headers(),
            json=updates,
        )
        resp.raise_for_status()
        result = resp.json()
        logger.info("bids_updated", count=len(updates), result=result)
        return result

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def update_campaign_budget(self, campaign_id: int, new_budget: float) -> dict:
        """Update a campaign's daily budget."""
        resp = self.client.put(
            f"{self.base_url}/sp/campaigns",
            headers=self._get_headers(),
            json=[{"campaignId": campaign_id, "dailyBudget": new_budget}],
        )
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def add_negative_keywords(self, negatives: list[dict]) -> dict:
        """
        Add negative keywords to campaigns.

        negatives: [{"campaignId": 123, "keywordText": "free", "matchType": "negativeExact"}]
        """
        resp = self.client.post(
            f"{self.base_url}/sp/negativeKeywords",
            headers=self._get_headers(),
            json=negatives,
        )
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def add_keywords(self, keywords: list[dict]) -> dict:
        """
        Add new keywords to ad groups.

        keywords: [{"campaignId": 1, "adGroupId": 2, "keywordText": "...", "matchType": "exact", "bid": 1.0}]
        """
        resp = self.client.post(
            f"{self.base_url}/sp/keywords",
            headers=self._get_headers(),
            json=keywords,
        )
        resp.raise_for_status()
        return resp.json()

    def pause_keywords(self, keyword_ids: list[int]) -> dict:
        """Pause a list of keywords."""
        updates = [{"keywordId": kid, "state": "paused"} for kid in keyword_ids]
        resp = self.client.put(
            f"{self.base_url}/sp/keywords",
            headers=self._get_headers(),
            json=updates,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Campaign Creation
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def create_campaign(self, campaign: dict) -> dict:
        """
        Create a Sponsored Products campaign.

        campaign: {
            "name": "SP | Earbuds | Auto",
            "campaignType": "sponsoredProducts",
            "targetingType": "auto" | "manual",
            "dailyBudget": 25.00,
            "startDate": "20260311",
            "state": "enabled" | "paused",
        }
        """
        resp = self.client.post(
            f"{self.base_url}/sp/campaigns",
            headers=self._get_headers(),
            json=[campaign],
        )
        resp.raise_for_status()
        result = resp.json()
        logger.info("campaign_created", name=campaign["name"], result=result)
        return result

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def create_ad_group(self, ad_group: dict) -> dict:
        """
        Create an ad group within a campaign.

        ad_group: {
            "campaignId": 123,
            "name": "AG | Earbuds | Exact",
            "defaultBid": 0.75,
            "state": "enabled",
        }
        """
        resp = self.client.post(
            f"{self.base_url}/sp/adGroups",
            headers=self._get_headers(),
            json=[ad_group],
        )
        resp.raise_for_status()
        result = resp.json()
        logger.info("ad_group_created", name=ad_group["name"], result=result)
        return result

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def create_product_ad(self, product_ad: dict) -> dict:
        """
        Create a product ad (attach an ASIN to an ad group).

        product_ad: {
            "campaignId": 123,
            "adGroupId": 456,
            "asin": "B0XXXXXXXXX",
            "state": "enabled",
        }
        """
        resp = self.client.post(
            f"{self.base_url}/sp/productAds",
            headers=self._get_headers(),
            json=[product_ad],
        )
        resp.raise_for_status()
        result = resp.json()
        logger.info("product_ad_created", asin=product_ad["asin"], result=result)
        return result
