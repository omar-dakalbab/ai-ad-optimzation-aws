"""
Amazon Selling Partner API (SP-API) Client
Retrieves sales, inventory, and product data.

Uses the python-amazon-sp-api library for simplified access.
"""

from datetime import date, timedelta
from typing import Optional

import structlog
from sp_api.api import Orders, CatalogItems, Reports, Inventories
from sp_api.base import Marketplaces

from configs.settings import Settings

logger = structlog.get_logger()


class SellingPartnerClient:
    """Wrapper around SP-API for sales and product data."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.credentials = {
            "refresh_token": self.settings.sp_api_refresh_token,
            "lwa_app_id": self.settings.sp_api_lwa_app_id,
            "lwa_client_secret": self.settings.sp_api_lwa_client_secret,
            "aws_access_key": self.settings.sp_api_aws_access_key,
            "aws_secret_key": self.settings.sp_api_aws_secret_key,
            "role_arn": self.settings.sp_api_role_arn,
        }
        self.marketplace = Marketplaces.US

    # ------------------------------------------------------------------
    # Orders & Sales
    # ------------------------------------------------------------------

    def get_orders(self, start_date: date, end_date: date) -> list[dict]:
        """
        Fetch orders within a date range.
        Returns order-level data including ASINs, quantities, and revenue.
        """
        orders_api = Orders(credentials=self.credentials, marketplace=self.marketplace)

        all_orders = []
        response = orders_api.get_orders(
            CreatedAfter=start_date.isoformat(),
            CreatedBefore=end_date.isoformat(),
            OrderStatuses=["Shipped", "Unshipped"],
        )

        orders = response.payload.get("Orders", [])
        all_orders.extend(orders)

        # Handle pagination
        next_token = response.payload.get("NextToken")
        while next_token:
            response = orders_api.get_orders(NextToken=next_token)
            orders = response.payload.get("Orders", [])
            all_orders.extend(orders)
            next_token = response.payload.get("NextToken")

        logger.info("orders_fetched", count=len(all_orders), start=str(start_date))
        return all_orders

    def get_order_items(self, order_id: str) -> list[dict]:
        """Fetch line items for a specific order (ASIN, quantity, price)."""
        orders_api = Orders(credentials=self.credentials, marketplace=self.marketplace)
        response = orders_api.get_order_items(order_id)
        return response.payload.get("OrderItems", [])

    # ------------------------------------------------------------------
    # Product Catalog
    # ------------------------------------------------------------------

    def get_product_details(self, asin: str) -> dict:
        """Fetch product details: title, category, price, rating."""
        catalog_api = CatalogItems(credentials=self.credentials, marketplace=self.marketplace)
        response = catalog_api.get_catalog_item(asin=asin, includedData=["summaries", "attributes"])
        return response.payload

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    def get_inventory_levels(self) -> list[dict]:
        """Fetch FBA inventory levels for all products."""
        inv_api = Inventories(credentials=self.credentials, marketplace=self.marketplace)
        response = inv_api.get_inventory_summary_marketplace(
            details=True,
            marketplaceIds=[self.settings.marketplace_id],
        )
        inventories = response.payload.get("inventorySummaries", [])
        logger.info("inventory_fetched", count=len(inventories))
        return inventories

    # ------------------------------------------------------------------
    # Sales Reports (Business Reports)
    # ------------------------------------------------------------------

    def request_sales_report(self, start_date: date, end_date: date) -> str:
        """
        Request a Sales and Traffic Business Report.
        Returns report_id to poll.
        """
        reports_api = Reports(credentials=self.credentials, marketplace=self.marketplace)
        response = reports_api.create_report(
            reportType="GET_SALES_AND_TRAFFIC_REPORT",
            dataStartTime=start_date.isoformat(),
            dataEndTime=end_date.isoformat(),
            marketplaceIds=[self.settings.marketplace_id],
            reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"},
        )
        report_id = response.payload["reportId"]
        logger.info("sales_report_requested", report_id=report_id)
        return report_id

    def get_report_document(self, report_id: str) -> dict:
        """Download a completed report."""
        reports_api = Reports(credentials=self.credentials, marketplace=self.marketplace)
        # First check status
        status = reports_api.get_report(report_id)
        if status.payload["processingStatus"] != "DONE":
            return {"status": status.payload["processingStatus"]}

        doc_id = status.payload["reportDocumentId"]
        doc = reports_api.get_report_document(doc_id, decrypt=True)
        return doc.payload
