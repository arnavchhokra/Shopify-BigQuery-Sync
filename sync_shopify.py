"""
Shopify → BigQuery sync via dlt (dlt-hub verified source).

Credentials are passed entirely via environment variables — no .dlt/secrets.toml
files are needed, so no extra GitHub Secrets are required beyond what already
exists in the repo.

Environment variables (all sourced from existing GitHub Secrets):
  SHOPIFY_SHOP            – shop subdomain, e.g. "my-store"
  SHOPIFY_CLIENT_ID       – Shopify app client ID  (used to obtain the access token)
  SHOPIFY_CLIENT_SECRET   – Shopify app client secret
  GCP_PROJECT_ID          – GCP project for BigQuery
  GOOGLE_APPLICATION_CREDENTIALS – path to GCP service-account JSON key file
                                   (written by the workflow before this runs)

Optional:
  BACKFILL_START_DATE     – ISO-8601 date/datetime for the earliest data to pull
                             on the very first (or forced) backfill, e.g. "2022-01-01".
                             dlt persists cursor state between runs, so on incremental
                             runs this is ignored and the stored cursor is used instead.
                             Default: "2022-01-01"
"""

import os
import sys

import requests as http_requests
import dlt
from dlt.common import pendulum
from shopify_dlt import shopify_source  # pulled into repo by: dlt init shopify_dlt bigquery


# ---------------------------------------------------------------------------
# Streams to sync
# ---------------------------------------------------------------------------
STREAMS = [
    "orders",
    "customers",
    "products",
    "inventory_items",
    "fulfillments",
    "transactions",
    "refunds",
    "discount_codes",
    "price_rules",
    "gift_cards",
    "abandoned_checkouts",
]


def get_access_token(shop: str, client_id: str, client_secret: str) -> str:
    """Exchange client_id/secret for a fresh Admin API access token."""
    url = f"https://{shop}.myshopify.com/admin/oauth/access_token"
    resp = http_requests.post(
        url,
        json={"client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def main() -> None:
    shop = os.environ["SHOPIFY_SHOP"]
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not token:
        token = get_access_token(
            shop,
            os.environ["SHOPIFY_CLIENT_ID"],
            os.environ["SHOPIFY_CLIENT_SECRET"],
        )

    shop_url = f"https://{shop}.myshopify.com"

    # BACKFILL_START_DATE is only the initial lower bound.
    # After the first run, dlt's incremental state takes over automatically.
    raw_start = os.environ.get("BACKFILL_START_DATE", "2022-01-01")
    start_date = pendulum.parse(raw_start)

    # ------------------------------------------------------------------
    # Pipeline definition
    # GOOGLE_APPLICATION_CREDENTIALS env var is picked up automatically
    # by the google-auth library that dlt's BigQuery destination uses.
    # ------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name="shopify_to_bigquery",
        destination=dlt.destinations.bigquery(
            project_id=os.environ["GCP_PROJECT_ID"],
        ),
        dataset_name="raw_shopify_gh",
    )

    source = shopify_source(
        private_app_password=token,
        shop_url=shop_url,
        start_date=start_date,
    ).with_resources(*STREAMS)

    print(
        f"Starting dlt sync | shop={shop_url} | start_date={start_date.isoformat()} "
        f"| streams={STREAMS}"
    )

    # write_disposition="merge" → dlt UPSERTs rows using the primary key
    # declared in shopify_dlt, so re-running never creates duplicate rows.
    load_info = pipeline.run(source, write_disposition="merge")

    print(load_info)
    print("Sync complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Sync failed: {exc}", file=sys.stderr)
        sys.exit(1)
