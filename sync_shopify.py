import os
import sys
import requests
import airbyte as ab

def get_access_token(shop: str, client_id: str, client_secret: str) -> str:
    url = f"https://{shop}.myshopify.com/admin/oauth/access_token"
    resp = requests.post(
        url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def main():
    shop = os.environ["SHOPIFY_SHOP"]
    token = get_access_token(
        shop,
        os.environ["SHOPIFY_CLIENT_ID"],
        os.environ["SHOPIFY_CLIENT_SECRET"],
    )

    source = ab.get_source(
        "source-shopify",
        install_if_missing=True,
        config={
            "shop": shop,
            "credentials": {
                "auth_method": "api_password",
                "api_password": token,
            },
            "start_date": "2022-01-01",
        },
        streams=[
            "orders", "transactions", "order_refunds", "tender_transactions",
            "customers", "customer_address",
            "products", "product_variants", "product_images",
            "collections", "custom_collections", "smart_collections", "collects",
            "inventory_items", "inventory_levels", "locations",
            "fulfillment_orders",
            "discount_codes", "price_rules", "abandoned_checkouts",
            "order_risks", "draft_orders",
            "metafield_products", "metafield_customers",
            "metafield_orders", "metafield_collections",
            "shop",
        ],
    )

    print("Validating connection...")
    source.check()

    cache = ab.caches.BigQueryCache(
        project_name=os.environ["GCP_PROJECT_ID"],
        dataset_name="raw_shopify",
        credentials_path=os.environ["GCP_SA_KEY_PATH"],
    )

    print("Starting sync...")
    result = source.read(cache=cache)
    print(f"Sync complete. Streams processed: {list(result.processed_streams)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Sync failed: {e}", file=sys.stderr)
        sys.exit(1)
