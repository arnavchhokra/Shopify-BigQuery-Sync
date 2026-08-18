import requests as _requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


def _retry_session() -> _requests.Session:
    """Requests session that auto-retries on 429 / 5xx, honouring Retry-After."""
    session = _requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=2,          # waits: 2s, 4s, 8s, 16s … up to ~34 min total
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _make_client(shop_url: str, token: str) -> RESTClient:
    """Build a RESTClient with Shopify's Link-header paginator and retry logic."""
    return RESTClient(
        base_url=f"{shop_url.rstrip('/')}/admin/api/2024-01",
        headers={"X-Shopify-Access-Token": token},
        paginator=HeaderLinkPaginator(),
        session=_retry_session(),
    )


def _items(response_json, key: str):
    """Yield individual records from a Shopify REST response dict."""
    data = response_json.get(key, [])
    if isinstance(data, list):
        yield from data
    elif data:
        yield data


@dlt.source(name="shopify_extended")
def shopify_extended_source(token: str, shop_url: str):
    client = _make_client(shop_url, token)

    # ------------------------------------------------------------------
    # 1. Price Rules (Standalone)
    # ------------------------------------------------------------------
    @dlt.resource(name="price_rules", write_disposition="merge", primary_key="id")
    def price_rules():
        for page in client.paginate("price_rules.json"):
            yield from _items(page.response.json(), "price_rules")

    # ------------------------------------------------------------------
    # 2. Discount Codes (Child of Price Rules)
    # ------------------------------------------------------------------
    @dlt.transformer(data_from=price_rules, name="discount_codes", write_disposition="merge", primary_key="id")
    def discount_codes(price_rule):
        for page in client.paginate(f"price_rules/{price_rule['id']}/discount_codes.json"):
            yield from _items(page.response.json(), "discount_codes")

    # ------------------------------------------------------------------
    # 3. Abandoned Checkouts (Standalone)
    # ------------------------------------------------------------------
    @dlt.resource(name="abandoned_checkouts", write_disposition="merge", primary_key="id")
    def abandoned_checkouts():
        for page in client.paginate("checkouts.json"):
            yield from _items(page.response.json(), "checkouts")

    # ------------------------------------------------------------------
    # 4. Gift Cards (Standalone — Shopify Plus only; soft-fails)
    # ------------------------------------------------------------------
    @dlt.resource(name="gift_cards", write_disposition="merge", primary_key="id")
    def gift_cards():
        try:
            for page in client.paginate("gift_cards.json"):
                yield from _items(page.response.json(), "gift_cards")
        except Exception as e:
            import logging
            logging.warning(f"gift_cards skipped (likely not Shopify Plus): {e}")

    # ------------------------------------------------------------------
    # 5-7. Order sub-resources: fulfillments, transactions, refunds
    #   We paginate orders ourselves so we get individual order dicts,
    #   not page-lists from the verified source which would cause
    #   "list indices must be integers or slices, not str".
    # ------------------------------------------------------------------
    @dlt.resource(name="fulfillments", write_disposition="merge", primary_key="id")
    def fulfillments():
        for order_page in client.paginate("orders.json", params={"status": "any", "limit": 250}):
            for order in _items(order_page.response.json(), "orders"):
                for page in client.paginate(f"orders/{order['id']}/fulfillments.json"):
                    yield from _items(page.response.json(), "fulfillments")

    @dlt.resource(name="transactions", write_disposition="merge", primary_key="id")
    def transactions():
        for order_page in client.paginate("orders.json", params={"status": "any", "limit": 250}):
            for order in _items(order_page.response.json(), "orders"):
                for page in client.paginate(f"orders/{order['id']}/transactions.json"):
                    yield from _items(page.response.json(), "transactions")

    @dlt.resource(name="refunds", write_disposition="merge", primary_key="id")
    def refunds():
        for order_page in client.paginate("orders.json", params={"status": "any", "limit": 250}):
            for order in _items(order_page.response.json(), "orders"):
                for page in client.paginate(f"orders/{order['id']}/refunds.json"):
                    yield from _items(page.response.json(), "refunds")

    # ------------------------------------------------------------------
    # 8. Inventory Items (derived from product variant inventory_item_ids)
    # ------------------------------------------------------------------
    @dlt.resource(name="inventory_items", write_disposition="merge", primary_key="id")
    def inventory_items():
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for product_page in client.paginate("products.json", params={"limit": 250}):
            for product in _items(product_page.response.json(), "products"):
                variants = product.get("variants", [])
                item_ids = [str(v["inventory_item_id"]) for v in variants if v.get("inventory_item_id")]
                for chunk in chunks(item_ids, 100):
                    if not chunk:
                        continue
                    for page in client.paginate("inventory_items.json", params={"ids": ",".join(chunk)}):
                        yield from _items(page.response.json(), "inventory_items")

    return [
        price_rules,
        discount_codes,
        abandoned_checkouts,
        gift_cards,
        fulfillments,
        transactions,
        refunds,
        inventory_items,
    ]
