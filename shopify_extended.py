import dlt
from dlt.sources.helpers.rest_client import RESTClient

@dlt.source(name="shopify_extended")
def shopify_extended_source(token: str, shop_url: str, orders_resource, products_resource):
    # Ensure shop_url doesn't have a trailing slash
    shop_url = shop_url.rstrip("/")
    client = RESTClient(
        base_url=f"{shop_url}/admin/api/2024-01",
        headers={"X-Shopify-Access-Token": token}
    )

    def get_items(page_data, key):
        """Safely extract a list of records from a dlt PageData or plain dict."""
        if hasattr(page_data, "response"):
            return page_data.response.json().get(key, [])
        if isinstance(page_data, dict):
            return page_data.get(key, [])
        return list(page_data) if page_data else []

    # 1. Price Rules (Standalone)
    @dlt.resource(name="price_rules", write_disposition="merge", primary_key="id")
    def price_rules():
        for page in client.paginate("price_rules.json"):
            yield from get_items(page, "price_rules")

    # 2. Discount Codes (Child of Price Rules) — receives individual price_rule dicts
    @dlt.transformer(data_from=price_rules, name="discount_codes", write_disposition="merge", primary_key="id")
    def discount_codes(price_rule):
        for page in client.paginate(f"price_rules/{price_rule['id']}/discount_codes.json"):
            yield from get_items(page, "discount_codes")

    # 3. Abandoned Checkouts (Standalone)
    @dlt.resource(name="abandoned_checkouts", write_disposition="merge", primary_key="id")
    def abandoned_checkouts():
        for page in client.paginate("checkouts.json"):
            yield from get_items(page, "checkouts")

    # 4. Gift Cards (Standalone — Shopify Plus only; fails gracefully)
    @dlt.resource(name="gift_cards", write_disposition="merge", primary_key="id")
    def gift_cards():
        try:
            for page in client.paginate("gift_cards.json"):
                yield from get_items(page, "gift_cards")
        except Exception as e:
            import logging
            logging.warning(f"Could not fetch gift cards (often requires Shopify Plus): {e}")

    # 5. Fulfillments (Child of Orders)
    @dlt.transformer(data_from=orders_resource, name="fulfillments", write_disposition="merge", primary_key="id")
    def fulfillments(order):
        for page in client.paginate(f"orders/{order['id']}/fulfillments.json"):
            yield from get_items(page, "fulfillments")

    # 6. Transactions (Child of Orders)
    @dlt.transformer(data_from=orders_resource, name="transactions", write_disposition="merge", primary_key="id")
    def transactions(order):
        for page in client.paginate(f"orders/{order['id']}/transactions.json"):
            yield from get_items(page, "transactions")

    # 7. Refunds (Child of Orders)
    @dlt.transformer(data_from=orders_resource, name="refunds", write_disposition="merge", primary_key="id")
    def refunds(order):
        for page in client.paginate(f"orders/{order['id']}/refunds.json"):
            yield from get_items(page, "refunds")

    # 8. Inventory Items (derived from Product variants' inventory_item_id)
    @dlt.transformer(data_from=products_resource, name="inventory_items", write_disposition="merge", primary_key="id")
    def inventory_items(product):
        variants = product.get("variants", [])
        item_ids = [str(v["inventory_item_id"]) for v in variants if v.get("inventory_item_id")]

        # Shopify allows max 100 ids per request
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for chunk in chunks(item_ids, 100):
            if not chunk:
                continue
            ids_str = ",".join(chunk)
            for page in client.paginate("inventory_items.json", params={"ids": ids_str}):
                yield from get_items(page, "inventory_items")

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
