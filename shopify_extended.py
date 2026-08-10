import dlt
from dlt.sources.helpers.rest_client import RESTClient

@dlt.source(name="shopify_extended")
def shopify_extended_source(token: str, shop_url: str, orders_resource, products_resource):
    client = RESTClient(
        base_url=f"https://{shop_url}.myshopify.com/admin/api/2024-01",
        headers={"X-Shopify-Access-Token": token}
    )

    # 1. Price Rules (Standalone)
    @dlt.resource(name="price_rules", write_disposition="merge", primary_key="id")
    def price_rules():
        for page in client.paginate("price_rules.json"):
            yield page.get("price_rules", [])

    # 2. Discount Codes (Child of Price Rules)
    @dlt.transformer(data_from=price_rules, name="discount_codes", write_disposition="merge", primary_key="id")
    def discount_codes(price_rule):
        for page in client.paginate(f"price_rules/{price_rule['id']}/discount_codes.json"):
            yield page.get("discount_codes", [])

    # 3. Abandoned Checkouts (Standalone)
    @dlt.resource(name="abandoned_checkouts", write_disposition="merge", primary_key="id")
    def abandoned_checkouts():
        for page in client.paginate("checkouts.json"):
            yield page.get("checkouts", [])

    # 4. Gift Cards (Standalone - Plus Only usually)
    @dlt.resource(name="gift_cards", write_disposition="merge", primary_key="id")
    def gift_cards():
        try:
            for page in client.paginate("gift_cards.json"):
                yield page.get("gift_cards", [])
        except Exception as e:
            import logging
            logging.warning(f"Could not fetch gift cards (often requires Shopify Plus): {e}")

    # 5-7. Order-dependent resources (Fulfillments, Transactions, Refunds)
    @dlt.transformer(data_from=orders_resource, name="fulfillments", write_disposition="merge", primary_key="id")
    def fulfillments(order):
        for page in client.paginate(f"orders/{order['id']}/fulfillments.json"):
            yield page.get("fulfillments", [])

    @dlt.transformer(data_from=orders_resource, name="transactions", write_disposition="merge", primary_key="id")
    def transactions(order):
        for page in client.paginate(f"orders/{order['id']}/transactions.json"):
            yield page.get("transactions", [])

    @dlt.transformer(data_from=orders_resource, name="refunds", write_disposition="merge", primary_key="id")
    def refunds(order):
        for page in client.paginate(f"orders/{order['id']}/refunds.json"):
            yield page.get("refunds", [])

    # 8. Inventory Items (Dependent on Products' variants)
    @dlt.transformer(data_from=products_resource, name="inventory_items", write_disposition="merge", primary_key="id")
    def inventory_items(product):
        # Extract inventory_item_ids from the product's variants
        variants = product.get("variants", [])
        item_ids = [str(v["inventory_item_id"]) for v in variants if v.get("inventory_item_id")]
        
        # Shopify allows max 100 ids per request
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for chunk in chunks(item_ids, 100):
            if not chunk: continue
            ids_str = ",".join(chunk)
            for page in client.paginate("inventory_items.json", params={"ids": ids_str}):
                yield page.get("inventory_items", [])

    return [
        price_rules, 
        discount_codes, 
        abandoned_checkouts, 
        gift_cards,
        fulfillments,
        transactions,
        refunds,
        inventory_items
    ]
