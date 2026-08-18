"""
Shopify Segments → dlt resources, via GraphQL Admin API.

Segments and segment membership have no REST equivalent — they're GraphQL-only —
so unlike shopify_extended.py these hit the GraphQL endpoint directly inside plain
generators. Wraps as dlt resources so they get merge/upsert, schema inference, and
load in the same pipeline.run() call as everything else.

Tables produced:
  segments             – one row per segment (name, query, dates) — merge on id
  segment_members      – current membership snapshot — merge on (segment_id, customer_id)
"""

import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dlt

GRAPHQL_API_VERSION = "2025-01"  # Segments API requires 2024-10+

# Module-level session shared across all GraphQL calls — retries on 429/5xx
# and honours Shopify's Retry-After header automatically.
_session = requests.Session()
_retry = Retry(
    total=10,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True,
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))


def _graphql(shop_url: str, token: str, query: str, variables: dict = None) -> dict:
    url = f"{shop_url.rstrip('/')}/admin/api/{GRAPHQL_API_VERSION}/graphql.json"
    resp = _session.post(
        url,
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Shopify GraphQL error: {data['errors']}")
    return data["data"]


def _list_all_segments(shop_url: str, token: str) -> list:
    query = """
    query($cursor: String) {
      segments(first: 100, after: $cursor) {
        edges { node { id name query creationDate lastEditDate } }
        pageInfo { hasNextPage endCursor }
      }
    }"""
    segments, cursor = [], None
    while True:
        data = _graphql(shop_url, token, query, {"cursor": cursor})
        conn = data["segments"]
        segments += [e["node"] for e in conn["edges"]]
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
    return segments


def _fetch_segment_members(shop_url: str, token: str, segment_id: str) -> list:
    """Async job pattern: create the query, poll until done, then cursor-paginate results."""
    create_q = """
    mutation($id: ID!) {
      customerSegmentMembersQueryCreate(input: {segmentId: $id}) {
        customerSegmentMembersQuery { id }
        userErrors { message }
      }
    }"""
    data = _graphql(shop_url, token, create_q, {"id": segment_id})
    result = data["customerSegmentMembersQueryCreate"]
    if result["userErrors"]:
        raise RuntimeError(f"Segment query error for {segment_id}: {result['userErrors']}")
    query_id = result["customerSegmentMembersQuery"]["id"]

    poll_q = "query($id: ID!) { customerSegmentMembersQuery(id: $id) { done } }"
    for _ in range(60):
        done = _graphql(shop_url, token, poll_q, {"id": query_id})["customerSegmentMembersQuery"]["done"]
        if done:
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Segment members query for {segment_id} timed out after 2 minutes")

    members_q = """
    query($segId: ID!, $qId: ID, $cursor: String) {
      customerSegmentMembers(segmentId: $segId, queryId: $qId, first: 250, after: $cursor) {
        edges {
          node {
            id
            firstName
            lastName
            numberOfOrders
            amountSpent { amount }
            defaultPhoneNumber { phoneNumber }
            defaultEmailAddress { emailAddress }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }"""
    members, cursor = [], None
    while True:
        data = _graphql(shop_url, token, members_q, {"segId": segment_id, "qId": query_id, "cursor": cursor})
        conn = data["customerSegmentMembers"]
        members += [e["node"] for e in conn["edges"]]
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
    return members


@dlt.source(name="shopify_segments")
def shopify_segments_source(token: str, shop_url: str):

    @dlt.resource(name="segments", write_disposition="merge", primary_key="id")
    def segments():
        yield from _list_all_segments(shop_url, token)

    @dlt.transformer(
        data_from=segments,
        name="segment_members",
        write_disposition="merge",
        primary_key=["segment_id", "customer_id"],
    )
    def segment_members(segment):
        members = _fetch_segment_members(shop_url, token, segment["id"])
        for m in members:
            customer_id = m["id"].rsplit("/", 1)[-1]
            yield {
                "segment_id":       segment["id"],
                "customer_id":      customer_id,
                "first_name":       m.get("firstName"),
                "last_name":        m.get("lastName"),
                "phone":            (m.get("defaultPhoneNumber") or {}).get("phoneNumber"),
                "email":            (m.get("defaultEmailAddress") or {}).get("emailAddress"),
                "number_of_orders": m.get("numberOfOrders"),
                "amount_spent":     float((m.get("amountSpent") or {}).get("amount") or 0) or None,
            }
        # brief pause between segments to respect GraphQL cost throttle
        time.sleep(0.5)

    return [segments, segment_members]
