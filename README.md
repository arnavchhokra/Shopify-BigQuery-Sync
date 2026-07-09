# shopify-bigquery-pipeline

A free, serverless pipeline that syncs Shopify store data into Google BigQuery on a schedule — no servers to manage, no monthly platform fees.

Built on [PyAirbyte](https://github.com/airbytehq/PyAirbyte) (Airbyte's connector library used as a plain Python package) and deployed as a GitHub Actions cron job.

## Why this exists

Most Shopify-to-BigQuery options fall into two camps:

- **Managed ELT platforms** (Fivetran, Airbyte Cloud, Stitch) — reliable, full data coverage, but priced per row/credit and can get expensive as order volume grows.
- **Self-hosted platforms** (Airbyte OSS via `abctl`/Kubernetes) — free to run, but require a persistent server (minimum ~4 CPU / 8GB RAM) just to stay available, even between syncs.

This project takes a third path: it uses Airbyte's actual Shopify connector code, but runs it as a plain Python script that starts, syncs, writes to BigQuery, and exits. That fits entirely inside GitHub Actions' free tier — there's no server, no idle compute cost, and no infrastructure to patch.

## What data it syncs

Full Shopify Admin API coverage, not just orders and products:

| Category | Streams |
|---|---|
| Commerce | `orders`, `transactions`, `order_refunds`, `tender_transactions` |
| Customers | `customers`, `customer_address` |
| Catalog | `products`, `product_variants`, `product_images` |
| Collections | `collections`, `custom_collections`, `smart_collections`, `collects` |
| Inventory & fulfillment | `inventory_items`, `inventory_levels`, `locations`, `fulfillment_orders` |
| Marketing & discounts | `discount_codes`, `price_rules`, `abandoned_checkouts` |
| Risk & drafts | `order_risks`, `draft_orders` |
| Metadata | `metafield_products`, `metafield_customers`, `metafield_orders`, `metafield_collections`, `shop` |

Every stream lands in its own BigQuery table under the dataset you configure, with Airbyte's incremental sync state tracked automatically in an `_airbyte_state` table — so re-runs only pull new or changed records, not a full re-sync every time.

## How it works

1. A GitHub Actions workflow runs on a schedule (default: every 6 hours, configurable).
2. The script exchanges your Shopify app's `client_id`/`client_secret` for a short-lived (24-hour) Admin API access token using Shopify's [Client Credentials Grant](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/client-credentials-grant) — no long-lived token to store or rotate manually.
3. PyAirbyte's Shopify connector pulls the configured streams.
4. Data is written directly to a BigQuery dataset via PyAirbyte's `BigQueryCache`, which creates the dataset and tables automatically on first run if they don't already exist.
5. The runner and any downloaded credentials are torn down at the end of the job — nothing persists outside BigQuery and your GitHub secrets.

## Setup

### 1. Create a Shopify custom app

In your Shopify admin: **Settings → Apps and sales channels → Develop apps → Create an app**.

Grant these Admin API scopes (read-only):
```
read_orders, read_products, read_customers, read_inventory,
read_fulfillments, read_locations, read_price_rules, read_discounts,
read_draft_orders, read_marketing_events
```

Install the app on your store, then copy the **Client ID** and **Client secret** from the app's API credentials page.

### 2. Create a GCP service account

In your GCP project: **IAM & Admin → Service Accounts → Create Service Account**, with roles:
- `BigQuery Data Editor`
- `BigQuery Job User`

Generate a JSON key and download it — you'll paste its contents into a GitHub secret, not commit it to the repo.

### 3. Clone and configure

```bash
git clone https://github.com/<you>/shopify-bigquery-pipeline.git
cd shopify-bigquery-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 4. Set GitHub repository secrets and variables

Under **Settings → Secrets and variables → Actions**:

**Secrets:**
| Name | Value |
|---|---|
| `SHOPIFY_SHOP` | your store subdomain, e.g. `mystorename` (no `.myshopify.com`) |
| `SHOPIFY_CLIENT_ID` | from your Shopify custom app |
| `SHOPIFY_CLIENT_SECRET` | from your Shopify custom app |
| `GCP_PROJECT_ID` | your GCP project ID |
| `GCP_SA_KEY_JSON` | full contents of the service account JSON key |

**Variables:**
| Name | Value |
|---|---|
| `BQ_DATASET` | e.g. `shopify_raw` |

### 5. Run it

Push to GitHub, then trigger a manual run from the **Actions** tab to confirm everything works before letting the schedule take over.

```
Actions → shopify-bigquery-sync → Run workflow
```

Check your BigQuery dataset — you should see one table per synced stream.

## Configuration

**Sync frequency** — edit the cron schedule in `.github/workflows/shopify_sync.yml`:
```yaml
on:
  schedule:
    - cron: '0 */6 * * *'   # every 6 hours; adjust as needed
```

**Backfill start date** — set in `sync_shopify.py`:
```python
"start_date": "2022-01-01",
```

**Streams to sync** — edit the `streams=[...]` list in `sync_shopify.py` to add or remove any of the streams listed above.

## Security notes

- All credentials are stored as encrypted GitHub Actions secrets, never committed to the repo.
- The 24-hour Shopify access token is generated fresh on every run and only ever held in memory — it's never printed or logged.
- The downloaded GCP service account key is written to disk only for the duration of the job and explicitly deleted in a cleanup step that runs even if the sync fails.
- This design is safe to run in a public repository: the workflow file and code are visible to anyone, but no secret values ever appear in logs or version control.

## Cost

At typical single-store D2C volumes, this runs entirely within free tiers:

- **GitHub Actions**: free for public repos (unlimited minutes); private repos get 2,000 free minutes/month, and each sync run typically takes a few minutes.
- **BigQuery**: 10GB storage and 1TB query processing free per month — well above what a single store's raw data footprint requires.

## Limitations

- PyAirbyte is a library, not a managed platform — there's no built-in UI, alerting, or retry dashboard. Failures show up as a failed GitHub Actions run; set up [notifications](https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/notifications-for-workflow-runs) if you want to be proactively alerted.
- One Shopify store per deployment. Multi-store support would require parameterizing the shop/credentials per run.
- No built-in transformation layer — this handles extract-and-load only. Pair it with [dbt](https://www.getdbt.com/) if you want staged/modeled tables downstream.

## License

MIT
