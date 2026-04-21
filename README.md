# Cloudflare AI User Bot Analytics

Fetches daily Cloudflare HTTP request data via the GraphQL Analytics API and loads it into BigQuery. Reports requests, page views, and approximate unique IPs per user agent per day — with AI user-triggered fetchers (Claude-User, ChatGPT-User, Perplexity-User) classified by bot family.

**Pipeline:** Cloud Scheduler → Python script → Cloudflare GraphQL API → BigQuery

---

## Prerequisites

- A Cloudflare Pro (or higher) account
- A GCP project with the BigQuery API enabled
- Python 3.11+
- `gcloud` CLI authenticated (`gcloud auth application-default login`)

---

## Step 1 — Create the BigQuery dataset and table

Replace `YOUR_PROJECT` with your GCP project ID in all commands below.

**Create the dataset:**

```bash
bq mk --location=US YOUR_PROJECT:cloudflare_analytics
```

**Create the table:**

```sql
CREATE TABLE `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
(
  date       DATE   NOT NULL,
  user_agent STRING NOT NULL,
  bot_family STRING,
  requests   INT64,
  page_views INT64,
  uniques    INT64
)
PARTITION BY date;
```

Run this in the [BigQuery console](https://console.cloud.google.com/bigquery) or via `bq query`:

```bash
bq query --use_legacy_sql=false '
CREATE TABLE `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
(
  date       DATE   NOT NULL,
  user_agent STRING NOT NULL,
  bot_family STRING,
  requests   INT64,
  page_views INT64,
  uniques    INT64
)
PARTITION BY date;
'
```

The `bot_family` column is populated for known AI user-triggered fetchers and `NULL` for all others:

| `user_agent` contains | `bot_family` |
|---|---|
| `Claude-User` | `Claude-User` |
| `ChatGPT-User` | `ChatGPT-User` |
| `Perplexity-User` | `Perplexity-User` |
| anything else | `NULL` |

---

## Step 2 — Get your Cloudflare credentials

You need two values from the Cloudflare dashboard:

- **Zone ID** — Dashboard → your domain → Overview → *Zone ID* (bottom right)
- **API Token** — Dashboard → My Profile → API Tokens → Create Token → use the *Read analytics and logs* template, then scope it to *Zone → Analytics → Read* and *Zone → Logs → Read* for your domain

---

## Step 3 — Run the script

Install dependencies:

```bash
pip install -r requirements.txt
```

Run for yesterday (default):

```bash
python extract.py
```

Or pass a specific date:

```bash
python extract.py 2026-04-20
```

The script is safe to re-run — it replaces the partition for the target date each time.

---

## Querying the data

**AI user bot traffic by day:**

```sql
SELECT
  date,
  bot_family,
  SUM(requests)   AS requests,
  SUM(page_views) AS page_views,
  SUM(uniques)    AS uniques
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE bot_family IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**All user agents for a given day:**

```sql
SELECT *
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE date = '2026-04-20'
ORDER BY requests DESC;
```
