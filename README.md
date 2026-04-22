# Cloudflare Bot Analytics

Fetches daily Cloudflare HTTP request data via the GraphQL Analytics API and loads it into BigQuery. Reports request counts per user agent per day, filtered to bots that Cloudflare has verified via reverse-DNS lookup of the client IP.

AI user-triggered fetchers (`Claude-User`, `ChatGPT-User`, `Perplexity-User`) show up under the `AI Assistant` verified bot category — they're fetched by servers at OpenAI / Anthropic / Perplexity when their models need to retrieve a URL for a user's prompt, so their client IPs belong to those companies and get verified.

**Pipeline:** Cloud Scheduler → Python script → Cloudflare GraphQL API → BigQuery

---

## Prerequisites

- A Cloudflare Pro (or higher) account
- A GCP project with the BigQuery API enabled
- Python 3.9+
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
  date                  DATE   NOT NULL,
  user_agent            STRING NOT NULL,
  verified_bot_category STRING NOT NULL,
  path                  STRING NOT NULL,
  requests              INT64
)
PARTITION BY date;
```

Run this in the [BigQuery console](https://console.cloud.google.com/bigquery) or via `bq query`:

```bash
bq query --use_legacy_sql=false '
CREATE TABLE `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
(
  date                  DATE   NOT NULL,
  user_agent            STRING NOT NULL,
  verified_bot_category STRING NOT NULL,
  path                  STRING NOT NULL,
  requests              INT64
)
PARTITION BY date;
'
```

`verified_bot_category` is Cloudflare's classification of the bot (e.g. `Search Engine Crawler`, `AI Crawler`, `AI Assistant`, `Academic Research`). Rows are only written when Cloudflare has verified the bot's identity via reverse-DNS of the client IP; unverified traffic is discarded.

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

**Initial backfill** — load all the data Cloudflare still has (~7 days on Pro, leaving a day of margin under the 8-day retention limit):

```bash
python extract.py --days 7
```

**Daily run** — fills yesterday's partition plus any gaps from the last 7 days:

```bash
python extract.py
```

**Force-reload a specific date** — useful if you need to replay one day:

```bash
python extract.py --date 2026-04-20
```

All dates in this pipeline — CLI arguments, BigQuery partitions, and Cloudflare API filters — are interpreted in UTC, matching Cloudflare's analytics clock.

Each run checks BigQuery for which dates in the lookback window already have data and only fetches the missing ones, so re-runs are cheap and a failed run is automatically retried on the next run — no data gaps.

> **Retention limit:** Cloudflare's `httpRequestsAdaptiveGroups` dataset is limited by plan tier — roughly 8 days on Pro, 30 days on Business, 6 months on Enterprise. Queries older than the limit are rejected by the API. For long-term history, schedule a daily run so data accumulates in your BigQuery table beyond Cloudflare's retention window.

---

## Exploring raw requests in Cloudflare Log Search

In the Cloudflare dashboard, go to **Analytics & Logs → Log Search** and use this filter to see the raw AI user bot requests the script aggregates:

```
ClientRequestUserAgent ~ "Claude-User|ChatGPT-User|Perplexity-User"
```

`~` matches on a regular expression, so this returns any request whose user agent contains any of the three bot identifiers.

---

## Querying the data

**AI user-triggered fetcher traffic by day:**

```sql
SELECT
  date,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE verified_bot_category = 'AI Assistant'
GROUP BY 1
ORDER BY 1 DESC;
```

**All bot traffic for a given day, grouped by category:**

```sql
SELECT
  verified_bot_category,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE date = '2026-04-20'
GROUP BY 1
ORDER BY requests DESC;
```

**Top pages visited by AI assistants:**

```sql
SELECT
  path,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE verified_bot_category = 'AI Assistant'
GROUP BY 1
ORDER BY requests DESC
LIMIT 20;
```

---

## Notes on metric discrepancies

Numbers from this table won't exactly match what you see in the Cloudflare dashboard. A few things to be aware of:

- **Only verified bots are stored.** Cloudflare classifies a request's bot identity via reverse-DNS on the client IP. If the IP doesn't belong to the claimed bot operator (e.g. a request carrying the `ChatGPT-User` UA from an IP outside OpenAI's range), Cloudflare leaves `verifiedBotCategory` empty. This pipeline drops those rows because they're almost always either spoofed user agents or noise. The tradeoff: if an AI operator rotates to a new IP range before Cloudflare updates its verified list, you may briefly miss a small amount of legitimate traffic.
- **Only `requests` (count) is exported.** Cloudflare's `httpRequestsAdaptiveGroups.sum.visits` field behaved inconsistently at this granularity, and the Cloudflare dashboard computes "visits" by applying filters to the same `count` field rather than using `sum.visits`. To avoid confusion this pipeline only stores a raw request count.
- **Adaptive sampling.** The `httpRequestsAdaptiveGroups` dataset is sampled at the edge for high-volume zones. Small sites are usually unsampled, but counts may still drift slightly from pre-aggregated datasets used elsewhere in Cloudflare.
- **Even the Cloudflare dashboard is not perfectly self-consistent.** Switching between "requests" and "visits" metrics has been observed to produce counterintuitive totals (e.g. requests < visits for the same filter). Treat dashboard numbers as approximate.
