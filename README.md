# Cloudflare Bot Analytics

Fetches daily Cloudflare HTTP request data via the GraphQL Analytics API and loads it into BigQuery. Reports request counts per user agent per day for bots identified by either:

- a project-maintained list of known bot user-agent tokens (the `bot_family` column), or
- Cloudflare's verified-bot classification, which reverse-DNS-checks the client IP against the bot operator's domains (the `verified_bot_category` column)

A row is written if either field is populated; non-bot traffic is discarded.

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
  bot_family            STRING,
  verified_bot_category STRING,
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
  bot_family            STRING,
  verified_bot_category STRING,
  path                  STRING NOT NULL,
  requests              INT64
)
PARTITION BY date;
'
```

### Classification: `bot_family` and `verified_bot_category`

The two classification columns are independent and either, both, or neither can be populated for a given request:

- **`bot_family`** is matched by the script from the user agent string against a curated list in [extract.py](extract.py) (the `BOT_FAMILIES` dict). The list covers AI user-triggered fetchers, AI training crawlers, AI search bots, and traditional search engines. To track a new bot, add a `(pattern → family)` entry to that dict.
- **`verified_bot_category`** comes from Cloudflare's [public verified-bot list](https://radar.cloudflare.com/bots#verified-bots). Cloudflare only verifies bots whose operators have explicitly registered with the program (publishing IP ranges or domain claims); for those, Cloudflare reverse-DNS-checks each request's client IP and tags it with a category like `Search Engine Crawler`, `AI Crawler`, `AI Assistant`, or `Academic Research`. Bot UAs whose operators have not registered (which currently includes Claude-User and Perplexity-User) are never tagged, regardless of source IP.

**Why both?** Cloudflare's verified-bot program is opt-in for operators, so its coverage of newer AI fetchers is incomplete. A diagnostic against the API showed:

| User agent | % verified as `AI Assistant` |
|---|---|
| ChatGPT-User | ~50% |
| Claude-User | 0% |
| Perplexity-User | 0% |

OpenAI has registered ChatGPT-User but apparently not all of its egress IP ranges; Anthropic and Perplexity haven't registered their `-User` fetchers at all. Maintaining our own `bot_family` list ensures these fetchers are still captured, while `verified_bot_category` provides reliable, IP-confirmed coverage of bots that *are* registered (Googlebot, Bingbot, GPTBot, etc.) plus any verified bots not in our list.

### Static asset filtering

Requests for static assets (`.css`, `.js`, images, fonts, videos, archives) are filtered out and never written — the table only stores requests for pages and document-like content (HTML, PDF, TXT, XML, JSON, paths with no extension, etc.). The exclusion list lives in `ASSET_EXTENSIONS` in [extract.py](extract.py) and can be edited there.

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

**Force-reload the whole window** — rewrites every partition in the lookback, e.g. after changing the filter logic:

```bash
python extract.py --days 7 --force
```

All dates in this pipeline — CLI arguments, BigQuery partitions, and Cloudflare API filters — are interpreted in UTC, matching Cloudflare's analytics clock.

Each run checks BigQuery for which dates in the lookback window already have data and only fetches the missing ones, so re-runs are cheap and a failed run is automatically retried on the next run — no data gaps.

> **Retention limit:** Cloudflare's `httpRequestsAdaptiveGroups` dataset is limited by plan tier — roughly 8 days on Pro, 30 days on Business, 6 months on Enterprise. Queries older than the limit are rejected by the API. For long-term history, schedule a daily run so data accumulates in your BigQuery table beyond Cloudflare's retention window.

---

## Step 4 — Deploy to GCP (optional)

Run the script on a daily schedule using **Cloud Run Jobs** + **Cloud Scheduler**. All commands below assume `gcloud` is authenticated and `gcloud config set project YOUR_PROJECT` has already been run.

### 4.1 — Enable the required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com
```

### 4.2 — Store the Cloudflare API token in Secret Manager

```bash
source .env
printf "%s" "$CLOUDFLARE_API_TOKEN" | gcloud secrets create cloudflare-api-token --data-file=-
```

The Zone ID and project ID are non-sensitive and will be set as plain environment variables on the job.

### 4.3 — Create the service account

This account is used by both the Cloud Run Job (to read the secret and write to BigQuery) and Cloud Scheduler (to invoke the job).

```bash
PROJECT_ID=$(gcloud config get-value project)
SA_EMAIL="cf-analytics-extractor@${PROJECT_ID}.iam.gserviceaccount.com"
REGION="us-central1"

gcloud iam service-accounts create cf-analytics-extractor \
  --display-name="Cloudflare Analytics Extractor"

# Read + write BigQuery data
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

# Run BigQuery load/query jobs
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

# Read the Cloudflare API token secret
gcloud secrets add-iam-policy-binding cloudflare-api-token \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor"
```

### 4.4 — Deploy the Cloud Run Job

This builds the container from the `Dockerfile` via Cloud Build and deploys it as a Cloud Run Job:

```bash
gcloud run jobs deploy cf-analytics-extractor \
  --source=. \
  --region="${REGION}" \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},CLOUDFLARE_ZONE_ID=${CLOUDFLARE_ZONE_ID}" \
  --set-secrets="CLOUDFLARE_API_TOKEN=cloudflare-api-token:latest"
```

Run the job once manually to verify everything works:

```bash
gcloud run jobs execute cf-analytics-extractor --region="${REGION}" --wait
```

### 4.5 — Schedule daily runs

Allow the service account to invoke the job, then create the Cloud Scheduler entry:

```bash
gcloud run jobs add-iam-policy-binding cf-analytics-extractor \
  --region="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http cf-analytics-daily \
  --location="${REGION}" \
  --schedule="0 2 * * *" \
  --time-zone="UTC" \
  --http-method=POST \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/cf-analytics-extractor:run" \
  --oauth-service-account-email="${SA_EMAIL}"
```

The job will run every day at 02:00 UTC. It uses the default `--days 7` lookback, so any failed run self-heals on the next execution.

### Updating the deployed job

After any change to `extract.py`, `requirements.txt`, or the `Dockerfile`, redeploy with:

```bash
gcloud run jobs deploy cf-analytics-extractor --source=. --region="${REGION}"
```

To run an ad-hoc backfill against the deployed job (e.g. after changing filter logic):

```bash
gcloud run jobs execute cf-analytics-extractor \
  --region="${REGION}" \
  --wait \
  --args="--days,7,--force"
```

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
  bot_family,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE bot_family IN ('Claude-User', 'ChatGPT-User', 'Perplexity-User')
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**All bot traffic for a given day, grouped by family:**

```sql
SELECT
  COALESCE(bot_family, verified_bot_category) AS bot,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE date = '2026-04-25'
GROUP BY 1
ORDER BY requests DESC;
```

**Top pages visited by AI user-triggered fetchers:**

```sql
SELECT
  path,
  SUM(requests) AS requests
FROM `YOUR_PROJECT.cloudflare_analytics.user_agent_requests_daily`
WHERE bot_family IN ('Claude-User', 'ChatGPT-User', 'Perplexity-User')
GROUP BY 1
ORDER BY requests DESC
LIMIT 20;
```

---

## Notes on metric discrepancies

Numbers from this table won't exactly match what you see in the Cloudflare dashboard. A few things to be aware of:

- **Some `bot_family`-only rows are unverified.** When `verified_bot_category` is NULL but `bot_family` is set, the row matched our user-agent list but the bot's operator hasn't registered with [Cloudflare's verified-bot program](https://radar.cloudflare.com/bots#verified-bots), so Cloudflare didn't IP-check it. This is the norm for Claude-User and Perplexity-User. Those rows can also include UA-spoofed traffic — if you only want high-confidence rows, filter on `verified_bot_category IS NOT NULL`.
- **Only `requests` (count) is exported.** Cloudflare's `httpRequestsAdaptiveGroups.sum.visits` field behaved inconsistently at this granularity, and the Cloudflare dashboard computes "visits" by applying filters to the same `count` field rather than using `sum.visits`. To avoid confusion this pipeline only stores a raw request count.
- **Adaptive sampling.** The `httpRequestsAdaptiveGroups` dataset is sampled at the edge for high-volume zones. Small sites are usually unsampled, but counts may still drift slightly from pre-aggregated datasets used elsewhere in Cloudflare.
- **Even the Cloudflare dashboard is not perfectly self-consistent.** Switching between "requests" and "visits" metrics has been observed to produce counterintuitive totals (e.g. requests < visits for the same filter). Treat dashboard numbers as approximate.
