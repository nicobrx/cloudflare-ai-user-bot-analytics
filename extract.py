#!/usr/bin/env python3
"""Fetches Cloudflare HTTP request data by user agent and path, loads it into BigQuery.

By default, looks back 7 days and fills in any missing date partitions. Pass
--days N to widen the lookback (useful for initial backfills), or --date
YYYY-MM-DD to force-reload a single date.
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

CF_API_TOKEN = os.environ["CLOUDFLARE_API_TOKEN"]
CF_ZONE_ID = os.environ["CLOUDFLARE_ZONE_ID"]
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_TABLE = f"{GCP_PROJECT_ID}.cloudflare_analytics.user_agent_requests_daily"

ASSET_EXTENSIONS = {
    ".css", ".js", ".mjs", ".map",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".avif", ".bmp", ".tiff",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".mp4", ".mp3", ".webm", ".ogg", ".wav", ".m4a", ".mov", ".avi",
    ".zip", ".tar", ".gz", ".rar", ".7z",
}

# Pattern → bot family. Order matters: more specific patterns must precede
# substrings of themselves (e.g. Applebot-Extended before Applebot). Matching
# is case-insensitive.
BOT_FAMILIES = {
    # AI user-triggered fetchers (real-time, on behalf of a user prompt)
    "Claude-User":        "Claude-User",
    "ChatGPT-User":       "ChatGPT-User",
    "Perplexity-User":    "Perplexity-User",

    # AI training crawlers
    "Applebot-Extended":  "Applebot-Extended",
    "Google-Extended":    "Google-Extended",
    "Meta-ExternalAgent": "Meta-ExternalAgent",
    "anthropic-ai":       "ClaudeBot",
    "ClaudeBot":          "ClaudeBot",
    "PerplexityBot":      "PerplexityBot",
    "GPTBot":             "GPTBot",
    "Bytespider":         "Bytespider",
    "Amazonbot":          "Amazonbot",
    "cohere-ai":          "cohere-ai",
    "Diffbot":            "Diffbot",
    "YouBot":             "YouBot",
    "CCBot":              "CCBot",

    # AI search bots
    "OAI-SearchBot":      "OAI-SearchBot",

    # Traditional search engines
    "DuckDuckBot":        "DuckDuckBot",
    "Baiduspider":        "Baiduspider",
    "YandexBot":          "YandexBot",
    "Applebot":           "Applebot",
    "Googlebot":          "Googlebot",
    "Bingbot":            "Bingbot",
    "Slurp":              "Slurp",
}

SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("user_agent", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("bot_family", "STRING"),
    bigquery.SchemaField("verified_bot_category", "STRING"),
    bigquery.SchemaField("path", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("requests", "INT64"),
]

QUERY = """
query ($zoneTag: String, $startDate: Date, $endDate: Date) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequestsAdaptiveGroups(
        filter: {date_geq: $startDate, date_leq: $endDate}
        limit: 10000
      ) {
        count
        dimensions {
          date
          userAgent
          verifiedBotCategory
          clientRequestPath
        }
      }
    }
  }
}
"""


def fetch(target_date: str) -> list[dict]:
    resp = requests.post(
        "https://api.cloudflare.com/client/v4/graphql",
        headers={"Authorization": f"Bearer {CF_API_TOKEN}"},
        json={
            "query": QUERY,
            "variables": {
                "zoneTag": CF_ZONE_ID,
                "startDate": target_date,
                "endDate": target_date,
            },
        },
    )
    resp.raise_for_status()
    body = resp.json()
    if errors := body.get("errors"):
        raise RuntimeError(f"Cloudflare API errors: {errors}")
    return body["data"]["viewer"]["zones"][0]["httpRequestsAdaptiveGroups"]


def is_content_path(path: str) -> bool:
    """True if the path looks like a page or document, not a static asset."""
    filename = path.rsplit("/", 1)[-1].lower()
    if "." not in filename:
        return True
    ext = "." + filename.rsplit(".", 1)[-1]
    return ext not in ASSET_EXTENSIONS


def classify_bot_family(user_agent: str) -> Optional[str]:
    """Map a user agent string to a bot family by substring match (case-insensitive)."""
    ua = user_agent.lower()
    for pattern, family in BOT_FAMILIES.items():
        if pattern.lower() in ua:
            return family
    return None


def to_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for g in groups:
        ua = g["dimensions"]["userAgent"] or ""
        category = g["dimensions"]["verifiedBotCategory"] or None
        family = classify_bot_family(ua)
        if not family and not category:
            continue
        path = g["dimensions"]["clientRequestPath"] or ""
        if not is_content_path(path):
            continue
        rows.append({
            "date": g["dimensions"]["date"],
            "user_agent": ua,
            "bot_family": family,
            "verified_bot_category": category,
            "path": path,
            "requests": g["count"],
        })
    return rows


def load_to_bq(client: bigquery.Client, rows: list[dict], target_date: str) -> None:
    partition = target_date.replace("-", "")
    job = client.load_table_from_json(
        rows,
        f"{BQ_TABLE}${partition}",
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()


def existing_dates(client: bigquery.Client, start: date, end: date) -> set[date]:
    query = f"""
        SELECT DISTINCT date
        FROM `{BQ_TABLE}`
        WHERE date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    """
    return {row["date"] for row in client.query(query).result()}


def dates_to_process(client: bigquery.Client, args: argparse.Namespace) -> list[date]:
    if args.date:
        return [date.fromisoformat(args.date)]

    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    expected = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    if args.force:
        return sorted(expected)
    missing = expected - existing_dates(client, start, end)
    return sorted(missing)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group()
    g.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days for gap-filling (default: 7)",
    )
    g.add_argument(
        "--date",
        type=str,
        help="Force-reload a specific date (YYYY-MM-DD), overwriting any existing partition",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="With --days, reload every date in the window instead of only missing ones",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    client = bigquery.Client(project=GCP_PROJECT_ID)

    dates = dates_to_process(client, args)
    if not dates:
        print(f"No missing dates in the last {args.days} days — nothing to do.")
        return

    print(f"Processing {len(dates)} date(s): {dates[0]} → {dates[-1]}")
    for d in dates:
        ds = d.isoformat()
        print(f"\n{ds}")
        groups = fetch(ds)
        print(f"  {len(groups)} groups returned")
        rows = to_rows(groups)
        print(f"  {len(rows)} bot rows")
        if rows:
            load_to_bq(client, rows, ds)
            print(f"  Loaded {len(rows)} rows")
        else:
            print("  No bot traffic — skipping load")


if __name__ == "__main__":
    main()
