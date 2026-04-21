#!/usr/bin/env python3
"""Fetches yesterday's Cloudflare HTTP request data by user agent and loads it into BigQuery."""

import os
import sys
from datetime import date, timedelta

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

CF_API_TOKEN = os.environ["CLOUDFLARE_API_TOKEN"]
CF_ZONE_ID = os.environ["CLOUDFLARE_ZONE_ID"]
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_TABLE = f"{GCP_PROJECT_ID}.cloudflare_analytics.user_agent_requests_daily"

AI_BOT_FAMILIES = ["Claude-User", "ChatGPT-User", "Perplexity-User"]

SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("bot_family", "STRING"),
    bigquery.SchemaField("requests", "INT64"),
    bigquery.SchemaField("page_views", "INT64"),
    bigquery.SchemaField("uniques", "INT64"),
]

QUERY = """
query ($zoneTag: string, $startDate: Date, $endDate: Date) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequestsAdaptiveGroups(
        filter: {date_geq: $startDate, date_leq: $endDate}
        orderBy: [clientRequestUserAgent_ASC]
        limit: 10000
      ) {
        dimensions {
          date
          clientRequestUserAgent
        }
        sum {
          requests
          pageViews
        }
        uniq {
          uniques
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


def classify_bot_family(user_agent: str) -> str | None:
    for family in AI_BOT_FAMILIES:
        if family in user_agent:
            return family
    return None


def to_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for g in groups:
        ua = g["dimensions"]["clientRequestUserAgent"] or ""
        rows.append({
            "date": g["dimensions"]["date"],
            "user_agent": ua,
            "bot_family": classify_bot_family(ua),
            "requests": g["sum"]["requests"],
            "page_views": g["sum"]["pageViews"],
            "uniques": g["uniq"]["uniques"],
        })
    return rows


def load_to_bq(rows: list[dict], target_date: str) -> None:
    client = bigquery.Client(project=GCP_PROJECT_ID)
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


def main() -> None:
    target_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today() - timedelta(days=1))
    print(f"Fetching {target_date}")

    groups = fetch(target_date)
    print(f"  {len(groups)} user agent groups")

    rows = to_rows(groups)
    ai_rows = sum(1 for r in rows if r["bot_family"])
    print(f"  {ai_rows} AI user bot rows")

    if rows:
        load_to_bq(rows, target_date)
        print(f"  Loaded {len(rows)} rows to BigQuery")
    else:
        print("  No data — skipping BQ write")


if __name__ == "__main__":
    main()
