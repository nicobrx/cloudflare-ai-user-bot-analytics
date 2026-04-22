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

SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("user_agent", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("verified_bot_category", "STRING", mode="REQUIRED"),
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
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    if errors := body.get("errors"):
        raise RuntimeError(f"Cloudflare API errors: {errors}")
    return body["data"]["viewer"]["zones"][0]["httpRequestsAdaptiveGroups"]


def to_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for g in groups:
        category = g["dimensions"]["verifiedBotCategory"] or ""
        if not category:
            continue
        rows.append({
            "date": g["dimensions"]["date"],
            "user_agent": g["dimensions"]["userAgent"] or "",
            "verified_bot_category": category,
            "requests": g["count"],
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
    print(f"  {len(rows)} verified bot rows")

    if rows:
        load_to_bq(rows, target_date)
        print(f"  Loaded {len(rows)} rows to BigQuery")
    else:
        print("  No data — skipping BQ write")


if __name__ == "__main__":
    main()
