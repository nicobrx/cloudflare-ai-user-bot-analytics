#!/usr/bin/env python3
"""Fetches Cloudflare HTTP request data by user agent and path, loads it into BigQuery.

By default, looks back 7 days and fills in any missing date partitions. Pass
--days N to widen the lookback (useful for initial backfills), or --date
YYYY-MM-DD to force-reload a single date.
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone

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
            "path": g["dimensions"]["clientRequestPath"] or "",
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
        print(f"  {len(rows)} verified bot rows")
        if rows:
            load_to_bq(client, rows, ds)
            print(f"  Loaded {len(rows)} rows")
        else:
            print("  No verified bot traffic — skipping load")


if __name__ == "__main__":
    main()
