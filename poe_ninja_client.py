#!/usr/bin/env python3
"""Simple poe.ninja economy fetcher.

Examples:
  python poe_ninja_client.py
  python poe_ninja_client.py --league Necropolis --type Currency
  python poe_ninja_client.py --type Fragment --limit 20 --csv fragment.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List

API_BASE = "https://poe.ninja/api/data/currencyoverview"
DEFAULT_LEAGUE = "Mirage"
DEFAULT_TYPE = "Currency"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def fetch_currency_overview(league: str, type_name: str, timeout: int = 20) -> Dict[str, Any]:
    query = urllib.parse.urlencode({"league": league, "type": type_name})
    url = f"{API_BASE}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code} for URL: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error for URL {url}: {exc.reason}") from exc

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Failed to decode JSON response.") from exc


def rows_for_csv(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in lines:
        receive = line.get("receive", {}) or {}
        pay = line.get("pay", {}) or {}
        rows.append(
            {
                "currencyTypeName": line.get("currencyTypeName"),
                "chaosEquivalent": line.get("chaosEquivalent"),
                "receive_value": receive.get("value"),
                "receive_count": receive.get("count"),
                "pay_value": pay.get("value"),
                "pay_count": pay.get("count"),
            }
        )
    return rows


def write_csv(path: str, lines: List[Dict[str, Any]]) -> None:
    rows = rows_for_csv(lines)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_preview(lines: List[Dict[str, Any]], limit: int) -> None:
    preview = lines[:limit]
    if not preview:
        print("No data rows returned.")
        return

    for i, line in enumerate(preview, start=1):
        name = line.get("currencyTypeName", "<unknown>")
        chaos = line.get("chaosEquivalent")
        print(f"{i:>3}. {name} | chaosEquivalent={chaos}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch economy data from poe.ninja")
    parser.add_argument("--league", default=DEFAULT_LEAGUE, help="League name, e.g. Standard")
    parser.add_argument("--type", dest="type_name", default=DEFAULT_TYPE, help="Type name, e.g. Currency or Fragment")
    parser.add_argument("--limit", type=int, default=10, help="Preview row count")
    parser.add_argument("--csv", help="Optional output CSV path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.limit < 0:
        print("--limit must be >= 0", file=sys.stderr)
        return 2

    try:
        data = fetch_currency_overview(args.league, args.type_name)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    lines = data.get("lines", [])
    print(f"Fetched {len(lines)} rows for league='{args.league}', type='{args.type_name}'")
    print_preview(lines, args.limit)

    if args.csv:
        write_csv(args.csv, lines)
        print(f"CSV written: {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
