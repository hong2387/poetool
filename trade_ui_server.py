#!/usr/bin/env python3
"""Local UI server for browsing poe.ninja trade data."""

from __future__ import annotations

import json
import os
import sys
import time
from collections import deque
from threading import Event, Lock
from datetime import datetime, timedelta, timezone
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = "127.0.0.1"
try:
    PORT = int(os.getenv("PORT", "8000"))
except ValueError:
    PORT = 8000
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
API_BASE = "https://poe.ninja/api/data"
POE1_API_BASE = "https://poe.ninja/poe1/api/economy"
INDEX_STATE_URL = "https://poe.ninja/poe1/api/data/index-state"
TRADE_API_BASE = "https://www.pathofexile.com/api/trade/exchange"
EMBEDDED_POESESSID = ""
ROOT = Path(__file__).resolve().parent
FROZEN_ROOT = Path(getattr(sys, "_MEIPASS", ROOT))
INDEX_PATH = FROZEN_ROOT / "trade_ui.html"
LIVE_SIGNAL_TTL_SEC = 45
OVERVIEW_TTL_SEC = 30
LIVE_MAX_ITEMS_PER_REQUEST = 4
TRADE_COOLDOWN_SEC = 0
ORDERBOOK_TTL_SEC = 45
AUTO_LIVE_REQUESTS = False
TRADE_MIN_INTERVAL_SEC = 1.2
TRADE_RETRY_MAX = 2
TRADE_RETRY_BACKOFF_SEC = 2.0
TRADE_BUDGET_PER_MIN = 8
TRADE_BUDGET_WINDOW_SEC = 60.0
TRADE_QUEUE_MAX_WAIT_SEC = 20.0
TRADE_POST_CACHE_TTL_SEC = 12.0
DETAILSID_TTL_SEC = 120
DEPTH_TOP_N = 5
HISTORY_POINTS = 100
HISTORY_LOOKBACK_DAYS = 2

_CACHE_LOCK = Lock()
_SIGNAL_CACHE: dict[tuple[str, str, str], tuple[float, dict]] = {}
_OVERVIEW_CACHE: dict[tuple[str, str], tuple[float, dict[str, dict]]] = {}
_ORDERBOOK_CACHE: dict[tuple[str, str, str], tuple[float, dict]] = {}
_DETAILSID_CACHE: dict[tuple[str, str], tuple[float, dict[str, str]]] = {}
_TRADE_COOLDOWN_UNTIL: dict[str, float] = {}
_LAST_TRADE_REQUEST_AT = 0.0
_TRADE_REQUEST_TIMES: deque[float] = deque()
_TRADE_POST_CACHE: dict[str, tuple[float, dict | list]] = {}
_TRADE_INFLIGHT: dict[str, Event] = {}


class TradeUIHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/trade_ui.html"):
            return self._serve_index()
        if parsed.path == "/api/trades":
            return self._serve_trades(parsed.query)
        if parsed.path == "/api/leagues":
            return self._serve_leagues()
        if parsed.path == "/api/price-spreads":
            return self._serve_price_spreads(parsed.query)
        if parsed.path == "/api/orderbook":
            return self._serve_orderbook(parsed.query)

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def _serve_index(self) -> None:
        if not INDEX_PATH.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "trade_ui.html not found")
            return

        body = INDEX_PATH.read_bytes()
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            # Browser request was canceled/closed while response was being written.
            return

    def _serve_trades(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        league = _first(params.get("league"), "Keepers")
        type_name = _first(params.get("type"), "Scarab")
        requested_overview = _first(params.get("overview"), "exchange")

        upstream_query = urllib.parse.urlencode({"league": league, "type": type_name})
        fallback_used = False
        resolved_overview = requested_overview

        if requested_overview == "exchange":
            upstream_url = f"{POE1_API_BASE}/exchange/current/overview?{upstream_query}"
            data, error = _fetch_json(upstream_url)
            # Some categories (for example SkillGem) have no rows in exchange overview.
            # Fall back to itemoverview so the UI can still show market data.
            lines = data.get("lines") if isinstance(data, dict) else None
            if error is None and isinstance(lines, list) and len(lines) == 0:
                fallback_url = f"{API_BASE}/itemoverview?{upstream_query}"
                fallback_data, fallback_error = _fetch_json(fallback_url)
                fallback_lines = fallback_data.get("lines") if isinstance(fallback_data, dict) else None
                if fallback_error is None and isinstance(fallback_lines, list) and len(fallback_lines) > 0:
                    upstream_url = fallback_url
                    data = fallback_data
                    resolved_overview = "item"
                    fallback_used = True
        else:
            overview_path = "itemoverview" if requested_overview == "item" else "currencyoverview"
            upstream_url = f"{API_BASE}/{overview_path}?{upstream_query}"
            data, error = _fetch_json(upstream_url)
        if error is not None:
            return self._json(HTTPStatus.BAD_GATEWAY, error)

        return self._json(
            HTTPStatus.OK,
            {
                "league": league,
                "type": type_name,
                "overview": resolved_overview,
                "requested_overview": requested_overview,
                "fallback_used": fallback_used,
                "source_url": upstream_url,
                "data": data,
            },
        )

    def _serve_leagues(self) -> None:
        data, error = _fetch_json(INDEX_STATE_URL)
        if error is not None:
            return self._json(HTTPStatus.BAD_GATEWAY, error)

        economy = data.get("economyLeagues") if isinstance(data, dict) else None
        old_economy = data.get("oldEconomyLeagues") if isinstance(data, dict) else None
        if not isinstance(economy, list):
            economy = []
        if not isinstance(old_economy, list):
            old_economy = []

        names: list[str] = []
        seen: set[str] = set()
        for league in economy + old_economy:
            if not isinstance(league, dict):
                continue
            name = str(league.get("name", "")).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)

        return self._json(
            HTTPStatus.OK,
            {
                "source_url": INDEX_STATE_URL,
                "leagues": names,
            },
        )

    def _serve_price_spreads(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        league = _first(params.get("league"), "Keepers")
        type_name = _first(params.get("type"), "Scarab")
        ids = [str(x).strip() for x in params.get("id", []) if str(x).strip()]
        if not ids:
            return self._json(HTTPStatus.BAD_REQUEST, {"error": "Missing id query parameter"})

        uniq_ids: list[str] = []
        seen: set[str] = set()
        for item_id in ids:
            if item_id in seen:
                continue
            seen.add(item_id)
            uniq_ids.append(item_id)
        signals: dict[str, dict] = {}
        cached_hits = 0
        unresolved: list[str] = []
        for item_id in uniq_ids:
            cached = _cache_get_signal(league, type_name, item_id)
            if cached is None:
                unresolved.append(item_id)
                continue
            signals[item_id] = cached
            cached_hits += 1

        history_hits = 0
        for item_id in unresolved:
            signal = _fetch_history_price_signal(league, type_name, item_id)
            if signal is None:
                continue
            signals[item_id] = signal
            history_hits += 1
            _cache_set_signal(league, type_name, item_id, signal)

        warning = "history_100pt_only_mode"
        if len(signals) < len(uniq_ids):
            warning = "history_100pt_partial_or_missing"

        return self._json(
            HTTPStatus.OK,
            {
                "league": league,
                "type": type_name,
                "count": len(signals),
                "signals": signals,
                "warning": warning,
                "stats": {
                    "requested": len(uniq_ids),
                    "cached_hits": cached_hits,
                    "history_hits": history_hits,
                },
            },
        )

    def _serve_orderbook(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        league = _first(params.get("league"), "Keepers")
        type_name = _first(params.get("type"), "Scarab")
        item_id = _first(params.get("id"), "")
        if not item_id:
            return self._json(HTTPStatus.BAD_REQUEST, {"error": "Missing id query parameter"})

        cached = _cache_get_orderbook(league, type_name, item_id)
        if cached is not None:
            cached["cache_hit"] = True
            return self._json(HTTPStatus.OK, cached)

        headers = _trade_headers(league)
        trade_cooldown = _is_trade_cooldown(league)
        divine_to_chaos = None
        if not trade_cooldown:
            divine_to_chaos = _fetch_divine_to_chaos(league, headers)
            if divine_to_chaos is None or divine_to_chaos <= 0:
                _set_trade_cooldown(league)
                trade_cooldown = True

        available: list[dict] = []
        competitive: list[dict] = []
        if not trade_cooldown and divine_to_chaos is not None:
            available = _fetch_depth_entries(league, item_id, "sell", headers, divine_to_chaos)
            competitive = _fetch_depth_entries(league, item_id, "buy", headers, divine_to_chaos)

        if not available and not competitive:
            overview = _get_overview_estimates_cached(league, type_name)
            base = overview.get(item_id)
            if isinstance(base, dict):
                unit = _safe_float(base.get("buy_rate"))
                cur = str(base.get("buy_currency", "")).strip() or "chaos"
                unit_chaos = _safe_float(base.get("buy_chaos"))
                if unit is not None and unit_chaos is not None:
                    available = [
                        {
                            "currency": cur,
                            "unit": unit,
                            "unit_chaos": unit_chaos,
                            "quantity": 0.0,
                            "listings": 1,
                            "latest_indexed": "",
                        }
                    ]

        warning = ""
        if trade_cooldown:
            warning = "trade_api_cooldown"
        elif not available and not competitive:
            warning = "no_live_depth"

        market_ratio = ""
        if available:
            market_ratio = _depth_ratio_text(available[0])
        elif competitive:
            market_ratio = _depth_ratio_text(competitive[0])

        payload = {
            "league": league,
            "type": type_name,
            "id": item_id,
            "warning": warning,
            "divine_to_chaos": divine_to_chaos,
            "market_ratio": market_ratio,
            "available_trades": available[:10],
            "competitive_trades": competitive[:10],
            "cache_hit": False,
        }
        _cache_set_orderbook(league, type_name, item_id, payload)
        return self._json(HTTPStatus.OK, payload)

    def _json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            # Browser request was canceled/closed while response was being written.
            return

    def log_message(self, _fmt: str, *_args: object) -> None:
        return


def _first(values: list[str] | None, default: str) -> str:
    if not values:
        return default
    value = values[0].strip()
    return value or default


def _cache_get_signal(league: str, type_name: str, item_id: str) -> dict | None:
    key = (league, type_name, item_id)
    now = time.time()
    with _CACHE_LOCK:
        cached = _SIGNAL_CACHE.get(key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at < now:
            _SIGNAL_CACHE.pop(key, None)
            return None
        return dict(payload)


def _cache_set_signal(league: str, type_name: str, item_id: str, signal: dict) -> None:
    key = (league, type_name, item_id)
    with _CACHE_LOCK:
        _SIGNAL_CACHE[key] = (time.time() + LIVE_SIGNAL_TTL_SEC, dict(signal))


def _cache_get_overview(league: str, type_name: str) -> dict[str, dict] | None:
    key = (league, type_name)
    now = time.time()
    with _CACHE_LOCK:
        cached = _OVERVIEW_CACHE.get(key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at < now:
            _OVERVIEW_CACHE.pop(key, None)
            return None
        return dict(payload)


def _cache_set_overview(league: str, type_name: str, overview: dict[str, dict]) -> None:
    key = (league, type_name)
    with _CACHE_LOCK:
        _OVERVIEW_CACHE[key] = (time.time() + OVERVIEW_TTL_SEC, dict(overview))


def _cache_get_orderbook(league: str, type_name: str, item_id: str) -> dict | None:
    key = (league, type_name, item_id)
    now = time.time()
    with _CACHE_LOCK:
        cached = _ORDERBOOK_CACHE.get(key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at < now:
            _ORDERBOOK_CACHE.pop(key, None)
            return None
        return dict(payload)


def _cache_set_orderbook(league: str, type_name: str, item_id: str, payload: dict) -> None:
    key = (league, type_name, item_id)
    with _CACHE_LOCK:
        _ORDERBOOK_CACHE[key] = (time.time() + ORDERBOOK_TTL_SEC, dict(payload))


def _get_details_id_map(league: str, type_name: str) -> dict[str, str]:
    key = (league, type_name)
    now = time.time()
    with _CACHE_LOCK:
        cached = _DETAILSID_CACHE.get(key)
        if cached is not None:
            exp, payload = cached
            if exp >= now:
                return dict(payload)
            _DETAILSID_CACHE.pop(key, None)

    query = urllib.parse.urlencode({"league": league, "type": type_name})
    url = f"{POE1_API_BASE}/exchange/current/overview?{query}"
    data, error = _fetch_json(url)
    if error is not None or not isinstance(data, dict):
        return {}
    items = data.get("items")
    if not isinstance(items, list):
        return {}
    mapping: dict[str, str] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        iid = str(it.get("id", "")).strip()
        did = str(it.get("detailsId", "")).strip()
        if iid and did:
            mapping[iid] = did
    with _CACHE_LOCK:
        _DETAILSID_CACHE[key] = (time.time() + DETAILSID_TTL_SEC, dict(mapping))
    return mapping


def _is_trade_cooldown(league: str) -> bool:
    if TRADE_COOLDOWN_SEC <= 0:
        return False
    now = time.time()
    with _CACHE_LOCK:
        until = _TRADE_COOLDOWN_UNTIL.get(league, 0.0)
    return until > now


def _set_trade_cooldown(league: str, seconds: int = TRADE_COOLDOWN_SEC) -> None:
    if TRADE_COOLDOWN_SEC <= 0:
        return
    with _CACHE_LOCK:
        _TRADE_COOLDOWN_UNTIL[league] = time.time() + max(1, seconds)


def _fetch_json(url: str) -> tuple[dict | list | None, dict | None]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        return None, {"error": f"Upstream HTTP error {exc.code}", "url": url, "code": exc.code}
    except urllib.error.URLError as exc:
        return None, {"error": "Upstream network error", "reason": str(exc.reason), "url": url}

    try:
        return json.loads(payload), None
    except json.JSONDecodeError:
        return None, {"error": "Upstream returned invalid JSON", "url": url}


def _divine_to_chaos_from_core(core: dict) -> float | None:
    rates = core.get("rates") if isinstance(core, dict) else None
    primary = str(core.get("primary", "")).strip() if isinstance(core, dict) else ""
    if not isinstance(rates, dict):
        return None
    if primary == "chaos":
        v = rates.get("divine")
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f <= 0:
            return None
        return 1.0 / f
    if primary == "divine":
        v = rates.get("chaos")
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f <= 0:
            return None
        return f
    return None


def _to_chaos_rate(currency_id: str, rate: float, divine_to_chaos: float) -> float | None:
    if currency_id == "chaos":
        return rate
    if currency_id == "divine":
        return rate * divine_to_chaos
    return None


def _trade_headers(league: str) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://www.pathofexile.com/trade/exchange/{league}",
        "Origin": "https://www.pathofexile.com",
        "Content-Type": "application/json",
    }
    poesessid = _resolve_poesessid()
    if poesessid:
        headers["Cookie"] = f"POESESSID={poesessid}"
    return headers


def _resolve_poesessid() -> str:
    env_val = os.getenv("POESESSID", "").strip()
    if env_val:
        return env_val
    return EMBEDDED_POESESSID.strip()


def _wait_trade_slot() -> None:
    global _LAST_TRADE_REQUEST_AT
    if TRADE_MIN_INTERVAL_SEC <= 0:
        return
    with _CACHE_LOCK:
        now = time.time()
        elapsed = now - _LAST_TRADE_REQUEST_AT
        wait_sec = TRADE_MIN_INTERVAL_SEC - elapsed
        if wait_sec > 0:
            # Release lock during sleep to avoid blocking unrelated cache operations.
            pass
        else:
            _LAST_TRADE_REQUEST_AT = now
            return
    if wait_sec > 0:
        time.sleep(wait_sec)
    with _CACHE_LOCK:
        _LAST_TRADE_REQUEST_AT = time.time()


def _trade_req_key(url: str, payload: dict) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"{url}|{body}"


def _cache_get_trade_post(req_key: str) -> dict | list | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _TRADE_POST_CACHE.get(req_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at < now:
            _TRADE_POST_CACHE.pop(req_key, None)
            return None
        # Keep return payload isolated from accidental mutation.
        return json.loads(json.dumps(payload))


def _cache_set_trade_post(req_key: str, payload: dict | list) -> None:
    with _CACHE_LOCK:
        _TRADE_POST_CACHE[req_key] = (time.time() + TRADE_POST_CACHE_TTL_SEC, payload)


def _acquire_trade_budget_slot() -> bool:
    if TRADE_BUDGET_PER_MIN <= 0:
        return True

    deadline = time.time() + max(0.0, TRADE_QUEUE_MAX_WAIT_SEC)
    while True:
        wait_for = 0.0
        now = time.time()
        with _CACHE_LOCK:
            while _TRADE_REQUEST_TIMES and now - _TRADE_REQUEST_TIMES[0] >= TRADE_BUDGET_WINDOW_SEC:
                _TRADE_REQUEST_TIMES.popleft()
            if len(_TRADE_REQUEST_TIMES) < TRADE_BUDGET_PER_MIN:
                _TRADE_REQUEST_TIMES.append(now)
                return True
            oldest = _TRADE_REQUEST_TIMES[0]
            wait_for = max(0.05, TRADE_BUDGET_WINDOW_SEC - (now - oldest))

        if now + wait_for > deadline:
            return False
        time.sleep(wait_for)


def _post_json(url: str, payload: dict, headers: dict[str, str]) -> tuple[dict | list | None, dict | None]:
    body = json.dumps(payload).encode("utf-8")
    last_err: dict | None = None
    attempts = max(1, TRADE_RETRY_MAX)
    req_key = _trade_req_key(url, payload)

    cached = _cache_get_trade_post(req_key)
    if cached is not None:
        return cached, None

    own_request = False
    waiter: Event | None = None
    with _CACHE_LOCK:
        inflight = _TRADE_INFLIGHT.get(req_key)
        if inflight is None:
            waiter = Event()
            _TRADE_INFLIGHT[req_key] = waiter
            own_request = True
        else:
            waiter = inflight

    if not own_request:
        waiter.wait(timeout=TRADE_QUEUE_MAX_WAIT_SEC + 5.0)
        cached_after_wait = _cache_get_trade_post(req_key)
        if cached_after_wait is not None:
            return cached_after_wait, None
        return None, {"error": "Local dedupe wait timeout", "url": url, "code": 598}

    try:
        for attempt in range(1, attempts + 1):
            if not _acquire_trade_budget_slot():
                last_err = {
                    "error": "Local trade budget queue timeout",
                    "url": url,
                    "code": 597,
                }
                break
            _wait_trade_slot()
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    raw = response.read()
                try:
                    payload_obj = json.loads(raw)
                    _cache_set_trade_post(req_key, payload_obj)
                    return payload_obj, None
                except json.JSONDecodeError:
                    last_err = {"error": "Upstream returned invalid JSON", "url": url}
                    break
            except urllib.error.HTTPError as exc:
                last_err = {"error": f"Upstream HTTP error {exc.code}", "url": url, "code": exc.code}
                if exc.code == 429 and attempt < attempts:
                    time.sleep(TRADE_RETRY_BACKOFF_SEC * attempt)
                    continue
                break
            except urllib.error.URLError as exc:
                last_err = {"error": "Upstream network error", "reason": str(exc.reason), "url": url}
                if attempt < attempts:
                    time.sleep(0.5 * attempt)
                    continue
                break

        return None, last_err or {"error": "Upstream unknown error", "url": url}
    finally:
        with _CACHE_LOCK:
            inflight = _TRADE_INFLIGHT.pop(req_key, None)
            if inflight is not None:
                inflight.set()


def _parse_utc(ts: str) -> datetime | None:
    text = str(ts or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_first_exchange_offer(entry: dict) -> dict | None:
    listing = entry.get("listing") if isinstance(entry, dict) else None
    if not isinstance(listing, dict):
        return None
    offers = listing.get("offers")
    if not isinstance(offers, list) or not offers:
        return None
    offer = offers[0]
    if not isinstance(offer, dict):
        return None
    exchange = offer.get("exchange")
    item = offer.get("item")
    if not isinstance(exchange, dict) or not isinstance(item, dict):
        return None
    return {"listing": listing, "exchange": exchange, "item": item}


def _pick_best_quote(quotes: list[dict], side: str) -> dict | None:
    if not quotes:
        return None

    def sort_key(q: dict) -> tuple[float, float, float]:
        price = float(q.get("price_chaos", 0.0))
        stock = float(q.get("stock", 0.0))
        ts = q.get("indexed_ts")
        ts_val = ts.timestamp() if isinstance(ts, datetime) else 0.0
        if side == "buy":
            return (price, -stock, -ts_val)
        return (-price, -stock, -ts_val)

    return sorted(quotes, key=sort_key)[0]


def _recent_quotes(quotes: list[dict], minutes: int = 5) -> list[dict]:
    if not quotes:
        return []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=max(1, minutes))
    out: list[dict] = []
    for q in quotes:
        ts = q.get("indexed_ts")
        if isinstance(ts, datetime) and ts >= cutoff:
            out.append(q)
    return out


def _avg_quote_price_chaos(quotes: list[dict]) -> float | None:
    if not quotes:
        return None
    vals: list[float] = []
    for q in quotes:
        try:
            n = float(q.get("price_chaos"))
        except (TypeError, ValueError):
            continue
        if n > 0:
            vals.append(n)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _fetch_divine_to_chaos(league: str, headers: dict[str, str]) -> float | None:
    url = f"{TRADE_API_BASE}/{urllib.parse.quote(league)}"
    payload = {
        "query": {"status": {"option": "online"}, "have": ["chaos"], "want": ["divine"]},
        "sort": {"have": "asc"},
    }
    data, error = _post_json(url, payload, headers)
    if error is not None:
        code = error.get("code")
        if code in (403, 429):
            _set_trade_cooldown(league)
        return None
    if not isinstance(data, dict):
        return None
    result = data.get("result")
    if not isinstance(result, dict):
        return None
    for entry in result.values():
        offer = _extract_first_exchange_offer(entry)
        if offer is None:
            continue
        exchange = offer["exchange"]
        item = offer["item"]
        if str(exchange.get("currency")) != "chaos" or str(item.get("currency")) != "divine":
            continue
        try:
            chaos_amt = float(exchange.get("amount"))
            divine_amt = float(item.get("amount"))
        except (TypeError, ValueError):
            continue
        if chaos_amt <= 0 or divine_amt <= 0:
            continue
        return chaos_amt / divine_amt
    return None


def _collect_trade_quotes(
    league: str,
    item_id: str,
    side: str,
    headers: dict[str, str],
    divine_to_chaos: float,
) -> list[dict]:
    url = f"{TRADE_API_BASE}/{urllib.parse.quote(league)}"
    if side == "buy":
        payload = {
            "query": {"status": {"option": "online"}, "have": ["chaos", "divine"], "want": [item_id]},
            "sort": {"have": "asc"},
        }
    else:
        payload = {
            "query": {"status": {"option": "online"}, "have": [item_id], "want": ["chaos", "divine"]},
            "sort": {"want": "desc"},
        }

    data, error = _post_json(url, payload, headers)
    if error is not None:
        code = error.get("code")
        if code in (403, 429):
            _set_trade_cooldown(league)
        return []
    if not isinstance(data, dict):
        return []
    result = data.get("result")
    if not isinstance(result, dict):
        return []

    quotes: list[dict] = []
    for entry in result.values():
        offer = _extract_first_exchange_offer(entry)
        if offer is None:
            continue
        listing = offer["listing"]
        exchange = offer["exchange"]
        item = offer["item"]
        ex_cur = str(exchange.get("currency", "")).strip()
        it_cur = str(item.get("currency", "")).strip()
        indexed = _parse_utc(str(listing.get("indexed", "")))
        stock_val = item.get("stock")
        try:
            stock = float(stock_val)
        except (TypeError, ValueError):
            stock = 0.0

        if side == "buy":
            if it_cur != item_id or ex_cur not in {"chaos", "divine"}:
                continue
            try:
                pay_amt = float(exchange.get("amount"))
                get_amt = float(item.get("amount"))
            except (TypeError, ValueError):
                continue
            if pay_amt <= 0 or get_amt <= 0:
                continue
            unit = pay_amt / get_amt
            price_chaos = unit if ex_cur == "chaos" else unit * divine_to_chaos
            quotes.append(
                {
                    "currency": ex_cur,
                    "unit": unit,
                    "price_chaos": price_chaos,
                    "stock": stock,
                    "indexed_ts": indexed,
                    "indexed": listing.get("indexed", ""),
                }
            )
        else:
            if ex_cur != item_id or it_cur not in {"chaos", "divine"}:
                continue
            try:
                give_amt = float(exchange.get("amount"))
                recv_amt = float(item.get("amount"))
            except (TypeError, ValueError):
                continue
            if give_amt <= 0 or recv_amt <= 0:
                continue
            unit = recv_amt / give_amt
            price_chaos = unit if it_cur == "chaos" else unit * divine_to_chaos
            quotes.append(
                {
                    "currency": it_cur,
                    "unit": unit,
                    "price_chaos": price_chaos,
                    "stock": stock,
                    "indexed_ts": indexed,
                    "indexed": listing.get("indexed", ""),
                }
            )

    return quotes


def _fetch_live_price_signal(
    league: str,
    item_id: str,
    headers: dict[str, str],
    divine_to_chaos: float,
) -> dict | None:
    buy_quotes = _collect_trade_quotes(league, item_id, "buy", headers, divine_to_chaos)
    sell_quotes = _collect_trade_quotes(league, item_id, "sell", headers, divine_to_chaos)

    buy_recent = _recent_quotes(buy_quotes, minutes=5)
    sell_recent = _recent_quotes(sell_quotes, minutes=5)

    # Preferred: 5-minute average from recent live market samples.
    buy_chaos = _avg_quote_price_chaos(buy_recent)
    sell_chaos = _avg_quote_price_chaos(sell_recent)

    # Fallback: no recent sample -> use all currently indexed quotes.
    if buy_chaos is None:
        buy_chaos = _avg_quote_price_chaos(buy_quotes)
    if sell_chaos is None:
        sell_chaos = _avg_quote_price_chaos(sell_quotes)

    # Keep market direction consistent: buy <= sell.
    if isinstance(buy_chaos, float) and isinstance(sell_chaos, float) and buy_chaos > sell_chaos:
        buy_chaos, sell_chaos = sell_chaos, buy_chaos

    best_buy = _pick_best_quote(buy_recent or buy_quotes, "buy")
    best_sell = _pick_best_quote(sell_recent or sell_quotes, "sell")
    if buy_chaos is None and sell_chaos is None and best_buy is None and best_sell is None:
        return None
    delta_chaos = None
    delta_pct = None
    if isinstance(buy_chaos, float) and isinstance(sell_chaos, float) and buy_chaos > 0:
        delta_chaos = sell_chaos - buy_chaos
        delta_pct = (delta_chaos / buy_chaos) * 100.0

    signal = {
        "source": "live_recent_5m_avg",
        "buy_currency": "chaos" if buy_chaos is not None else "",
        "buy_rate": buy_chaos,
        "buy_chaos": buy_chaos,
        "buy_indexed": best_buy.get("indexed") if best_buy else "",
        "buy_stock": best_buy.get("stock") if best_buy else None,
        "sell_currency": "chaos" if sell_chaos is not None else "",
        "sell_rate": sell_chaos,
        "sell_chaos": sell_chaos,
        "sell_indexed": best_sell.get("indexed") if best_sell else "",
        "sell_stock": best_sell.get("stock") if best_sell else None,
        "delta_chaos": delta_chaos,
        "delta_pct": delta_pct,
        "divine_to_chaos": divine_to_chaos,
        "buy_quotes": len(buy_quotes),
        "sell_quotes": len(sell_quotes),
        "buy_recent_5m": len(buy_recent),
        "sell_recent_5m": len(sell_recent),
    }
    return signal


def _safe_float(v: object) -> float | None:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n


def _depth_ratio_text(entry: dict) -> str:
    unit = _safe_float(entry.get("unit"))
    if unit is None or unit <= 0:
        return ""
    if unit >= 1:
        return f"1 : {unit:.2f}".rstrip("0").rstrip(".")
    inv = 1 / unit
    return f"{inv:.2f} : 1".rstrip("0").rstrip(".")


def _fetch_depth_entries(
    league: str,
    item_id: str,
    side: str,
    headers: dict[str, str],
    divine_to_chaos: float,
) -> list[dict]:
    url = f"{TRADE_API_BASE}/{urllib.parse.quote(league)}"
    if side == "sell":
        payload = {
            "query": {"status": {"option": "online"}, "have": ["chaos", "divine"], "want": [item_id]},
            "sort": {"have": "asc"},
        }
    else:
        payload = {
            "query": {"status": {"option": "online"}, "have": [item_id], "want": ["chaos", "divine"]},
            "sort": {"want": "desc"},
        }
    data, error = _post_json(url, payload, headers)
    if error is not None:
        code = error.get("code")
        if code in (403, 429):
            _set_trade_cooldown(league)
        return []
    if not isinstance(data, dict):
        return []
    result = data.get("result")
    if not isinstance(result, dict):
        return []

    # Bucket by unit-rate + currency, then sum available stock.
    buckets: dict[tuple[str, float], dict] = {}
    count = 0
    for entry in result.values():
        if count >= 250:
            break
        offer = _extract_first_exchange_offer(entry)
        if offer is None:
            continue
        listing = offer["listing"]
        exchange = offer["exchange"]
        item = offer["item"]
        ex_cur = str(exchange.get("currency", "")).strip()
        it_cur = str(item.get("currency", "")).strip()
        ex_amt = _safe_float(exchange.get("amount"))
        it_amt = _safe_float(item.get("amount"))
        if ex_amt is None or it_amt is None or ex_amt <= 0 or it_amt <= 0:
            continue

        if side == "sell":
            if it_cur != item_id or ex_cur not in {"chaos", "divine"}:
                continue
            unit = ex_amt / it_amt
            currency = ex_cur
            stock = _safe_float(item.get("stock")) or 0.0
        else:
            if ex_cur != item_id or it_cur not in {"chaos", "divine"}:
                continue
            unit = it_amt / ex_amt
            currency = it_cur
            stock = _safe_float(exchange.get("stock")) or 0.0

        unit_key = round(unit, 4)
        key = (currency, unit_key)
        bucket = buckets.get(key)
        if bucket is None:
            unit_chaos = unit if currency == "chaos" else unit * divine_to_chaos
            bucket = {
                "currency": currency,
                "unit": unit,
                "unit_chaos": unit_chaos,
                "quantity": 0.0,
                "listings": 0,
                "latest_indexed": "",
            }
            buckets[key] = bucket

        bucket["quantity"] += stock
        bucket["listings"] += 1
        indexed = str(listing.get("indexed", ""))
        if indexed and indexed > str(bucket.get("latest_indexed", "")):
            bucket["latest_indexed"] = indexed
        count += 1

    rows = list(buckets.values())
    if side == "sell":
        rows.sort(key=lambda x: (x["unit_chaos"], -x["quantity"]))
    else:
        rows.sort(key=lambda x: (-x["unit_chaos"], -x["quantity"]))
    return rows


def _weighted_depth_price_chaos(rows: list[dict], top_n: int = DEPTH_TOP_N) -> float | None:
    if not rows:
        return None
    picks = rows[: max(1, top_n)]
    weighted_sum = 0.0
    weight_sum = 0.0
    for r in picks:
        try:
            price = float(r.get("unit_chaos"))
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        qty = _safe_float(r.get("quantity"))
        listings = _safe_float(r.get("listings"))
        weight = qty if (qty is not None and qty > 0) else (listings if listings is not None and listings > 0 else 1.0)
        weighted_sum += price * weight
        weight_sum += weight
    if weight_sum <= 0:
        return None
    return weighted_sum / weight_sum


def _fetch_orderbook_price_signal(
    league: str,
    item_id: str,
    headers: dict[str, str],
    divine_to_chaos: float,
) -> dict | None:
    available = _fetch_depth_entries(league, item_id, "sell", headers, divine_to_chaos)
    competitive = _fetch_depth_entries(league, item_id, "buy", headers, divine_to_chaos)
    if not available and not competitive:
        return None

    buy_chaos = _weighted_depth_price_chaos(available, DEPTH_TOP_N)
    sell_chaos = _weighted_depth_price_chaos(competitive, DEPTH_TOP_N)
    if buy_chaos is None and sell_chaos is None:
        return None

    buy_div = (buy_chaos / divine_to_chaos) if (buy_chaos is not None and divine_to_chaos > 0) else None
    sell_div = (sell_chaos / divine_to_chaos) if (sell_chaos is not None and divine_to_chaos > 0) else None
    delta_chaos = None
    delta_pct = None
    if isinstance(buy_chaos, float) and isinstance(sell_chaos, float) and sell_chaos > 0:
        # Spread aligned with in-game intuition: buy side (ask) - sell side (bid)
        delta_chaos = buy_chaos - sell_chaos
        delta_pct = (delta_chaos / sell_chaos) * 100.0

    return {
        "source": "orderbook_depth_estimate",
        "buy_currency": "divine" if buy_div is not None else "",
        "buy_rate": buy_div,
        "buy_chaos": buy_chaos,
        "buy_indexed": available[0].get("latest_indexed", "") if available else "",
        "buy_stock": sum((_safe_float(x.get("quantity")) or 0.0) for x in available[:DEPTH_TOP_N]) if available else None,
        "sell_currency": "divine" if sell_div is not None else "",
        "sell_rate": sell_div,
        "sell_chaos": sell_chaos,
        "sell_indexed": competitive[0].get("latest_indexed", "") if competitive else "",
        "sell_stock": sum((_safe_float(x.get("quantity")) or 0.0) for x in competitive[:DEPTH_TOP_N]) if competitive else None,
        "delta_chaos": delta_chaos,
        "delta_pct": delta_pct,
        "divine_to_chaos": divine_to_chaos,
        "buy_quotes": min(len(available), DEPTH_TOP_N),
        "sell_quotes": min(len(competitive), DEPTH_TOP_N),
        "depth_top_n": DEPTH_TOP_N,
    }


def _fetch_history_price_signal(league: str, type_name: str, item_id: str) -> dict | None:
    candidate_ids: list[str] = [item_id]
    details_map = _get_details_id_map(league, type_name)
    mapped = details_map.get(item_id)
    if mapped and mapped not in candidate_ids:
        candidate_ids.append(mapped)

    data: dict | None = None
    for cid in candidate_ids:
        details_query = urllib.parse.urlencode({"league": league, "type": type_name, "id": cid})
        details_url = f"{POE1_API_BASE}/exchange/current/details?{details_query}"
        payload, error = _fetch_json(details_url)
        if error is None and isinstance(payload, dict):
            data = payload
            break
    if data is None:
        return None

    pairs = data.get("pairs")
    core = data.get("core")
    if not isinstance(pairs, list) or not isinstance(core, dict):
        return None
    divine_to_chaos = _divine_to_chaos_from_core(core)
    if divine_to_chaos is None or divine_to_chaos <= 0:
        return None

    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    lookback_days = max(1, HISTORY_LOOKBACK_DAYS)
    cutoff = day_start - timedelta(days=lookback_days - 1)
    best: dict | None = None
    best_volume = -1.0
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        currency_id = str(pair.get("id", "")).strip()
        if currency_id not in {"chaos", "divine"}:
            continue
        history = pair.get("history")
        if not isinstance(history, list) or not history:
            continue

        points: list[dict] = []
        volume_sum = 0.0
        for entry in history:
            if not isinstance(entry, dict):
                continue
            ts = _parse_utc(str(entry.get("timestamp", "")))
            if ts is None or ts < cutoff:
                continue
            try:
                rate = float(entry.get("rate"))
            except (TypeError, ValueError):
                continue
            if rate <= 0:
                continue
            vol = entry.get("volumePrimaryValue")
            try:
                vol_f = float(vol)
            except (TypeError, ValueError):
                vol_f = 0.0
            chaos_rate = _to_chaos_rate(currency_id, rate, divine_to_chaos)
            if chaos_rate is None or chaos_rate <= 0:
                continue
            points.append(
                {
                    "ts": ts,
                    "raw_rate": rate,
                    "chaos_rate": chaos_rate,
                    "vol": max(0.0, vol_f),
                }
            )

        if not points:
            continue

        recent = points[:HISTORY_POINTS]
        chaos_vals = sorted([p["chaos_rate"] for p in recent if p["chaos_rate"] > 0])
        if not chaos_vals:
            continue

        avg_chaos = sum(chaos_vals) / len(chaos_vals)
        # 2-cluster split at median. Works well for clearly bi-modal distributions.
        median = chaos_vals[len(chaos_vals) // 2]
        low = [x for x in chaos_vals if x <= median]
        high = [x for x in chaos_vals if x > median]
        if not low:
            low = chaos_vals[:1]
        if not high:
            high = chaos_vals[-1:]

        low_avg = sum(low) / len(low)
        high_avg = sum(high) / len(high)
        # Cluster separability: between-center distance compared to overall mean.
        sep_ratio = ((high_avg - low_avg) / avg_chaos) if avg_chaos > 0 else 0.0
        has_clear_spread = sep_ratio >= 0.05  # <5% 視為單一族群，不拆買賣差

        if has_clear_spread:
            buy_chaos = low_avg
            sell_chaos = high_avg
        else:
            buy_chaos = avg_chaos
            sell_chaos = avg_chaos

        # Keep market direction consistent: buy <= sell.
        if buy_chaos > sell_chaos:
            buy_chaos, sell_chaos = sell_chaos, buy_chaos

        for p in recent:
            volume_sum += p["vol"]

        latest_ts = recent[0]["ts"].isoformat()
        delta_chaos = sell_chaos - buy_chaos
        delta_pct = (delta_chaos / buy_chaos * 100.0) if buy_chaos > 0 else None

        source_label = "history_100pt_2d_cluster" if has_clear_spread else "history_100pt_2d_single"
        candidate = {
            "source": source_label,
            "buy_currency": "chaos",
            "buy_rate": buy_chaos,
            "buy_chaos": buy_chaos,
            "buy_indexed": latest_ts,
            "buy_stock": volume_sum,
            "sell_currency": "chaos",
            "sell_rate": sell_chaos,
            "sell_chaos": sell_chaos,
            "sell_indexed": latest_ts,
            "sell_stock": volume_sum,
            "delta_chaos": delta_chaos,
            "delta_pct": delta_pct,
            "divine_to_chaos": divine_to_chaos,
            "buy_quotes": len(recent),
            "sell_quotes": len(recent),
            "history_points": len(recent),
            "cluster_sep_ratio": sep_ratio,
        }
        if volume_sum > best_volume:
            best = candidate
            best_volume = volume_sum

    return best


def _primary_to_chaos(primary_value: float, core: dict) -> float | None:
    primary = str(core.get("primary", "")).strip() if isinstance(core, dict) else ""
    rates = core.get("rates") if isinstance(core, dict) else {}
    if primary == "chaos":
        return primary_value
    if primary == "divine":
        chaos_rate = rates.get("chaos") if isinstance(rates, dict) else None
        try:
            r = float(chaos_rate)
        except (TypeError, ValueError):
            return None
        return primary_value * r
    if isinstance(rates, dict) and "chaos" in rates:
        try:
            r = float(rates.get("chaos"))
        except (TypeError, ValueError):
            return None
        return primary_value * r
    return None


def _fetch_overview_estimates(league: str, type_name: str) -> dict[str, dict]:
    query = urllib.parse.urlencode({"league": league, "type": type_name})
    url = f"{POE1_API_BASE}/exchange/current/overview?{query}"
    data, error = _fetch_json(url)
    if error is not None or not isinstance(data, dict):
        return {}
    core = data.get("core")
    lines = data.get("lines")
    if not isinstance(lines, list) or not isinstance(core, dict):
        return {}
    primary = str(core.get("primary", "")).strip()
    out: dict[str, dict] = {}
    for line in lines:
        if not isinstance(line, dict):
            continue
        item_id = str(line.get("id", "")).strip()
        if not item_id:
            continue
        try:
            primary_value = float(line.get("primaryValue"))
        except (TypeError, ValueError):
            continue
        chaos_value = _primary_to_chaos(primary_value, core)
        if chaos_value is None:
            continue
        out[item_id] = {
            "source": "overview_estimate",
            "buy_currency": primary,
            "buy_rate": primary_value,
            "buy_chaos": chaos_value,
            "buy_indexed": "",
            "buy_stock": None,
            "sell_currency": primary,
            "sell_rate": primary_value,
            "sell_chaos": chaos_value,
            "sell_indexed": "",
            "sell_stock": None,
            "delta_chaos": 0.0,
            "delta_pct": 0.0,
            "divine_to_chaos": _divine_to_chaos_from_core(core),
            "buy_quotes": 1,
            "sell_quotes": 1,
        }
    return out


def _get_overview_estimates_cached(league: str, type_name: str) -> dict[str, dict]:
    cached = _cache_get_overview(league, type_name)
    if cached is not None:
        return cached
    overview = _fetch_overview_estimates(league, type_name)
    _cache_set_overview(league, type_name, overview)
    return overview


def main() -> int:
    server = ThreadingHTTPServer((HOST, PORT), TradeUIHandler)
    print(f"Trade UI running at http://{HOST}:{PORT}")
    print(f"Serving UI from: {INDEX_PATH}")
    env_val = os.getenv("POESESSID", "").strip()
    embedded_val = EMBEDDED_POESESSID.strip()
    if env_val:
        print("Trade API auth: POESESSID loaded from environment.")
    elif embedded_val:
        print("Trade API auth: POESESSID loaded from embedded code constant.")
    else:
        print("Trade API auth: anonymous mode (set POESESSID env var for higher stability).")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
