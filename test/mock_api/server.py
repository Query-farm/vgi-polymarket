#!/usr/bin/env python3
"""An offline stand-in for Polymarket's three public APIs, used by test/sql.

One server answers all three hosts under a path prefix each, so the worker is
pointed at it with its own base-URL overrides::

    POLYMARKET_GAMMA_URL=http://127.0.0.1:<port>/gamma
    POLYMARKET_CLOB_URL=http://127.0.0.1:<port>/clob
    POLYMARKET_DATA_URL=http://127.0.0.1:<port>/data

The data is deterministic, so the SQL tests can assert exact values:

* The Counter-Strike market and its two order books are the **recorded**
  responses in ``notebooks/fixtures/`` (real Gamma and CLOB bodies, verified
  against the manifest's SHA-256 on start-up).
* Everything else is synthetic: two "Testville" markets (one open, one
  resolved), their events, 150 tags, 60 series, a trade tape, holders, a
  wallet's positions and activity, and one price history.

The routes mirror the real ones closely enough for the worker's paging to be
exercised: the keyset catalog pages two rows at a time, the Data API v2 feeds
two rows per cursor page, and ``/tags`` and ``/series`` page by offset at the
limit the worker asks for.

Stdlib only. Prints ``PORT:<n>`` on stdout once listening.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
    python3 test/mock_api/server.py --port 8765
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

FIXTURES = Path(__file__).resolve().parents[2] / "notebooks" / "fixtures"


def _load_recorded() -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """The recorded market and books, hash-checked against the manifest."""
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    bodies: dict[str, Any] = {}
    for entry in manifest["entries"]:
        raw = (FIXTURES / entry["filename"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise SystemExit(f"recorded fixture {entry['filename']} does not match its manifest hash")
        bodies[entry["filename"]] = json.loads(raw)
    market = bodies["market.json"]["markets"][0]
    books = {}
    for name in ("book-0.json", "book-1.json"):
        for book in bodies[name]:
            books[book["asset_id"]] = book
    return market, books


RECORDED_MARKET, BOOKS = _load_recorded()
CS_CONDITION = RECORDED_MARKET["conditionId"]
CS_EVENT = dict(RECORDED_MARKET["events"][0])
CS_TOKENS = json.loads(RECORDED_MARKET["clobTokenIds"])

RAIN_CONDITION = "0x" + "aa" * 32
OLD_CONDITION = "0x" + "bb" * 32
WALLET = "0x" + "11" * 20
OTHER_WALLET = "0x" + "22" * 20

RAIN_MARKET: dict[str, Any] = {
    "id": "9000001",
    "question": "Will it rain in Testville on 2026-12-01?",
    "conditionId": RAIN_CONDITION,
    "slug": "will-it-rain-in-testville",
    "outcomes": '["Yes", "No"]',
    "outcomePrices": '["0.62", "0.38"]',
    "clobTokenIds": '["1001", "1002"]',
    "active": True,
    "closed": False,
    "archived": False,
    "acceptingOrders": True,
    "enableOrderBook": True,
    "startDate": "2026-11-01T00:00:00Z",
    "endDate": "2026-12-01T23:59:59Z",
    "createdAt": "2026-10-31T12:00:00Z",
    "updatedAt": "2026-11-14T00:00:00Z",
    "lastTradePrice": 0.62,
    "bestBid": 0.61,
    "bestAsk": 0.63,
    "spread": 0.02,
    "volume": "1500.5",
    "volume24hr": 250.25,
    "volume1wk": 900,
    "volume1mo": 1500.5,
    "liquidity": "4200",
    "orderPriceMinTickSize": 0.01,
    "orderMinSize": 5,
    "negRisk": False,
    "resolutionSource": "https://example.invalid/weather",
    "description": "Resolves Yes if any rain is recorded in Testville on 1 December 2026.",
    "competitive": 0.5,
    "events": [{"id": "2000001", "slug": "testville-weather"}],
}

OLD_MARKET: dict[str, Any] = {
    "id": "9000002",
    "question": "Will Testville elect a new mayor in 2025?",
    "conditionId": OLD_CONDITION,
    "slug": "testville-mayor-2025",
    "outcomes": '["Yes", "No"]',
    "outcomePrices": '["1", "0"]',
    "clobTokenIds": '["1003", "1004"]',
    "active": True,
    "closed": True,
    "archived": False,
    "acceptingOrders": False,
    "enableOrderBook": True,
    "startDate": "2025-01-01T00:00:00Z",
    "endDate": "2025-11-05T00:00:00Z",
    "volume": "98765.4321",
    "volume24hr": 0,
    "liquidity": "0",
    "negRisk": False,
    "description": "Resolved.",
    "events": [{"id": "2000002", "slug": "testville-election-2025"}],
}

#: Market -> the tag ids Gamma would match it on. Kept out of the bodies.
MARKET_TAGS = {CS_CONDITION: {"100639"}, RAIN_CONDITION: {"21"}, OLD_CONDITION: {"2"}}
MARKETS = [RECORDED_MARKET, RAIN_MARKET, OLD_MARKET]

CS_EVENT_FULL = {
    **CS_EVENT,
    "tags": [{"id": "100639", "label": "Esports", "slug": "esports"}],
    "markets": [RECORDED_MARKET],
}
EVENTS: list[dict[str, Any]] = [
    CS_EVENT_FULL,
    {
        "id": "2000001",
        "slug": "testville-weather",
        "ticker": "testville-weather",
        "title": "Testville weather",
        "description": "Daily weather questions for Testville.",
        "active": True,
        "closed": False,
        "archived": False,
        "featured": True,
        "negRisk": False,
        "startDate": "2026-11-01T00:00:00Z",
        "endDate": "2026-12-01T23:59:59Z",
        "creationDate": "2026-10-31T12:00:00Z",
        "volume": 1500.5,
        "volume24hr": 250.25,
        "liquidity": 4200,
        "openInterest": 800,
        "commentCount": 3,
        "seriesSlug": "testville-daily",
        "tags": [{"id": "21", "label": "Weather", "slug": "weather"}],
        "markets": [RAIN_MARKET],
    },
    {
        "id": "2000002",
        "slug": "testville-election-2025",
        "ticker": "testville-election-2025",
        "title": "Testville mayoral election 2025",
        "description": "Who runs Testville.",
        "active": True,
        "closed": True,
        "archived": False,
        "featured": False,
        "negRisk": False,
        "startDate": "2025-01-01T00:00:00Z",
        "endDate": "2025-11-05T00:00:00Z",
        "volume": 98765.4321,
        "commentCount": 41,
        "tags": [{"id": "2", "label": "Politics", "slug": "politics"}],
        "markets": [OLD_MARKET],
    },
]

_NAMED_TAGS = [
    {"id": "2", "label": "Politics", "slug": "politics"},
    {"id": "21", "label": "Weather", "slug": "weather"},
    {"id": "100639", "label": "Esports", "slug": "esports"},
]
#: 150 tags, so a scan crosses the 100-row offset page.
TAGS = _NAMED_TAGS + [
    {
        "id": str(1000 + i),
        "label": f"Topic {i:03d}",
        "slug": f"topic-{i:03d}",
        "createdAt": "2024-01-01T00:00:00Z",
    }
    for i in range(1, 148)
]

_RECURRENCES = ("daily", "weekly", "monthly")
#: 60 series, so a scan crosses the 50-row offset page.
SERIES = [
    {
        "id": str(5000 + i),
        "slug": f"series-{i:02d}",
        "ticker": f"series-{i:02d}",
        "title": f"Series {i:02d}",
        "seriesType": "single",
        "recurrence": _RECURRENCES[i % 3],
        "active": True,
        "closed": False,
        "archived": False,
        "featured": i == 0,
        "volume24hr": float(i),
        "commentCount": i,
    }
    for i in range(60)
]

#: Price points for the Spirit token; the last sample repeats, as the origin does.
PRICE_HISTORY = {
    CS_TOKENS[0]: [
        {"t": 1794600000, "p": 0.55},
        {"t": 1794603600, "p": 0.61},
        {"t": 1794607200, "p": 0.74},
        {"t": 1794610800, "p": 0.915},
        {"t": 1794610800, "p": 0.915},
    ]
}

TRADES = [
    {
        "transaction_hash": f"0xt{i}",
        "condition_id": CS_CONDITION,
        "token_id": CS_TOKENS[i % 2],
        "outcome": ("Spirit", "MOUZ")[i % 2],
        "outcome_index": i % 2,
        "side": ("BUY", "SELL")[i % 3 == 2],
        "price": ("0.91", "0.09", "0.9")[i % 3],
        "size": 10 * (i + 1),
        "timestamp": 1794648000 - 60 * i,
        "proxy_wallet": (WALLET, OTHER_WALLET)[i % 2],
        "name": ("alice", "")[i % 2],
        "pseudonym": "Test-Trader",
        "title": RECORDED_MARKET["question"],
        "event_slug": CS_EVENT["slug"],
    }
    for i in range(5)
]

HOLDERS = [
    {
        "token_id": CS_TOKENS[0],
        "holders": [
            {"proxy_wallet": WALLET, "amount": 5000.5, "outcome_index": 0, "name": "alice", "verified": True},
            {
                "proxy_wallet": OTHER_WALLET,
                "amount": 1200,
                "outcome_index": 0,
                "name": "",
                "pseudonym": "Bob-Two",
            },
        ],
    },
    {
        "token_id": CS_TOKENS[1],
        "holders": [{"proxy_wallet": OTHER_WALLET, "amount": 300, "outcome_index": 1, "bio": "hedger"}],
    },
]

POSITIONS = {
    WALLET: [
        {
            "proxy_wallet": WALLET,
            "condition_id": CS_CONDITION,
            "token_id": CS_TOKENS[0],
            "outcome": "Spirit",
            "outcome_index": 0,
            "title": RECORDED_MARKET["question"],
            "event_slug": CS_EVENT["slug"],
            "current_size": 5000.5,
            "avg_price": 0.8,
            "current_price": 0.915,
            "entry_cost_usdc": 4000.4,
            "current_value": 4575.4575,
            "unrealized_pnl": 575.0575,
            "percent_pnl": 14.375,
            "realized_pnl": 0,
            "total_size": 5000.5,
            "redeemable": False,
            "end_date": "2026-11-14",
        },
        {
            "proxy_wallet": WALLET,
            "condition_id": RAIN_CONDITION,
            "token_id": "1001",
            "outcome": "Yes",
            "outcome_index": 0,
            "title": RAIN_MARKET["question"],
            "event_slug": "testville-weather",
            "current_size": 100,
            "avg_price": 0.5,
            "current_price": 0.62,
            "entry_cost_usdc": 50,
            "current_value": 62,
            "unrealized_pnl": 12,
            "percent_pnl": 24,
            "realized_pnl": 0,
            "total_size": 100,
            "redeemable": False,
            "end_date": "1970-01-01",
        },
        {
            "proxy_wallet": WALLET,
            "condition_id": OLD_CONDITION,
            "token_id": "1003",
            "outcome": "Yes",
            "outcome_index": 999,
            "title": OLD_MARKET["question"],
            "event_slug": "testville-election-2025",
            "current_size": 10,
            "avg_price": 0.4,
            "current_price": 1,
            "entry_cost_usdc": 4,
            "current_value": 10,
            "unrealized_pnl": 6,
            "percent_pnl": 150,
            "realized_pnl": 0,
            "total_size": 10,
            "redeemable": True,
            "end_date": "2025-11-05",
        },
    ]
}

ACTIVITY = {
    WALLET: [
        {
            "proxy_wallet": WALLET,
            "timestamp": 1794648000,
            "type": "TRADE",
            "condition_id": CS_CONDITION,
            "token_id": CS_TOKENS[0],
            "outcome": "Spirit",
            "outcome_index": 0,
            "side": "BUY",
            "price": "0.91",
            "size": 10,
            "usdc_size": 9.1,
            "transaction_hash": "0xa1",
            "title": RECORDED_MARKET["question"],
            "event_slug": CS_EVENT["slug"],
            "name": "alice",
            "pseudonym": "Test-Trader",
        },
        {
            "proxy_wallet": WALLET,
            "timestamp": 1794640000,
            "type": "YIELD",
            "condition_id": "",
            "token_id": "",
            "outcome": "",
            "outcome_index": 999,
            "side": "",
            "price": "0",
            "size": 0,
            "usdc_size": 0.42,
            "transaction_hash": "0xa2",
            "title": "",
            "event_slug": "",
            "name": "alice",
            "pseudonym": "Test-Trader",
        },
        {
            "proxy_wallet": WALLET,
            "timestamp": 1794630000,
            "type": "REDEEM",
            "condition_id": OLD_CONDITION,
            "token_id": "1003",
            "outcome": "Yes",
            "outcome_index": 0,
            "side": "",
            "price": "1",
            "size": 10,
            "usdc_size": 10,
            "transaction_hash": "0xa3",
            "title": OLD_MARKET["question"],
            "event_slug": "testville-election-2025",
            "name": "alice",
            "pseudonym": "Test-Trader",
        },
        {
            "proxy_wallet": WALLET,
            "timestamp": 1794620000,
            "type": "TRADE",
            "condition_id": RAIN_CONDITION,
            "token_id": "1001",
            "outcome": "Yes",
            "outcome_index": 0,
            "side": "BUY",
            "price": "0.5",
            "size": 100,
            "usdc_size": 50,
            "transaction_hash": "0xa4",
            "title": RAIN_MARKET["question"],
            "event_slug": "testville-weather",
            "name": "alice",
            "pseudonym": "Test-Trader",
        },
    ]
}

#: Rows per page on the keyset catalog and the Data API feeds: small, so every
#: multi-row answer in the tests crosses at least one cursor.
PAGE = 2
_CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _paged(rows: list[Any], cursor: str | None, limit: int) -> tuple[list[Any], str | None]:
    start = int(cursor) if cursor else 0
    size = max(1, min(PAGE, limit))
    chunk = rows[start : start + size]
    nxt = start + size
    return chunk, (str(nxt) if nxt < len(rows) else None)


def _flag(value: str | None) -> bool | None:
    return None if value is None else value == "true"


def _best(book: dict[str, Any]) -> tuple[str, str]:
    bid = max(book["bids"], key=lambda level: float(level["price"]))["price"]
    ask = min(book["asks"], key=lambda level: float(level["price"]))["price"]
    return bid, ask


class Handler(BaseHTTPRequestHandler):
    """Route ``/gamma``, ``/clob`` and ``/data`` requests to the canned data."""

    def log_message(self, format: str, *args: object) -> None:
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    # ------------------------------------------------------------------ GET

    def do_GET(self) -> None:
        """Handle the read endpoints."""
        url = urlparse(self.path)
        q = parse_qs(url.query)

        def one(name: str) -> str | None:
            return q.get(name, [None])[0]

        path = url.path
        if path == "/health":
            self._send(200, {"status": "ok"})
        elif path == "/gamma/markets/keyset":
            self._markets(q, one)
        elif path == "/gamma/events/keyset":
            self._events(one)
        elif path.startswith("/gamma/events/"):
            event_id = unquote(path.removeprefix("/gamma/events/"))
            found = [e for e in EVENTS if e["id"] == event_id]
            if found:
                self._send(200, found[0])
            else:
                self._send(404, {"type": "not found error", "error": "id not found"})
        elif path == "/gamma/public-search":
            words = (one("q") or "").lower().split()
            status = one("events_status")
            hits = [e for e in EVENTS if words and all(w in e["title"].lower() for w in words)]
            if status == "active":
                hits = [e for e in hits if not e["closed"]]
            elif status == "resolved":
                hits = [e for e in hits if e["closed"]]
            page = int(one("page") or 1)
            self._send(200, {"events": hits if page == 1 else [], "pagination": {"hasMore": False}})
        elif path in ("/gamma/tags", "/gamma/series"):
            rows = TAGS if path.endswith("tags") else SERIES
            offset, limit = int(one("offset") or 0), int(one("limit") or 100)
            self._send(200, rows[offset : offset + limit])
        elif path == "/clob/book":
            book = BOOKS.get(one("token_id") or "")
            if book:
                self._send(200, book)
            else:
                self._send(404, {"error": "No orderbook exists for the requested token id"})
        elif path == "/clob/prices-history":
            self._send(200, {"history": PRICE_HISTORY.get(one("market") or "", [])})
        elif path.startswith("/data/v2/"):
            self._data(path.removeprefix("/data/v2"), one)
        else:
            self._send(404, {"error": f"no such endpoint: {path}"})

    def _markets(self, q: dict[str, list[str]], one: Any) -> None:
        rows = list(MARKETS)
        if ids := q.get("condition_ids"):
            rows = [m for m in rows if m["conditionId"] in ids]
        if slug := one("slug"):
            rows = [m for m in rows if m["slug"] == slug]
        if tags := q.get("tag_id"):
            rows = [m for m in rows if MARKET_TAGS[m["conditionId"]] & set(tags)]
        # Gamma's market catalog treats a missing `closed` as closed=false.
        closed = _flag(one("closed")) or False
        rows = [m for m in rows if m["closed"] == closed]
        if (active := _flag(one("active"))) is not None:
            rows = [m for m in rows if m["active"] == active]
        chunk, nxt = _paged(rows, one("after_cursor"), int(one("limit") or 100))
        self._send(200, {"markets": chunk, "next_cursor": nxt})

    def _events(self, one: Any) -> None:
        rows = list(EVENTS)
        if slug := one("slug"):
            rows = [e for e in rows if e["slug"] == slug]
        if tag := one("tag_id"):
            rows = [e for e in rows if any(t["id"] == tag for t in e.get("tags", []))]
        if series := one("series_slug"):
            rows = [e for e in rows if e.get("seriesSlug") == series]
        if (closed := _flag(one("closed"))) is not None:
            rows = [e for e in rows if e["closed"] == closed]
        if (featured := _flag(one("featured"))) is not None:
            rows = [e for e in rows if e["featured"] == featured]
        chunk, nxt = _paged(rows, one("after_cursor"), int(one("limit") or 100))
        self._send(200, {"events": chunk, "next_cursor": nxt})

    def _data(self, path: str, one: Any) -> None:
        if one("offset") is not None:
            self._send(400, {"error": "offset is not supported; use cursor"})
            return
        limit = int(one("limit") or 1000)
        if limit > 1000:
            self._send(400, {"error": "limit must be <= 1000"})
            return
        if path in ("/trades", "/holders"):
            condition = one("condition") or ""
            if not _CONDITION_RE.match(condition):
                self._send(400, {"error": "invalid condition id"})
                return
            if path == "/holders":
                groups = (
                    [{**g, "holders": g["holders"][:limit]} for g in HOLDERS]
                    if condition == CS_CONDITION
                    else []
                )
                self._send(200, {"data": groups, "pagination": {"has_more": False, "next_cursor": None}})
                return
            rows = [t for t in TRADES if t["condition_id"] == condition]
            if side := one("side"):
                rows = [t for t in rows if t["side"] == side]
        elif path == "/positions":
            rows = POSITIONS.get(one("user") or "", [])
        elif path == "/activity":
            rows = ACTIVITY.get(one("user") or "", [])
            if kind := one("type"):
                rows = [a for a in rows if a["type"] == kind]
        else:
            self._send(404, {"error": f"no such endpoint: {path}"})
            return
        chunk, nxt = _paged(rows, one("cursor"), limit)
        self._send(200, {"data": chunk, "pagination": {"has_more": nxt is not None, "next_cursor": nxt}})

    # ----------------------------------------------------------------- POST

    def do_POST(self) -> None:
        """Handle the CLOB's batch reads (the only POSTs the worker may send)."""
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            body = json.loads(raw or b"[]")
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid json"})
            return
        tokens = [str(item.get("token_id")) for item in body if isinstance(item, dict)]
        known = [t for t in dict.fromkeys(tokens) if t in BOOKS]
        path = urlparse(self.path).path
        if path == "/clob/books":
            self._send(200, [BOOKS[t] for t in known])
        elif path == "/clob/midpoints":
            out = {}
            for t in known:
                bid, ask = _best(BOOKS[t])
                out[t] = str(round((float(bid) + float(ask)) / 2, 4))
            self._send(200, out)
        elif path == "/clob/prices":
            out = {}
            for t in known:
                bid, ask = _best(BOOKS[t])
                out[t] = {"BUY": bid, "SELL": ask}
            self._send(200, out)
        else:
            self._send(404, {"error": f"no such endpoint: {path}"})


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
