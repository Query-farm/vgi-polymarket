"""Read-only HTTP access to Polymarket's three public APIs.

Polymarket does not have *an* API. It has three, on three hosts, with three
different paging styles and two different identifiers for the same market:

* **Gamma** (:data:`GAMMA_BASE_URL`) — the catalog: tags, series, events,
  markets. Keyset-paged with an opaque ``after_cursor``.
* **CLOB** (:data:`CLOB_BASE_URL`) — live trading data: order books, quotes,
  price history. Keyed on a **token id**.
* **Data** (:data:`DATA_BASE_URL`) — the trade tape and holder lists. Keyed on a
  **condition id**. Offset-paged.

Only two functions in this module reach the network — :func:`_get` and
:func:`_post_read` — and they are what the read-only guard
(``tests/test_readonly_guard.py``) asserts against.

Why a POST chokepoint exists at all
-----------------------------------
Polymarket's batch read endpoints (``/books``, ``/prices``, ``/midpoints``) are
``POST``-with-a-body even though they are pure reads; the ``GET`` spellings
answer HTTP 400. Batching is not a nicety here — an order book costs ~147 ms
individually and 100 of them cost 160 ms in one batched call, measured at ~81x —
so refusing to POST would mean refusing to batch.

The guard is therefore about *paths*, not verbs: :func:`_post_read` will only
issue a request to a path in :data:`READ_ONLY_POST_PATHS`, checked at call time
and not merely by convention. Every path on that list is a documented read
endpoint that returns data and changes nothing. Order entry lives behind
``/order`` and requires an EIP-712 wallet signature this worker cannot produce
and never constructs.
"""

from __future__ import annotations

import os
import re
import time
from urllib.parse import quote as urlquote
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

#: The catalog API: tags, series, events, markets. Everything with a `question`
#: or a `title` on it comes from here.
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

#: The central limit order book: books, quotes, price history. Keyed on token id.
CLOB_BASE_URL = "https://clob.polymarket.com"

#: The trade tape and holder lists. Keyed on condition id.
DATA_BASE_URL = "https://data-api.polymarket.com"

#: The market and event catalog paths. Note the ``/keyset`` suffix: the bare
#: ``/markets`` and ``/events`` still answer, but with ``Deprecation: true``,
#: ``Sunset: Fri, 01 May 2026`` and ``Warning: 299 - "use /markets/keyset"``.
#: They also offset-page, which skips and repeats rows when the catalog shifts
#: underneath a walk — a real hazard on a catalog this active.
MARKETS_PATH = "/markets/keyset"
EVENTS_PATH = "/events/keyset"

#: The only paths :func:`_post_read` will send a body to. Polymarket exposes its
#: batch *reads* as POST — the GET spellings are a 400 — so the read-only
#: property cannot be expressed as "never POST". It is expressed as this list
#: instead, enforced at call time and asserted by the CI guard.
#:
#: Each is a pure read: it takes a list of token ids and returns their current
#: book or price. None of them accepts an order, a signature, or a wallet.
READ_ONLY_POST_PATHS = frozenset({"/books", "/prices", "/midpoints"})

#: Gamma silently **clamps** an oversized `limit` rather than rejecting it: ask
#: for 500 and you get 100, with no error and nothing in the response saying more
#: was requested. A caller that assumes it got what it asked for under-reads.
#: Probed against the live API — see ``tests/test_live.py::TestPageLimits``.
GAMMA_PAGE_LIMIT = 100

#: `/series` clamps lower still, at 50.
SERIES_PAGE_LIMIT = 50

#: `/public-search` counts its page size *per result type* and paginates by page
#: number rather than by offset or cursor — a third paging style on the same
#: host. Kept modest because a search is browsed, not walked.
SEARCH_PAGE_LIMIT = 20

#: The Data API's trade tape accepts larger pages and does not appear to clamp.
DATA_PAGE_LIMIT = 500

#: `/markets/keyset` takes `condition_ids` as a repeated parameter, which turns
#: a per-row market lookup into one request per this many rows. Verified to 60
#: against the live API; kept there rather than probed to failure, because the
#: limit is undocumented and a URL that is too long fails as a 414 rather than
#: as something a caller can act on.
CONDITION_ID_BATCH_LIMIT = 60

#: `POST /books` and friends take a list of token ids. Measured at 200 with no
#: sign of a cap, and the batch is what makes a LATERAL over a whole tag
#: affordable: 100 books individually took 12.85s against 0.16s batched, ~81x.
TOKEN_BATCH_LIMIT = 200

#: Hard stop on cursor following, so a runaway scan cannot hang a query forever.
#: Polymarket's open-market catalog runs to thousands of rows and the full
#: archive is far larger, which is why the scans stream a page at a time.
#: Hitting this is an error, not a quiet truncation.
MAX_PAGES = 500

#: Connect and read timeouts for every request this module makes.
#: `POST /books` with 200 tokens is the slowest thing here at ~1.1s, so the read
#: timeout has room without being long enough to hide a hung host.
TIMEOUT = httpx.Timeout(30.0, connect=10.0)

#: Grace window (seconds) for serving a stale cached result when a refetch
#: fails. Polymarket sends no ETag and no Last-Modified on anything, so a cached
#: result can only expire — it can never be cheaply revalidated.
STALE_IF_ERROR = 300

#: ``max-age=N``, anchored to a directive boundary so it cannot match the tail
#: of some other token (``s-maxage=99`` must not read as ``max-age=99``).
_MAX_AGE = re.compile(r"(?:^|[\s,])max-age=(\d+)")

#: Directives that forbid reuse outright, whatever ``max-age`` also says.
_NO_REUSE = re.compile(r"(?:^|[\s,])(?:no-store|no-cache|private)(?:\s*[,=]|\s*$)")

#: Statuses worth retrying: the rate limiter, plus the CDN's transient 5xx.
_RATE_LIMITED = 429
_RETRYABLE_STATUSES = frozenset({_RATE_LIMITED, 500, 502, 503, 504})

#: Exponential backoff for a retryable request: ~0.5s, 1s, 2s, 4s, 8s.
_RETRY_ATTEMPTS = 5
_RETRY_BASE_SECONDS = 0.5


@dataclass(slots=True)
class CacheHint:
    """The origin's own freshness opinion, collected across a call's responses.

    Polymarket declares ``Cache-Control`` far more consistently than most APIs,
    and the values differ enough per endpoint to be worth forwarding rather than
    inventing: ``/tags`` says ``max-age=1800``, the Gamma catalog says 300, the
    Data API's tape says 300, ``/prices-history`` says 90, and the live CLOB
    endpoints (``/book``, ``/midpoint``, ``/price``) declare nothing at all.

    ``max_age`` is the **minimum** seen across a paged call — the shortest-lived
    page bounds the whole result — and stays ``None`` when the origin declared
    nothing, which is itself the signal that the data is live.
    """

    max_age: int | None = None
    #: True once any response arrived without a usable freshness directive.
    saw_uncacheable: bool = field(default=False)

    def observe(self, response: httpx.Response) -> None:
        """Fold one response's Cache-Control into the running hint.

        A response is cacheable only if it declares a non-zero ``max-age`` *and*
        declares nothing that forbids reuse. ``max-age=0`` is "already stale",
        which is not an entry worth holding: storing one would pair a zero
        lifetime with :data:`STALE_IF_ERROR` and quietly license five minutes of
        stale serving from an origin that offered none.
        """
        directives = response.headers.get("cache-control", "")
        match = _MAX_AGE.search(directives)
        if match is None or _NO_REUSE.search(directives):
            self.saw_uncacheable = True
            return
        seconds = int(match.group(1))
        if seconds == 0:
            self.saw_uncacheable = True
            return
        self.max_age = seconds if self.max_age is None else min(self.max_age, seconds)

    @property
    def cacheable(self) -> bool:
        """Whether every response in this call carried a usable freshness directive."""
        return self.max_age is not None and not self.saw_uncacheable


def gamma_base_url() -> str:
    """The Gamma base URL, overridable with ``POLYMARKET_GAMMA_URL``."""
    return os.environ.get("POLYMARKET_GAMMA_URL", GAMMA_BASE_URL).rstrip("/")


def clob_base_url() -> str:
    """The CLOB base URL, overridable with ``POLYMARKET_CLOB_URL``."""
    return os.environ.get("POLYMARKET_CLOB_URL", CLOB_BASE_URL).rstrip("/")


def data_base_url() -> str:
    """The Data API base URL, overridable with ``POLYMARKET_DATA_URL``."""
    return os.environ.get("POLYMARKET_DATA_URL", DATA_BASE_URL).rstrip("/")


def open_client() -> httpx.Client:
    """Open a client carrying this module's timeouts.

    Callers that issue a burst of per-row fetches should open one of these and
    pass it in, so the whole batch shares a connection pool across all three
    hosts. It is the only httpx object the rest of the package constructs, which
    keeps the timeout policy in one place.
    """
    return httpx.Client(timeout=TIMEOUT)


class PolymarketError(RuntimeError):
    """A non-2xx response from a Polymarket API, carrying the status and body."""

    def __init__(self, status: int, url: str, body: str) -> None:
        super().__init__(f"Polymarket API {status} for {url}: {body[:400]}")
        self.status = status
        self.url = url


class PolymarketPageLimitError(RuntimeError):
    """A paged call followed :data:`MAX_PAGES` pages without reaching the end.

    Raised rather than returning the truncated prefix: a silently short result
    reads as real data and nothing downstream can tell the difference.

    The market catalog is the reason this is reachable in normal use. It was
    measured at over 120,000 rows — still not exhausted after 1,200 pages and
    155 seconds — so a selective predicate the API cannot evaluate makes the
    scan walk the whole thing to prove a negative. A ``WHERE`` on a column with
    no Gamma equivalent (``volume_24hr``, say) is exactly that shape, and it now
    fails loudly here instead of quietly returning unfiltered rows, which is
    what it did before pushdown was actually applied.
    """

    def __init__(self, url: str, rows: int) -> None:
        super().__init__(
            f"Polymarket API {url}: still paging after {MAX_PAGES} pages ({rows} rows). "
            "The predicate matched too little to satisfy the query and the API cannot "
            "evaluate it, so the scan had to walk the whole catalog (120k+ rows). "
            "Narrow it with an argument the API understands (tag_id, closed, active, "
            "slug, condition_id), use search() for a text query, or add a LIMIT."
        )
        self.url = url
        self.rows = rows


class NotFound(Exception):
    """A resource that does not exist, as distinct from one that is empty.

    The CLOB answers an unknown token id with HTTP 404 and a JSON error body.
    Under a correlated LATERAL that must not fail the query — one delisted
    token among three hundred would otherwise take the whole scan down — so the
    per-row callers catch this and emit no rows for that input, which is the
    same thing an empty book produces.
    """


def _send(
    request_url: str,
    build: Any,
    *,
    client: httpx.Client | None,
    hint: CacheHint | None,
) -> dict[str, Any] | list[Any]:
    """Issue ``build(http)`` with retries and turn the response into JSON.

    Shared by :func:`_get` and :func:`_post_read` so that the retry policy, the
    error mapping and the freshness accounting cannot drift between the two.
    ``build`` is a callable taking the client and returning a response, which is
    what keeps the verb decision at the call site where the guard can see it.

    Every transient failure is retried with exponential backoff: the rate
    limiter (429), the CDN's 5xx, and transport errors. All three hosts sit
    behind Cloudflare and none of them sends ``Retry-After``, so a client has
    nothing to obey but its own backoff.
    """
    owned = client is None
    http = client or open_client()
    try:
        for attempt in range(_RETRY_ATTEMPTS):
            last_attempt = attempt == _RETRY_ATTEMPTS - 1
            try:
                response = build(http)
            except httpx.TransportError:
                if last_attempt:
                    raise
            else:
                if response.status_code not in _RETRYABLE_STATUSES or last_attempt:
                    break
            time.sleep(_RETRY_BASE_SECONDS * (2**attempt))
    finally:
        if owned:
            http.close()
    # Only fold a served response into the freshness hint; a 429 or a 502
    # carries the CDN's error policy, not the resource's.
    if hint is not None and response.status_code < 400:
        hint.observe(response)
    if response.status_code == 404:
        raise NotFound(request_url)
    if response.status_code >= 400:
        raise PolymarketError(response.status_code, request_url, response.text)
    return response.json()


def _get(
    base: str,
    path: str,
    params: dict[str, Any] | Sequence[tuple[str, Any]] | None = None,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any] | list[Any]:
    """GET ``base + path`` and return the decoded JSON body.

    One of the module's two outbound-HTTP chokepoints. It issues ``GET`` and
    nothing else.

    Args:
        base: One of the three host base URLs.
        path: Path below that base, with a leading slash.
        params: Query parameters; ``None`` values are dropped. A sequence of
            pairs rather than a mapping when a parameter must repeat, which
            Gamma's array filters (``condition_ids``, ``tag_id``) require.
        client: Optional caller-owned client, so a fan-out can reuse one
            connection pool.
        hint: Optional :class:`CacheHint` to fold this response's
            ``Cache-Control`` into.

    Raises:
        NotFound: The resource does not exist (HTTP 404).
        PolymarketError: Any other non-2xx, after retries.
    """
    pairs = list(params.items()) if isinstance(params, dict) else list(params or ())
    clean: Any = [(k, v) for k, v in pairs if v is not None]
    url = f"{base}{path}"
    return _send(url, lambda http: http.get(url, params=clean), client=client, hint=hint)


def _post_read(
    path: str,
    body: list[dict[str, Any]],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any] | list[Any]:
    """POST a batch **read** to the CLOB and return the decoded JSON body.

    The second and last outbound-HTTP chokepoint. Polymarket exposes its batch
    reads as POST-with-a-body — ``GET /books`` is a hard 400 — so the read-only
    property of this worker cannot be "never POST"; it is instead that this
    function refuses any path outside :data:`READ_ONLY_POST_PATHS`.

    That check is here, at call time, rather than left to review: it is the
    thing standing between this module and Polymarket's order-entry endpoints,
    which sit on the same host.

    Raises:
        ValueError: ``path`` is not a known read-only batch endpoint. This is a
            programming error, not a runtime condition.
    """
    if path not in READ_ONLY_POST_PATHS:
        raise ValueError(
            f"refusing to POST to {path!r}: this worker is read-only, and only "
            f"{sorted(READ_ONLY_POST_PATHS)} are read endpoints"
        )
    url = f"{clob_base_url()}{path}"
    return _send(url, lambda http: http.post(url, json=body), client=client, hint=hint)


# --------------------------------------------------------------------------
# Gamma: the catalog
# --------------------------------------------------------------------------


def keyset_page(
    path: str,
    key: str,
    params: Sequence[tuple[str, Any]] | dict[str, Any] | None = None,
    *,
    cursor: str | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    page_limit: int = GAMMA_PAGE_LIMIT,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch exactly one keyset page, returning its rows and the next cursor.

    The primitive behind the paged scans. A table function holds the cursor in
    its scan state and calls this once per tick, so DuckDB sees rows from the
    first page rather than waiting for the last — which is what keeps a
    ``LIMIT 10`` over a large catalog responsive, and, since a scan blocked
    inside its first batch cannot be cancelled, what keeps such a query from
    wedging the client outright.

    The cursor parameter is ``after_cursor``. This is worth stating because the
    response field it comes from is called ``next_cursor``, and passing it back
    as ``cursor`` — the obvious guess, since that is the field it came from —
    is accepted with HTTP 200 and **silently ignored**, so the walk re-serves
    page one forever. Nothing in the response distinguishes that from real
    progress.

    A ``None`` cursor means this was the final page.
    """
    pairs = list(params.items()) if isinstance(params, dict) else list(params or ())
    pairs.append(("limit", page_limit))
    if cursor:
        pairs.append(("after_cursor", cursor))
    payload = _get(gamma_base_url(), path, pairs, client=client, hint=hint)
    if not isinstance(payload, dict):
        raise PolymarketError(200, path, f"expected a keyset envelope, got {type(payload).__name__}")
    rows = payload.get(key) or []
    next_cursor = payload.get("next_cursor") or None
    # An empty page ends the walk even when a cursor comes back with it.
    return list(rows), (next_cursor if rows else None)


def offset_page(
    base: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    offset: int = 0,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    page_limit: int = GAMMA_PAGE_LIMIT,
) -> list[dict[str, Any]]:
    """Fetch one offset page from an endpoint that returns a bare JSON array.

    ``/tags``, ``/series`` and the Data API's tape page this way rather than
    with a cursor, and they return the array itself rather than an envelope.
    A short page means the end of the walk.
    """
    page_params = {**(params or {}), "limit": page_limit, "offset": offset}
    payload = _get(base, path, page_params, client=client, hint=hint)
    if not isinstance(payload, list):
        raise PolymarketError(200, path, f"expected a JSON array, got {type(payload).__name__}")
    return payload


def _offset_all(
    base: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    page_limit: int = GAMMA_PAGE_LIMIT,
) -> list[dict[str, Any]]:
    """Walk an offset-paged endpoint to exhaustion, or to ``limit`` rows.

    Used only where the fetch is bounded by construction — one market's trades,
    one market's holders. The unbounded catalog walks are scans instead, so they
    can stream. Running out of :data:`MAX_PAGES` raises rather than returning a
    short result that looks complete.
    """
    rows: list[dict[str, Any]] = []
    for page in range(MAX_PAGES):
        wanted = page_limit if limit is None else max(1, min(page_limit, limit - len(rows)))
        batch = offset_page(
            base,
            path,
            params,
            offset=page * page_limit,
            client=client,
            hint=hint,
            page_limit=wanted,
        )
        rows.extend(batch)
        if limit is not None and len(rows) >= limit:
            return rows[:limit]
        # A page shorter than requested is the last one.
        if len(batch) < wanted:
            return rows
    raise PolymarketPageLimitError(f"{base}{path}", len(rows))


def markets_by_condition_id(
    condition_ids: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, dict[str, Any]]:
    """Look up many markets at once by condition id, keyed by condition id.

    ``condition_ids`` is a repeated query parameter, so a per-row market lookup
    under a LATERAL costs one request per :data:`CONDITION_ID_BATCH_LIMIT` rows
    rather than one per row. Duplicates are collapsed, since the result is a
    mapping the caller fans back out.

    An id that matches nothing simply does not appear in the result. That is the
    only signal available — Gamma answers a filter that matches nothing with an
    empty list, not a 404.
    """
    unique = list(dict.fromkeys(c for c in condition_ids if c))
    found: dict[str, dict[str, Any]] = {}
    for start in range(0, len(unique), CONDITION_ID_BATCH_LIMIT):
        chunk = unique[start : start + CONDITION_ID_BATCH_LIMIT]
        rows, _ = keyset_page(
            "/markets/keyset",
            "markets",
            [("condition_ids", c) for c in chunk],
            client=client,
            hint=hint,
        )
        for row in rows:
            if condition_id := row.get("conditionId"):
                found[str(condition_id)] = row
    return found


def event(
    event_id: str,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any]:
    """One event by its numeric Gamma id.

    The response also carries the event's markets. They are returned by
    ``event_markets()`` rather than here, reshaped to the same columns
    ``markets()`` uses — Gamma's market endpoint has no event filter, so the
    inlined copy is the only route to them.

    The id is percent-encoded into the path rather than interpolated. It reaches
    this function from SQL, so it is caller-controlled, and an unencoded ``..``
    segment does not merely produce a bad request — it silently retargets the
    call: ``event('16183/../../markets')`` resolves to ``GET /markets`` and
    returns a different endpoint's rows reshaped as an event. Every other
    caller-supplied value in this module travels as a query parameter, which
    httpx encodes; this is the one that forms part of the path.

    Raises:
        NotFound: No event has that id.
    """
    path = f"/events/{urlquote(str(event_id), safe='')}"
    payload = _get(gamma_base_url(), path, client=client, hint=hint)
    return payload if isinstance(payload, dict) else {}


def search_page(
    query: str,
    *,
    page: int = 1,
    events_status: str | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """One page of full-text search results, as events.

    Gamma's only text search. It matters because nothing else here searches:
    without it, "find the Bitcoin markets" means scanning the whole catalog and
    filtering in SQL, which is tens of thousands of rows to answer a question
    the site answers instantly.

    Results come back as **events** with their markets inlined, so this returns
    the same shape :func:`events_page` does and chains into ``event_markets()``.
    Paginated by ``page`` (1-based), which is a third paging style on the same
    host — neither the keyset cursor the catalog uses nor the offset the tag and
    series lists use.
    """
    payload = _get(
        gamma_base_url(),
        "/public-search",
        {
            "q": query,
            "page": max(page, 1),
            "limit_per_type": SEARCH_PAGE_LIMIT,
            "events_status": events_status,
            # Tags and profiles would arrive as separate result types with a
            # different shape; this function returns events, so it asks for
            # events.
            "search_tags": "false",
            "search_profiles": "false",
        },
        client=client,
        hint=hint,
    )
    if not isinstance(payload, dict):
        raise PolymarketError(200, "/public-search", f"expected an object, got {type(payload).__name__}")
    return list(payload.get("events") or [])


def activity(
    proxy_wallet: str,
    *,
    activity_type: str | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """One wallet's activity feed: trades, redemptions, yield and rebates.

    The history counterpart to :func:`positions`, which reports only what a
    wallet holds *now*. Note the feed is broader than a trade tape — most
    entries on a long-lived wallet are ``YIELD`` accruals rather than trades —
    so a caller after executions should filter on the type.
    """
    return _offset_all(
        data_base_url(),
        "/activity",
        {"user": proxy_wallet, "type": activity_type},
        limit=limit,
        client=client,
        hint=hint,
        page_limit=DATA_PAGE_LIMIT,
    )


def series_page(
    *,
    offset: int = 0,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """One page of the series catalog.

    ``exclude_events=true`` is not optional in practice. Each series inlines
    every event beneath it, and those events inline their markets: 50 series
    come to 1.73 MB with the events and 31.6 KB without, a 55x difference. Over
    the whole 2,392-series catalog that is the gap between ~83 MB and ~1.5 MB,
    for columns this table does not expose either way.
    """
    return offset_page(
        gamma_base_url(),
        "/series",
        {"exclude_events": "true"},
        offset=offset,
        client=client,
        hint=hint,
        page_limit=SERIES_PAGE_LIMIT,
    )


def tags_page(
    *,
    offset: int = 0,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """One page of the tag catalog — Polymarket's topic axis, ~7,300 rows."""
    return offset_page(gamma_base_url(), "/tags", offset=offset, client=client, hint=hint)


# --------------------------------------------------------------------------
# CLOB: books, quotes, price history — all keyed on TOKEN id
# --------------------------------------------------------------------------


def book(
    token_id: str,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any]:
    """The resting-order book for one outcome token.

    Keyed on the **token** id, not the condition id: a condition id here is a
    404, which is at least loud. Contrast :func:`price_history`, where the same
    mistake is silent.

    Raises:
        NotFound: No book exists for that token id.
    """
    payload = _get(clob_base_url(), "/book", {"token_id": token_id}, client=client, hint=hint)
    return payload if isinstance(payload, dict) else {}


def books(
    token_ids: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, dict[str, Any]]:
    """Order books for many tokens at once, keyed by token id.

    The batched twin of :func:`book`, and the reason a ``LATERAL`` over a whole
    tag is affordable: measured at 12.85s for 100 books individually against
    0.16s batched, roughly 81x, and — far more to the point — one request
    against the rate limiter instead of a hundred.

    This is a ``POST`` because Polymarket offers no ``GET`` spelling; see
    :func:`_post_read` for why that does not make the worker writable.

    A token with no book is **absent from the response** rather than present and
    empty, so the caller must tolerate a missing key: asking for 100 tokens
    returned 98 entries in testing. Absent and empty mean the same thing to the
    caller here — no resting orders — so both produce no rows.
    """
    unique = list(dict.fromkeys(t for t in token_ids if t))
    found: dict[str, dict[str, Any]] = {}
    for start in range(0, len(unique), TOKEN_BATCH_LIMIT):
        chunk = unique[start : start + TOKEN_BATCH_LIMIT]
        payload = _post_read("/books", [{"token_id": t} for t in chunk], client=client, hint=hint)
        for entry in payload if isinstance(payload, list) else []:
            if asset_id := entry.get("asset_id"):
                found[str(asset_id)] = entry
    return found


def midpoints(
    token_ids: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, str]:
    """Midpoint price for many tokens at once, keyed by token id.

    Answers a flat ``{token_id: "0.4455"}`` mapping, unlike :func:`prices`,
    which nests a side beneath each token. The two batch endpoints disagree
    about their response shape; both spellings are Polymarket's.
    """
    unique = list(dict.fromkeys(t for t in token_ids if t))
    found: dict[str, str] = {}
    for start in range(0, len(unique), TOKEN_BATCH_LIMIT):
        chunk = unique[start : start + TOKEN_BATCH_LIMIT]
        payload = _post_read("/midpoints", [{"token_id": t} for t in chunk], client=client, hint=hint)
        for token, mid in (payload or {}).items() if isinstance(payload, dict) else ():
            found[str(token)] = str(mid)
    return found


def prices(
    token_ids: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, dict[str, str]]:
    """Best bid and best ask for many tokens at once, keyed by token id.

    One request asks for both sides of every token, so the body carries two
    entries per token and the response nests them as
    ``{token_id: {"BUY": "0.044", "SELL": "0.045"}}``.

    **``BUY`` is the best bid and ``SELL`` is the best ask**, which is the
    opposite of the obvious reading. Verified against the raw book: ``BUY``
    equals ``max(bids.price)`` and ``SELL`` equals ``min(asks.price)``. See
    :func:`~vgi_polymarket.schemas.quote_row`, which is where the two are named.
    """
    unique = list(dict.fromkeys(t for t in token_ids if t))
    found: dict[str, dict[str, str]] = {}
    for start in range(0, len(unique), TOKEN_BATCH_LIMIT):
        chunk = unique[start : start + TOKEN_BATCH_LIMIT]
        body = [{"token_id": t, "side": side} for t in chunk for side in ("BUY", "SELL")]
        payload = _post_read("/prices", body, client=client, hint=hint)
        for token, sides in (payload or {}).items() if isinstance(payload, dict) else ():
            found[str(token)] = {str(k): str(v) for k, v in (sides or {}).items()}
    return found


def price_history(
    token_id: str,
    *,
    interval: str | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
    fidelity: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """The price time series for one outcome token.

    Two traps live in this one call, both Polymarket's:

    * The parameter is named ``market`` but wants a **token id**. Passing a
      condition id — the thing actually called a market everywhere else — is
      answered with HTTP 200 and ``{"history": []}``, which reads exactly like a
      token that never traded.
    * The window parameters are camelCase (``startTs``, ``endTs``). The
      snake_case spellings are accepted, ignored, and answered with the default
      window, so the wrong guess quietly returns the wrong period rather than
      an error.

    ``interval`` and an explicit window are alternatives, not companions;
    ``fidelity`` is the bar width in minutes and applies to both.
    """
    params: dict[str, Any] = {"market": token_id, "fidelity": fidelity}
    if start_ts or end_ts:
        params["startTs"] = start_ts or None
        params["endTs"] = end_ts or None
    else:
        params["interval"] = interval or "1w"
    payload = _get(clob_base_url(), "/prices-history", params, client=client, hint=hint)
    history = payload.get("history") if isinstance(payload, dict) else None
    return list(history or [])


# --------------------------------------------------------------------------
# Data API: the tape and holders — keyed on CONDITION id
# --------------------------------------------------------------------------


def trades(
    condition_id: str,
    *,
    side: str | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """The public trade tape for one market, newest first.

    Note the identifier. This endpoint's ``market`` parameter wants a
    **condition id**, where the CLOB's identically-named parameter wants a token
    id — and a token id here returns ``[]`` with HTTP 200 rather than an error.
    The two APIs use the same parameter name for different keys, and one of the
    two ways to get it wrong is silent in each direction.
    """
    return _offset_all(
        data_base_url(),
        "/trades",
        {"market": condition_id, "side": side},
        limit=limit,
        client=client,
        hint=hint,
        page_limit=DATA_PAGE_LIMIT,
    )


def positions(
    proxy_wallet: str,
    *,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """Every open position held by one wallet, newest-valued first.

    The reverse of :func:`holders`: that goes from a market to its wallets, this
    goes from a wallet to its markets. Keyed on the wallet address rather than
    on either market identifier — a third key, and the only endpoint here that
    takes it.

    Declares the shortest TTL Polymarket publishes (``max-age=5``): a position's
    value moves with the market price, so it is nearly live data.
    """
    return _offset_all(
        data_base_url(),
        "/positions",
        {"user": proxy_wallet},
        limit=limit,
        client=client,
        hint=hint,
        page_limit=DATA_PAGE_LIMIT,
    )


def holders(
    condition_id: str,
    *,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """The largest position holders in one market, grouped by outcome token.

    Keyed on the condition id, like :func:`trades` and unlike everything on the
    CLOB. The response is one entry per token, each carrying its own ranked
    holder list, so it is flattened by the caller into one row per holder.
    """
    payload = _get(
        data_base_url(),
        "/holders",
        {"market": condition_id, "limit": limit},
        client=client,
        hint=hint,
    )
    return list(payload) if isinstance(payload, list) else []
