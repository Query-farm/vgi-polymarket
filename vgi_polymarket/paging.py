"""Paging as VGI scan state, in the two styles Polymarket uses.

Walking a paged endpoint to exhaustion inside one call is the obvious
implementation and the wrong one: a blended row-transform must emit everything
for its input in a single ``process()``, so a ``LIMIT 10`` over the market
catalog still pays for every page, and because a scan blocked inside its first
batch cannot be cancelled, the query does not merely run slowly — it wedges the
client until the worker gives up.

The framework already has the answer. A ``TableFunctionGenerator`` carries state
between ``process()`` ticks, so the page position lives in that state and each
tick fetches exactly one page and emits exactly one batch. DuckDB sees rows
immediately, a ``LIMIT`` stops the walk early, and cancellation lands between
ticks instead of never.

The trade-off is real and worth stating: a stateful scan cannot be the inner
side of a correlated ``LATERAL``. So the rule this package follows is *if the
endpoint pages, the function pages* — paged endpoints become scans, while the
bounded ones (one book, one quote, one market's tape) stay blended and stay
composable.

Polymarket pages three ways, on the same host, and the differences are not
cosmetic:

* **Keyset** (``/markets/keyset``, ``/events/keyset``) — an opaque
  ``after_cursor``. Stable under concurrent writes: a market created mid-walk
  cannot shift rows across a page boundary. Necessarily serial, because page
  N+1's address is inside page N's response.
* **Offset** (``/tags``, ``/series``, and the Data API's feeds) — a row count to
  skip. A catalog that changes mid-walk can repeat or skip a row, which is
  acceptable for slow-changing reference data and would not be for markets. Its
  one advantage is that the pages are independent, so they are fetched
  concurrently here.
* **Page number** (``/public-search``) — 1-based, counted per result type.

Each gets its own emitter below.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from vgi.cache_control import CacheControl
from vgi.table_function import ProcessParams
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.polymarket_api import STALE_IF_ERROR, CacheHint
from vgi_polymarket.schemas import batch_from_rows


@dataclass(kw_only=True)
class KeysetScanState(ArrowSerializableDataclass):
    """Where a cursor-paged scan has got to.

    The framework persists this between ``process()`` ticks. ``cursor`` is
    Gamma's opaque continuation token; an empty string means "not started",
    which the cursor alone cannot tell apart from "finished" — hence ``done``.

    ``pages`` is the runaway guard. A ``LIMIT`` stops this scan early, but an
    *aggregate* over an unfiltered ``markets()`` has no reason to stop before
    the end of the catalog — and a predicate DuckDB cannot turn into a constant
    (``WHERE condition_id = (SELECT ...)``) becomes exactly that. Without a cap
    such a query walks every page Polymarket has, which is not a query anyone
    means to run.
    """

    cursor: str = ""
    done: bool = False
    pages: int = 0


@dataclass(kw_only=True)
class OffsetScanState(ArrowSerializableDataclass):
    """How many rows an offset-paged scan has already emitted.

    ``offset`` is the next row to ask for, so it doubles as the running count.
    ``done`` is set when a short page arrives, which is the only end-of-walk
    signal these endpoints give — they answer past the end with ``[]`` rather
    than with an error.
    """

    offset: int = 0
    done: bool = False


@dataclass(kw_only=True)
class SearchScanState(ArrowSerializableDataclass):
    """The next 1-based page of search results and whether the scan is done."""

    page: int = 1
    done: bool = False
    pages: int = 0


def _cache_control(hint: CacheHint) -> CacheControl | None:
    """Translate the origin's own Cache-Control into VGI cache metadata.

    Polymarket declares ``max-age`` on every catalog endpoint, so forwarding its
    actual policy — rather than inventing a TTL — means the result cache follows
    the site automatically if those values ever change. ``None`` means the origin
    called this live, and the result is left uncached.
    """
    if not hint.cacheable:
        return None
    return CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)


def emit_keyset_page(
    params: ProcessParams[Any],
    state: KeysetScanState,
    out: OutputCollector,
    *,
    path: str,
    key: str,
    filters: Sequence[tuple[str, Any]],
    shape: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    """Fetch one keyset page into one batch, and record where to resume.

    Args:
        params: The tick's parameters; ``output_schema`` is the projected one.
        state: The cursor carried between ticks.
        out: Collector for the batch, or for ``finish()`` once the walk is done.
        path: Gamma path below the base, e.g. ``/markets/keyset``.
        key: Key in the response envelope holding this page's rows.
        filters: Query parameters as pairs, so an array filter can repeat.
        shape: Row reshaper mapping one raw payload to this package's columns.
    """
    if state.done:
        out.finish()
        return
    if state.pages >= api.MAX_PAGES:
        # Raised rather than quietly finishing: a scan that stops early looks
        # exactly like a complete result, and nothing downstream can tell the
        # difference. The fix is at the call site — filter, or add a LIMIT.
        raise api.PolymarketPageLimitError(path, state.pages * api.GAMMA_PAGE_LIMIT)
    hint = CacheHint()
    raw, cursor = api.keyset_page(path, key, filters, cursor=state.cursor or None, hint=hint)
    state.pages += 1
    state.cursor = cursor or ""
    state.done = cursor is None
    # Cache metadata rides on the first emitted batch, which for a paged scan is
    # the first page. Every page carries it so the walk is self-describing at
    # any point, but the first is the one the client reads.
    out.emit(
        batch_from_rows([shape(row) for row in raw], params.output_schema),
        cache_control=_cache_control(hint),
    )


#: How many offset pages to fetch at once. This is the one advantage offset
#: paging has over a cursor: page N+1's address does not depend on page N's
#: response, so the pages can be fetched concurrently. It matters — the tag
#: catalog is ~7,300 rows at 100 a page, so a serial walk is 73 round trips,
#: and a `WHERE label ILIKE ...` has to reach the end of it. Measured at 8.5s
#: serially against well under 1s at this width.
#:
#: Eight is chosen against the measured ceiling: these endpoints sustain ~20
#: requests a second without throttling, and a burst of eight stays inside that
#: while the retry in :func:`~vgi_polymarket.polymarket_api._send` covers a 429
#: if the budget ever tightens.
PAGE_FETCH_CONCURRENCY = 8


def _merge(into: CacheHint, other: CacheHint) -> None:
    """Fold one page's freshness opinion into the running hint.

    Concurrent fetches cannot share a :class:`CacheHint` — it is mutable and not
    thread-safe — so each gets its own and they are merged here, preserving the
    same rule the serial path uses: the shortest-lived page bounds the result,
    and one uncacheable page makes the whole result uncacheable.
    """
    if other.saw_uncacheable:
        into.saw_uncacheable = True
    if other.max_age is not None:
        into.max_age = other.max_age if into.max_age is None else min(into.max_age, other.max_age)


def emit_search_page(
    params: ProcessParams[Any],
    state: SearchScanState,
    out: OutputCollector,
    *,
    fetch: Callable[..., tuple[list[dict[str, Any]], bool]],
    shape: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    """Fetch one page-numbered search page, and record where to resume.

    ``/public-search`` paginates by 1-based page number rather than by offset or
    cursor — a third style on the same host. Gamma supplies
    ``pagination.hasMore``; that signal decides whether to continue, rather than
    guessing from the number of event rows in a page.

    Serial rather than concurrent, unlike the catalog scans: a search is browsed
    with a ``LIMIT``, not walked to the end, so fetching pages ahead would spend
    requests on rows nobody asked for.
    """
    if state.done:
        out.finish()
        return
    if state.pages >= api.MAX_PAGES:
        raise api.PolymarketPageLimitError("/public-search", state.pages * api.SEARCH_PAGE_LIMIT)
    hint = CacheHint()
    raw, has_more = fetch(page=state.page, hint=hint)
    state.page += 1
    state.pages += 1
    state.done = not has_more
    out.emit(
        batch_from_rows([shape(row) for row in raw], params.output_schema),
        cache_control=_cache_control(hint),
    )


def emit_offset_page(
    params: ProcessParams[Any],
    state: OffsetScanState,
    out: OutputCollector,
    *,
    fetch: Callable[..., list[dict[str, Any]]],
    shape: Callable[[dict[str, Any]], dict[str, Any]],
    page_limit: int,
) -> None:
    """Fetch a run of offset pages into one batch, and record where to resume.

    ``fetch`` takes an offset and a :class:`CacheHint` and returns the raw page.
    A page shorter than ``page_limit`` ends the walk — these endpoints answer
    past the end with an empty array rather than an error, so a short page is
    the only signal available.

    :data:`PAGE_FETCH_CONCURRENCY` pages are fetched at once and concatenated in
    order. Order is preserved because the results are indexed by their offset
    rather than by completion, so a slow page cannot reshuffle the scan; and the
    walk still stops at the first short page, with any pages fetched beyond it
    discarded rather than appended, which is what keeps a concurrent fetch from
    inventing rows past the end.
    """
    if state.done:
        out.finish()
        return
    hint = CacheHint()
    offsets = [state.offset + i * page_limit for i in range(PAGE_FETCH_CONCURRENCY)]
    hints = [CacheHint() for _ in offsets]
    with ThreadPoolExecutor(max_workers=PAGE_FETCH_CONCURRENCY) as pool:
        pages = list(pool.map(lambda pair: fetch(offset=pair[0], hint=pair[1]), zip(offsets, hints)))

    rows: list[dict[str, Any]] = []
    for page, page_hint in zip(pages, hints, strict=True):
        _merge(hint, page_hint)
        rows.extend(page)
        if len(page) < page_limit:
            # The end of the catalog. Anything fetched past it is empty anyway,
            # but stopping here keeps `offset` honest for the next tick.
            state.done = True
            break
    state.offset += len(rows)
    out.emit(
        batch_from_rows([shape(row) for row in rows], params.output_schema),
        cache_control=_cache_control(hint),
    )
