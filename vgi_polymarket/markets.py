"""The catalog surface: markets, market, events, event.

``markets()`` and ``events()`` are **paged scans** — the endpoints behind them
are cursor-paged, so they hold Gamma's ``after_cursor`` in VGI scan state and
emit one API page per tick. ``market()`` and ``event()`` are **blended**
(:class:`~vgi.table_in_out_function.RowTransformFunction`) point lookups, whose
positional argument *is* the per-row input column, so one registration serves
both a literal call and a correlated LATERAL::

    SELECT * FROM polymarket.main.market('0xa467b14d...');

    SELECT e.title, m.question
    FROM polymarket.main.events(tag_id => '21') e,
         LATERAL polymarket.main.market(...) m;

Three constraints from the blended contract shape these signatures:

* No ``finalize``/``finish`` — DuckDB forbids ``FinalExecute`` under correlated
  LATERAL, so nothing here may accumulate across batches.
* Positional args are read off ``batch`` by declared name; they are **not**
  surfaced on ``params.args``.
* A positional ``const`` arg is rejected, and DuckDB cannot bind a *named*
  argument on a table-in-out function at all — so every knob these functions
  offer is a required positional. An optional one would be unreachable.

Each blended function is 1->N, so every ``emit`` carries ``parent_rows``
provenance mapping output rows back to the input row that produced them.
Without it the batched-LATERAL operator cannot stamp the correlated columns onto
the right rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, cast

import pyarrow as pa
from vgi.arguments import Arg
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi.table_in_out_function import RowTransformFunction
from vgi_rpc.rpc import OutputCollector

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.meta import docs, examples
from vgi_polymarket.paging import (
    KeysetScanState,
    SearchScanState,
    emit_keyset_page,
    emit_search_page,
)
from vgi_polymarket.polymarket_api import STALE_IF_ERROR, CacheHint, NotFound
from vgi_polymarket.schemas import (
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    batch_from_rows,
    event_row,
    market_row,
)

if TYPE_CHECKING:
    from vgi.protocol import VgiOutputCollector


def origin_cache_control(hint: CacheHint) -> CacheControl | None:
    """Translate the origin's own Cache-Control into VGI cache metadata.

    Polymarket declares ``max-age`` on every catalog endpoint (300s on markets
    and events, 1800s on tags), so forwarding its actual policy — rather than
    inventing a TTL — means the result cache follows the site automatically.
    ``None`` means the origin called this live, and the result is left uncached.
    """
    if not hint.cacheable:
        return None
    return CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)


def emit_fanout(
    out: OutputCollector,
    schema: pa.Schema,
    rows: Sequence[dict[str, Any]],
    parent_rows: Sequence[int],
    cache_control: CacheControl | None = None,
) -> None:
    """Emit a 1->N batch with per-output-row provenance and optional cacheability.

    ``parent_rows[i]`` is the index, within this call's input batch, of the row
    that produced output row ``i``. Cache metadata rides on the first emitted
    batch, which is this one — each of these functions emits exactly once.

    ``schema`` is the caller's ``params.output_schema``, which is the *projected*
    schema: with ``projection_pushdown`` declared, a ``SELECT question`` narrows
    it to one column and only that column is built. Rows are dicts, so the
    columns projected away simply are not read.
    """
    batch = batch_from_rows(rows, schema)
    cast("VgiOutputCollector", out).emit(batch, parent_rows=list(parent_rows), cache_control=cache_control)


def _flag(value: str) -> str | None:
    """Turn a three-state string argument into Gamma's ``true``/``false``/absent.

    Gamma's boolean filters are strings, and *omitting* one is meaningfully
    different from passing either value: ``closed`` absent returns open and
    settled markets together. A tri-state string argument is the only way to
    offer that third option, since an Arrow boolean argument has no "unset".
    """
    return value.strip().lower() or None


#: Gamma **silently ignores** a query parameter it does not recognise: a filter
#: that does not exist returns the unfiltered first page with HTTP 200. A typo
#: therefore cannot be caught at runtime, so only parameters verified against the
#: live API and the published OpenAPI document are ever sent.
#:
#: Translating a predicate here is an optimisation, not the thing that makes it
#: correct. Every predicate is applied to the rows regardless, by the framework's
#: filtering collector — see `Meta.auto_apply_filters`. What this buys is the
#: chance to have Gamma narrow the result *before* it is fetched, which is the
#: difference between one request and a walk of a 120,000-row catalog. Declining
#: to translate one costs bandwidth; translating one *wrongly* would drop rows
#: the API never returned, and nothing downstream could recover them.
#:
#: These are the market columns whose equality predicates map exactly onto a
#: Gamma filter of the same meaning. `event_id` is deliberately absent:
#: `/markets/keyset` has no such parameter, and sending one would be silently
#: dropped, so the predicate is left for DuckDB alone.
_MARKET_PUSHDOWN = {
    "condition_id": "condition_ids",
    "slug": "slug",
    "market_id": "id",
}

#: The same, for events.
_EVENT_PUSHDOWN = {
    "event_id": "id",
    "slug": "slug",
}


def _pushed(params: ProcessParams[Any], mapping: dict[str, str], already: set[str]) -> list[tuple[str, Any]]:
    """Turn a pushed-down WHERE into Gamma query parameters, conservatively.

    Declining to translate a predicate costs bandwidth, never correctness: the
    filters are applied to the rows either way, by the framework's filtering
    collector. Translating one *wrongly* is the dangerous direction, because a
    row Gamma never returned cannot be recovered downstream — the engine has
    already dropped its own filter above this scan. So only exact
    correspondences are used, and an explicit named argument always wins.
    """
    filters = params.current_pushdown_filters
    if filters is None:
        return []
    pairs: list[tuple[str, Any]] = []
    for column, parameter in mapping.items():
        if parameter in already:
            continue
        scalar = filters.get_column_constant(column)
        value = scalar.as_py() if scalar is not None else None
        if isinstance(value, str) and value:
            pairs.append((parameter, value))
    return pairs


@dataclass(slots=True, frozen=True, kw_only=True)
class MarketsArgs:
    """``markets()`` — every filter is optional and named.

    Empty string means "not supplied". A ``str | None`` annotation resolves to
    the Arrow null type, which the DuckDB extension cannot cast a VARCHAR into
    ("Unimplemented type for cast (VARCHAR -> NULL)"), so every optional
    argument here uses a sentinel default instead of None.
    """

    tag_id: Annotated[
        str, Arg("tag_id", doc="Restrict to one topic, by the id from the tags table", default="")
    ] = ""
    slug: Annotated[str, Arg("slug", doc="Look up one market by its polymarket.com slug", default="")] = ""
    condition_id: Annotated[
        str, Arg("condition_id", doc="Look up one market by its on-chain id", default="")
    ] = ""
    closed: Annotated[
        str,
        Arg(
            "closed",
            doc="Restrict to markets that have stopped trading, or to those still open",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""
    active: Annotated[
        str,
        Arg(
            "active",
            doc="Restrict to markets live on the site, or to those that are not",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""
    order_by: Annotated[
        str,
        Arg(
            "order_by",
            doc="Field to sort by before paging",
            default="",
            choices=["", "volume24hr", "volume", "liquidity", "startDate", "endDate", "competitive"],
        ),
    ] = ""
    ascending: Annotated[
        str,
        Arg(
            "ascending",
            doc="Sort direction to pair with order_by",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""


def _market_filters(args: MarketsArgs) -> list[tuple[str, Any]]:
    """The Gamma query parameters one ``markets()`` call asks for."""
    pairs: list[tuple[str, Any]] = []
    if args.tag_id:
        pairs.append(("tag_id", args.tag_id))
    if args.slug:
        pairs.append(("slug", args.slug))
    if args.condition_id:
        pairs.append(("condition_ids", args.condition_id))
    if flag := _flag(args.closed):
        pairs.append(("closed", flag))
    if flag := _flag(args.active):
        pairs.append(("active", flag))
    if args.order_by:
        pairs.append(("order", args.order_by))
    if flag := _flag(args.ascending):
        pairs.append(("ascending", flag))
    return pairs


MARKETS_DOCS = docs(
    category="markets",
    result_schema=MARKET_SCHEMA,
    llm=(
        "The entry point to Polymarket's market data: one row per tradeable question, with its "
        "current price, volume and lifecycle state. Reach for this to browse or filter — start "
        "from a `tag_id` out of the `tags` table, or pass `order_by => 'volume24hr'` to see what is "
        "busy right now. The `outcomes` column is the important one: it carries each outcome's "
        "`token_id`, which is what `orderbook()`, `quote()` and `price_history()` key on. The "
        "`condition_id` column is a DIFFERENT key, and is what `trades()` and `holders()` take. "
        "Passing one where the other belongs usually returns an empty result rather than an error."
    ),
    md=(
        "Markets are the tradeable questions on Polymarket. Each has two or more outcomes, and "
        "each outcome trades as its own CLOB token.\n\n"
        "### The two identifiers\n\n"
        "This is the thing to get right, because getting it wrong is usually silent:\n\n"
        "- `condition_id` (`0x` + 64 hex) identifies the **market**. `trades()` and `holders()` "
        "take it.\n"
        "- `token_id` (a long decimal string, inside `outcomes`) identifies **one outcome**. "
        "`orderbook()`, `quote()` and `price_history()` take it.\n\n"
        "Both are called `market` in Polymarket's own URLs, which is where the confusion comes "
        "from.\n\n"
        "### Reading `outcomes`\n\n"
        "Polymarket sends outcome names, prices and token ids as three separate JSON-encoded "
        "strings whose only relationship is positional. They are zipped here into one list of "
        "structs so the correspondence cannot be got wrong. Reach the individual outcomes by "
        "unnesting that column in a LATERAL — the shipped example queries show the exact "
        "form, which needs `recursive := true` to expose the struct's fields as columns.\n\n"
        "### Prices are probabilities, where the market is liquid\n\n"
        "Every price column is USDC per share between 0 and 1, and a share pays $1 if its "
        "outcome happens — so a price of 0.62 reads as a 62% implied probability. They are "
        "`DECIMAL`, never `DOUBLE`.\n\n"
        "The prices inside `outcomes` are Polymarket's displayed figures, normalised to sum to "
        "1 across a market's outcomes. That means they always *look* like a coherent forecast, "
        "even when nothing is resting on the book. Before treating one as a probability, check "
        "the real quote: `quote()` returns the `spread` and `orderbook()` the size behind it. A "
        "market quoting 0.02 bid against 0.99 ask has a displayed price and no market.\n\n"
        "### Streaming\n\n"
        "Rows arrive one API page at a time, so a `LIMIT` stops early instead of paying for the "
        "whole catalog. The cost is that this cannot be the inner side of a correlated "
        "`LATERAL` — drive a join from it, not into it. Use `market()` for the per-row lookup."
    ),
    example_queries=examples(
        (
            "The busiest open markets right now, with their implied probabilities",
            (
                "SELECT question, outcomes, volume_24hr FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 20"
            ),
        ),
        (
            "Every outcome of every crypto market, one row per outcome",
            (
                "SELECT m.question, o.outcome, o.price, o.token_id "
                "FROM polymarket.main.markets(tag_id => '21', closed => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o ORDER BY m.volume_24hr DESC LIMIT 50"
            ),
        ),
    ),
)


@init_single_worker
class MarketsFunction(TableFunctionGenerator[MarketsArgs, KeysetScanState]):
    """The market catalog, streamed one API page per tick.

    A paging scan rather than a blended row transform because the endpoint
    behind it is cursor-paged. A blended function must emit everything for its
    input in a single ``process()`` call, so it would walk every page before
    DuckDB saw one row; a ``LIMIT 10`` would still pay for the whole catalog,
    and — since a scan blocked inside its first batch cannot be cancelled —
    would wedge the client rather than merely be slow. Functions whose fetch is
    naturally bounded stay blended, and so stay usable under a LATERAL.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARKET_SCHEMA

    class Meta:
        name = "markets"
        description = "Browse Polymarket's tradeable questions with their prices and volume"
        categories = ["market-data"]
        # Projection must stay on wherever filters are accepted: a pushed
        # filter carries a column *index* relative to the projection, so the
        # projected batch is the only correct thing to evaluate it against.
        projection_pushdown = True
        filter_pushdown = True
        # Not optional alongside `filter_pushdown`. Accepting pushdown makes the
        # engine drop its own filter above the scan, but the filters are only
        # *delivered* — as `params.current_pushdown_filters`, and to the
        # collector wrapper that applies them — when this is set too. With one
        # flag and not the other, every predicate on this function was silently
        # discarded and `WHERE slug = 'nope'` returned rows.
        auto_apply_filters = True
        tags = MARKETS_DOCS
        examples = [
            FunctionExample(
                sql=(
                    "SELECT question, outcomes, volume_24hr FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 20"
                ),
                description="The busiest open markets right now, with their implied probabilities",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MarketsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[MarketsArgs]) -> KeysetScanState:
        return KeysetScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[MarketsArgs],
        state: KeysetScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of markets, then remember where to resume."""
        filters = _market_filters(params.args)
        filters += _pushed(params, _MARKET_PUSHDOWN, {name for name, _ in filters})
        emit_keyset_page(
            params,
            state,
            out,
            path=api.MARKETS_PATH,
            key="markets",
            filters=filters,
            shape=market_row,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class ConditionIdArgs:
    """A lone condition-id input column."""

    condition_id: Annotated[str, Arg(0, doc="On-chain market id to look up")]


class MarketFunction(RowTransformFunction[ConditionIdArgs]):
    """One market by condition id — the point lookup to ``markets()``'s browse.

    Batched: ``/markets/keyset`` takes ``condition_ids`` as a repeated
    parameter, so a LATERAL over 300 markets costs 5 requests rather than 300.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARKET_SCHEMA

    class Meta:
        name = "market"
        description = "Look up one Polymarket market by its on-chain condition id"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="markets",
            result_schema=MARKET_SCHEMA,
            llm=(
                "A single market's current snapshot, by exact condition id. Use this when you "
                "already have a condition id — from a trade row, say — and want the question "
                "behind it; use `markets()` when you want to browse or filter. Same columns as "
                "`markets()`, so the two are interchangeable downstream, and unlike `markets()` "
                "this composes under a correlated LATERAL."
            ),
            md=(
                "One row for one market, looked up by `condition_id`.\n\n"
                "### When to use this instead of `markets()`\n\n"
                "`markets()` is the browse path and streams a catalog; this is the point lookup, "
                "and it is the one that works as the inner side of a `LATERAL`. It is the "
                "natural way back from a trade or a holding to the question being traded.\n\n"
                "### Cost\n\n"
                "Requests are batched behind the scenes: Gamma accepts up to 60 condition ids "
                "per call, so a join over hundreds of markets costs a handful of requests, not "
                "hundreds.\n\n"
                "### An unknown id returns nothing\n\n"
                "Gamma answers a filter that matches nothing with an empty list rather than a "
                "404, so a condition id that does not exist produces no rows — the same as one "
                "that does exist but was filtered away."
            ),
            example_queries=examples(
                (
                    "The question behind the busiest open market",
                    (
                        "SELECT m.question, m.volume_24hr FROM ("
                        "SELECT condition_id FROM polymarket.main.markets("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) t, "
                        "LATERAL polymarket.main.market(t.condition_id) m"
                    ),
                ),
                (
                    "Join a market's trade tape back to its question",
                    (
                        "SELECT m.question, count(*) AS prints FROM ("
                        "SELECT condition_id FROM polymarket.main.markets("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 3) s, "
                        "LATERAL polymarket.main.market(s.condition_id) m, "
                        "LATERAL polymarket.main.trades(s.condition_id, 100, '') t GROUP BY m.question"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT m.question, m.volume_24hr FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) t, "
                    "LATERAL polymarket.main.market(t.condition_id) m"
                ),
                description="The question behind the busiest open market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ConditionIdArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ConditionIdArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        ids = batch.column("condition_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            # The whole input batch is fetched in one or two requests and then
            # fanned back out in input order, so provenance is unchanged and a
            # repeated id still produces its row once per input row.
            found = api.markets_by_condition_id(
                [str(i) for i in ids if i is not None], client=client, hint=hint
            )
            for index, condition_id in enumerate(ids):
                if condition_id is None:
                    continue
                if raw := found.get(str(condition_id)):
                    rows.append(market_row(raw))
                    parents.append(index)
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class EventsArgs:
    """``events()`` — every filter is optional and named."""

    tag_id: Annotated[
        str, Arg("tag_id", doc="Restrict to one topic, by the id from the tags table", default="")
    ] = ""
    series_slug: Annotated[
        str, Arg("series_slug", doc="Restrict to the events of one recurring series", default="")
    ] = ""
    slug: Annotated[str, Arg("slug", doc="Look up one event by its polymarket.com slug", default="")] = ""
    closed: Annotated[
        str,
        Arg(
            "closed",
            doc="Restrict to events that have finished, or to those still running",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""
    featured: Annotated[
        str,
        Arg(
            "featured",
            doc="Restrict to events Polymarket promotes on its front page",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""
    title_search: Annotated[
        str, Arg("title_search", doc="Case-insensitive substring to match against the title", default="")
    ] = ""
    order_by: Annotated[
        str,
        Arg(
            "order_by",
            doc="Field to sort by before paging",
            default="",
            choices=["", "volume24hr", "volume", "liquidity", "startDate", "endDate", "competitive"],
        ),
    ] = ""
    ascending: Annotated[
        str,
        Arg(
            "ascending",
            doc="Sort direction to pair with order_by",
            default="",
            choices=["", "true", "false"],
        ),
    ] = ""


def _event_filters(args: EventsArgs) -> list[tuple[str, Any]]:
    """The Gamma query parameters one ``events()`` call asks for."""
    pairs: list[tuple[str, Any]] = []
    if args.tag_id:
        pairs.append(("tag_id", args.tag_id))
    if args.series_slug:
        pairs.append(("series_slug", args.series_slug))
    if args.slug:
        pairs.append(("slug", args.slug))
    if flag := _flag(args.closed):
        pairs.append(("closed", flag))
    if flag := _flag(args.featured):
        pairs.append(("featured", flag))
    if args.title_search:
        pairs.append(("title_search", args.title_search))
    if args.order_by:
        pairs.append(("order", args.order_by))
    if flag := _flag(args.ascending):
        pairs.append(("ascending", flag))
    return pairs


EVENTS_DOCS = docs(
    category="reference",
    result_schema=EVENT_SCHEMA,
    llm=(
        "The layer between a topic and its markets: one row per event, with its aggregate volume, "
        "liquidity, open interest and topic tags. Reach for this to find a specific `event_id` — "
        "which `event_markets()` takes to return that event's markets — or to compare whole events rather than "
        "individual questions. An event groups the markets that resolve together: one NFL game, "
        "or one election with a market per candidate."
    ),
    md=(
        "An event groups the markets that resolve together. A single-question event has one "
        "market under it; an election event has one per candidate.\n\n"
        "### Narrowing from here\n\n"
        "Take an `event_id` and pass it to `event_markets()` to get that event's markets. There "
        "is deliberately no `event_slug` filter on `markets()`: `/markets/keyset` has no such "
        "parameter, and Gamma answers an unrecognised filter by ignoring it and returning "
        "everything, so offering one would silently return the wrong rows.\n\n"
        "### Tags stay nested\n\n"
        "`tags` is a list of `{tag_id, label, slug}` structs — flattening it would fan every "
        "event out into one row per tag. `unnest` the column when you do want that shape.\n\n"
        "### Money is float-encoded here\n\n"
        "Gamma sends event-level volume and liquidity as JSON numbers where the market level "
        "sends the same quantities as strings. Both are carried to `DECIMAL` through the same "
        "converter, so the two levels compare exactly.\n\n"
        "### Paging\n\n"
        "Rows stream a page at a time, so a `LIMIT` stops early. Gamma clamps the page size to "
        "100 silently rather than erroring on a larger request."
    ),
    example_queries=examples(
        (
            "The biggest open events by 24-hour volume",
            (
                "SELECT slug, title, volume_24hr, liquidity FROM polymarket.main.events("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 20"
            ),
        ),
        (
            "Which topics an event is filed under, one row per tag",
            (
                "SELECT e.title, (unnest(e.tags)).label AS topic FROM polymarket.main.events("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') e LIMIT 40"
            ),
        ),
    ),
)


@init_single_worker
class EventsFunction(TableFunctionGenerator[EventsArgs, KeysetScanState]):
    """Events, streamed one API page per tick.

    Paged as a scan for the same reason as :class:`MarketsFunction`: the
    endpoint is cursor-paged, so walking it inside one ``process()`` would defeat
    a ``LIMIT`` and could wedge the client.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        name = "events"
        description = "Browse the events that group Polymarket's markets by resolution"
        categories = ["market-data"]
        # See MarketsFunction.Meta: these three go together, and `filter_pushdown`
        # without `auto_apply_filters` silently discards every predicate.
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        tags = EVENTS_DOCS
        examples = [
            FunctionExample(
                sql=(
                    "SELECT slug, title, volume_24hr, liquidity FROM polymarket.main.events("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 20"
                ),
                description="The biggest open events by 24-hour volume",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[EventsArgs]) -> KeysetScanState:
        return KeysetScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventsArgs],
        state: KeysetScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of events, then remember where to resume."""
        filters = _event_filters(params.args)
        filters += _pushed(params, _EVENT_PUSHDOWN, {name for name, _ in filters})
        emit_keyset_page(
            params,
            state,
            out,
            path=api.EVENTS_PATH,
            key="events",
            filters=filters,
            shape=event_row,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class EventIdArgs:
    """A lone event-id input column."""

    event_id: Annotated[str, Arg(0, doc="Gamma id of the event to look up")]


class EventFunction(RowTransformFunction[EventIdArgs]):
    """One event by id — the point lookup to ``events()``'s browse."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        name = "event"
        description = "Look up one Polymarket event by its Gamma id"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=EVENT_SCHEMA,
            llm=(
                "A single event by its exact Gamma id, with the same columns `events()` returns. "
                "Use this when you already hold an `event_id` — every market row carries one — "
                "and want the event's aggregate volume, its resolution date or its topic tags. "
                "`events()` is the browse path; this is the lookup, and this is the one that "
                "works under a correlated LATERAL."
            ),
            md=(
                "One row for one event.\n\n"
                "### When to use this instead of `events()`\n\n"
                "`events()` streams a catalog and cannot be the inner side of a `LATERAL`. This "
                "fetches one event and can, so it is the way to go from a set of markets back up "
                "to the events they belong to.\n\n"
                "### What it does not return\n\n"
                "Gamma's response also carries the event's markets — usually most of the "
                "payload. They are not returned here: `event_markets()` is the way to ask for "
                "those, and it reshapes them to the same columns `markets()` returns.\n\n"
                "### One request per row\n\n"
                "Unlike `market()`, this endpoint has no batch form, so a LATERAL costs one "
                "request per input row, so drive it from a bounded set rather than a whole catalog."
            ),
            example_queries=examples(
                (
                    "The event behind the busiest open market",
                    (
                        "SELECT e.title, e.volume_24hr, e.end_date FROM ("
                        "SELECT event_id FROM polymarket.main.markets("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                        "LATERAL polymarket.main.event(m.event_id) e"
                    ),
                ),
                (
                    "Open interest of the events behind the busiest markets",
                    (
                        "SELECT DISTINCT e.title, e.open_interest FROM ("
                        "SELECT event_id FROM polymarket.main.markets("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 5) m, "
                        "LATERAL polymarket.main.event(m.event_id) e "
                        "ORDER BY e.open_interest DESC NULLS LAST"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT e.title, e.volume_24hr, e.end_date FROM ("
                    "SELECT event_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.event(m.event_id) e"
                ),
                description="The event behind the busiest open market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventIdArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventIdArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        ids = batch.column("event_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, event_id in enumerate(ids):
                if event_id is None:
                    continue
                try:
                    raw = api.event(str(event_id), client=client, hint=hint)
                except NotFound:
                    # One missing event must not fail the other 299 rows in the
                    # batch; no rows is the same answer an empty event gives.
                    continue
                if raw:
                    rows.append(event_row(raw))
                    parents.append(index)
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint),
        )


class EventMarketsFunction(RowTransformFunction[EventIdArgs]):
    """Every market under one event, in the same columns ``markets()`` returns.

    This exists because ``/markets/keyset`` has **no** event filter. The obvious
    design — ``markets(event_slug => ...)`` — cannot be built: Gamma ignores an
    unrecognised query parameter and answers with the unfiltered first page, so
    such an argument would silently return the wrong markets rather than fail.
    The event's own payload carries its markets, so that is where they come
    from.

    Markets read out of an event payload have no nested ``events`` key, so the
    event's id and slug are stamped onto each row before reshaping — the same
    same shape of fix as any other denormalised parent: the child does not
    repeat it, so the caller supplies it.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARKET_SCHEMA

    class Meta:
        name = "event_markets"
        description = "Every tradeable market grouped under one Polymarket event"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="markets",
            result_schema=MARKET_SCHEMA,
            llm=(
                "The markets under one event, by event id — an election's candidates, a game's "
                "outcomes. Returns exactly the columns `markets()` does, so it is "
                "interchangeable downstream, and unlike `markets()` it composes under a "
                "correlated LATERAL. This is the ONLY way to get an event's markets: "
                "`markets()` has no event filter, because Gamma's market endpoint does not "
                "offer one."
            ),
            md=(
                "One row per market under one event, in `markets()`' columns.\n\n"
                "### Why this is a separate function\n\n"
                "`/markets/keyset` has no event parameter. Gamma ignores query parameters it "
                "does not recognise and answers with the unfiltered first page, so an "
                "`event_slug` argument on `markets()` would return the wrong rows with no error "
                "at all. The event's own payload carries its markets, so they are read from "
                "there instead.\n\n"
                "### Composing\n\n"
                "Drive it from `events()` to expand a set of events into their markets, then "
                "`unnest(outcomes)` to reach the tokens the order book keys on.\n\n"
                "### One request per row\n\n"
                "There is no batch form, so a LATERAL costs one request per event. Pass "
                "a bounded set of events rather than a whole catalog."
            ),
            example_queries=examples(
                (
                    "Every candidate market under the biggest open event",
                    (
                        "SELECT m.question, m.outcomes, m.volume_24hr FROM ("
                        "SELECT event_id FROM polymarket.main.events("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) e, "
                        "LATERAL polymarket.main.event_markets(e.event_id) m "
                        "ORDER BY m.volume_24hr DESC NULLS LAST"
                    ),
                ),
                (
                    "How many markets each of the busiest events carries",
                    (
                        "SELECT e.title, count(*) AS markets FROM ("
                        "SELECT event_id, title FROM polymarket.main.events("
                        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 5) e, "
                        "LATERAL polymarket.main.event_markets(e.event_id) m GROUP BY e.title "
                        "ORDER BY markets DESC"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT m.question, m.outcomes, m.volume_24hr FROM ("
                    "SELECT event_id FROM polymarket.main.events("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) e, "
                    "LATERAL polymarket.main.event_markets(e.event_id) m "
                    "ORDER BY m.volume_24hr DESC NULLS LAST"
                ),
                description="Every candidate market under the biggest open event",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventIdArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventIdArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        ids = batch.column("event_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, event_id in enumerate(ids):
                if event_id is None:
                    continue
                try:
                    raw = api.event(str(event_id), client=client, hint=hint)
                except NotFound:
                    continue
                for market in raw.get("markets") or []:
                    if not isinstance(market, dict):
                        continue
                    # The parent is not repeated inside its own children, so
                    # stamp it on: without this every row reports a NULL event.
                    market = {
                        **market,
                        "event_id": raw.get("id"),
                        "event_slug": raw.get("slug"),
                    }
                    rows.append(market_row(market))
                    parents.append(index)
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """``search(query)`` — full-text search over events."""

    query: Annotated[str, Arg(0, doc="Words to search for in event titles and descriptions")]
    events_status: Annotated[
        str,
        Arg(
            "events_status",
            doc="Restrict to events in this state",
            default="",
            choices=["", "active", "resolved"],
        ),
    ] = ""


@init_single_worker
class SearchFunction(TableFunctionGenerator[SearchArgs, SearchScanState]):
    """Full-text search over events — the only text search Polymarket offers.

    Without this, finding "the Bitcoin markets" means scanning the whole catalog
    and filtering in SQL: tens of thousands of rows fetched over the network to
    answer a question the site answers instantly. The predicate cannot be pushed
    down, because Gamma's market and event filters match ids and slugs, never
    free text.

    Results are events, in the same columns :class:`EventsFunction` returns, so
    they chain into ``event_markets()`` for the tradeable questions beneath.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        name = "search"
        description = "Find events by searching their text, the way the site's search box does"
        categories = ["market-data"]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=EVENT_SCHEMA,
            llm=(
                "Full-text search over events — the way in when you know the subject but not the "
                "tag or the slug. Reach for this FIRST for any question phrased in words "
                "('markets about the Fed', 'anything on Bitcoin'), because no other object here "
                "searches text: filtering `markets()` on a title predicate downloads the whole "
                "catalog to do it. Returns the same columns `events()` does, so chain it into "
                "`event_markets()` to reach the tradeable questions."
            ),
            md=(
                "Gamma's search endpoint, returning events with the same columns `events()` "
                "gives.\n\n"
                "### Why this exists\n\n"
                "It is the only text search in the catalog. Gamma's other filters match ids, "
                "slugs and tags — never free text — so a `WHERE title ILIKE '%bitcoin%'` on "
                "`events()` cannot be pushed down and pages the entire catalog over the network "
                "to filter locally. This asks the search index instead.\n\n"
                "### Where to go next\n\n"
                "Take an `event_id` and pass it to `event_markets()` for the markets beneath, "
                "then unnest their outcomes to reach prices and books.\n\n"
                "### Paging\n\n"
                "Results stream a page at a time and are ranked by relevance, so a `LIMIT` takes "
                "the best matches rather than an arbitrary slice."
            ),
            example_queries=examples(
                (
                    "Find the events matching a subject, best matches first",
                    "SELECT event_id, title, volume_24hr FROM polymarket.main.search('bitcoin') LIMIT 10",
                ),
                (
                    "Search for a subject, then the tradeable markets under the best match",
                    (
                        "SELECT m.question, m.outcomes FROM (SELECT event_id FROM "
                        "polymarket.main.search('federal reserve') LIMIT 1) e, "
                        "LATERAL polymarket.main.event_markets(e.event_id) m"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=("SELECT event_id, title, volume_24hr FROM polymarket.main.search('bitcoin') LIMIT 10"),
                description="Find the events matching a subject, best matches first",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SearchArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchArgs]) -> SearchScanState:
        return SearchScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[SearchArgs],
        state: SearchScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of search results, then remember where to resume."""

        def fetch(*, page: int, hint: CacheHint) -> tuple[list[dict[str, Any]], bool]:
            return api.search_page_with_more(
                params.args.query,
                page=page,
                events_status=params.args.events_status or None,
                hint=hint,
            )

        emit_search_page(
            params,
            state,
            out,
            fetch=fetch,
            shape=event_row,
        )


MARKET_FUNCTIONS: list[type] = [
    MarketsFunction,
    MarketFunction,
    EventsFunction,
    EventFunction,
    EventMarketsFunction,
    SearchFunction,
]
