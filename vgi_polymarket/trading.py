"""Live trading data: order books, quotes, price history, trades, holders.

All five are **blended** (:class:`~vgi.table_in_out_function.RowTransformFunction`)
table functions, because each one's fetch is bounded by construction — one
token's book, one token's price curve, one market's tape. That is what lets a
single registration serve both a literal call and a correlated LATERAL::

    SELECT * FROM polymarket.main.orderbook('3233822019...');

    SELECT o.outcome, b.side, b.price, b.size
    FROM polymarket.main.markets(tag_id => '21') m,
         LATERAL (SELECT unnest(m.outcomes, recursive := true)) o,
         LATERAL polymarket.main.orderbook(o.token_id) b;

**Which identifier goes where** is the thing to get right, and it splits exactly
along the host boundary:

* The CLOB (:func:`orderbook`, :func:`quote`, :func:`price_history`) keys on a
  **token id** — one outcome.
* The Data API (:func:`trades`, :func:`holders`) keys on a **condition id** —
  the whole market.

Polymarket calls the query parameter ``market`` in both places. Passing a
condition id to ``price_history`` is answered with HTTP 200 and an empty series;
passing a token id to ``trades`` is answered with HTTP 200 and an empty list.
Neither is an error, so the argument names here say which is which, and the
column comments repeat it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import RowTransformFunction
from vgi_rpc.rpc import OutputCollector

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.markets import emit_fanout, opt_in_cache_control, origin_cache_control
from vgi_polymarket.meta import docs, examples
from vgi_polymarket.polymarket_api import STALE_IF_ERROR, CacheHint, NotFound
from vgi_polymarket.schemas import (
    ACTIVITY_SCHEMA,
    HOLDER_SCHEMA,
    ORDERBOOK_SCHEMA,
    POSITION_SCHEMA,
    PRICE_HISTORY_SCHEMA,
    QUOTE_SCHEMA,
    TRADE_SCHEMA,
    activity_row,
    flatten_book,
    flatten_holders,
    flatten_price_history,
    position_row,
    quote_row,
    trade_row,
)

#: A price-history window that ends in the past can never change again, so it is
#: cached for a day rather than for the origin's 90-second directive.
_CLOSED_WINDOW_TTL = 86_400


def _history_cache_control(*, end_ts: int, fidelity: int, now: int) -> CacheControl | None:
    """Cacheability for a price-history window, decided by whether it is closed.

    A sampled point is immutable once its bar has elapsed, so a window ending
    before the current bar began can never change and is safely cached for a
    long time. A window running up to now contains a point that is still moving,
    and gets the origin's own 90 seconds instead — the CLOB does declare a TTL
    here, unlike on the book.
    """
    bar_seconds = max(fidelity, 1) * 60
    if end_ts > now - bar_seconds:
        return None
    return CacheControl(ttl=_CLOSED_WINDOW_TTL, stale_if_error=STALE_IF_ERROR, per_value=True)


@dataclass(slots=True, frozen=True, kw_only=True)
class TokenArgs:
    """A lone outcome-token input column, plus an opt-in cache TTL."""

    token_id: Annotated[str, Arg(0, doc="Outcome token to read, from a market's outcomes list")]
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this result for; 0 turns caching off", default=0, ge=0),
    ] = 0


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderbookArgs:
    """``orderbook(token_id)`` with an optional depth cap."""

    token_id: Annotated[str, Arg(0, doc="Outcome token to read, from a market's outcomes list")]
    #: 0 means "no cap". `POST /books` takes no depth parameter, so the whole
    #: book arrives and the best levels are kept here instead.
    depth: Annotated[
        int, Arg("depth", doc="Keep only this many levels per side; 0 keeps all of them", default=0, ge=0)
    ] = 0
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this book for; 0 turns caching off", default=0, ge=0),
    ] = 0


def _cap_depth(levels: list[dict[str, Any]], depth: int) -> list[dict[str, Any]]:
    """Keep only the best ``depth`` levels per side.

    "Best" is the top of each side's book, which is opposite by side: the
    highest bid a buyer will pay, and the lowest ask a seller will take. Sorting
    both the same way would silently return the *worst* asks.
    """
    from vgi_polymarket.schemas import to_decimal

    out: list[dict[str, Any]] = []
    for side, reverse in (("bid", True), ("ask", False)):
        ranked = sorted(
            (level for level in levels if level["side"] == side),
            key=lambda level: to_decimal(level["price"]) or 0,
            reverse=reverse,
        )
        out.extend(ranked[:depth])
    return out


class OrderbookFunction(RowTransformFunction[OrderbookArgs]):
    """Resting-order book for one outcome token, one row per (side, price level)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ORDERBOOK_SCHEMA

    class Meta:
        name = "orderbook"
        description = "Resting order-book depth for one outcome, one row per side and price level"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="market-depth",
            result_schema=ORDERBOOK_SCHEMA,
            llm=(
                "Resting order-book depth for ONE OUTCOME TOKEN, flattened to one row per side "
                "and price level so it aggregates and joins like an ordinary table. Takes a "
                "`token_id` from a market's `outcomes` list — NOT a condition id, which is a "
                "404 here. Reach for this to see liquidity behind the best quote: spread, depth "
                "at a price, or the shape of one side. For just the top of book use `quote()`, "
                "which is one row instead of many."
            ),
            md=(
                "The CLOB returns a book as nested `bids` and `asks`; this flattens it to one "
                "row per (side, price level).\n\n"
                "### It takes a token id\n\n"
                "Each outcome of a market trades as its own token with its own book. Get the "
                "`token_id` from `unnest(markets.outcomes)`. A condition id here is an HTTP 404 "
                "— which at least fails loudly, unlike `price_history()`, where the same "
                "mistake returns an empty series.\n\n"
                "### Reading the two sides\n\n"
                "`bid` rows are buyers, `ask` rows are sellers, and the outcomes of a binary "
                "market mirror each other: a Yes bid at 0.40 is economically a No ask at 0.60. "
                "`depth` would keep the best N levels per side — highest bids, lowest asks — "
                "but see the note below on the optional arguments.\n\n"
                "### Cost under LATERAL\n\n"
                "Books are fetched in batches of up to 200 tokens per request, which is what "
                "makes a join across a whole topic affordable: measured at 12.85s for 100 books "
                "individually against 0.16s batched. No query change is needed; it happens "
                "automatically.\n\n"
                "### An empty result\n\n"
                "A token with no resting orders contributes no rows, and so does a token the "
                "batch endpoint omits — both mean 'nothing is resting here'."
            ),
            example_queries=examples(
                (
                    "Full depth on both sides of one outcome's book",
                    "SELECT b.side, b.price, b.size FROM ("
                    "SELECT o.token_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                    "LATERAL polymarket.main.orderbook(t.token_id) b "
                    "ORDER BY b.side, b.price DESC",
                ),
                (
                    "Total resting size per side across a whole topic, in one query",
                    "SELECT b.side, sum(b.size) AS resting FROM polymarket.main.markets("
                    "tag_id => '21', closed => 'false') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.orderbook(o.token_id) b "
                    "GROUP BY b.side",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT b.side, sum(b.size) AS resting FROM polymarket.main.markets("
                    "tag_id => '21', closed => 'false') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.orderbook(o.token_id) b "
                    "GROUP BY b.side"
                ),
                description="Total resting size per side across a whole topic, in one query",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[OrderbookArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[OrderbookArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tokens = batch.column("token_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        with api.open_client() as client:
            # One request per 200 tokens rather than one per token. The whole
            # input batch is fetched up front and then fanned back out in input
            # order, so provenance is unchanged and a repeated token still
            # produces its rows once per input row.
            books = api.books([str(t) for t in tokens if t is not None], client=client)
            for index, token_id in enumerate(tokens):
                if token_id is None:
                    continue
                levels = flatten_book(str(token_id), books.get(str(token_id)) or {})
                if depth := params.args.depth:
                    levels = _cap_depth(levels, depth)
                rows.extend(levels)
                parents.extend([index] * len(levels))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


class QuoteFunction(RowTransformFunction[TokenArgs]):
    """Top of book for one outcome token: best bid, best ask, midpoint, spread."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = QUOTE_SCHEMA

    class Meta:
        name = "quote"
        description = "Current best bid, best ask, midpoint and spread for one outcome"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="market-depth",
            result_schema=QUOTE_SCHEMA,
            llm=(
                "The current top of book for ONE OUTCOME TOKEN, in a single row: best bid, best "
                "ask, midpoint and spread. Takes a `token_id` from a market's `outcomes` list. "
                "This is the cheapest way to ask 'what is this outcome trading at right now' — "
                "prefer it over `orderbook()` when you do not need depth, and over the price "
                "columns on `markets()` when you need the number to be live rather than as of "
                "Gamma's last refresh."
            ),
            md=(
                "One row per token, assembled from the CLOB's batched quote endpoints.\n\n"
                "### Midpoint is the number you usually want — if the spread is tight\n\n"
                "`midpoint` sits halfway between best bid and best ask and is what Polymarket "
                "displays as the price. Because a share pays $1 if its outcome happens, it reads "
                "as an implied probability: 0.62 is 62%.\n\n"
                "**Check `spread` before believing it.** A midpoint is only a forecast to the "
                "extent that someone is willing to trade near it. On a thin market the two sides "
                "can be almost unrelated — a real book quoted 0.020 bid against 0.990 ask, whose "
                "midpoint of 0.505 is not a coin flip but the average of two numbers nobody "
                "would trade at. That is why this function returns `spread` alongside "
                "`midpoint` rather than only the price: a wide spread means the number above it "
                "is noise, and `orderbook()` will show how little is resting behind it.\n\n"
                "### Naming\n\n"
                "The CLOB reports quotes as `BUY` and `SELL`, and the mapping is the opposite "
                "of the obvious one: `BUY` is the best **bid** and `SELL` is the best **ask**, "
                "verified against the raw book. They are renamed here to the side of the book "
                "they actually sit on, so `best_ask >= best_bid` always holds and `spread` is "
                "never negative.\n\n"
                "### Cost under LATERAL\n\n"
                "Two batched requests per 200 tokens regardless of how many rows drive it, so a "
                "quote across a whole topic is cheap.\n\n"
                "### Freshness\n\n"
                "The CLOB declares no cacheability on quotes, so nothing is cached by default, "
                "and `cache_ttl` would enable per-value memoization — but see the note on the "
                "optional arguments below.\n\n"
                "### The optional arguments do not bind yet\n\n"
                "`cache_ttl` is declared, and its default is what every call receives, but "
                "DuckDB's binder cannot currently bind a named argument on a table-in-out "
                "function — so passing one is a binder error today rather than a tuning knob. "
                "The defaults are chosen to be the right answer untuned. Named arguments on "
                "`markets()` and `events()` are unaffected: those are ordinary scans."
            ),
            example_queries=examples(
                (
                    "Live implied probability of every outcome in a topic",
                    "SELECT m.question, o.outcome, q.midpoint, q.spread "
                    "FROM polymarket.main.markets(tag_id => '21', closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.quote(o.token_id) q LIMIT 40",
                ),
                (
                    "The widest spreads among busy markets — where liquidity is thin",
                    "SELECT m.question, o.outcome, q.spread FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.quote(o.token_id) q "
                    "WHERE q.spread IS NOT NULL ORDER BY q.spread DESC LIMIT 20",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT m.question, o.outcome, q.midpoint, q.spread "
                    "FROM polymarket.main.markets(tag_id => '21', closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.quote(o.token_id) q LIMIT 40"
                ),
                description="Live implied probability of every outcome in a topic",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TokenArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[TokenArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tokens = batch.column("token_id").to_pylist()
        wanted = [str(t) for t in tokens if t is not None]
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        with api.open_client() as client:
            # Three batched calls for the whole input batch. The book is fetched
            # too, for last_trade_price and tick_size, which the quote endpoints
            # do not carry.
            books = api.books(wanted, client=client)
            mids = api.midpoints(wanted, client=client)
            sides = api.prices(wanted, client=client)
            for index, token_id in enumerate(tokens):
                if token_id is None:
                    continue
                token = str(token_id)
                rows.append(
                    quote_row(
                        token,
                        book=books.get(token) or {},
                        midpoint=mids.get(token),
                        sides=sides.get(token),
                    )
                )
                parents.append(index)
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class PriceHistoryArgs:
    """``price_history(token_id)`` over a lookback period or an explicit window."""

    token_id: Annotated[str, Arg(0, doc="Outcome token to chart, from a market's outcomes list")]
    period: Annotated[
        str,
        Arg(
            "period",
            doc="How far back to look, ending now; ignored when start_ts is given",
            default="1w",
            choices=["1h", "6h", "1d", "1w", "1m", "max"],
        ),
    ] = "1w"
    fidelity: Annotated[
        int,
        Arg("fidelity", doc="Minutes between sampled points", default=60, ge=1, le=10080),
    ] = 60
    #: 0 means "not supplied" — see the sentinel note on MarketsArgs.
    start_ts: Annotated[
        int,
        Arg("start_ts", doc="Window start as an epoch second; 0 uses the period instead", default=0, ge=0),
    ] = 0
    end_ts: Annotated[
        int, Arg("end_ts", doc="Window end as an epoch second; 0 means now", default=0, ge=0)
    ] = 0


class PriceHistoryFunction(RowTransformFunction[PriceHistoryArgs]):
    """The price curve for one outcome token over time.

    Note this is a price *curve*, not OHLC bars: Polymarket publishes a single
    sampled price per point, so there is no open/high/low/close to flatten.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = PRICE_HISTORY_SCHEMA

    class Meta:
        name = "price_history"
        description = "How one outcome's price moved over time, as a sampled series"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="history",
            result_schema=PRICE_HISTORY_SCHEMA,
            llm=(
                "The historical price series for ONE OUTCOME TOKEN — the time-series view. Reach "
                "for it to chart or backtest how an outcome's implied probability moved, rather "
                "than where it stands now. Takes a `token_id` from a market's `outcomes` list. "
                "Passing a condition id instead returns an EMPTY SERIES with no error, so an "
                "empty result usually means the wrong identifier rather than an untraded market."
            ),
            md=(
                "A sampled price curve for one outcome token.\n\n"
                "### It takes a token id, and the failure is silent\n\n"
                "The underlying parameter is called `market`, but it wants a **token id**. A "
                "condition id is answered with HTTP 200 and an empty history — indistinguishable "
                "from an outcome that never traded. Get the token from "
                "`unnest(markets.outcomes)`.\n\n"
                "### Not OHLC\n\n"
                "Polymarket publishes one price per sampled instant, so each row is a single "
                "`price` rather than a bar. `fidelity` is the gap between samples in minutes; "
                "`period` is how far back to look.\n\n"
                "### Window\n\n"
                "`period` and an explicit `start_ts`/`end_ts` window are alternatives — "
                "passing `start_ts` takes precedence. A window that ends in the past contains "
                "only settled points and is cached for a day; one running up to now carries a "
                "still-moving last point and takes the origin's 90-second TTL.\n\n"
                "### Prices are probabilities\n\n"
                "Each `price` is USDC per share between 0 and 1, so the curve reads as the "
                "market's implied probability over time. These are traded prices rather than a "
                "normalised display figure, so a flat stretch means nothing traded — not that "
                "the market held an opinion steady. Pair it with `quote()` if you need to know "
                "whether there is a market behind the curve right now."
            ),
            example_queries=examples(
                (
                    "A week of hourly implied probability for the busiest open market",
                    "SELECT h.ts, h.price FROM ("
                    "SELECT o.token_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                    "LATERAL polymarket.main.price_history(t.token_id) h ORDER BY h.ts",
                ),
                (
                    "How far each outcome moved over the last day",
                    "SELECT o.outcome, max(h.price) - min(h.price) AS swing "
                    "FROM (SELECT outcomes FROM polymarket.main.markets(closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') LIMIT 3) m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.price_history(o.token_id) h "
                    "GROUP BY o.outcome ORDER BY swing DESC",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT h.ts, h.price FROM ("
                    "SELECT o.token_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                    "LATERAL polymarket.main.price_history(t.token_id) h ORDER BY h.ts"
                ),
                description="A week of hourly implied probability for the busiest open market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PriceHistoryArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[PriceHistoryArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        now = int(time.time())
        tokens = batch.column("token_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        start_ts = params.args.start_ts or None
        end_ts = params.args.end_ts or (now if start_ts else 0)
        with api.open_client() as client:
            for index, token_id in enumerate(tokens):
                if token_id is None:
                    continue
                try:
                    history = api.price_history(
                        str(token_id),
                        # The wire parameter is still called `interval`; the SQL
                        # argument is `period` because `interval` is a DuckDB type
                        # keyword and `interval => '1w'` is a parser error.
                        interval=params.args.period,
                        start_ts=start_ts,
                        end_ts=end_ts or None,
                        fidelity=params.args.fidelity,
                        client=client,
                        hint=hint,
                    )
                except NotFound:
                    continue
                flat = flatten_price_history(str(token_id), history)
                rows.extend(flat)
                parents.extend([index] * len(flat))
        # A closed window beats the origin's TTL; an open one falls back to it.
        cache_control = (
            _history_cache_control(end_ts=end_ts, fidelity=params.args.fidelity, now=now)
            if start_ts
            else None
        ) or origin_cache_control(hint)
        emit_fanout(out, params.output_schema, rows, parents, cache_control)


@dataclass(slots=True, frozen=True, kw_only=True)
class TradesArgs:
    """``trades(condition_id)`` over the market's tape."""

    condition_id: Annotated[str, Arg(0, doc="On-chain market id whose tape to read")]
    side: Annotated[
        str,
        Arg(
            "side",
            doc="Keep only trades where the taker went this way",
            default="",
            choices=["", "BUY", "SELL"],
        ),
    ] = ""
    max_rows: Annotated[
        int, Arg("max_rows", doc="Stop after this many trades per market; 0 fetches all", default=1000, ge=0)
    ] = 1000
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this tape for; 0 turns caching off", default=0, ge=0),
    ] = 0


class TradesFunction(RowTransformFunction[TradesArgs]):
    """The public trade tape for one market, newest first.

    Polymarket settles on a public chain, so the tape carries who traded as well
    as what — the counterparty's proxy wallet and profile name come back on
    every print, which is not something a conventional exchange's tape can offer.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = TRADE_SCHEMA

    class Meta:
        name = "trades"
        description = "Executed trades on one Polymarket market, with the trader behind each"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="history",
            result_schema=TRADE_SCHEMA,
            llm=(
                "Individual executed trades on ONE MARKET — the public tape, newest first. "
                "Takes a `condition_id`, NOT a token id: a token id here returns an empty list "
                "with no error. Reach for this when you need actual prints rather than quotes: "
                "trade-by-trade flow, taker direction, a volume-weighted price, or WHO traded — "
                "each row carries the counterparty's wallet and profile name, because "
                "Polymarket settles on a public chain."
            ),
            md=(
                "One row per execution, with the trader attached.\n\n"
                "### It takes a condition id\n\n"
                "This is the Data API, which keys on the market rather than the outcome. A "
                "token id returns `[]` with HTTP 200 — no error, just nothing. Each row's "
                "`token_id` column tells you which outcome changed hands.\n\n"
                "### Public counterparties\n\n"
                "`proxy_wallet`, `trader_name` and `pseudonym` identify the taker. This is "
                "ordinary public chain data, and it is what makes flow analysis possible here "
                "in a way it is not on a conventional exchange.\n\n"
                "### Volume\n\n"
                "An active market can have a very long tape, so `max_rows` defaults to 1000, which "
                "is what every call gets — see the note below on the optional arguments. "
                "Pass 0 for the whole history, and expect it to take a while.\n\n"
                "### Prices arrive as floats\n\n"
                "The Data API sends price and size as JSON numbers rather than strings. They are "
                "recovered to exact `DECIMAL` through the shortest round-tripping "
                "representation, so a 0.955 print reads as 0.955 and not as 0.9549999999."
            ),
            example_queries=examples(
                (
                    "The most recent prints on the busiest open market",
                    "SELECT t.ts, t.side, t.outcome, t.price, t.size, t.trader_name FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.trades(m.condition_id) t "
                    "ORDER BY t.ts DESC",
                ),
                (
                    "Volume-weighted average price by outcome, from the tape",
                    "SELECT t.outcome, sum(t.price * t.size) / sum(t.size) AS vwap FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.trades(m.condition_id) t "
                    "GROUP BY t.outcome",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT t.ts, t.side, t.outcome, t.price, t.size, t.trader_name FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.trades(m.condition_id) t "
                    "ORDER BY t.ts DESC"
                ),
                description="The most recent prints on the busiest open market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TradesArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[TradesArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        ids = batch.column("condition_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, condition_id in enumerate(ids):
                if condition_id is None:
                    continue
                found = api.trades(
                    str(condition_id),
                    side=params.args.side or None,
                    limit=params.args.max_rows or None,
                    client=client,
                    hint=hint,
                )
                rows.extend(trade_row(raw) for raw in found)
                parents.extend([index] * len(found))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint) or opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class HoldersArgs:
    """``holders(condition_id)`` — the largest positions in one market."""

    condition_id: Annotated[str, Arg(0, doc="On-chain market id whose holders to list")]
    max_rows: Annotated[
        int, Arg("max_rows", doc="How many holders to return per outcome", default=100, ge=1, le=1000)
    ] = 100
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this list for; 0 turns caching off", default=0, ge=0),
    ] = 0


class HoldersFunction(RowTransformFunction[HoldersArgs]):
    """The largest position holders in one market, one row per holder per outcome."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = HOLDER_SCHEMA

    class Meta:
        name = "holders"
        description = "Who holds the largest positions in one Polymarket market"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="positions",
            result_schema=HOLDER_SCHEMA,
            llm=(
                "The largest position holders in ONE MARKET, one row per holder per outcome. "
                "Takes a `condition_id`, like `trades()` and unlike the order book. This has no "
                "equivalent on a conventional exchange: Polymarket settles on a public chain, so "
                "who is holding what is public. Reach for it to ask who is on each side of a "
                "question, or how concentrated a position is."
            ),
            md=(
                "One row per holder per outcome, ranked by size.\n\n"
                "### Why this exists at all\n\n"
                "Positions are on-chain, so the holder list is public data rather than something "
                "an exchange chooses to publish. There is no equivalent on Kalshi or a "
                "conventional venue.\n\n"
                "### Shape\n\n"
                "The API groups holders under each outcome token; that grouping is flattened "
                "here and the `token_id` stamped onto every row, so a holder row can say which "
                "side of the question it is a position in. `amount` is shares of that outcome, "
                "and each share pays $1 if the outcome happens.\n\n"
                "### Concentration\n\n"
                "Summing `amount` by `token_id` and comparing the top holder to the rest is the "
                "usual way to ask whether a price is one whale's opinion or a crowd's."
            ),
            example_queries=examples(
                (
                    "The biggest positions in the busiest open market",
                    "SELECT h.outcome_index, h.holder_name, h.amount FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC",
                ),
                (
                    "How concentrated each side of a market is",
                    "SELECT h.token_id, count(*) AS holders, sum(h.amount) AS shares, "
                    "max(h.amount) / sum(h.amount) AS top_holder_share FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h GROUP BY h.token_id",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT h.outcome_index, h.holder_name, h.amount FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC"
                ),
                description="The biggest positions in the busiest open market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[HoldersArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[HoldersArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        ids = batch.column("condition_id").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, condition_id in enumerate(ids):
                if condition_id is None:
                    continue
                found = flatten_holders(
                    str(condition_id),
                    api.holders(
                        str(condition_id),
                        limit=params.args.max_rows,
                        client=client,
                        hint=hint,
                    ),
                )
                rows.extend(found)
                parents.extend([index] * len(found))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint) or opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class PositionsArgs:
    """``positions(proxy_wallet)`` — every open position one wallet holds."""

    proxy_wallet: Annotated[str, Arg(0, doc="Wallet address whose open positions to list")]
    max_rows: Annotated[
        int,
        Arg("max_rows", doc="Stop after this many positions per wallet; 0 fetches all", default=500, ge=0),
    ] = 500
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this list for; 0 turns caching off", default=0, ge=0),
    ] = 0


class PositionsFunction(RowTransformFunction[PositionsArgs]):
    """Every open position one wallet holds, with its unrealised P&L.

    The reverse of :class:`HoldersFunction`: that goes from a market to its
    wallets, this goes from a wallet to its markets. Together they close the
    loop that only a public-chain venue can offer — find the whale in a market,
    then see everything else the whale is betting on.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = POSITION_SCHEMA

    class Meta:
        name = "positions"
        description = "Every open position one wallet holds, with its profit and loss"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="positions",
            result_schema=POSITION_SCHEMA,
            llm=(
                "Every open position one wallet holds, with entry price, current value and "
                "unrealised P&L. Takes a wallet address — a THIRD key, distinct from both market "
                "identifiers — which you get from `holders()` or from the trader columns on "
                "`trades()`. This is the reverse of `holders()`: that answers 'who is in this "
                "market', this answers 'what else is this person in'. Reach for it to follow a "
                "large or successful trader across the whole site."
            ),
            md=(
                "One row per open position, keyed on the holder's wallet.\n\n"
                "### The third key\n\n"
                "This is the only function here keyed on a wallet address rather than on a "
                "market or an outcome. Get one from `holders()` (who is in this market) or from "
                "the `proxy_wallet` column of `trades()` (who just traded).\n\n"
                "### Closing the loop\n\n"
                "`holders()` goes market to wallet; this goes wallet to market. Chaining them is "
                "how you get from 'this market moved' to 'here is who moved it and what else "
                "they hold' — which is possible only because Polymarket settles on a public "
                "chain.\n\n"
                "### Reading the P&L\n\n"
                "`cash_pnl` is unrealised profit in USDC and `percent_pnl` is the same as a "
                "percentage — a percentage, not a fraction, so -44.5 means down 44.5%. "
                "`avg_price` against `cur_price` gives the direction directly, and both are "
                "probabilities between 0 and 1. `redeemable` marks a resolved market whose "
                "position can be cashed out.\n\n"
                "### Freshness\n\n"
                "Polymarket declares `max-age=5` here, the shortest TTL it publishes anywhere, "
                "because a position's value moves with the market price."
            ),
            example_queries=examples(
                (
                    "What else the biggest holder in a busy market is betting on",
                    "SELECT p.title, p.outcome, p.size, p.cash_pnl FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet) p "
                    "ORDER BY p.current_value DESC",
                ),
                (
                    "How that holder's book is doing overall",
                    "SELECT count(*) AS positions, sum(p.current_value) AS value, "
                    "sum(p.cash_pnl) AS unrealised FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet) p",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT p.title, p.outcome, p.size, p.cash_pnl FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet) p "
                    "ORDER BY p.current_value DESC"
                ),
                description="What else the biggest holder in a busy market is betting on",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PositionsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[PositionsArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        wallets = batch.column("proxy_wallet").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, wallet in enumerate(wallets):
                if wallet is None:
                    continue
                found = api.positions(
                    str(wallet),
                    limit=params.args.max_rows or None,
                    client=client,
                    hint=hint,
                )
                rows.extend(position_row(raw) for raw in found)
                parents.extend([index] * len(found))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint) or opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class ActivityArgs:
    """``activity(proxy_wallet)`` — one wallet's history."""

    proxy_wallet: Annotated[str, Arg(0, doc="Wallet address whose history to read")]
    activity_type: Annotated[
        str,
        Arg(
            "activity_type",
            doc="Keep only entries of this kind",
            default="",
            # Every value observed across ~19,600 entries from 60 wallets
            # sampled over three tags — not guessed, and not a small sample:
            # an earlier 12-wallet pass missed REWARD, CONVERSION and
            # REFERRAL_REWARD entirely, which is why the live suite asserts
            # this list against what the API actually emits.
            choices=[
                "",
                "TRADE",
                "REDEEM",
                "YIELD",
                "MAKER_REBATE",
                "MERGE",
                "TAKER_REBATE",
                "REWARD",
                "SPLIT",
                "CONVERSION",
                "REFERRAL_REWARD",
            ],
        ),
    ] = ""
    max_rows: Annotated[
        int, Arg("max_rows", doc="Stop after this many entries per wallet; 0 fetches all", default=500, ge=0)
    ] = 500
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to reuse this feed for; 0 turns caching off", default=0, ge=0),
    ] = 0


class ActivityFunction(RowTransformFunction[ActivityArgs]):
    """One wallet's history: trades, redemptions, yield and rebates.

    The past to :class:`PositionsFunction`'s present. Together with
    :class:`HoldersFunction` they make the chain fully traversable: a market
    names its holders, a holder names its positions, and a position's wallet
    names everything it has ever done.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = ACTIVITY_SCHEMA

    class Meta:
        name = "activity"
        description = "One wallet's history of trades, redemptions, yield and rebates"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="positions",
            result_schema=ACTIVITY_SCHEMA,
            llm=(
                "Everything one wallet has done, newest first — the history counterpart to "
                "`positions()`, which reports only what it holds now. Takes a wallet address, "
                "the same key `positions()` takes. Reach for it to reconstruct how a trader "
                "built a position, or when they got out. Note the feed is broader than a trade "
                "tape: on a long-lived wallet most entries are `YIELD` accruals, so filter on "
                "`activity_type = 'TRADE'` when you want executions."
            ),
            md=(
                "One row per event in a wallet's history.\n\n"
                "### Wider than a trade tape\n\n"
                "`activity_type` distinguishes `TRADE` from `REDEEM` (cashing out a resolved "
                "market), `YIELD` (interest accrued on collateral), `MAKER_REBATE` and "
                "`TAKER_REBATE`, and the `SPLIT`/`MERGE` operations that convert between a "
                "dollar and a full set of outcome shares. Only `TRADE` rows carry a price and a "
                "side; the rest leave those NULL rather than reporting a misleading zero. Across "
                "a sample of active wallets, trades were roughly 92% of all entries.\n\n"
                "### Completing the chain\n\n"
                "`holders()` goes market to wallet, `positions()` goes wallet to its current "
                "markets, and this goes wallet to everything it has ever done. All three are "
                "public because Polymarket settles on a public chain.\n\n"
                "### Volume\n\n"
                "An active wallet has a very long history, so `max_rows` defaults to 500."
            ),
            example_queries=examples(
                (
                    "How the biggest holder in a busy market built their position",
                    "SELECT a.ts, a.activity_type, a.side, a.price, a.size FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet) a ORDER BY a.ts DESC",
                ),
                (
                    "What kinds of activity a large wallet actually generates",
                    "SELECT a.activity_type, count(*) AS entries FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet) a GROUP BY a.activity_type "
                    "ORDER BY entries DESC",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT a.activity_type, count(*) AS entries FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet) a GROUP BY a.activity_type "
                    "ORDER BY entries DESC"
                ),
                description="What kinds of activity a large wallet actually generates",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActivityArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ActivityArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        wallets = batch.column("proxy_wallet").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, wallet in enumerate(wallets):
                if wallet is None:
                    continue
                found = api.activity(
                    str(wallet),
                    activity_type=params.args.activity_type or None,
                    limit=params.args.max_rows or None,
                    client=client,
                    hint=hint,
                )
                rows.extend(activity_row(raw) for raw in found)
                parents.extend([index] * len(found))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            origin_cache_control(hint) or opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


TRADING_FUNCTIONS: list[type] = [
    OrderbookFunction,
    QuoteFunction,
    PriceHistoryFunction,
    TradesFunction,
    HoldersFunction,
    PositionsFunction,
    ActivityFunction,
]
