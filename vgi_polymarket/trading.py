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
from vgi_polymarket.markets import emit_fanout, origin_cache_control
from vgi_polymarket.meta import docs, examples
from vgi_polymarket.polymarket_api import STALE_IF_ERROR, CacheHint, NotFound
from vgi_polymarket.schemas import (
    ACTIVITY_SCHEMA,
    FILL_SCHEMA,
    HOLDER_SCHEMA,
    ORDERBOOK_SCHEMA,
    POSITION_SCHEMA,
    PRICE_HISTORY_SCHEMA,
    QUOTE_SCHEMA,
    TRADE_SCHEMA,
    activity_row,
    fill_from_book,
    flatten_book,
    flatten_holders,
    flatten_price_history,
    position_row,
    quote_row,
    trade_row,
)


@dataclass(slots=True, frozen=True, kw_only=True)
class TokenArgs:
    """A lone outcome-token input column."""

    token_id: Annotated[str, Arg(0, doc="Outcome token to read, from a market's outcomes list")]


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderbookArgs:
    """A lone outcome-token input column.

    There is deliberately no ``depth`` argument. The batched endpoint returns
    the whole book regardless of what is asked for, so a cap could only ever be
    applied here — and SQL already expresses it exactly, without a knob:

        QUALIFY row_number() OVER (
            PARTITION BY side ORDER BY price DESC
        ) <= 5

    An argument that does no work the query language cannot is surface to
    maintain rather than capability to offer.
    """

    token_id: Annotated[str, Arg(0, doc="Outcome token to read, from a market's outcomes list")]


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
                "There is no `depth` argument: the batched endpoint returns the whole book "
                "either way, and SQL caps it exactly — `QUALIFY row_number() OVER (PARTITION BY "
                "side ORDER BY price DESC) <= 5`.\n\n"
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
                rows.extend(levels)
                parents.extend([index] * len(levels))
        emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            None,
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
                "The CLOB declares no cacheability on quotes, so nothing here is cached. That "
                "is the honest policy for a top-of-book read: the number is only worth having "
                "if it is current."
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
            None,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class PriceHistoryArgs:
    """``price_history(token_id, period, fidelity)`` — all three required.

    The window is the substance of this call, not a decoration on it, so it is
    positional and mandatory. That is also the only shape that works: DuckDB
    cannot bind a named argument on a table-in-out function, and the framework
    rejects a positional with a default, so an optional knob here would be one
    nobody could ever pass.

    There is no explicit ``start_ts``/``end_ts`` window for the same reason —
    two more required arguments to serve a rarer case is a bad trade, and
    ``period`` covers it.
    """

    token_id: Annotated[str, Arg(0, doc="Outcome token to chart, from a market's outcomes list")]
    period: Annotated[
        str,
        Arg(1, doc="How far back to look, ending now", choices=["1h", "6h", "1d", "1w", "1m", "max"]),
    ]
    fidelity: Annotated[int, Arg(2, doc="Minutes between sampled points", ge=1, le=10080)]


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
                    "LATERAL polymarket.main.price_history(t.token_id, '1w', 60) h ORDER BY h.ts",
                ),
                (
                    "How far each outcome moved over the last day",
                    "SELECT o.outcome, max(h.price) - min(h.price) AS swing "
                    "FROM (SELECT outcomes FROM polymarket.main.markets(closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') LIMIT 3) m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.price_history(o.token_id, '1w', 60) h "
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
                    "LATERAL polymarket.main.price_history(t.token_id, '1w', 60) h ORDER BY h.ts"
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
        # Positional arguments on a blended function are per-row INPUT COLUMNS,
        # read off `batch`. They are not surfaced on `params.args`, which still
        # holds the dataclass defaults — reading the window from there silently
        # fetched the default one for every row.
        tokens = batch.column("token_id").to_pylist()
        periods = batch.column("period").to_pylist()
        fidelities = batch.column("fidelity").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, (token_id, period, fidelity) in enumerate(
                zip(tokens, periods, fidelities, strict=True)
            ):
                if token_id is None:
                    continue
                try:
                    history = api.price_history(
                        str(token_id),
                        # The wire parameter is still called `interval`; the SQL
                        # argument is `period` because `interval` is a DuckDB
                        # type keyword and `interval => '1w'` is a parser error.
                        interval=str(period) if period else "1w",
                        fidelity=int(fidelity) if fidelity else 60,
                        client=client,
                        hint=hint,
                    )
                except NotFound:
                    continue
                flat = flatten_price_history(str(token_id), history)
                rows.extend(flat)
                parents.extend([index] * len(flat))
        # The window always ends now, so the last point is still forming; the
        # origin's own 90-second TTL is the right and only policy here.
        emit_fanout(out, params.output_schema, rows, parents, origin_cache_control(hint))


@dataclass(slots=True, frozen=True, kw_only=True)
class TradesArgs:
    """``trades(condition_id, max_rows, side)`` — all three required.

    ``max_rows`` is mandatory rather than defaulted because an active market's
    tape is effectively unbounded, and ``side`` because it changes what the API
    returns rather than merely what SQL keeps. Pass ``0`` for every row and
    ``''`` for both sides.
    """

    condition_id: Annotated[str, Arg(0, doc="On-chain market id whose tape to read")]
    max_rows: Annotated[int, Arg(1, doc="Stop after this many trades; 0 fetches the whole tape", ge=0)]
    side: Annotated[
        str,
        Arg(
            2, doc="Keep only trades the taker went this way on; empty for both", choices=["", "BUY", "SELL"]
        ),
    ]


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
                "An active market can have a very long tape, which is why `max_rows` is "
                "required rather than defaulted — the call should say how much it wants. Pass "
                "0 for the whole history, and expect it to take a while. "
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
                    "LATERAL polymarket.main.trades(m.condition_id, 100, '') t "
                    "ORDER BY t.ts DESC",
                ),
                (
                    "Volume-weighted average price by outcome, from the tape",
                    "SELECT t.outcome, sum(t.price * t.size) / sum(t.size) AS vwap FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.trades(m.condition_id, 100, '') t "
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
                    "LATERAL polymarket.main.trades(m.condition_id, 100, '') t "
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
        caps = batch.column("max_rows").to_pylist()
        sides = batch.column("side").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, (condition_id, cap, side) in enumerate(zip(ids, caps, sides, strict=True)):
                if condition_id is None:
                    continue
                found = api.trades(
                    str(condition_id),
                    side=str(side) if side else None,
                    limit=int(cap) if cap else None,
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
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class HoldersArgs:
    """``holders(condition_id, max_rows)`` — the largest positions in one market."""

    condition_id: Annotated[str, Arg(0, doc="On-chain market id whose holders to list")]
    max_rows: Annotated[int, Arg(1, doc="How many holders to return per outcome", ge=1, le=1000)]


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
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC",
                ),
                (
                    "How concentrated each side of a market is",
                    "SELECT h.token_id, count(*) AS holders, sum(h.amount) AS shares, "
                    "max(h.amount) / sum(h.amount) AS top_holder_share FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h GROUP BY h.token_id",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT h.outcome_index, h.holder_name, h.amount FROM ("
                    "SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
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
        caps = batch.column("max_rows").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, (condition_id, cap) in enumerate(zip(ids, caps, strict=True)):
                if condition_id is None:
                    continue
                found = flatten_holders(
                    str(condition_id),
                    api.holders(
                        str(condition_id),
                        limit=int(cap) if cap else None,
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
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class PositionsArgs:
    """``positions(proxy_wallet, max_rows)`` — every open position one wallet holds."""

    proxy_wallet: Annotated[str, Arg(0, doc="Wallet address whose open positions to list")]
    max_rows: Annotated[int, Arg(1, doc="Stop after this many positions; 0 fetches all of them", ge=0)]


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
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet, 100) p "
                    "ORDER BY p.current_value DESC",
                ),
                (
                    "How that holder's book is doing overall",
                    "SELECT count(*) AS positions, sum(p.current_value) AS value, "
                    "sum(p.cash_pnl) AS unrealised FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet, 100) p",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT p.title, p.outcome, p.size, p.cash_pnl FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.positions(w.proxy_wallet, 100) p "
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
        caps = batch.column("max_rows").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, (wallet, cap) in enumerate(zip(wallets, caps, strict=True)):
                if wallet is None:
                    continue
                found = api.positions(
                    str(wallet),
                    limit=int(cap) if cap else None,
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
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class ActivityArgs:
    """``activity(proxy_wallet, max_rows, activity_type)`` — one wallet's history.

    ``activity_type`` is positional rather than optional because it changes what
    the API returns, not merely what SQL keeps: most of a long-lived wallet's
    feed is yield accrual, so filtering after the fetch would spend the row
    budget on entries you did not want. Pass ``''`` for everything.
    """

    proxy_wallet: Annotated[str, Arg(0, doc="Wallet address whose history to read")]
    max_rows: Annotated[int, Arg(1, doc="Stop after this many entries; 0 fetches the whole history", ge=0)]
    activity_type: Annotated[
        str,
        Arg(
            2,
            doc="Keep only entries of this kind; empty for all of them",
            # Every value observed across ~19,600 entries from 60 wallets
            # sampled over three tags — not guessed, and not a small sample: an
            # earlier 12-wallet pass missed REWARD, CONVERSION and
            # REFERRAL_REWARD entirely, which is why the live suite asserts this
            # list against what the API actually emits.
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
    ]


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
                "An active wallet has a very long history, so `max_rows` is required; pass 0 for all of it."
            ),
            example_queries=examples(
                (
                    "How the biggest holder in a busy market built their position",
                    "SELECT a.ts, a.activity_type, a.side, a.price, a.size FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet, 500, '') a ORDER BY a.ts DESC",
                ),
                (
                    "What kinds of activity a large wallet actually generates",
                    "SELECT a.activity_type, count(*) AS entries FROM ("
                    "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                    "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet, 500, '') a GROUP BY a.activity_type "
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
                    "LATERAL polymarket.main.holders(m.condition_id, 20) h "
                    "ORDER BY h.amount DESC LIMIT 1) w, "
                    "LATERAL polymarket.main.activity(w.proxy_wallet, 500, '') a GROUP BY a.activity_type "
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
        caps = batch.column("max_rows").to_pylist()
        kinds = batch.column("activity_type").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        with api.open_client() as client:
            for index, (wallet, cap, kind) in enumerate(zip(wallets, caps, kinds, strict=True)):
                if wallet is None:
                    continue
                found = api.activity(
                    str(wallet),
                    activity_type=str(kind) if kind else None,
                    limit=int(cap) if cap else None,
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
            origin_cache_control(hint),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class FillArgs:
    """``fill_price(token_id, side, size)`` — price a request against the book."""

    token_id: Annotated[str, Arg(0, doc="Outcome token the order would trade")]
    side: Annotated[
        str,
        Arg(1, doc="Direction of the order: buy consumes asks, sell consumes bids", choices=["buy", "sell"]),
    ]
    size: Annotated[int, Arg(2, doc="Shares the order is for", ge=1)]


class FillPriceFunction(RowTransformFunction[FillArgs]):
    """What a given size would actually cost, having walked the book."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FILL_SCHEMA

    class Meta:
        name = "fill_price"
        description = "What a given order size would actually cost after walking the book"
        categories = ["market-data", "blended"]
        projection_pushdown = True
        tags = docs(
            category="market-depth",
            result_schema=FILL_SCHEMA,
            llm=(
                "The price a given order size would actually get, having consumed the resting "
                "book level by level. Takes a `token_id`, a side, and a share count. Reach for "
                "this instead of `quote()` whenever size matters: a market can quote a one-tick "
                "spread and hold fifteen shares behind it, so the midpoint is a real price at "
                "which almost nothing trades. `avg_price` is what the position costs per share, "
                "`slippage` is what the size cost you over the touch, and `fully_filled` says "
                "whether the book could cover the request at all."
            ),
            md=(
                "One row per request, computed from the same batched book `orderbook()` "
                "returns — so it costs no extra call when driven alongside it.\n\n"
                "### Why the touch is not enough\n\n"
                "Spread measures the distance between the best bid and the best ask. It says "
                "nothing about how much is standing there. A live market quoted a 0.001 spread "
                "on 15 shares; another quoted 0.97 on a book with one order a side. Neither "
                "number tells you what a position would cost, and this does.\n\n"
                "### Reading the result\n\n"
                "`avg_price` is size-weighted across every level consumed. `slippage` is the "
                "distance from the touch, signed so that it is always the cost of size rather "
                "than a direction — never negative. `fully_filled = false` means the book ran "
                "out: the row prices the portion that could fill, and the rest of the order "
                "would move the price further than this reports.\n\n"
                "### Sizing a strategy\n\n"
                "Comparing `avg_price` across candidates at the size you actually intend to "
                "trade is a different ranking from comparing midpoints, and it is the one that "
                "survives contact with the book."
            ),
            example_queries=examples(
                (
                    "What 5,000 shares would really cost on the busiest market",
                    "SELECT f.side, f.avg_price, f.touch_price, f.slippage, f.fully_filled "
                    "FROM (SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                    "LATERAL polymarket.main.fill_price(t.token_id, 'buy', 5000) f",
                ),
                (
                    "Which outcomes in a topic can absorb real size, and which cannot",
                    "SELECT m.question, o.outcome, f.avg_price, f.filled_size, f.fully_filled "
                    "FROM polymarket.main.markets(tag_id => '21', closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
                    "LATERAL polymarket.main.fill_price(o.token_id, 'buy', 10000) f "
                    "WHERE NOT f.fully_filled LIMIT 20",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT f.side, f.avg_price, f.touch_price, f.slippage, f.fully_filled "
                    "FROM (SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                    "order_by => 'volume24hr', ascending => 'false') m, "
                    "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                    "LATERAL polymarket.main.fill_price(t.token_id, 'buy', 5000) f"
                ),
                description="What 5,000 shares would really cost on the busiest market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FillArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[FillArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tokens = batch.column("token_id").to_pylist()
        sides = batch.column("side").to_pylist()
        sizes = batch.column("size").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        with api.open_client() as client:
            # Same batched fetch orderbook() uses: 200 tokens to a request.
            books = api.books([str(t) for t in tokens if t is not None], client=client)
            for index, (token_id, side, size) in enumerate(zip(tokens, sides, sizes, strict=True)):
                if token_id is None or size is None:
                    continue
                rows.append(
                    fill_from_book(
                        str(token_id),
                        str(side or "buy").lower(),
                        size,
                        books.get(str(token_id)) or {},
                    )
                )
                parents.append(index)
        emit_fanout(out, params.output_schema, rows, parents, None)


TRADING_FUNCTIONS: list[type] = [
    OrderbookFunction,
    QuoteFunction,
    PriceHistoryFunction,
    TradesFunction,
    HoldersFunction,
    PositionsFunction,
    ActivityFunction,
    FillPriceFunction,
]
