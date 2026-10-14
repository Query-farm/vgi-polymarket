"""Arrow schemas, and the three ways Polymarket encodes a number.

Polymarket is inconsistent about this in a way that matters, because two of the
three encodings lose precision if parsed naively:

* **Decimal strings** — ``"0.0445"``, ``"189399.21791"``. Exact on the wire.
  Gamma sends market-level money this way, and the CLOB sends every price and
  size this way.
* **JSON floats** — ``0.955``, ``12858502.949756002``. Gamma sends *event*-level
  money this way (the same quantity the market level sends as a string), and the
  Data API sends every trade price and size this way.
* **JSON-encoded arrays inside strings** — ``'["Yes", "No"]'``,
  ``'["0.0445", "0.9555"]'``. The outcome names, prices and token ids each
  arrive as a separate JSON document nested in a string field.

The float case is the interesting one. ``float`` cannot hold ``0.955``, so
``Decimal(0.955)`` is ``0.95499999999999996...``. Going through ``repr`` first —
``Decimal(str(0.955))`` — recovers ``0.955`` exactly, because Python prints the
shortest decimal string that round-trips to the same float. That is exact for
every tick-sized price Polymarket can quote (the finest tick is 0.001) and for
any size with a sane number of digits, and it is the only way to get a decimal
back out of a float without inventing precision. :func:`to_decimal` does this
for floats and parses strings directly.

The nested-JSON case is where a wrong answer is easiest to ship. ``outcomes``,
``outcomePrices`` and ``clobTokenIds`` are three independent JSON strings whose
meaning is entirely positional: element *i* of each describes the same outcome.
:func:`zip_outcomes` joins them once, here, so that a consumer cannot get the
correspondence wrong — and so a market whose three arrays disagree in length
yields the outcomes they agree on rather than a misaligned row.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import pyarrow as pa

from vgi_polymarket.meta import field

#: Prices: 0 to 1, quoted to at most 4 decimal places at the finest tick
#: Polymarket offers (0.001). Six places of headroom costs nothing and covers
#: the midpoint, which can land halfway between two ticks.
PRICE = pa.decimal128(18, 6)

#: Sizes, volumes and liquidity, in USDC. Six places because the Data API
#: reports position sizes to that precision (``644533.794669``).
AMOUNT = pa.decimal128(28, 6)

#: Polymarket timestamps are RFC 3339 on Gamma and epoch seconds on the CLOB and
#: Data APIs. :func:`to_timestamp` takes either.
TIMESTAMP = pa.timestamp("us", tz="UTC")

#: One tradeable outcome of a market: its name, the CLOB token that trades it,
#: and its current price. Kept as a nested list on the market row rather than
#: fanned out, because element *i* of Polymarket's three parallel arrays is the
#: only thing that ties these together — see :func:`zip_outcomes`.
OUTCOME_STRUCT = pa.struct(
    [
        ("outcome_index", pa.int32()),
        ("outcome", pa.string()),
        ("token_id", pa.string()),
        ("price", PRICE),
    ]
)


def to_decimal(value: Any) -> Decimal | None:
    """Parse a Polymarket number into a Decimal, or None if absent/unparseable.

    Handles both wire encodings. A string is parsed directly; a float goes
    through ``repr`` first, which recovers the decimal the sender meant rather
    than the binary approximation that reached us — ``0.955`` rather than
    ``0.954999999999999960031971113...``.

    Total by construction: ``Decimal`` happily builds ``NaN`` and ``Infinity``
    from strings, which Arrow then rejects, so both are filtered out here and a
    single odd value cannot fail the batch it arrived in.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        # str() of a float is its shortest round-tripping repr, which is the
        # decimal the sender wrote. str() of a str is a no-op.
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def fits_decimal(value: Decimal, kind: pa.Decimal128Type) -> bool:
    """Whether ``value`` is exactly representable in ``kind``.

    Arrow raises on a value needing more scale ("Rescaling Decimal value would
    cause data loss") or more precision than the column declares, and one such
    value would take down the whole scan.
    """
    shifted = value.scaleb(kind.scale)
    if shifted != shifted.to_integral_value():
        return False
    return abs(shifted) < Decimal(10) ** kind.precision


def _too_large(value: Decimal, kind: pa.Decimal128Type) -> bool:
    """Whether ``value`` overflows ``kind``'s precision even after rounding."""
    return abs(value.scaleb(kind.scale).to_integral_value()) >= Decimal(10) ** kind.precision


def to_column_decimal(value: Any, kind: pa.Decimal128Type, *, exact: bool) -> Decimal | None:
    """Parse a number and fit it to a decimal column, or yield NULL.

    The two policies differ, and the difference is the point:

    ``exact=True`` (**prices**) refuses to round: a value that will not fit the
    column's scale becomes NULL instead, because rounding money is exactly the
    silent loss this module exists to prevent. That is safe here because it is also *unreachable* in
    normal operation — Polymarket's finest tick is 0.001 and a midpoint splits
    it to 0.0005, so nothing legitimate needs more than the six decimal places
    the column declares. A price that does not fit is a misread, and NULL is the
    honest report of one.

    ``exact=False`` (**amounts** — volume, liquidity, size) rounds instead.
    These arrive as JSON floats that the origin itself computed in binary
    floating point, so a 24-hour volume reaches us as ``2306265.4030640004``:
    sixteen decimal places, of which the last ten are representation noise
    rather than USDC. Refusing to round would NULL almost every volume and
    liquidity figure on the site — discarding real data to protect precision
    that was never there. Rounding is half-even, and a value too *large* for the
    column is still NULL, since that is a genuine overflow rather than noise.
    """
    parsed = to_decimal(value)
    if parsed is None:
        return None
    if exact:
        return parsed if fits_decimal(parsed, kind) else None
    if _too_large(parsed, kind):
        return None
    # ROUND_HALF_EVEN is Decimal's default and does not bias a large aggregate.
    return parsed.quantize(Decimal(1).scaleb(-kind.scale))


def to_timestamp(value: Any) -> datetime | None:
    """Parse an RFC 3339 string, or epoch seconds, into an aware UTC datetime.

    Total by construction, like :func:`to_decimal`. Python's ``datetime`` spans
    only years 1 to 9999, and which of ``ValueError`` or ``OverflowError`` an
    out-of-range epoch raises is platform-dependent, while ``fromtimestamp`` can
    raise ``OSError`` for extreme inputs — so all three are caught. A far-future
    sentinel arriving as an epoch integer must not take a scan down.

    Gamma also emits a non-RFC-3339 spelling on some fields
    (``"2023-11-02 21:03:54.55+00"``, a space instead of the ``T``), which
    ``fromisoformat`` accepts on Python 3.11+.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (ValueError, OverflowError, OSError):
            return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (ValueError, OverflowError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def to_json_list(value: Any) -> list[Any]:
    """Decode one of Gamma's JSON-inside-a-string array fields.

    ``outcomes``, ``outcomePrices`` and ``clobTokenIds`` are each a JSON
    document carried in a string field rather than a JSON array in their own
    right. Anything that does not decode to a list yields an empty list, so a
    malformed field costs its own market's outcomes and nothing else.
    """
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except (ValueError, TypeError):
        return []
    return decoded if isinstance(decoded, list) else []


def zip_outcomes(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Join a market's three parallel outcome arrays into one list of structs.

    Gamma describes a market's outcomes across three separately JSON-encoded
    string fields whose only relationship is positional: ``outcomes[i]``,
    ``outcomePrices[i]`` and ``clobTokenIds[i]`` describe the same outcome.
    Reuniting them here means a consumer never has to, and cannot get the
    correspondence wrong.

    The arrays are not guaranteed to agree in length — an unfunded market can
    carry outcome names with no tokens behind them yet — so this yields as many
    outcomes as all three cover, and treats a missing element as NULL rather
    than shifting the rest of the list up to fill the gap.
    """
    names = to_json_list(row.get("outcomes"))
    prices = to_json_list(row.get("outcomePrices"))
    tokens = to_json_list(row.get("clobTokenIds"))
    count = max(len(names), len(prices), len(tokens))

    def at(values: list[Any], index: int) -> Any:
        return values[index] if index < len(values) else None

    return [
        {
            "outcome_index": index,
            "outcome": None if at(names, index) is None else str(at(names, index)),
            "token_id": None if at(tokens, index) is None else str(at(tokens, index)),
            "price": at(prices, index),
        }
        for index in range(count)
    ]


def _decimal_array(values: Sequence[Any], kind: pa.Decimal128Type) -> list[Decimal | None]:
    """Parse a column of numbers under the policy its declared type implies.

    Prices are exact-or-NULL; amounts are rounded to scale. See
    :func:`to_column_decimal` for why the two differ.
    """
    exact = kind == PRICE
    return [to_column_decimal(value, kind, exact=exact) for value in values]


def column(rows: Sequence[dict[str, Any]], key: str, spec: pa.Field) -> pa.Array:
    """Extract ``key`` from every row and build the Arrow array ``spec`` declares.

    The conversion is chosen from the field's declared type, so a schema edit is
    the only thing needed to change how a column is parsed. The list-of-struct
    case is handled explicitly because its ``price`` member needs the same
    decimal treatment as a top-level price column.
    """
    values = [row.get(key) for row in rows]
    if spec.type == pa.list_(OUTCOME_STRUCT):
        return pa.array(
            [
                None
                if value is None
                else [
                    {
                        "outcome_index": entry.get("outcome_index"),
                        "outcome": entry.get("outcome"),
                        "token_id": entry.get("token_id"),
                        "price": _decimal_array([entry.get("price")], PRICE)[0],
                    }
                    for entry in value
                ]
                for value in values
            ],
            type=spec.type,
        )
    if pa.types.is_decimal(spec.type):
        return pa.array(_decimal_array(values, cast("pa.Decimal128Type", spec.type)), type=spec.type)
    if pa.types.is_timestamp(spec.type):
        return pa.array([to_timestamp(v) for v in values], type=spec.type)
    if pa.types.is_boolean(spec.type):
        return pa.array([None if v is None else bool(v) for v in values], type=spec.type)
    if pa.types.is_integer(spec.type):
        return pa.array([None if v is None else int(v) for v in values], type=spec.type)
    if pa.types.is_string(spec.type):
        return pa.array([None if v is None else str(v) for v in values], type=spec.type)
    return pa.array(values, type=spec.type)


def batch_from_rows(rows: Sequence[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build one RecordBatch by pulling each schema field out of ``rows`` by name."""
    return pa.RecordBatch.from_arrays([column(rows, f.name, f) for f in schema], schema=schema)


MARKET_SCHEMA = pa.schema(
    [
        field(
            "condition_id",
            pa.string(),
            "On-chain id of this market (0x + 64 hex). The key trades() and holders() take — "
            "but NOT the key the order book takes, which is a token_id from `outcomes`.",
        ),
        field("market_id", pa.string(), "Gamma's own numeric id for the market, as a string."),
        field("slug", pa.string(), "URL slug on polymarket.com; also a filter argument on markets()."),
        field(
            "question", pa.string(), "The question this market settles, e.g. 'Xi Jinping out before 2027?'."
        ),
        field(
            "event_slug",
            pa.string(),
            "Slug of the event grouping this market with the others resolving alongside it.",
        ),
        field("event_id", pa.string(), "Gamma id of that event; the key event() takes."),
        field(
            "outcomes",
            pa.list_(OUTCOME_STRUCT),
            "One entry per tradeable outcome: its name, its CLOB token_id, and its current "
            "price. Unnest this to reach orderbook(), which keys on token_id.",
        ),
        field(
            "active",
            pa.bool_(),
            "Whether the market is live on the site. An inactive market is not necessarily closed.",
        ),
        field("closed", pa.bool_(), "Whether trading has ended. A closed market may not yet be resolved."),
        field("archived", pa.bool_(), "Whether Polymarket has archived the market out of its listings."),
        field(
            "accepting_orders",
            pa.bool_(),
            "Whether the book is currently accepting orders; the closest thing to 'is it tradeable now'.",
        ),
        field("enable_order_book", pa.bool_(), "Whether this market trades on the CLOB at all."),
        field("start_date", TIMESTAMP, "When the market opened for trading."),
        field("end_date", TIMESTAMP, "When the market is scheduled to resolve."),
        field("created_at", TIMESTAMP, "When the market was created in Gamma."),
        field("updated_at", TIMESTAMP, "When Gamma last modified this market's record."),
        field("last_trade_price", PRICE, "Price of the most recent trade, 0 to 1."),
        field("best_bid", PRICE, "Best resting bid on the first outcome's token."),
        field("best_ask", PRICE, "Best resting ask on the first outcome's token."),
        field("spread", PRICE, "Difference between best ask and best bid."),
        field("volume", AMOUNT, "Lifetime traded volume in USDC."),
        field(
            "volume_24hr", AMOUNT, "Traded volume over the last 24 hours; the liquidity signal to filter on."
        ),
        field("volume_1wk", AMOUNT, "Traded volume over the last week."),
        field("volume_1mo", AMOUNT, "Traded volume over the last month."),
        field("liquidity", AMOUNT, "USDC resting in the order book on both sides."),
        field(
            "one_day_price_change",
            PRICE,
            "Change in price over the last day, in dollars per share; negative when the price fell.",
        ),
        field("one_week_price_change", PRICE, "Change in price over the last week."),
        field(
            "order_price_min_tick_size",
            PRICE,
            "Smallest price increment the book accepts, e.g. 0.001. Prices are exact multiples of this.",
        ),
        field("order_min_size", AMOUNT, "Smallest order the book accepts, in shares."),
        field(
            "neg_risk",
            pa.bool_(),
            "Whether this market is part of a negative-risk (mutually exclusive) group.",
        ),
        field("resolution_source", pa.string(), "What Polymarket settles the market against, when stated."),
        field("description", pa.string(), "The full resolution criteria — how the question is decided."),
        field("competitive", PRICE, "Polymarket's own 0-to-1 score for how contested the market is."),
    ]
)

#: One row per price level per side, flattened out of the CLOB's nested bids and
#: asks so a book joins and aggregates like an ordinary table.
ORDERBOOK_SCHEMA = pa.schema(
    [
        field("token_id", pa.string(), "Outcome token this price level belongs to."),
        field("condition_id", pa.string(), "Market that token belongs to, as the CLOB reports it."),
        field("side", pa.string(), "Which side rests here: 'bid' (buyers) or 'ask' (sellers)."),
        field("price", PRICE, "Price of this level, 0 to 1, in USDC per share."),
        field("size", AMOUNT, "Shares resting at this price level."),
        field("book_timestamp", TIMESTAMP, "When the CLOB produced this snapshot of the book."),
    ]
)

#: Pre-fee displayed-book price for a given size. The answer the top-of-book
#: cannot give: a 0.001 spread means nothing if only 15 shares rest behind it.
FILL_SCHEMA = pa.schema(
    [
        field("token_id", pa.string(), "Outcome token the order would trade."),
        field("side", pa.string(), "Direction priced: 'buy' consumes asks, 'sell' consumes bids."),
        field("requested_size", AMOUNT, "Shares asked for."),
        field("filled_size", AMOUNT, "Shares the resting book could actually supply."),
        field(
            "fully_filled",
            pa.bool_(),
            "Whether the book covered the whole request. False means the rest of the order "
            "would move the price further than this row reports.",
        ),
        field(
            "avg_price",
            PRICE,
            "Size-weighted average price across every level consumed, before any applicable "
            "venue fees — the displayed-book fill price rather than the touch.",
        ),
        field("worst_price", PRICE, "Price of the last level consumed; the far edge of the fill."),
        field("touch_price", PRICE, "Best price on that side, for comparison with avg_price."),
        field(
            "slippage",
            PRICE,
            "avg_price minus touch_price for a buy, reversed for a sell. Always the cost of "
            "size, so never negative.",
        ),
        field("levels_consumed", pa.int32(), "How many price levels the order would clear."),
        field("notional", AMOUNT, "Pre-fee USDC notional of the filled portion."),
    ]
)

#: Top-of-book for one token, in one row: what markets() carries for its first
#: outcome, but for any token and fetched live.
QUOTE_SCHEMA = pa.schema(
    [
        field("token_id", pa.string(), "Outcome token quoted."),
        field("best_bid", PRICE, "Highest price a buyer is currently resting at."),
        field("best_ask", PRICE, "Lowest price a seller is currently resting at."),
        field(
            "midpoint", PRICE, "Halfway between best bid and best ask; the usual 'the price' of an outcome."
        ),
        field("spread", PRICE, "best_ask minus best_bid; the cost of crossing the book."),
        field("last_trade_price", PRICE, "Price of the most recent execution on this token."),
        field("min_order_size", AMOUNT, "Smallest order this token's book accepts, in shares."),
        field("tick_size", PRICE, "Smallest price increment this token's book accepts."),
    ]
)

#: The price time series for one token. One row per sampled instant — this is a
#: price curve, not an OHLC bar: Polymarket publishes a single price per point.
PRICE_HISTORY_SCHEMA = pa.schema(
    [
        field("token_id", pa.string(), "Outcome token this series covers."),
        field("ts", TIMESTAMP, "Instant this price was sampled at; the column to order and join on."),
        field(
            "price",
            PRICE,
            "Price at that instant, 0 to 1, readable directly as the market's implied probability.",
        ),
    ]
)

#: The public trade tape. One row per execution, with the counterparty's public
#: profile alongside — Polymarket settles on a public chain, so who traded is
#: part of the tape rather than something to be joined in.
TRADE_SCHEMA = pa.schema(
    [
        field("transaction_hash", pa.string(), "On-chain transaction that settled this trade."),
        field("condition_id", pa.string(), "Market the trade executed on."),
        field("token_id", pa.string(), "Outcome token that changed hands."),
        field("outcome", pa.string(), "Name of that outcome, e.g. 'Yes'."),
        field("outcome_index", pa.int32(), "Position of that outcome in the market's outcome list."),
        field("side", pa.string(), "Direction the taker went: 'BUY' or 'SELL'."),
        field("price", PRICE, "Execution price, 0 to 1, in USDC per share."),
        field("size", AMOUNT, "Shares exchanged."),
        field("ts", TIMESTAMP, "When the trade executed; the cursor to poll on."),
        field("proxy_wallet", pa.string(), "Address of the trader's Polymarket proxy wallet."),
        field("trader_name", pa.string(), "Display name the trader has set, when they have one."),
        field("pseudonym", pa.string(), "Polymarket's generated pseudonym for the trader."),
        field("title", pa.string(), "Question the market asks, denormalised onto each trade by the API."),
        field("event_slug", pa.string(), "Slug of the event the market belongs to."),
    ]
)

#: The largest position holders, flattened from the API's per-token grouping to
#: one row per holder per token.
HOLDER_SCHEMA = pa.schema(
    [
        field("condition_id", pa.string(), "Market these positions are in."),
        field("token_id", pa.string(), "Outcome token held."),
        field("outcome_index", pa.int32(), "Position of that outcome in the market's outcome list."),
        field("proxy_wallet", pa.string(), "Address of the holder's Polymarket proxy wallet."),
        field("amount", AMOUNT, "Shares of this outcome held."),
        field("holder_name", pa.string(), "Display name the holder has set, when they have one."),
        field("pseudonym", pa.string(), "Polymarket's generated pseudonym for the holder."),
        field("bio", pa.string(), "Profile text the holder has written."),
        field("verified", pa.bool_(), "Whether Polymarket has verified this account."),
    ]
)

#: One wallet's open position in one outcome, with its P&L. This is the reverse
#: of HOLDER_SCHEMA: holders go market -> wallets, positions go wallet ->
#: markets. Only possible because Polymarket settles on a public chain.
POSITION_SCHEMA = pa.schema(
    [
        field("proxy_wallet", pa.string(), "Address of the wallet holding this position."),
        field("condition_id", pa.string(), "Market the position is in; the key trades() takes."),
        field("token_id", pa.string(), "Outcome token held; the key orderbook() and quote() take."),
        field("outcome", pa.string(), "Name of the outcome held, e.g. 'Yes'."),
        field("outcome_index", pa.int32(), "Position of that outcome in the market's outcome list."),
        field("title", pa.string(), "Question the market asks, denormalised onto each position."),
        field("event_slug", pa.string(), "Slug of the event the market belongs to."),
        field("size", AMOUNT, "Shares of this outcome currently held."),
        field("avg_price", PRICE, "Average price paid per share, 0 to 1, in USDC."),
        field("cur_price", PRICE, "Current price per share, 0 to 1; compare with avg_price for direction."),
        field("initial_value", AMOUNT, "USDC paid to open the position, net of fees."),
        field("current_value", AMOUNT, "USDC the position is worth at the current price."),
        field("cash_pnl", AMOUNT, "Unrealised profit or loss in USDC; negative means a loss."),
        field("percent_pnl", PRICE, "Unrealised profit or loss as a percentage of what was paid."),
        field("realized_pnl", AMOUNT, "Profit or loss in USDC already taken by selling."),
        field("total_bought", AMOUNT, "Shares bought over the life of the position, before any sales."),
        field(
            "redeemable", pa.bool_(), "Whether the market has resolved and the position can be cashed out."
        ),
        field("end_date", TIMESTAMP, "When the market is scheduled to resolve."),
    ]
)

#: Events group the markets that resolve together. Money is float-encoded here
#: and string-encoded on the market row beneath — the same quantity, two wire
#: formats — which is why every number goes through the same converter.
EVENT_SCHEMA = pa.schema(
    [
        field(
            "event_id", pa.string(), "Gamma's numeric id for the event, as a string; the key event() takes."
        ),
        field("slug", pa.string(), "URL slug on polymarket.com; also a filter argument on markets()."),
        field("ticker", pa.string(), "Short label Polymarket uses for the event."),
        field("title", pa.string(), "Question or headline the event asks."),
        field("description", pa.string(), "Longer description of what the event covers."),
        field("active", pa.bool_(), "Whether the event is live on the site."),
        field("closed", pa.bool_(), "Whether every market under the event has stopped trading."),
        field("archived", pa.bool_(), "Whether Polymarket has archived the event out of its listings."),
        field("featured", pa.bool_(), "Whether Polymarket is featuring the event on its front page."),
        field("neg_risk", pa.bool_(), "Whether the markets under this event are mutually exclusive."),
        field("market_count", pa.int32(), "How many markets sit under this event."),
        field("start_date", TIMESTAMP, "When the event opened."),
        field("end_date", TIMESTAMP, "When the event is scheduled to resolve."),
        field("creation_date", TIMESTAMP, "When the event was created."),
        field("updated_at", TIMESTAMP, "When Gamma last modified this event's record."),
        field("volume", AMOUNT, "Lifetime traded volume across every market under the event, in USDC."),
        field("volume_24hr", AMOUNT, "Traded volume across the event over the last 24 hours."),
        field("liquidity", AMOUNT, "USDC resting in the books of every market under the event."),
        field("open_interest", AMOUNT, "USDC value of positions currently outstanding under the event."),
        field("comment_count", pa.int32(), "Count of comments left on the event page."),
        field("competitive", PRICE, "Polymarket's own 0-to-1 score for how contested the event is."),
        field(
            "tags",
            pa.list_(pa.struct([("tag_id", pa.string()), ("label", pa.string()), ("slug", pa.string())])),
            "Topic tags on the event, each a {tag_id, label, slug} struct. Kept nested — flattening "
            "would fan every event out into one row per tag.",
        ),
    ]
)

#: One entry in a wallet's activity feed. Broader than TRADE_SCHEMA: the same
#: feed carries redemptions, yield accrual and rebates, which are not trades and
#: leave most of the market columns empty.
ACTIVITY_SCHEMA = pa.schema(
    [
        field("proxy_wallet", pa.string(), "Address of the wallet this activity belongs to."),
        field("ts", TIMESTAMP, "When it happened; the column to order and poll on."),
        field(
            "activity_type",
            pa.string(),
            "What happened: TRADE, REDEEM, YIELD, MERGE, SPLIT, CONVERSION, or one of the "
            "MAKER_REBATE / TAKER_REBATE / REWARD / REFERRAL_REWARD incentive payments. Only "
            "TRADE rows carry a price and a side; the rest leave them NULL.",
        ),
        field("condition_id", pa.string(), "Market involved; NULL for activity not tied to one."),
        field("token_id", pa.string(), "Outcome token involved; NULL for activity not tied to one."),
        field("outcome", pa.string(), "Name of that outcome, when there is one."),
        field("outcome_index", pa.int32(), "Position of that outcome in the market's outcome list."),
        field("side", pa.string(), "Direction for a TRADE: 'BUY' or 'SELL'; NULL otherwise."),
        field("price", PRICE, "Price per share, 0 to 1, for a TRADE; NULL otherwise."),
        field("size", AMOUNT, "Shares involved."),
        field("usdc_size", AMOUNT, "USDC value of the activity."),
        field("transaction_hash", pa.string(), "On-chain transaction that recorded it."),
        field("title", pa.string(), "Question the market asks, when the activity names one."),
        field("event_slug", pa.string(), "Slug of the event the market belongs to."),
        field("trader_name", pa.string(), "Display name the wallet has set, when it has one."),
        field("pseudonym", pa.string(), "Polymarket's generated pseudonym for the wallet."),
    ]
)

#: A series is a recurring question that opens a new event each occurrence —
#: 'NFL', say, which opens an event per game.
SERIES_SCHEMA = pa.schema(
    [
        field("series_id", pa.string(), "Gamma's numeric id for the series, as a string."),
        field("slug", pa.string(), "URL slug; the value events() accepts as its series_slug filter."),
        field("ticker", pa.string(), "Short label Polymarket uses for the series, e.g. 'nfl'."),
        field("title", pa.string(), "Human-readable name of the series, e.g. 'NFL'."),
        field("description", pa.string(), "What the series covers."),
        field("series_type", pa.string(), "How the series is structured, e.g. 'single'."),
        field("recurrence", pa.string(), "How often a new event opens, e.g. 'daily' or 'weekly'."),
        field("active", pa.bool_(), "Whether the series is live on the site."),
        field("closed", pa.bool_(), "Whether the series has ended."),
        field("archived", pa.bool_(), "Whether Polymarket has archived the series."),
        field("featured", pa.bool_(), "Whether Polymarket is featuring the series."),
        field("volume_24hr", AMOUNT, "Traded volume across the series over the last 24 hours, in USDC."),
        field("comment_count", pa.int32(), "Count of comments left across the series' event pages."),
        field("start_date", TIMESTAMP, "When the series began."),
        field("created_at", TIMESTAMP, "When the series was created in Gamma."),
        field("updated_at", TIMESTAMP, "When Gamma last modified this series' record."),
    ]
)

#: Tags are Polymarket's topic axis and the practical entry point to the
#: catalog: `markets()` and `events()` both filter on tag_id.
TAG_SCHEMA = pa.schema(
    [
        field(
            "tag_id",
            pa.string(),
            "Gamma's numeric id for the tag, as a string; the key markets() filters on.",
        ),
        field("label", pa.string(), "Display name of the tag, e.g. 'Crypto' or 'Politics'."),
        field("slug", pa.string(), "URL slug for the tag."),
        field("created_at", TIMESTAMP, "When the tag was created."),
        field("updated_at", TIMESTAMP, "When Gamma last modified the tag."),
        field("published_at", TIMESTAMP, "When the tag was published, when it has been."),
    ]
)


def market_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Gamma market payload into this package's column names.

    Gamma's payload is ~90 camelCase fields wide, many of them internal
    deployment flags. This selects the ones worth exposing, renames them to the
    snake_case the rest of the catalog uses, joins the three parallel outcome
    arrays, and lifts the event id and slug up from the nested ``events`` list —
    which carries exactly one entry in every payload observed, but is a list, so
    it is read defensively.

    A market read out of an event's own payload has **no** nested ``events``
    key: Gamma does not repeat the parent inside its own children. Those rows
    arrive with ``event_id``/``event_slug`` already stamped on by the caller,
    which is why those two are read from the row itself before falling back to
    the nested parent. Without that, every market reached through
    ``event_markets()`` would report a NULL event.
    """
    events = raw.get("events") or []
    parent = events[0] if events and isinstance(events[0], dict) else {}
    return {
        "condition_id": raw.get("conditionId"),
        "market_id": raw.get("id"),
        "slug": raw.get("slug"),
        "question": raw.get("question"),
        "event_slug": raw.get("event_slug") or parent.get("slug"),
        "event_id": raw.get("event_id") or parent.get("id"),
        "outcomes": zip_outcomes(raw),
        "active": raw.get("active"),
        "closed": raw.get("closed"),
        "archived": raw.get("archived"),
        "accepting_orders": raw.get("acceptingOrders"),
        "enable_order_book": raw.get("enableOrderBook"),
        "start_date": raw.get("startDate"),
        "end_date": raw.get("endDate"),
        "created_at": raw.get("createdAt"),
        "updated_at": raw.get("updatedAt"),
        "last_trade_price": raw.get("lastTradePrice"),
        "best_bid": raw.get("bestBid"),
        "best_ask": raw.get("bestAsk"),
        "spread": raw.get("spread"),
        "volume": raw.get("volume"),
        "volume_24hr": raw.get("volume24hr"),
        "volume_1wk": raw.get("volume1wk"),
        "volume_1mo": raw.get("volume1mo"),
        "liquidity": raw.get("liquidity"),
        "one_day_price_change": raw.get("oneDayPriceChange"),
        "one_week_price_change": raw.get("oneWeekPriceChange"),
        "order_price_min_tick_size": raw.get("orderPriceMinTickSize"),
        "order_min_size": raw.get("orderMinSize"),
        "neg_risk": raw.get("negRisk"),
        "resolution_source": raw.get("resolutionSource"),
        "description": raw.get("description"),
        "competitive": raw.get("competitive"),
    }


def event_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Gamma event payload into this package's column names.

    ``markets`` is dropped rather than nested. The inlined copy is unpaged and
    can run to hundreds of full market records — it is most of the payload —
    and ``event_markets()`` returns the same rows in the shape the rest of the
    catalog uses.
    """
    tags = raw.get("tags") or []
    return {
        "event_id": raw.get("id"),
        "slug": raw.get("slug"),
        "ticker": raw.get("ticker"),
        "title": raw.get("title"),
        "description": raw.get("description"),
        "active": raw.get("active"),
        "closed": raw.get("closed"),
        "archived": raw.get("archived"),
        "featured": raw.get("featured"),
        "neg_risk": raw.get("negRisk"),
        "market_count": len(raw.get("markets") or []) or None,
        "start_date": raw.get("startDate"),
        "end_date": raw.get("endDate"),
        "creation_date": raw.get("creationDate"),
        "updated_at": raw.get("updatedAt"),
        "volume": raw.get("volume"),
        "volume_24hr": raw.get("volume24hr"),
        "liquidity": raw.get("liquidity"),
        "open_interest": raw.get("openInterest"),
        "comment_count": raw.get("commentCount"),
        "competitive": raw.get("competitive"),
        "tags": [
            {"tag_id": t.get("id"), "label": t.get("label"), "slug": t.get("slug")}
            for t in tags
            if isinstance(t, dict)
        ]
        or None,
    }


def series_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Gamma series payload into this package's column names."""
    return {
        "series_id": raw.get("id"),
        "slug": raw.get("slug"),
        "ticker": raw.get("ticker"),
        "title": raw.get("title"),
        "description": raw.get("description"),
        "series_type": raw.get("seriesType"),
        "recurrence": raw.get("recurrence"),
        "active": raw.get("active"),
        "closed": raw.get("closed"),
        "archived": raw.get("archived"),
        "featured": raw.get("featured"),
        "volume_24hr": raw.get("volume24hr"),
        "comment_count": raw.get("commentCount"),
        "start_date": raw.get("startDate"),
        "created_at": raw.get("createdAt"),
        "updated_at": raw.get("updatedAt"),
    }


def tag_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Gamma tag payload into this package's column names."""
    return {
        "tag_id": raw.get("id"),
        "label": raw.get("label"),
        "slug": raw.get("slug"),
        "created_at": raw.get("createdAt"),
        "updated_at": raw.get("updatedAt"),
        "published_at": raw.get("publishedAt"),
    }


def flatten_book(token_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a CLOB book into one row per (side, price level).

    The CLOB nests levels as ``{"bids": [{"price", "size"}, ...], "asks": [...]}``;
    an absent side simply contributes no rows, and a token with no book at all
    contributes none either.
    """
    condition_id = payload.get("market")
    timestamp = payload.get("timestamp")
    # The CLOB stamps its book timestamp in milliseconds, unlike every other
    # epoch it publishes; dividing here keeps to_timestamp's contract simple.
    moment = None
    if timestamp is not None:
        try:
            moment = int(timestamp) / 1000
        except (TypeError, ValueError):
            moment = None
    rows: list[dict[str, Any]] = []
    for key, side in (("bids", "bid"), ("asks", "ask")):
        for level in payload.get(key) or []:
            if not isinstance(level, dict):
                continue
            rows.append(
                {
                    "token_id": token_id,
                    "condition_id": condition_id,
                    "side": side,
                    "price": level.get("price"),
                    "size": level.get("size"),
                    "book_timestamp": moment,
                }
            )
    return rows


def quote_row(
    token_id: str,
    *,
    book: dict[str, Any],
    midpoint: str | None,
    sides: dict[str, str] | None,
) -> dict[str, Any]:
    """Assemble one top-of-book row from the three batch endpoints.

    **``BUY`` is the best bid and ``SELL`` is the best ask** — which is the
    opposite of the reading most people reach for, since "the buy price" sounds
    like what a buyer pays. It is not: verified against the raw book on live
    markets, ``/prices`` ``BUY`` equals ``max(bids.price)`` and ``SELL`` equals
    ``min(asks.price)``, and Gamma's own ``bestBid``/``bestAsk`` agree.

    Getting this backwards is invisible in a single row — both numbers are
    plausible prices — and shows up only as a *negative* spread once you
    subtract them. That is what caught it here, and it is what
    ``tests/test_schemas.py::TestQuoteSides`` and the
    ``a_quote_is_ordered_bid_then_ask`` executable example now pin.

    ``spread`` is computed rather than fetched: the CLOB's own ``/spread`` is a
    third request per token with no batched form, and the subtraction is exact
    on decimals that both came from the same book.
    """
    sides = sides or {}
    best_bid = to_decimal(sides.get("BUY"))
    best_ask = to_decimal(sides.get("SELL"))
    spread = best_ask - best_bid if best_ask is not None and best_bid is not None else None
    return {
        "token_id": token_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": midpoint,
        "spread": spread,
        "last_trade_price": book.get("last_trade_price"),
        "min_order_size": book.get("min_order_size"),
        "tick_size": book.get("tick_size"),
    }


def fill_from_book(token_id: str, side: str, size: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Walk one side of a book and price a request of ``size`` shares.

    This is the number a spread cannot give you. A market can quote a
    one-tick spread and hold fifteen shares behind it; the touch is then a real
    price at which almost nothing trades. Consuming levels until the request is
    met turns "is this liquid?" into "what is the displayed-book notional?".

    ``side`` is the direction of the order, so a **buy** consumes asks from the
    cheapest up and a **sell** consumes bids from the dearest down. A book that
    cannot cover the request is reported as ``fully_filled = False`` with the
    portion it can, rather than an average over a fill that would not happen.
    Fees are deliberately outside this calculation; every output is derived
    solely from the displayed levels in ``payload``.
    """
    wanted = to_decimal(size) or Decimal(0)
    buying = side == "buy"
    levels = [
        (to_decimal(level.get("price")), to_decimal(level.get("size")))
        for level in (payload.get("asks" if buying else "bids") or [])
        if isinstance(level, dict)
    ]
    usable = [(p, q) for p, q in levels if p is not None and q is not None and q > 0]
    # Cheapest-first to buy, dearest-first to sell: the order a real sweep takes.
    usable.sort(key=lambda level: level[0], reverse=not buying)

    remaining, notional, filled, consumed = wanted, Decimal(0), Decimal(0), 0
    worst: Decimal | None = None
    for price, available in usable:
        if remaining <= 0:
            break
        take = min(remaining, available)
        notional += take * price
        filled += take
        remaining -= take
        worst = price
        consumed += 1

    touch = usable[0][0] if usable else None
    average = (notional / filled) if filled > 0 else None
    slip = None
    if average is not None and touch is not None:
        slip = (average - touch) if buying else (touch - average)
    # A size-weighted average is a *derived* quantity, not a quoted one. Dividing
    # by a share count routinely produces more places than any tick could carry
    # (0.45666...), and the exact-or-NULL rule that protects quoted prices would
    # discard it entirely. Rounding here is the correct reading of the same
    # principle: refuse to invent precision, but do not throw away an answer that
    # was never going to land on a tick.
    places = Decimal(1).scaleb(-PRICE.scale)
    if average is not None:
        average = average.quantize(places)
    if slip is not None:
        slip = slip.quantize(places)
    return {
        "token_id": token_id,
        "side": side,
        "requested_size": wanted,
        "filled_size": filled,
        "fully_filled": filled >= wanted > 0,
        "avg_price": average,
        "worst_price": worst,
        "touch_price": touch,
        "slippage": slip,
        "levels_consumed": consumed,
        "notional": notional,
    }


def flatten_price_history(token_id: str, history: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Name CLOB points and collapse repeated observations within this response.

    The origin can append its latest sample twice. Keep the first observation
    at each timestamp when the exact numeric prices agree; contradictory
    prices are an error rather than an arbitrary first/last choice. Compare
    before Arrow's decimal conversion, without rounding. Invalid/absent values
    still pass through for the usual NULL conversion, and output order stays
    unchanged. No observation state survives this function call.
    """
    rows: list[dict[str, Any]] = []
    seen: dict[datetime, Decimal] = {}
    for point in history:
        if not isinstance(point, dict):
            continue
        timestamp = to_timestamp(point.get("t"))
        price = to_decimal(point.get("p"))
        if timestamp is not None and price is not None:
            previous = seen.get(timestamp)
            if previous is not None:
                if previous != price:
                    raise ValueError(
                        f"Conflicting price-history observations for token {token_id!r} "
                        f"at {timestamp.isoformat()}"
                    )
                continue
            seen[timestamp] = price
        rows.append({"token_id": token_id, "ts": point.get("t"), "price": point.get("p")})
    return rows


def trade_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Data API trade into this package's column names."""
    return {
        "transaction_hash": raw.get("transactionHash"),
        "condition_id": raw.get("conditionId"),
        "token_id": raw.get("asset"),
        "outcome": raw.get("outcome"),
        "outcome_index": raw.get("outcomeIndex"),
        "side": raw.get("side"),
        "price": raw.get("price"),
        "size": raw.get("size"),
        "ts": raw.get("timestamp"),
        "proxy_wallet": raw.get("proxyWallet"),
        "trader_name": raw.get("name"),
        "pseudonym": raw.get("pseudonym"),
        "title": raw.get("title"),
        "event_slug": raw.get("eventSlug"),
    }


def position_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Data API position into this package's column names.

    ``percent_pnl`` arrives as a percentage (``-44.5128``), not a fraction, so
    it is carried as-is rather than divided — the column comment says so, and
    silently rescaling it would make it disagree with Polymarket's own display.
    """
    return {
        "proxy_wallet": raw.get("proxyWallet"),
        "condition_id": raw.get("conditionId"),
        "token_id": raw.get("asset"),
        "outcome": raw.get("outcome"),
        "outcome_index": raw.get("outcomeIndex"),
        "title": raw.get("title"),
        "event_slug": raw.get("eventSlug"),
        "size": raw.get("size"),
        "avg_price": raw.get("avgPrice"),
        "cur_price": raw.get("curPrice"),
        "initial_value": raw.get("initialValue"),
        "current_value": raw.get("currentValue"),
        "cash_pnl": raw.get("cashPnl"),
        "percent_pnl": raw.get("percentPnl"),
        "realized_pnl": raw.get("realizedPnl"),
        "total_bought": raw.get("totalBought"),
        "redeemable": raw.get("redeemable"),
        "end_date": raw.get("endDate"),
    }


#: The Data API uses this in `outcomeIndex` for activity that is not tied to an
#: outcome at all (yield, rebates). It is a sentinel, not a position, so it must
#: not reach a consumer as one.
_NO_OUTCOME = 999


def _blank_to_none(value: Any) -> Any:
    """Empty string to NULL.

    The activity feed uses ``""`` rather than omitting a field for the market
    columns of a non-trade row, and an empty string reaching SQL as a value
    reads as data — a condition id that is the empty string, rather than one
    that does not exist.
    """
    return None if value == "" else value


def activity_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Reshape one Data API activity entry into this package's column names."""
    index = raw.get("outcomeIndex")
    return {
        "proxy_wallet": raw.get("proxyWallet"),
        "ts": raw.get("timestamp"),
        "activity_type": _blank_to_none(raw.get("type")),
        "condition_id": _blank_to_none(raw.get("conditionId")),
        "token_id": _blank_to_none(raw.get("asset")),
        "outcome": _blank_to_none(raw.get("outcome")),
        "outcome_index": None if index == _NO_OUTCOME else index,
        "side": _blank_to_none(raw.get("side")),
        # A non-trade row reports price 0, which is a real price for a market
        # that is not going to happen — so it is nulled rather than carried.
        "price": raw.get("price") if raw.get("type") == "TRADE" else None,
        "size": raw.get("size"),
        "usdc_size": raw.get("usdcSize"),
        "transaction_hash": raw.get("transactionHash"),
        "title": _blank_to_none(raw.get("title")),
        "event_slug": _blank_to_none(raw.get("eventSlug")),
        "trader_name": _blank_to_none(raw.get("name")),
        "pseudonym": _blank_to_none(raw.get("pseudonym")),
    }


def flatten_holders(condition_id: str, payload: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten the Data API's per-token holder groups into one row per holder.

    The response is a list of ``{"token": ..., "holders": [...]}`` groups. The
    token id lives on the group rather than on each holder, so it is stamped
    down onto every row — otherwise a holder row could not say which outcome it
    is a position in.
    """
    rows: list[dict[str, Any]] = []
    for group in payload:
        if not isinstance(group, dict):
            continue
        token_id = group.get("token")
        for holder in group.get("holders") or []:
            if not isinstance(holder, dict):
                continue
            rows.append(
                {
                    "condition_id": condition_id,
                    "token_id": token_id or holder.get("asset"),
                    "outcome_index": holder.get("outcomeIndex"),
                    "proxy_wallet": holder.get("proxyWallet"),
                    "amount": holder.get("amount"),
                    "holder_name": holder.get("name"),
                    "pseudonym": holder.get("pseudonym"),
                    "bio": holder.get("bio"),
                    "verified": holder.get("verified"),
                }
            )
    return rows
