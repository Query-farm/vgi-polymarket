"""Conversion tests for Polymarket's three number encodings and nested shapes."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from vgi_polymarket.schemas import (
    AMOUNT,
    FILL_SCHEMA,
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    ORDERBOOK_SCHEMA,
    PRICE,
    TRADE_SCHEMA,
    batch_from_rows,
    fill_from_book,
    flatten_book,
    flatten_holders,
    flatten_price_history,
    market_row,
    quote_row,
    to_column_decimal,
    to_decimal,
    to_json_list,
    to_timestamp,
    zip_outcomes,
)


class TestDecimalStrings:
    """Gamma's market level and the whole CLOB send exact decimal strings."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("0.0445", Decimal("0.0445")), ("189399.21791", Decimal("189399.21791")), ("0", Decimal(0))],
    )
    def test_parses_exactly(self, raw: str, expected: Decimal) -> None:
        assert to_decimal(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "not-a-number"])
    def test_unparseable_is_null(self, raw: object) -> None:
        assert to_decimal(raw) is None

    @pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_is_null(self, raw: str) -> None:
        """Decimal builds these happily; Arrow then rejects the whole array."""
        assert to_decimal(raw) is None


class TestFloatsRoundTrip:
    """The Data API and Gamma's event level send JSON floats.

    A float cannot hold 0.955, so `Decimal(0.955)` is 0.9549999999999999600...
    Going through repr recovers the decimal the sender meant.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [(0.955, Decimal("0.955")), (0.1, Decimal("0.1")), (0.0445, Decimal("0.0445"))],
    )
    def test_repr_recovers_the_intended_decimal(self, raw: float, expected: Decimal) -> None:
        assert to_decimal(raw) == expected

    def test_the_naive_conversion_would_have_been_wrong(self) -> None:
        """Guard the guard: prove the thing this defends against is real."""
        assert Decimal(0.955) != Decimal("0.955")
        assert to_decimal(0.955) == Decimal("0.955")

    def test_booleans_are_not_numbers(self) -> None:
        """`bool` is an `int` in Python, so True would otherwise be 1."""
        assert to_decimal(True) is None


class TestColumnPolicies:
    """Prices are exact-or-NULL; amounts round. The difference is deliberate."""

    def test_a_price_finer_than_the_column_is_null(self) -> None:
        """Nothing legitimate is finer than 1e-6, so this signals a misread."""
        assert to_column_decimal("0.0000005", PRICE, exact=True) is None

    def test_a_price_that_fits_survives_exactly(self) -> None:
        assert to_column_decimal("0.0445", PRICE, exact=True) == Decimal("0.0445")

    def test_an_amount_with_float_noise_is_rounded_not_dropped(self) -> None:
        """The bug this policy fixes: nulling here voided almost every volume."""
        value = to_column_decimal(2306265.4030640004, AMOUNT, exact=False)
        assert value == Decimal("2306265.403064")

    def test_a_long_decimal_string_amount_is_also_rounded(self) -> None:
        assert to_column_decimal("12858602.949756013", AMOUNT, exact=False) == Decimal("12858602.949756")

    def test_an_amount_too_large_for_the_column_is_still_null(self) -> None:
        """Overflow is a genuine failure, not representation noise."""
        assert to_column_decimal("1e40", AMOUNT, exact=False) is None

    def test_volume_survives_a_real_payload(self) -> None:
        batch = batch_from_rows([{"volume_24hr": 2306265.4030640004}], MARKET_SCHEMA)
        assert batch.column("volume_24hr").to_pylist() == [Decimal("2306265.403064")]

    def test_one_bad_value_does_not_kill_its_batch(self) -> None:
        rows = [{"best_bid": "0.44"}, {"best_bid": "NaN"}, {"best_bid": "0.56"}]
        batch = batch_from_rows(rows, MARKET_SCHEMA)
        assert batch.column("best_bid").to_pylist() == [Decimal("0.44"), None, Decimal("0.56")]


class TestNestedJsonArrays:
    """Gamma sends outcome names, prices and token ids as JSON inside strings."""

    def test_decodes_a_json_string_array(self) -> None:
        assert to_json_list('["Yes", "No"]') == ["Yes", "No"]

    @pytest.mark.parametrize("raw", [None, "", "not json", '{"a": 1}', 17])
    def test_anything_undecodable_is_an_empty_list(self, raw: object) -> None:
        assert to_json_list(raw) == []


class TestZipOutcomes:
    """The three arrays are related only positionally, so they are zipped once."""

    def test_joins_the_three_arrays_by_index(self) -> None:
        row = {
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.0445", "0.9555"]',
            "clobTokenIds": '["3233", "2565"]',
        }
        assert zip_outcomes(row) == [
            {"outcome_index": 0, "outcome": "Yes", "token_id": "3233", "price": "0.0445"},
            {"outcome_index": 1, "outcome": "No", "token_id": "2565", "price": "0.9555"},
        ]

    def test_ragged_arrays_do_not_shift_the_correspondence(self) -> None:
        """A missing element must be NULL, never filled from the next outcome."""
        row = {"outcomes": '["Yes", "No"]', "outcomePrices": '["0.5"]', "clobTokenIds": "[]"}
        zipped = zip_outcomes(row)
        assert [o["outcome"] for o in zipped] == ["Yes", "No"]
        assert [o["price"] for o in zipped] == ["0.5", None]
        assert [o["token_id"] for o in zipped] == [None, None]

    def test_reaches_arrow_as_a_list_of_structs(self) -> None:
        row = market_row(
            {
                "conditionId": "0xabc",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0.0445", "0.9555"]',
                "clobTokenIds": '["3233", "2565"]',
            }
        )
        batch = batch_from_rows([row], MARKET_SCHEMA)
        outcomes = batch.column("outcomes").to_pylist()[0]
        assert [o["outcome"] for o in outcomes] == ["Yes", "No"]
        assert [o["token_id"] for o in outcomes] == ["3233", "2565"]
        assert outcomes[0]["price"] == Decimal("0.044500")

    def test_a_market_with_no_outcomes_is_an_empty_list(self) -> None:
        batch = batch_from_rows([market_row({"conditionId": "0xabc"})], MARKET_SCHEMA)
        assert batch.column("outcomes").to_pylist() == [[]]


class TestQuoteSides:
    """BUY is the best BID and SELL is the best ASK — the opposite of the guess.

    Getting this backwards is invisible in a single row, because both numbers
    are plausible prices. It shows up only as a negative spread, which is what
    caught it during development.
    """

    def test_buy_is_the_bid_and_sell_is_the_ask(self) -> None:
        row = quote_row("t", book={}, midpoint="0.645", sides={"BUY": "0.64", "SELL": "0.65"})
        assert row["best_bid"] == Decimal("0.64")
        assert row["best_ask"] == Decimal("0.65")

    def test_the_spread_is_never_negative(self) -> None:
        row = quote_row("t", book={}, midpoint="0.645", sides={"BUY": "0.64", "SELL": "0.65"})
        assert row["spread"] == Decimal("0.01")
        assert row["spread"] > 0

    def test_a_missing_side_leaves_the_spread_null(self) -> None:
        row = quote_row("t", book={}, midpoint=None, sides={"BUY": "0.64"})
        assert row["best_ask"] is None
        assert row["spread"] is None


class TestBookFlattening:
    def test_both_sides_become_labelled_rows(self) -> None:
        book = {
            "market": "0xabc",
            "timestamp": "1788195995094",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "7"}],
        }
        rows = flatten_book("tok", book)
        assert [(r["side"], r["price"]) for r in rows] == [("bid", "0.40"), ("ask", "0.60")]
        assert all(r["condition_id"] == "0xabc" for r in rows)

    def test_the_book_timestamp_is_milliseconds(self) -> None:
        """Every other epoch the CLOB publishes is in seconds; this one is not."""
        rows = flatten_book("tok", {"timestamp": "1788195995094", "bids": [{"price": "1", "size": "1"}]})
        moment = to_timestamp(rows[0]["book_timestamp"])
        assert moment is not None
        assert moment.year == 2026

    def test_an_empty_book_is_zero_rows_with_the_schema_intact(self) -> None:
        batch = batch_from_rows(flatten_book("tok", {}), ORDERBOOK_SCHEMA)
        assert batch.num_rows == 0
        assert batch.schema == ORDERBOOK_SCHEMA


class TestTimestamps:
    def test_rfc3339(self) -> None:
        assert to_timestamp("2026-08-31T17:04:31.66924Z") == datetime(
            2026, 8, 31, 17, 4, 31, 669240, tzinfo=UTC
        )

    def test_gamma_space_separated_spelling(self) -> None:
        """Some Gamma fields use a space instead of the T, and a 2-digit offset."""
        assert to_timestamp("2023-11-02 21:03:54.55+00") is not None

    def test_epoch_seconds(self) -> None:
        assert to_timestamp(1788195442) == datetime.fromtimestamp(1788195442, tz=UTC)

    @pytest.mark.parametrize("raw", [253402300800, 99999999999999, -99999999999999])
    def test_out_of_range_epoch_is_null(self, raw: int) -> None:
        """Python's datetime spans years 1-9999; a sentinel must not kill a scan."""
        assert to_timestamp(raw) is None

    def test_booleans_are_not_epochs(self) -> None:
        assert to_timestamp(True) is None

    def test_a_bad_date_does_not_kill_its_batch(self) -> None:
        rows = [
            {"event_id": "1", "end_date": "2027-01-01T04:59:00Z"},
            {"event_id": "2", "end_date": 253402300800},
        ]
        batch = batch_from_rows(rows, EVENT_SCHEMA)
        assert batch.num_rows == 2
        assert batch.column("end_date").to_pylist()[0] is not None
        assert batch.column("end_date").to_pylist()[1] is None


class TestEventParentStamping:
    """A market inside an event payload has no nested `events` key."""

    def test_a_top_level_market_reads_its_nested_parent(self) -> None:
        row = market_row({"conditionId": "0xabc", "events": [{"id": "99", "slug": "the-event"}]})
        assert row["event_id"] == "99"
        assert row["event_slug"] == "the-event"

    def test_a_stamped_market_keeps_what_the_caller_supplied(self) -> None:
        """Without this, every row from event_markets() reports a NULL event."""
        row = market_row({"conditionId": "0xabc", "event_id": "99", "event_slug": "the-event"})
        assert row["event_id"] == "99"
        assert row["event_slug"] == "the-event"


class TestOtherFlattening:
    def test_price_history_points_become_named_columns(self) -> None:
        rows = flatten_price_history("tok", [{"t": 1788112815, "p": 0.0445}])
        assert rows == [{"token_id": "tok", "ts": 1788112815, "price": 0.0445}]

    def test_holders_are_stamped_with_their_token(self) -> None:
        """The token lives on the group, not the holder, so it is pushed down."""
        payload = [{"token": "tok1", "holders": [{"proxyWallet": "0x1", "amount": 644533.794669}]}]
        rows = flatten_holders("0xabc", payload)
        assert rows[0]["token_id"] == "tok1"
        assert rows[0]["condition_id"] == "0xabc"

    def test_a_trade_price_survives_as_an_exact_decimal(self) -> None:
        from vgi_polymarket.schemas import trade_row

        batch = batch_from_rows(
            [trade_row({"transactionHash": "0x1", "price": 0.955, "size": 100, "timestamp": 1788195442})],
            TRADE_SCHEMA,
        )
        assert batch.column("price").to_pylist() == [Decimal("0.955000")]
        assert batch.column("size").to_pylist() == [Decimal("100.000000")]


class TestFillPrice:
    """Walking the book turns a spread into a cost.

    The touch is a real price at which possibly nothing trades: a live market
    quoted a one-tick spread on fifteen shares. These assertions pin the walk,
    including the case the top of book cannot express at all — a request the
    book cannot cover.
    """

    BOOK = {
        "asks": [
            {"price": "0.46", "size": "500"},
            {"price": "0.45", "size": "100"},
            {"price": "0.50", "size": "1000"},
        ],
        "bids": [{"price": "0.44", "size": "50"}, {"price": "0.40", "size": "900"}],
    }

    def test_a_buy_consumes_the_cheapest_asks_first(self) -> None:
        row = fill_from_book("t", "buy", 300, self.BOOK)
        # 100 @ 0.45 then 200 @ 0.46 = 137.0 over 300 shares.
        assert row["filled_size"] == Decimal(300)
        assert row["touch_price"] == Decimal("0.45")
        assert row["worst_price"] == Decimal("0.46")
        assert row["levels_consumed"] == 2
        assert row["notional"] == Decimal("137.00")

    def test_a_sell_consumes_the_dearest_bids_first(self) -> None:
        row = fill_from_book("t", "sell", 60, self.BOOK)
        assert row["touch_price"] == Decimal("0.44")
        assert row["worst_price"] == Decimal("0.40")
        assert row["levels_consumed"] == 2

    def test_slippage_is_the_cost_of_size_in_both_directions(self) -> None:
        """Signed so it is never negative: it is what size cost, not a direction."""
        assert fill_from_book("t", "buy", 300, self.BOOK)["slippage"] > 0
        assert fill_from_book("t", "sell", 60, self.BOOK)["slippage"] > 0

    def test_a_touch_sized_order_has_no_slippage(self) -> None:
        assert fill_from_book("t", "buy", 100, self.BOOK)["slippage"] == 0

    def test_a_book_that_cannot_cover_the_request_says_so(self) -> None:
        """The case the touch cannot express: there is not enough there."""
        row = fill_from_book("t", "buy", 100_000, self.BOOK)
        assert row["fully_filled"] is False
        assert row["filled_size"] == Decimal(1600)
        assert row["requested_size"] == Decimal(100_000)
        assert row["avg_price"] is not None, "the fillable portion is still priced"

    def test_an_empty_book_fills_nothing_without_raising(self) -> None:
        row = fill_from_book("t", "buy", 100, {})
        assert row["filled_size"] == Decimal(0)
        assert row["fully_filled"] is False
        assert row["avg_price"] is None and row["slippage"] is None

    def test_zero_size_levels_are_ignored(self) -> None:
        row = fill_from_book(
            "t", "buy", 10, {"asks": [{"price": "0.10", "size": "0"}, {"price": "0.20", "size": "10"}]}
        )
        assert row["touch_price"] == Decimal("0.20")

    def test_it_builds_against_the_declared_schema(self) -> None:
        batch = batch_from_rows([fill_from_book("t", "buy", 300, self.BOOK)], FILL_SCHEMA)
        assert batch.schema == FILL_SCHEMA
        assert batch.column("avg_price").to_pylist() == [Decimal("0.456667")]
