"""Tests against the real Polymarket APIs.

    pytest -m live

These pin facts about Polymarket that no offline test can: which parameter names
are honoured, which are silently ignored, where the page sizes clamp, and — above
all — which identifier each endpoint actually wants. Every one of those was found
empirically, and each would fail silently if Polymarket changed it.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.schemas import to_decimal

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def client():
    with api.open_client() as c:
        yield c


@pytest.fixture(scope="module")
def busy_market(client) -> dict:
    """The busiest open market, so tests never name a contract that will settle."""
    rows, _ = api.keyset_page(
        api.MARKETS_PATH,
        "markets",
        [("closed", "false"), ("order", "volume24hr"), ("ascending", "false")],
        client=client,
    )
    assert rows, "no open markets returned"
    return rows[0]


@pytest.fixture(scope="module")
def busy_token(busy_market) -> str:
    tokens = json.loads(busy_market["clobTokenIds"])
    assert tokens
    return str(tokens[0])


class TestPageLimits:
    """Gamma clamps an oversized page silently rather than erroring."""

    @pytest.mark.parametrize("path,key", [(api.MARKETS_PATH, "markets"), (api.EVENTS_PATH, "events")])
    def test_gamma_clamps_at_one_hundred(self, client, path: str, key: str) -> None:
        rows, _ = api.keyset_page(path, key, [], client=client, page_limit=500)
        assert len(rows) <= api.GAMMA_PAGE_LIMIT

    def test_series_clamps_at_fifty(self, client) -> None:
        rows = api.offset_page(
            api.gamma_base_url(),
            "/series",
            {"exclude_events": "true"},
            client=client,
            page_limit=500,
        )
        assert len(rows) <= api.SERIES_PAGE_LIMIT

    def test_an_oversized_limit_is_not_an_error(self, client) -> None:
        """No error — just a shorter page than the caller asked for."""
        rows, _ = api.keyset_page(api.MARKETS_PATH, "markets", [], client=client, page_limit=100_000)
        assert rows

    @pytest.mark.parametrize(
        ("path", "params", "maximum"),
        [
            ("/trades", {}, api.TRADES_MAX_ROWS),
            (
                "/activity",
                {"user": "0x0000000000000000000000000000000000000000"},
                api.ACTIVITY_MAX_ROWS,
            ),
        ],
    )
    def test_data_history_offset_caps(self, client, path: str, params: dict, maximum: int) -> None:
        """These hard errors are why the SQL functions cannot promise unlimited history."""
        response = client.get(
            f"{api.data_base_url()}{path}",
            params={**params, "limit": 1, "offset": maximum + 1},
        )
        assert response.status_code == 400
        assert "max historical" in response.text.lower()


class TestKeysetCursor:
    """The cursor must actually advance. Sending it wrong is a silent no-op."""

    def test_the_cursor_advances_across_pages(self, client) -> None:
        seen: list[str] = []
        cursor = None
        for _ in range(3):
            rows, cursor = api.keyset_page(
                api.MARKETS_PATH,
                "markets",
                [("closed", "false")],
                cursor=cursor,
                client=client,
                page_limit=5,
            )
            seen.extend(r["id"] for r in rows)
            if cursor is None:
                break
        assert len(seen) == len(set(seen)), "the cursor is re-serving the same page"
        assert len(seen) > 5

    def test_the_wrong_cursor_parameter_is_silently_ignored(self, client) -> None:
        """This is why the cursor name is pinned: `cursor` returns page one again."""
        first, cursor = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "false")], client=client, page_limit=5
        )
        assert cursor
        wrong = api._get(
            api.gamma_base_url(),
            api.MARKETS_PATH,
            [("closed", "false"), ("limit", 5), ("cursor", cursor)],
            client=client,
        )
        assert [r["id"] for r in wrong["markets"]] == [r["id"] for r in first], (
            "Polymarket now honours `cursor`; the paging code can be simplified"
        )


class TestSilentlyIgnoredFilters:
    """Gamma answers an unrecognised query parameter by ignoring it."""

    def test_an_unknown_filter_returns_unfiltered_rows(self, client) -> None:
        plain, _ = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "false")], client=client, page_limit=3
        )
        bogus, _ = api.keyset_page(
            api.MARKETS_PATH,
            "markets",
            [("closed", "false"), ("totally_bogus_param", "xyz")],
            client=client,
            page_limit=3,
        )
        assert [r["id"] for r in plain] == [r["id"] for r in bogus]

    def test_markets_has_no_event_filter(self, client) -> None:
        """The reason event_markets() exists rather than markets(event_slug => ...)."""
        plain, _ = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "false")], client=client, page_limit=3
        )
        with_event, _ = api.keyset_page(
            api.MARKETS_PATH,
            "markets",
            [("closed", "false"), ("event_slug", "some-event-slug")],
            client=client,
            page_limit=3,
        )
        assert [r["id"] for r in plain] == [r["id"] for r in with_event], (
            "markets/keyset now honours event_slug; markets() could take it directly"
        )

    def test_a_real_filter_does_narrow(self, client) -> None:
        """Guard the guard: prove the comparison above can distinguish anything."""
        rows, _ = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "true")], client=client, page_limit=5
        )
        assert rows and all(r["closed"] for r in rows)


class TestIdentifierSplit:
    """The headline trap: which key each host wants, and how the wrong one fails."""

    def test_the_order_book_takes_a_token_id(self, client, busy_token: str) -> None:
        book = api.book(busy_token, client=client)
        assert book.get("asset_id") == busy_token

    def test_a_condition_id_on_the_order_book_is_a_404(self, client, busy_market) -> None:
        """The loud failure — the only one of the three that is."""
        with pytest.raises(api.NotFound):
            api.book(busy_market["conditionId"], client=client)

    def test_a_condition_id_on_price_history_is_silently_empty(self, client, busy_market) -> None:
        """HTTP 200 and no rows, indistinguishable from an outcome that never traded."""
        history = api.price_history(busy_market["conditionId"], interval="1d", client=client)
        assert history == []

    def test_price_history_takes_a_token_id(self, client, busy_token: str) -> None:
        assert api.price_history(busy_token, interval="1w", fidelity=60, client=client)

    def test_the_trade_tape_takes_a_condition_id(self, client, busy_market) -> None:
        trades = api.trades(busy_market["conditionId"], limit=5, client=client)
        assert trades
        assert all(t["conditionId"] == busy_market["conditionId"] for t in trades)

    def test_a_token_id_on_the_trade_tape_is_silently_empty(self, client, busy_token: str) -> None:
        """The other silent direction."""
        assert api.trades(busy_token, limit=5, client=client) == []


class TestQuoteSides:
    """BUY is the best bid and SELL is the best ask — verified against the book."""

    def test_buy_matches_the_highest_bid_and_sell_the_lowest_ask(self, client, busy_token: str) -> None:
        book = api.book(busy_token, client=client)
        bids = [to_decimal(b["price"]) for b in book.get("bids") or []]
        asks = [to_decimal(a["price"]) for a in book.get("asks") or []]
        if not bids or not asks:
            pytest.skip("the busiest market has an empty book right now")
        sides = api.prices([busy_token], client=client)[busy_token]
        assert to_decimal(sides["BUY"]) == max(bids), "BUY is the best BID"
        assert to_decimal(sides["SELL"]) == min(asks), "SELL is the best ASK"

    def test_the_midpoint_sits_between_them(self, client, busy_token: str) -> None:
        sides = api.prices([busy_token], client=client)[busy_token]
        mid = to_decimal(api.midpoints([busy_token], client=client)[busy_token])
        bid, ask = to_decimal(sides["BUY"]), to_decimal(sides["SELL"])
        if None in (mid, bid, ask):
            pytest.skip("incomplete quote right now")
        assert bid <= mid <= ask


class TestBatching:
    def test_a_batched_book_matches_the_single_fetch(self, client, busy_token: str) -> None:
        single = api.book(busy_token, client=client)
        batched = api.books([busy_token], client=client)[busy_token]
        assert single["asset_id"] == batched["asset_id"]

    def test_many_books_come_back_in_one_request(self, client) -> None:
        rows, _ = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "false")], client=client, page_limit=50
        )
        tokens: list[str] = []
        for row in rows:
            tokens += [str(t) for t in json.loads(row.get("clobTokenIds") or "[]") if t]
        books = api.books(tokens, client=client)
        # Tokens with no book are absent rather than empty, so this is a floor.
        assert len(books) > len(tokens) // 2

    def test_condition_id_lookup_is_batched(self, client) -> None:
        rows, _ = api.keyset_page(
            api.MARKETS_PATH, "markets", [("closed", "false")], client=client, page_limit=20
        )
        ids = [r["conditionId"] for r in rows]
        found = api.markets_by_condition_id(ids, client=client)
        assert set(found) == set(ids)


class TestCacheHeaders:
    """The TTLs this worker forwards are Polymarket's, not invented."""

    def test_the_catalog_declares_freshness(self, client) -> None:
        hint = api.CacheHint()
        api.keyset_page(api.MARKETS_PATH, "markets", [], client=client, hint=hint)
        assert hint.cacheable and hint.max_age is not None

    def test_tags_declare_the_longest_ttl(self, client) -> None:
        hint = api.CacheHint()
        api.tags_page(client=client, hint=hint)
        assert hint.cacheable and hint.max_age >= 300

    def test_the_live_book_declares_nothing(self, client, busy_token: str) -> None:
        hint = api.CacheHint()
        api.book(busy_token, client=client, hint=hint)
        assert not hint.cacheable, "the CLOB now declares a TTL; the worker could forward it"


class TestShapes:
    def test_outcome_arrays_line_up(self, busy_market) -> None:
        """The three parallel arrays must agree, or the zip is misaligned."""
        names = json.loads(busy_market["outcomes"])
        prices = json.loads(busy_market["outcomePrices"])
        tokens = json.loads(busy_market["clobTokenIds"])
        assert len(names) == len(prices) == len(tokens)

    def test_outcome_prices_sum_to_about_one(self, busy_market) -> None:
        """A binary market's outcomes partition the probability space."""
        prices = [to_decimal(p) for p in json.loads(busy_market["outcomePrices"])]
        if len(prices) != 2 or any(p is None for p in prices):
            pytest.skip("not a two-outcome market")
        assert abs(sum(prices) - Decimal(1)) < Decimal("0.05")

    def test_an_event_inlines_its_markets(self, client, busy_market) -> None:
        """event_markets() depends on this, since markets() has no event filter."""
        event_id = (busy_market.get("events") or [{}])[0].get("id")
        if not event_id:
            pytest.skip("market carries no event")
        event = api.event(str(event_id), client=client)
        assert event.get("markets")

    def test_an_inlined_market_does_not_repeat_its_parent(self, client, busy_market) -> None:
        """Which is why event_markets() stamps the event id on by hand."""
        event_id = (busy_market.get("events") or [{}])[0].get("id")
        if not event_id:
            pytest.skip("market carries no event")
        event = api.event(str(event_id), client=client)
        assert "events" not in (event["markets"][0])

    def test_holders_group_by_token(self, client, busy_market) -> None:
        groups = api.holders(busy_market["conditionId"], limit=5, client=client)
        if not groups:
            pytest.skip("no holders reported right now")
        assert "token" in groups[0] and "holders" in groups[0]

    def test_series_excludes_events_when_asked(self, client) -> None:
        rows = api.series_page(client=client)
        assert rows and all("events" not in r for r in rows)


class TestPositions:
    """The wallet key — the third identifier, and the reverse of holders()."""

    @pytest.fixture(scope="class")
    @classmethod
    def a_wallet(cls, client, busy_market) -> str:
        groups = api.holders(busy_market["conditionId"], limit=5, client=client)
        for group in groups:
            for holder in group.get("holders") or []:
                if wallet := holder.get("proxyWallet"):
                    return str(wallet)
        pytest.skip("no holders reported right now")

    def test_a_wallet_has_positions(self, client, a_wallet: str) -> None:
        rows = api.positions(a_wallet, limit=5, client=client)
        assert rows
        assert all(r["proxyWallet"].lower() == a_wallet.lower() for r in rows)

    def test_a_position_carries_both_market_keys(self, client, a_wallet: str) -> None:
        """Which is what lets positions() feed back into the rest of the catalog."""
        rows = api.positions(a_wallet, limit=1, client=client)
        assert rows[0]["conditionId"].startswith("0x")
        assert rows[0]["asset"].isdigit()

    def test_prices_are_probabilities(self, client, a_wallet: str) -> None:
        rows = api.positions(a_wallet, limit=10, client=client)
        for row in rows:
            for key in ("avgPrice", "curPrice"):
                value = to_decimal(row.get(key))
                if value is not None:
                    assert 0 <= value <= 1, f"{key}={value}"

    def test_it_declares_the_shortest_ttl(self, client, a_wallet: str) -> None:
        """A position's value moves with the price, so Polymarket says max-age=5."""
        hint = api.CacheHint()
        api.positions(a_wallet, limit=1, client=client, hint=hint)
        assert hint.cacheable and hint.max_age <= 60

    def test_the_loop_closes(self, client, busy_market) -> None:
        """A position closes the loop when Gamma still retains its market.

        The Data API can retain positions after Gamma retires the corresponding
        catalog record, so absence is a documented cross-API lifecycle boundary,
        not proof that the condition id was malformed.
        """
        groups = api.holders(busy_market["conditionId"], limit=3, client=client)
        wallet = next(
            (h["proxyWallet"] for g in groups for h in (g.get("holders") or []) if h.get("proxyWallet")),
            None,
        )
        if not wallet:
            pytest.skip("no holders reported right now")
        positions = api.positions(str(wallet), limit=5, client=client)
        assert positions
        for position in positions:
            condition_id = str(position["conditionId"])
            found = api.markets_by_condition_id([condition_id], client=client)
            if found:
                assert condition_id in found
                return
        pytest.skip("Gamma has retired the markets named by this wallet's current positions")


class TestSearch:
    """The only text search Polymarket offers, and a third paging style."""

    def test_it_finds_events_by_subject(self, client) -> None:
        rows = api.search_page("bitcoin", client=client)
        assert rows
        assert any("bitcoin" in (r.get("title") or "").lower() for r in rows)

    def test_it_exposes_the_authoritative_continuation_signal(self, client) -> None:
        rows, has_more = api.search_page_with_more("trump", client=client)
        assert rows and isinstance(has_more, bool)

    def test_pages_are_distinct(self, client) -> None:
        """Paged by 1-based page number, not offset or cursor."""
        first = {r["id"] for r in api.search_page("trump", page=1, client=client)}
        second = {r["id"] for r in api.search_page("trump", page=2, client=client)}
        assert first and second
        assert first != second, "page 2 repeated page 1"

    def test_results_carry_every_event_column(self, client) -> None:
        """search() reuses EVENT_SCHEMA, so the payload must cover it."""
        from vgi_polymarket.schemas import EVENT_SCHEMA, batch_from_rows, event_row

        rows = api.search_page("election", client=client)
        batch = batch_from_rows([event_row(r) for r in rows], EVENT_SCHEMA)
        assert batch.num_rows == len(rows)
        assert batch.column("event_id").null_count == 0
        assert batch.column("title").null_count == 0

    def test_an_unmatched_query_is_empty_not_an_error(self, client) -> None:
        assert api.search_page("zzzqqqxxnotathing", client=client) == []


class TestActivity:
    """A wallet's history — the past to positions()'s present."""

    @pytest.fixture(scope="class")
    @classmethod
    def a_wallet(cls, client, busy_market) -> str:
        for group in api.holders(busy_market["conditionId"], limit=5, client=client):
            for holder in group.get("holders") or []:
                if wallet := holder.get("proxyWallet"):
                    return str(wallet)
        pytest.skip("no holders reported right now")

    def test_a_wallet_has_activity(self, client, a_wallet: str) -> None:
        rows = api.activity(a_wallet, limit=20, client=client)
        assert rows
        assert all(r["proxyWallet"].lower() == a_wallet.lower() for r in rows)

    def test_every_type_is_one_we_declare(self, client, a_wallet: str) -> None:
        """The `activity_type` choices must not drift from what the API emits."""
        import typing

        from vgi_polymarket.trading import ActivityArgs

        # `from __future__ import annotations` makes the field type a string,
        # so the Arg metadata has to be resolved rather than read off the field.
        hints = typing.get_type_hints(ActivityArgs, include_extras=True)
        declared = set(hints["activity_type"].__metadata__[0].choices) - {""}
        seen = {r.get("type") for r in api.activity(a_wallet, limit=300, client=client)}
        assert seen <= declared, f"undeclared activity types: {sorted(seen - declared)}"

    def test_the_type_filter_narrows(self, client, a_wallet: str) -> None:
        rows = api.activity(a_wallet, activity_type="TRADE", limit=50, client=client)
        if not rows:
            pytest.skip("this wallet has no trades in range")
        assert {r["type"] for r in rows} == {"TRADE"}

    def test_a_non_trade_entry_reports_no_price(self, client, a_wallet: str) -> None:
        """A zero price would read as a real price, so it is nulled."""
        from vgi_polymarket.schemas import activity_row

        rows = api.activity(a_wallet, limit=300, client=client)
        non_trades = [r for r in rows if r.get("type") != "TRADE"]
        if not non_trades:
            pytest.skip("this wallet has only trades in range")
        assert all(activity_row(r)["price"] is None for r in non_trades)
