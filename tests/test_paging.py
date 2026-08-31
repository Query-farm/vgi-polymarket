"""Paging mechanics — the failure mode here returns HTTP 200 forever.

Gamma's keyset cursor comes back as `next_cursor` and must be sent as
`after_cursor`. Sending it as `cursor` (the obvious guess, and the name the
sibling Kalshi worker uses) is accepted with HTTP 200 and silently ignored, so
the walk re-serves page one until MAX_PAGES. Nothing in the response
distinguishes that from real progress, so only an explicit test notices.
"""

from __future__ import annotations

import httpx
import pytest

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.paging import KeysetScanState, OffsetScanState


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestKeysetCursorParameter:
    def test_the_cursor_is_sent_as_after_cursor(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"markets": [{"id": "2"}], "next_cursor": ""})

        with _client(handler) as client:
            api.keyset_page("/markets/keyset", "markets", [], cursor="CURSOR1", client=client)

        assert seen.get("after_cursor") == "CURSOR1", (
            "the cursor must be sent as after_cursor; `cursor` is silently ignored by Gamma"
        )
        assert "cursor" not in seen or seen.get("cursor") != "CURSOR1"

    def test_no_cursor_is_sent_on_the_first_page(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"markets": [], "next_cursor": ""})

        with _client(handler) as client:
            api.keyset_page("/markets/keyset", "markets", [], client=client)
        assert "after_cursor" not in seen

    def test_the_page_size_is_sent(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"markets": [], "next_cursor": ""})

        with _client(handler) as client:
            api.keyset_page("/markets/keyset", "markets", [], client=client)
        assert seen.get("limit") == str(api.GAMMA_PAGE_LIMIT)


class TestKeysetTermination:
    def test_a_returned_cursor_continues_the_walk(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"markets": [{"id": "1"}], "next_cursor": "NEXT"})

        with _client(handler) as client:
            rows, cursor = api.keyset_page("/markets/keyset", "markets", [], client=client)
        assert rows and cursor == "NEXT"

    def test_an_absent_cursor_ends_the_walk(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"markets": [{"id": "1"}]})

        with _client(handler) as client:
            _, cursor = api.keyset_page("/markets/keyset", "markets", [], client=client)
        assert cursor is None

    def test_an_empty_page_ends_the_walk_even_with_a_cursor(self) -> None:
        """Otherwise a server that always returns a cursor would loop forever."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"markets": [], "next_cursor": "STILL-HERE"})

        with _client(handler) as client:
            rows, cursor = api.keyset_page("/markets/keyset", "markets", [], client=client)
        assert rows == [] and cursor is None

    def test_repeated_filters_are_sent_repeated(self) -> None:
        """Gamma's array filters repeat the key rather than comma-joining."""
        seen: list[tuple[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.extend(request.url.params.multi_items())
            return httpx.Response(200, json={"markets": [], "next_cursor": ""})

        with _client(handler) as client:
            api.keyset_page(
                "/markets/keyset",
                "markets",
                [("condition_ids", "0xa"), ("condition_ids", "0xb")],
                client=client,
            )
        assert seen.count(("condition_ids", "0xa")) == 1
        assert seen.count(("condition_ids", "0xb")) == 1


class TestScanState:
    def test_keyset_state_starts_unstarted(self) -> None:
        state = KeysetScanState()
        assert state.cursor == "" and state.done is False and state.pages == 0

    def test_offset_state_starts_at_zero(self) -> None:
        state = OffsetScanState()
        assert state.offset == 0 and state.done is False


class TestOffsetPaging:
    def test_a_full_page_continues_and_a_short_page_ends(self) -> None:
        """A short page is the only end-of-walk signal these endpoints give."""
        pages = [[{"id": str(i)} for i in range(50)], [{"id": "x"}]]
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            page = pages[calls["n"]] if calls["n"] < len(pages) else []
            calls["n"] += 1
            return httpx.Response(200, json=page)

        with _client(handler) as client:
            first = api.offset_page(api.gamma_base_url(), "/series", offset=0, client=client, page_limit=50)
            second = api.offset_page(api.gamma_base_url(), "/series", offset=50, client=client, page_limit=50)
        assert len(first) == 50
        assert len(second) < 50

    def test_the_offset_is_sent(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(dict(request.url.params))
            return httpx.Response(200, json=[])

        with _client(handler) as client:
            api.offset_page(api.gamma_base_url(), "/tags", offset=300, client=client)
        assert seen.get("offset") == "300"

    def test_series_excludes_its_inlined_events(self) -> None:
        """Without this the catalog is ~83 MB instead of ~1.5 MB."""
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(dict(request.url.params))
            return httpx.Response(200, json=[])

        with _client(handler) as client:
            api.series_page(client=client)
        assert seen.get("exclude_events") == "true"
        assert seen.get("limit") == str(api.SERIES_PAGE_LIMIT)


class TestPageLimitGuard:
    def test_a_runaway_offset_walk_raises_rather_than_truncating(self) -> None:
        """A short result that looks complete is worse than an error."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[{"id": "always-full"}] * 100)

        with _client(handler) as client:
            with pytest.raises(api.PolymarketPageLimitError):
                api._offset_all(api.data_base_url(), "/trades", client=client, page_limit=100)


class TestBatching:
    def test_books_are_chunked_at_the_token_limit(self) -> None:
        requested: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            requested.append(len(_json.loads(request.content)))
            return httpx.Response(200, json=[])

        tokens = [str(i) for i in range(api.TOKEN_BATCH_LIMIT + 25)]
        with _client(handler) as client:
            api.books(tokens, client=client)
        assert requested == [api.TOKEN_BATCH_LIMIT, 25]

    def test_duplicate_tokens_are_collapsed(self) -> None:
        """The result is a mapping the caller fans back out, so asking twice is waste."""
        requested: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            requested.append(len(_json.loads(request.content)))
            return httpx.Response(200, json=[])

        with _client(handler) as client:
            api.books(["a", "a", "b", "a"], client=client)
        assert requested == [2]

    def test_a_token_absent_from_the_response_is_simply_missing(self) -> None:
        """100 tokens returned 98 entries live; the caller must tolerate that."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[{"asset_id": "a", "bids": [], "asks": []}])

        with _client(handler) as client:
            books = api.books(["a", "b"], client=client)
        assert "a" in books and "b" not in books

    def test_condition_ids_are_chunked(self) -> None:
        seen: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(len(request.url.params.get_list("condition_ids")))
            return httpx.Response(200, json={"markets": [], "next_cursor": ""})

        ids = [f"0x{i}" for i in range(api.CONDITION_ID_BATCH_LIMIT + 10)]
        with _client(handler) as client:
            api.markets_by_condition_id(ids, client=client)
        assert seen == [api.CONDITION_ID_BATCH_LIMIT, 10]

    def test_prices_asks_for_both_sides_of_every_token(self) -> None:
        bodies: list[list[dict]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            bodies.append(_json.loads(request.content))
            return httpx.Response(200, json={})

        with _client(handler) as client:
            api.prices(["a", "b"], client=client)
        assert sorted((e["token_id"], e["side"]) for e in bodies[0]) == [
            ("a", "BUY"),
            ("a", "SELL"),
            ("b", "BUY"),
            ("b", "SELL"),
        ]


class TestConcurrentOffsetPages:
    """Offset pages are fetched concurrently — the one thing offset paging is good for.

    A cursor forces a serial walk, because page N+1's address is inside page N's
    response. An offset does not, and the tag catalog is 73 pages, so a serial
    walk measured 8.5s against well under 1s at this width.
    """

    @staticmethod
    def _emitting(pages: dict[int, list[dict]], page_limit: int):
        """A fake scan tick capturing what emit_offset_page produced."""
        import pyarrow as pa

        from vgi_polymarket.paging import OffsetScanState, emit_offset_page
        from vgi_polymarket.schemas import TAG_SCHEMA

        captured: dict[str, object] = {}

        class Out:
            def emit(self, batch, cache_control=None, **kw):
                captured["batch"] = batch
                captured["cache_control"] = cache_control

            def finish(self):
                captured["finished"] = True

        class Params:
            output_schema = TAG_SCHEMA

        def fetch(*, offset: int, hint):
            return pages.get(offset, [])

        state = OffsetScanState()
        emit_offset_page(
            Params(),
            state,
            Out(),
            fetch=fetch,
            shape=lambda r: {"tag_id": r["id"]},
            page_limit=page_limit,
        )
        assert isinstance(captured.get("batch"), pa.RecordBatch)
        return state, captured["batch"]

    def test_pages_are_concatenated_in_offset_order(self) -> None:
        """A slow page must not reshuffle the scan."""
        from vgi_polymarket.paging import PAGE_FETCH_CONCURRENCY

        limit = 2
        pages = {
            i * limit: [{"id": str(i * limit + j)} for j in range(limit)]
            for i in range(PAGE_FETCH_CONCURRENCY)
        }
        state, batch = self._emitting(pages, limit)
        assert batch.column("tag_id").to_pylist() == [str(i) for i in range(limit * PAGE_FETCH_CONCURRENCY)]
        assert state.offset == limit * PAGE_FETCH_CONCURRENCY
        assert state.done is False

    def test_a_short_page_ends_the_walk_and_truncates_the_run(self) -> None:
        """Pages fetched past the end must be discarded, not appended."""
        limit = 2
        pages = {0: [{"id": "a"}, {"id": "b"}], 2: [{"id": "c"}]}
        state, batch = self._emitting(pages, limit)
        assert batch.column("tag_id").to_pylist() == ["a", "b", "c"]
        assert state.done is True
        assert state.offset == 3

    def test_an_immediately_empty_page_ends_the_walk(self) -> None:
        state, batch = self._emitting({}, 2)
        assert batch.num_rows == 0
        assert state.done is True

    def test_the_shortest_page_bounds_the_cache_ttl(self) -> None:
        """Each concurrent fetch gets its own hint; they merge like the serial path."""
        from vgi_polymarket.paging import _merge
        from vgi_polymarket.polymarket_api import CacheHint

        into = CacheHint()
        _merge(into, CacheHint(max_age=1800))
        _merge(into, CacheHint(max_age=300))
        assert into.max_age == 300
        assert into.cacheable

    def test_one_uncacheable_page_poisons_the_merged_hint(self) -> None:
        from vgi_polymarket.paging import _merge
        from vgi_polymarket.polymarket_api import CacheHint

        into = CacheHint()
        _merge(into, CacheHint(max_age=1800))
        _merge(into, CacheHint(saw_uncacheable=True))
        assert not into.cacheable

    def test_the_fetch_really_is_concurrent(self) -> None:
        """Guard the guard: a serial implementation would take N x the time."""
        import time

        from vgi_polymarket.paging import PAGE_FETCH_CONCURRENCY, OffsetScanState, emit_offset_page
        from vgi_polymarket.schemas import TAG_SCHEMA

        class Out:
            def emit(self, batch, cache_control=None, **kw): ...
            def finish(self): ...

        class Params:
            output_schema = TAG_SCHEMA

        delay = 0.05

        def fetch(*, offset: int, hint):
            time.sleep(delay)
            return [{"id": str(offset)}] * 2

        start = time.monotonic()
        emit_offset_page(
            Params(),
            OffsetScanState(),
            Out(),
            fetch=fetch,
            shape=lambda r: {"tag_id": r["id"]},
            page_limit=2,
        )
        elapsed = time.monotonic() - start
        assert elapsed < delay * PAGE_FETCH_CONCURRENCY / 2, (
            f"{elapsed:.3f}s suggests the pages were fetched serially"
        )


class TestKeysetRunawayGuard:
    """A cursor walk must not be able to run forever.

    A LIMIT stops a keyset scan early, but an aggregate over an unfiltered
    `markets()` has no reason to stop before the end of the catalog — and a
    predicate DuckDB cannot fold to a constant (`WHERE condition_id = (SELECT
    ...)`) is exactly that. Before this guard such a query walked every page
    Polymarket has.
    """

    @staticmethod
    def _tick(state, pages_served: int = 1):
        import httpx
        import pyarrow as pa

        from vgi_polymarket.paging import emit_keyset_page
        from vgi_polymarket.schemas import TAG_SCHEMA

        captured: dict[str, object] = {}

        class Out:
            def emit(self, batch, cache_control=None, **kw):
                captured["batch"] = batch

            def finish(self):
                captured["finished"] = True

        class Params:
            output_schema = TAG_SCHEMA

        def handler(request: httpx.Request) -> httpx.Response:
            # Always another page: the runaway shape.
            return httpx.Response(200, json={"tags": [{"id": "x"}], "next_cursor": "MORE"})

        import vgi_polymarket.polymarket_api as api_mod

        original = api_mod.keyset_page

        def fake(*a, **kw):
            return [{"id": "x"}], "MORE"

        api_mod.keyset_page = fake
        try:
            emit_keyset_page(
                Params(),
                state,
                Out(),
                path="/markets/keyset",
                key="markets",
                filters=[],
                shape=lambda r: {"tag_id": r["id"]},
            )
        finally:
            api_mod.keyset_page = original
        assert isinstance(captured.get("batch"), pa.RecordBatch) or captured.get("finished")

    def test_a_page_is_counted_each_tick(self) -> None:
        state = KeysetScanState()
        self._tick(state)
        self._tick(state)
        assert state.pages == 2

    def test_a_runaway_walk_raises_rather_than_truncating(self) -> None:
        state = KeysetScanState(pages=api.MAX_PAGES)
        with pytest.raises(api.PolymarketPageLimitError):
            self._tick(state)

    def test_the_guard_does_not_fire_below_the_cap(self) -> None:
        state = KeysetScanState(pages=api.MAX_PAGES - 1)
        self._tick(state)
        assert state.pages == api.MAX_PAGES
