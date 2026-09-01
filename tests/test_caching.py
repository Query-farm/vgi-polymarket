"""Cache policy: what Polymarket declares, and what we forward to DuckDB.

Polymarket's own headers drive this. The Gamma catalog sends
`Cache-Control: public, max-age=300`, `/tags` says 1800, `/prices-history` says
90, and the live CLOB endpoints declare nothing at all. Nothing sends an ETag or
Last-Modified, so no result can ever be cheaply revalidated — only expired.
"""

from __future__ import annotations

import httpx
import pytest

from vgi_polymarket.markets import origin_cache_control
from vgi_polymarket.polymarket_api import CacheHint


def _response(cache_control: str | None) -> httpx.Response:
    headers = {"cache-control": cache_control} if cache_control is not None else {}
    return httpx.Response(200, headers=headers)


class TestCacheHint:
    @pytest.mark.parametrize(
        ("directive", "expected"),
        [("public, max-age=300", 300), ("public, max-age=1800", 1800), ("public, max-age=90", 90)],
    )
    def test_reads_the_declared_max_age(self, directive: str, expected: int) -> None:
        hint = CacheHint()
        hint.observe(_response(directive))
        assert hint.max_age == expected
        assert hint.cacheable

    def test_absent_directive_marks_live(self) -> None:
        """The CLOB's book and quote endpoints declare nothing."""
        hint = CacheHint()
        hint.observe(_response(None))
        assert hint.max_age is None
        assert not hint.cacheable

    def test_shortest_page_bounds_a_paged_call(self) -> None:
        hint = CacheHint()
        hint.observe(_response("max-age=1800"))
        hint.observe(_response("max-age=300"))
        assert hint.max_age == 300

    def test_one_uncacheable_page_poisons_the_whole_result(self) -> None:
        hint = CacheHint()
        hint.observe(_response("max-age=300"))
        hint.observe(_response(None))
        assert not hint.cacheable

    @pytest.mark.parametrize(
        "directive", ["no-store, max-age=300", "max-age=300, no-cache", "private, max-age=300"]
    )
    def test_a_reuse_ban_beats_a_max_age(self, directive: str) -> None:
        """Reading only the max-age would license stale serving the origin refused."""
        hint = CacheHint()
        hint.observe(_response(directive))
        assert not hint.cacheable

    def test_max_age_zero_is_not_a_cache_entry(self) -> None:
        hint = CacheHint()
        hint.observe(_response("public, max-age=0"))
        assert not hint.cacheable

    def test_max_age_is_matched_on_a_directive_boundary(self) -> None:
        hint = CacheHint()
        hint.observe(_response("s-maxage=99, max-age=300"))
        assert hint.max_age == 300


class TestNoOptInCaching:
    """There is no `cache_ttl` argument, and that is deliberate.

    It only ever governed the two CLOB endpoints that declare no freshness —
    and those are batched 200 tokens to a request, so the memoization it would
    have bought is small. Everything else runs on Polymarket's own declared
    TTLs. More to the point it was a *named* argument on a table-in-out
    function, which DuckDB cannot bind: nobody could ever have passed it.
    """

    def test_no_function_declares_a_cache_ttl_argument(self) -> None:
        import typing

        from vgi_polymarket.worker import _POLYMARKET_CATALOG

        offenders = []
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            args = getattr(function, "__orig_bases__", None)
            for parameter in function.get_metadata().parameters:
                if parameter.name == "cache_ttl":
                    offenders.append(function.Meta.name)
        assert offenders == [], f"cache_ttl is unreachable from SQL: {offenders}"


class TestOriginPolicy:
    def test_forwards_the_declared_ttl(self) -> None:
        control = origin_cache_control(CacheHint(max_age=300))
        assert control is not None
        assert control.ttl == 300
        assert control.stale_if_error == 300

    def test_live_data_is_left_uncached(self) -> None:
        assert origin_cache_control(CacheHint(saw_uncacheable=True)) is None


class TestRetry:
    """All three hosts sit behind Cloudflare and none sends Retry-After."""

    def test_retries_then_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_polymarket import polymarket_api as api

        monkeypatch.setattr(api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(429, json={"error": "rate limited"})
            return httpx.Response(200, json=[], headers={"cache-control": "max-age=300"})

        hint = CacheHint()
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            assert api.tags_page(client=client, hint=hint) == []
        assert calls["n"] == 3
        assert hint.max_age == 300, "the served response's policy counts, not the 429s"

    def test_gives_up_and_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_polymarket import polymarket_api as api

        monkeypatch.setattr(api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(429, json={"error": "rate limited"})

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(api.PolymarketError) as excinfo:
                api.tags_page(client=client)
        assert excinfo.value.status == 429
        assert calls["n"] == api._RETRY_ATTEMPTS

    def test_a_429_never_poisons_the_cache_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A rate-limit response carries the CDN's policy, not the resource's."""
        from vgi_polymarket import polymarket_api as api

        monkeypatch.setattr(api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, headers={"cache-control": "max-age=5"})
            return httpx.Response(200, json=[], headers={"cache-control": "max-age=1800"})

        hint = CacheHint()
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            api.tags_page(client=client, hint=hint)
        assert hint.max_age == 1800

    def test_a_4xx_is_not_retried(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_polymarket import polymarket_api as api

        monkeypatch.setattr(api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(400, json={"error": "bad request"})

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(api.PolymarketError):
                api.tags_page(client=client)
        assert calls["n"] == 1

    def test_a_404_is_a_distinct_signal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """One missing token must not fail the other 299 rows in a batch."""
        from vgi_polymarket import polymarket_api as api

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"error": "No orderbook exists"})

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(api.NotFound):
                api.book("nope", client=client)
