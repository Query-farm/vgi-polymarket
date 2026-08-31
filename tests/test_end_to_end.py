"""SQL executed against a real ATTACH, over both transports.

This file exists because of a bug no unit test could see. ``markets`` and
``events`` declared ``filter_pushdown = True`` without ``auto_apply_filters =
True``. Declaring the first makes the engine drop its own filter above the scan
— it now believes the worker applies the predicate — while the filters are only
*delivered* when the second is set too. So nobody filtered anything, and
``WHERE slug = 'NO-SUCH-SLUG'`` returned rows. No error, just wrong answers.

Every test in the rest of the suite passed throughout, because they all call the
API layer or the schema helpers directly. Nothing exercised the contract the
engine actually holds the worker to. That contract is what this file tests.

The engine is `Haybarn <https://github.com/Query-farm-haybarn/haybarn>`_, Query
Farm's DuckDB distribution and what VGI ships for. Both transports are covered:
stdio, where the engine launches the worker itself, and HTTP, where the worker
runs as a service. They share almost no code — stdio pipes Arrow IPC frames over
the process's own stdin and stdout, while HTTP wraps each call in a compressed
body through a WSGI stack — so a fault in one is invisible from the other.

Marked ``live``: a real ATTACH necessarily talks to Polymarket.
"""

from __future__ import annotations

import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.live

ROOT = Path(__file__).resolve().parent.parent

#: Run the worker out of this checkout rather than whatever ``uv`` would resolve
#: from the test process's working directory.
STDIO_LOCATION = f"uv run --project {ROOT} {ROOT / 'polymarket_worker.py'}"


def _run(location: str, sql: str, *, expect_error: bool = False) -> str:
    """Attach the worker at ``location`` and run ``sql``, returning the output.

    ``.mode list`` keeps the output one value per line, which is all these
    assertions need and far easier to read in a failure than a box-drawn table.
    """
    script = f"ATTACH 'polymarket' (TYPE vgi, LOCATION '{location}');\n.mode list\n{sql}\n"
    try:
        done = subprocess.run(
            ["uvx", "haybarn-cli"],
            input=script,
            capture_output=True,
            text=True,
            timeout=600,
            cwd=ROOT,
        )
    except FileNotFoundError:  # pragma: no cover - environment, not the worker
        pytest.skip("haybarn-cli is not available")
    except subprocess.TimeoutExpired:  # pragma: no cover
        pytest.fail(f"query did not finish in time: {sql}")
    output = done.stdout + done.stderr
    if expect_error:
        return output
    assert "Error" not in output, output
    return output


def _lines(output: str) -> list[str]:
    return [line.strip() for line in output.splitlines() if line.strip()]


def _is_true(output: str) -> bool:
    return "true" in [line.lower() for line in _lines(output)]


@pytest.fixture(scope="module")
def stdio() -> str:
    """The worker launched by the engine, one process per connection."""
    return STDIO_LOCATION


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def http() -> Iterator[str]:
    """The worker running as an HTTP service; yields its base URL."""
    port = _free_port()
    server = subprocess.Popen(
        ["uv", "run", "--project", str(ROOT), str(ROOT / "serve.py"), "--port", str(port)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if server.poll() is not None:  # pragma: no cover - environment
                pytest.skip("the HTTP server exited before it was ready")
            with socket.socket() as probe:
                probe.settimeout(0.5)
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(0.5)
        else:  # pragma: no cover - environment, not the worker
            pytest.skip("the HTTP server did not come up in time")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.terminate()
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:  # pragma: no cover
            server.kill()


class TestUnfilteredScansStillWork:
    """The control. Without it, filtering everything out would look like a pass."""

    def test_markets_returns_rows(self, stdio: str) -> None:
        out = _run(stdio, "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() LIMIT 5);")
        assert "5" in _lines(out)

    def test_events_returns_rows(self, stdio: str) -> None:
        out = _run(stdio, "SELECT count(*) FROM (SELECT slug FROM polymarket.main.events() LIMIT 5);")
        assert "5" in _lines(out)


class TestPredicatesAreActuallyApplied:
    """A predicate that matches nothing must return nothing.

    These are the exact queries that returned 5 before the fix.
    """

    @pytest.mark.parametrize(
        ("label", "sql"),
        [
            (
                "market slug",
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE slug = 'NO-SUCH-SLUG' LIMIT 5);",
            ),
            (
                "event slug",
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.events() "
                "WHERE slug = 'NOPE' LIMIT 5);",
            ),
            (
                "condition id",
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE condition_id = '0xdeadbeef' LIMIT 5);",
            ),
        ],
    )
    def test_an_impossible_predicate_returns_nothing(self, stdio: str, label: str, sql: str) -> None:
        assert "0" in _lines(_run(stdio, sql)), label


class TestPredicatesKeepTheRowsTheyShould:
    """The other half: filtering must not be over-eager either.

    A fix that returned nothing for everything would satisfy the tests above.
    """

    def test_a_real_slug_matches_its_own_market(self, stdio: str) -> None:
        out = _run(
            stdio,
            "SELECT bool_and(m.slug = (SELECT slug FROM polymarket.main.markets() LIMIT 1)) "
            "FROM polymarket.main.market("
            "(SELECT condition_id FROM polymarket.main.markets() LIMIT 1)) m;",
        )
        assert _is_true(out)

    def test_a_decimal_predicate_returns_rows_that_satisfy_it(self, stdio: str) -> None:
        """Decimal columns compared against integer literals.

        Arrow can refuse ``decimal128(p, s) > <int64>`` outright — *"Precision
        is not great enough for the result"* — which is why this is asserted
        rather than assumed. Bounded by an inner LIMIT so the scan stays cheap.
        """
        out = _run(
            stdio,
            "SELECT bool_and(volume_24hr > 1000) FROM (SELECT volume_24hr FROM ("
            "SELECT volume_24hr FROM polymarket.main.markets(closed => 'false', "
            "order_by => 'volume24hr', ascending => 'false') LIMIT 200"
            ") WHERE volume_24hr > 1000 LIMIT 5);",
        )
        assert _is_true(out)

    def test_a_boolean_predicate_returns_rows_that_satisfy_it(self, stdio: str) -> None:
        out = _run(
            stdio,
            "SELECT bool_and(NOT closed) FROM (SELECT closed FROM polymarket.main.markets("
            "closed => 'false') WHERE closed = false LIMIT 20);",
        )
        assert _is_true(out)


class TestAnUnpushableSelectivePredicateFailsLoudly:
    """The one case that cannot return 0, and must not return wrong rows either.

    ``volume_24hr`` has no Gamma filter, so a predicate on it can only be
    evaluated after fetching. Proving that nothing matches therefore means
    walking the whole market catalog — measured at over 120,000 rows and still
    not exhausted after 1,200 pages and 155 seconds.

    Before pushdown was applied this query returned 5 unfiltered rows instantly.
    It now raises ``PolymarketPageLimitError`` instead, which is the right
    trade: a loud failure naming the fix beats a silent wrong answer.
    """

    #: One walk of the catalog, reused by both assertions below — it is the
    #: slowest query in the suite and there is no reason to run it twice.
    SQL = (
        "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
        "WHERE volume_24hr > 999999999999 LIMIT 5);"
    )

    @pytest.fixture(scope="class")
    @classmethod
    def failure(cls, stdio: str) -> str:
        return _run(stdio, cls.SQL, expect_error=True)

    def test_it_raises_rather_than_returning_unfiltered_rows(self, failure: str) -> None:
        assert "PageLimit" in failure or "still paging" in failure, failure

    def test_the_error_names_a_way_out(self, failure: str) -> None:
        """An error a caller cannot act on is only marginally better than a wrong answer."""
        assert "tag_id" in failure or "search()" in failure or "LIMIT" in failure, failure


class TestAPushablePredicateAvoidsTheScanEntirely:
    """The escape hatch the error message points at, demonstrated.

    ``slug`` and ``condition_id`` map onto real Gamma filters, so an impossible
    value on one of them is answered by an empty first page rather than by
    walking 120,000 rows. This is the difference between the two halves of the
    pushdown story: translating a predicate into a query parameter is what keeps
    a selective filter cheap, and applying it client-side is what keeps it
    *correct*. Both are needed, and this asserts the first still works.
    """

    def test_an_impossible_slug_resolves_without_walking_the_catalog(self, stdio: str) -> None:
        start = time.monotonic()
        out = _run(
            stdio,
            "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
            "WHERE slug = 'NO-SUCH-SLUG-ANYWHERE' LIMIT 5);",
        )
        elapsed = time.monotonic() - start
        assert "0" in _lines(out)
        # A full walk took 155s+ and never finished; anything in this range can
        # only be the filter reaching Gamma. Generous, because the bound covers
        # engine startup as well as the query.
        assert elapsed < 60, f"took {elapsed:.1f}s — the slug filter is no longer pushed down"


class TestProjectionAndFilterTogether:
    """Filters are evaluated positionally against the *projected* batch.

    A pushed filter carries a column index relative to the projection the engine
    asked for, so a narrow SELECT plus a WHERE on a different column is the
    shape that catches an implementation evaluating against the full schema.
    """

    def test_a_narrow_projection_with_a_filter_on_another_column(self, stdio: str) -> None:
        out = _run(
            stdio,
            "SELECT count(*) FROM (SELECT question FROM polymarket.main.markets() "
            "WHERE slug = 'NO-SUCH-SLUG' LIMIT 5);",
        )
        assert "0" in _lines(out)

    def test_the_projected_column_is_the_one_returned(self, stdio: str) -> None:
        out = _run(
            stdio,
            "SELECT count(*) FROM (SELECT question FROM polymarket.main.markets() "
            "WHERE question IS NOT NULL LIMIT 3);",
        )
        assert "3" in _lines(out)


class TestHttpTransport:
    """The same catalog, reached over HTTP instead of stdio."""

    def test_a_table_scans(self, http: str) -> None:
        out = _run(http, "SELECT count(*) FROM (SELECT tag_id FROM polymarket.main.tags LIMIT 25);")
        assert "25" in _lines(out)

    def test_a_paged_scan_streams(self, http: str) -> None:
        out = _run(http, "SELECT count(*) FROM (SELECT question FROM polymarket.main.markets() LIMIT 5);")
        assert "5" in _lines(out)

    def test_decimals_survive_the_transport(self, http: str) -> None:
        """Arrow types must round-trip through the compressed body unchanged."""
        out = _run(http, "SELECT typeof(volume_24hr) FROM polymarket.main.markets() LIMIT 1;")
        assert "DECIMAL(28,6)" in _lines(out)

    def test_a_lateral_chain_works(self, http: str) -> None:
        """The batched-LATERAL exchange is the most transport-sensitive path."""
        out = _run(
            http,
            "SELECT bool_and(q.best_ask >= q.best_bid) IS NOT FALSE FROM ("
            "SELECT outcomes FROM polymarket.main.markets(closed => 'false', "
            "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
            "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
            "LATERAL polymarket.main.quote(o.token_id) q;",
        )
        assert _is_true(out)

    def test_filter_pushdown_applies_over_http_too(self, http: str) -> None:
        """Pushdown is negotiated per call, so it is worth checking per transport."""
        out = _run(
            http,
            "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
            "WHERE slug = 'NO-SUCH-SLUG' LIMIT 5);",
        )
        assert "0" in _lines(out)

    def test_a_wide_many_row_result_arrives_intact(self, http: str) -> None:
        """The shape most likely to expose a framing or compression fault."""
        out = _run(
            http,
            "SELECT bool_and(t.price BETWEEN 0 AND 1) IS NOT FALSE FROM ("
            "SELECT condition_id FROM polymarket.main.markets(closed => 'false', "
            "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
            "LATERAL polymarket.main.trades(m.condition_id) t;",
        )
        assert _is_true(out)
