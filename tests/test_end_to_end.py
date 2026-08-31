"""SQL executed against a real ATTACH.

This file exists because of a bug no unit test could see. ``markets`` and
``events`` declared ``filter_pushdown = True`` without ``auto_apply_filters =
True``. Declaring the first makes the DuckDB extension drop its own filter above
the scan — it now believes the worker applies the predicate — while the filters
are only *delivered* when the second is set too. So nobody filtered anything,
and ``WHERE slug = 'NO-SUCH-SLUG'`` returned rows. No error, just wrong answers.

Every test in the rest of the suite passed throughout, because they all call the
API layer or the schema helpers directly. Nothing exercised the contract the
extension actually holds the worker to. That contract is what this file tests.

Marked ``live``: a real ATTACH necessarily talks to Polymarket.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.live

ROOT = Path(__file__).resolve().parent.parent

#: Run the worker out of this checkout rather than whatever `uv` would resolve
#: from the test process's working directory.
LOCATION = f"uv run --project {ROOT} {ROOT / 'polymarket_worker.py'}"


@pytest.fixture(scope="module")
def con() -> Iterator[Any]:
    """A DuckDB connection with the worker attached.

    Unsigned extensions are allowed because the vgi extension ships from the
    community repository unsigned; this is a test process, not a deployment.
    ``pytz`` is imported eagerly because DuckDB needs it to materialize
    ``TIMESTAMP WITH TIME ZONE``, which most of these columns are.
    """
    duckdb = pytest.importorskip("duckdb")
    pytest.importorskip("pytz")
    connection = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        connection.execute("INSTALL vgi FROM community")
        connection.execute("LOAD vgi")
        connection.execute(f"ATTACH 'polymarket' (TYPE vgi, LOCATION '{LOCATION}')")
    except Exception as exc:  # pragma: no cover - environment, not the worker
        pytest.skip(f"cannot attach the worker: {exc}")
    yield connection
    connection.close()


def _scalar(con: Any, sql: str) -> Any:
    return con.execute(sql).fetchall()[0][0]


class TestUnfilteredScansStillWork:
    """The control. Without it, filtering everything out would look like a pass."""

    def test_markets_returns_rows(self, con: Any) -> None:
        assert _scalar(con, "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() LIMIT 5)") == 5

    def test_events_returns_rows(self, con: Any) -> None:
        assert _scalar(con, "SELECT count(*) FROM (SELECT slug FROM polymarket.main.events() LIMIT 5)") == 5


class TestPredicatesAreActuallyApplied:
    """A predicate that matches nothing must return nothing.

    These are the exact queries that returned 5 before the fix.
    """

    def test_an_impossible_market_slug_returns_nothing(self, con: Any) -> None:
        assert (
            _scalar(
                con,
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE slug = 'NO-SUCH-SLUG' LIMIT 5)",
            )
            == 0
        )

    def test_an_impossible_event_slug_returns_nothing(self, con: Any) -> None:
        assert (
            _scalar(
                con,
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.events() "
                "WHERE slug = 'NOPE' LIMIT 5)",
            )
            == 0
        )

    def test_an_impossible_condition_id_returns_nothing(self, con: Any) -> None:
        assert (
            _scalar(
                con,
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE condition_id = '0xdeadbeef' LIMIT 5)",
            )
            == 0
        )


class TestPredicatesKeepTheRowsTheyShould:
    """The other half: filtering must not be over-eager either.

    A fix that returned nothing for everything would satisfy the tests above.
    """

    def test_a_real_slug_returns_exactly_that_market(self, con: Any) -> None:
        slug = _scalar(con, "SELECT slug FROM polymarket.main.markets() LIMIT 1")
        rows = con.execute(f"SELECT slug FROM polymarket.main.markets() WHERE slug = '{slug}'").fetchall()
        assert rows, "a slug taken from the catalog matched nothing"
        assert {r[0] for r in rows} == {slug}

    def test_a_decimal_predicate_returns_rows_that_satisfy_it(self, con: Any) -> None:
        """Decimal columns compared against integer literals.

        Arrow can refuse ``decimal128(p, s) > <int64>`` outright — *"Precision
        is not great enough for the result"* — which is why this is asserted
        rather than assumed. Bounded by an inner LIMIT so the scan stays cheap.
        """
        rows = con.execute(
            "SELECT slug, volume_24hr FROM ("
            "SELECT slug, volume_24hr FROM polymarket.main.markets("
            "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 200"
            ") WHERE volume_24hr > 1000 LIMIT 5"
        ).fetchall()
        assert rows, "no market in the busiest 200 had 24h volume over 1000"
        assert all(volume > 1000 for _, volume in rows)

    def test_a_boolean_predicate_returns_rows_that_satisfy_it(self, con: Any) -> None:
        rows = con.execute(
            "SELECT closed FROM polymarket.main.markets(closed => 'false') WHERE closed = false LIMIT 20"
        ).fetchall()
        assert rows
        assert all(closed is False for (closed,) in rows)


class TestAnUnpushableSelectivePredicateFailsLoudly:
    """The one case that cannot return 0, and must not return wrong rows either.

    ``volume_24hr`` has no Gamma filter, so a predicate on it can only be
    evaluated after fetching. Proving that nothing matches therefore means
    walking the whole market catalog — measured at over 120,000 rows and still
    not exhausted after 1,200 pages and 155 seconds.

    Before pushdown was applied this query returned 5 unfiltered rows instantly.
    It now raises :class:`PolymarketPageLimitError` instead, which is the right
    trade: a loud failure naming the fix beats a silent wrong answer. This test
    pins that, so the behaviour cannot regress back to either extreme.
    """

    def test_it_raises_rather_than_returning_unfiltered_rows(self, con: Any) -> None:
        with pytest.raises(Exception) as excinfo:
            con.execute(
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE volume_24hr > 999999999999 LIMIT 5)"
            ).fetchall()
        message = str(excinfo.value)
        assert "PageLimit" in message or "still paging" in message, message

    def test_the_error_names_a_way_out(self, con: Any) -> None:
        """An error a caller cannot act on is only marginally better than a wrong answer."""
        with pytest.raises(Exception) as excinfo:
            con.execute(
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE volume_24hr > 999999999999 LIMIT 5)"
            ).fetchall()
        message = str(excinfo.value)
        assert "tag_id" in message or "search()" in message or "LIMIT" in message, message


class TestAPushablePredicateAvoidsTheScanEntirely:
    """The escape hatch the error message points at, demonstrated.

    ``slug`` and ``condition_id`` map onto real Gamma filters, so an impossible
    value on one of them is answered by an empty first page rather than by
    walking 120,000 rows. This is the difference between the two halves of the
    pushdown story: translating a predicate into a query parameter is what keeps
    a selective filter cheap, and applying it client-side is what keeps it
    *correct*. Both are needed, and this asserts the first still works.
    """

    def test_an_impossible_slug_resolves_without_walking_the_catalog(self, con: Any) -> None:
        import time

        start = time.monotonic()
        assert (
            _scalar(
                con,
                "SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() "
                "WHERE slug = 'NO-SUCH-SLUG-ANYWHERE' LIMIT 5)",
            )
            == 0
        )
        elapsed = time.monotonic() - start
        # A full walk took 155s+ and never finished; anything in this range can
        # only be the filter reaching Gamma.
        assert elapsed < 30, f"took {elapsed:.1f}s — the slug filter is no longer pushed down"

    def test_a_real_slug_is_equally_direct(self, con: Any) -> None:
        """Guard the guard: the timing above must not pass by matching nothing."""
        slug = _scalar(con, "SELECT slug FROM polymarket.main.markets() LIMIT 1")
        assert (
            _scalar(
                con,
                f"SELECT count(*) FROM (SELECT slug FROM polymarket.main.markets() WHERE slug = '{slug}')",
            )
            >= 1
        )


class TestProjectionAndFilterTogether:
    """Filters are evaluated positionally against the *projected* batch.

    A pushed filter carries a column index relative to the projection DuckDB
    asked for, so a narrow SELECT plus a WHERE on a different column is the
    shape that catches an implementation evaluating against the full schema.
    """

    def test_a_narrow_projection_with_a_filter_on_another_column(self, con: Any) -> None:
        assert (
            _scalar(
                con,
                "SELECT count(*) FROM (SELECT question FROM polymarket.main.markets() "
                "WHERE slug = 'NO-SUCH-SLUG' LIMIT 5)",
            )
            == 0
        )

    def test_the_projected_column_is_the_one_returned(self, con: Any) -> None:
        slug = _scalar(con, "SELECT slug FROM polymarket.main.markets() LIMIT 1")
        rows = con.execute(
            f"SELECT question FROM polymarket.main.markets() WHERE slug = '{slug}' LIMIT 1"
        ).fetchall()
        assert rows and isinstance(rows[0][0], str) and rows[0][0]
