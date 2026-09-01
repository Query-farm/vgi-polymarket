"""The catalog shape the DuckDB extension will see on ATTACH."""

from __future__ import annotations

import json
import re
from pathlib import Path

from vgi_polymarket.markets import (
    EventFunction,
    EventMarketsFunction,
    EventsFunction,
    MarketFunction,
    MarketsFunction,
    SearchFunction,
)
from vgi_polymarket.reference import AllSeriesFunction, AllTagsFunction
from vgi_polymarket.trading import (
    ActivityFunction,
    FillPriceFunction,
    HoldersFunction,
    OrderbookFunction,
    PositionsFunction,
    PriceHistoryFunction,
    QuoteFunction,
    TradesFunction,
)
from vgi_polymarket.worker import _POLYMARKET_CATALOG

PACKAGE = Path(__file__).resolve().parent.parent / "vgi_polymarket"

#: Bounded fetches: one market, one book, one price curve. These stay blended,
#: so they compose as the inner side of a correlated LATERAL.
BLENDED = [
    MarketFunction,
    EventFunction,
    EventMarketsFunction,
    OrderbookFunction,
    QuoteFunction,
    PriceHistoryFunction,
    TradesFunction,
    HoldersFunction,
    PositionsFunction,
    ActivityFunction,
    FillPriceFunction,
]

#: Cursor-paged endpoints. These are stateful scans: they emit one API page per
#: tick so a LIMIT stops early, which a blended function cannot do.
PAGED_SCANS = [MarketsFunction, EventsFunction]

#: Offset-paged catalog scans behind the two tables.
TABLE_SCANS = [AllTagsFunction, AllSeriesFunction]

#: Page-numbered scan. `/public-search` paginates by page number — a third style
#: on the same host — so it gets its own emitter and its own state semantics.
SEARCH_SCANS = [SearchFunction]


class TestBlendedRegistration:
    """Blended functions are what make correlated LATERAL work."""

    def test_positional_args_are_input_columns(self) -> None:
        for func in BLENDED:
            assert func.get_metadata().input_from_args is True, func.Meta.name

    def test_no_finalize_override(self) -> None:
        """DuckDB rejects LATERAL on a table function registering a finalize callback."""
        for func in BLENDED:
            assert func.has_finalize_override() is False, func.Meta.name


class TestPagedScans:
    """A paged endpoint must be a stateful scan, not a blended map.

    A blended function has to emit everything for its input in one `process()`
    call, so it walks every page before DuckDB sees a row — a `LIMIT 10` pays
    for the whole catalog, and since a scan blocked inside its first batch
    cannot be cancelled, the query wedges the client rather than merely being
    slow.
    """

    def test_paged_scans_are_not_blended(self) -> None:
        for func in [*PAGED_SCANS, *TABLE_SCANS, *SEARCH_SCANS]:
            assert func.get_metadata().input_from_args is False, func.Meta.name

    def test_keyset_scans_carry_cursor_state(self) -> None:
        from vgi_polymarket.paging import KeysetScanState

        for func in PAGED_SCANS:
            state = func.initial_state(None)  # type: ignore[arg-type]
            assert isinstance(state, KeysetScanState), func.Meta.name
            assert state.cursor == "" and state.done is False, func.Meta.name

    def test_table_scans_carry_offset_state(self) -> None:
        from vgi_polymarket.paging import OffsetScanState

        for func in [*TABLE_SCANS, *SEARCH_SCANS]:
            state = func.initial_state(None)  # type: ignore[arg-type]
            assert isinstance(state, OffsetScanState), func.Meta.name
            assert state.offset == 0 and state.done is False, func.Meta.name

    def test_every_function_is_classified(self) -> None:
        """Neither list can silently rot as functions are added."""
        schema = _POLYMARKET_CATALOG.schemas[0]
        classified = {f.Meta.name for f in [*BLENDED, *PAGED_SCANS, *TABLE_SCANS, *SEARCH_SCANS]}
        assert {f.Meta.name for f in schema.functions} == classified


class TestPushdownFlags:
    """`filter_pushdown` and `auto_apply_filters` must be declared together.

    This pairing is not a style preference. Declaring `filter_pushdown` makes
    the DuckDB extension drop its own filter above the scan — it now believes
    the worker applies the predicate. But the filters are only *delivered*
    (as `params.current_pushdown_filters`, and to the collector wrapper that
    applies them) when `auto_apply_filters` is set as well.

    With one flag and not the other, nobody filters anything and no error is
    raised: `WHERE slug = 'NO-SUCH-SLUG'` returned rows. That shipped, because
    no unit test can see it — the worker's own tests never run a predicate
    through DuckDB. `tests/test_end_to_end.py` is the test that can.
    """

    @staticmethod
    def _meta_flag(func: type, name: str) -> bool:
        return bool(getattr(func.Meta, name, False))

    def test_filter_pushdown_implies_auto_apply(self) -> None:
        offenders = [
            f.Meta.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if self._meta_flag(f, "filter_pushdown") and not self._meta_flag(f, "auto_apply_filters")
        ]
        assert offenders == [], (
            "these accept filter pushdown but never receive the filters, so every "
            f"predicate on them is silently discarded: {offenders}"
        )

    def test_auto_apply_implies_filter_pushdown(self) -> None:
        """The reverse pairing: asking for filters without accepting pushdown."""
        offenders = [
            f.Meta.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if self._meta_flag(f, "auto_apply_filters") and not self._meta_flag(f, "filter_pushdown")
        ]
        assert offenders == [], offenders

    def test_filtering_functions_keep_projection_pushdown(self) -> None:
        """A pushed filter carries a column index relative to the projection.

        `ConstantFilter.evaluate` reads `batch.column(column_index)`
        positionally, so the batch must be the projected one — which is what
        `paging.py` emits. Turning projection off would make those indices
        address the wrong columns, and every row would pass.
        """
        offenders = [
            f.Meta.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if self._meta_flag(f, "filter_pushdown") and not self._meta_flag(f, "projection_pushdown")
        ]
        assert offenders == [], offenders

    def test_the_functions_that_declare_pushdown_are_the_expected_ones(self) -> None:
        """Guard the guard: the checks above are vacuous if nothing declares it."""
        declaring = {
            f.Meta.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if self._meta_flag(f, "filter_pushdown")
        }
        assert declaring == {"markets", "events"}, declaring

    def test_no_blended_function_declares_filter_pushdown(self) -> None:
        """The framework's filtering collector corrupts 1->N provenance.

        `_FilteringOutputCollector.emit` filters the batch but passes
        `parent_rows` through unchanged, so a blended function's mapping from
        output rows back to input rows would no longer line up. A blended
        function that wants pushdown has to filter the batch and `parent_rows`
        in lockstep itself.
        """
        blended = {f.Meta.name for f in BLENDED}
        offenders = [
            f.Meta.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if f.Meta.name in blended and self._meta_flag(f, "filter_pushdown")
        ]
        assert offenders == [], (
            "a blended function accepting pushdown needs parent_rows filtered in "
            f"lockstep, which the framework does not do: {offenders}"
        )


class TestEveryArgumentIsReachable:
    """A declared argument nobody can pass is worse than no argument.

    DuckDB cannot bind a *named* argument on a table-in-out function — with or
    without LATERAL, with a literal or an explicit cast — and the framework
    rejects a positional with a default ("positional arguments cannot have a
    default value ... the default would never fire"). Between them, an optional
    argument on a blended function is unreachable: it appears in the signature,
    it is advertised in the catalog, and every attempt to use it is a binder
    error.

    So the blended functions take required positionals only. This test is what
    stops a well-meaning `cache_ttl` from being added back.
    """

    def test_no_blended_function_declares_a_named_argument(self) -> None:
        blended = {f.Meta.name for f in BLENDED}
        offenders: list[tuple[str, str]] = []
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            if function.Meta.name not in blended:
                continue
            for parameter in function.get_metadata().parameters:
                if isinstance(parameter.position, str):
                    offenders.append((function.Meta.name, parameter.name))
        assert offenders == [], (
            "these are named arguments on table-in-out functions, which DuckDB cannot "
            f"bind — nobody can pass them: {offenders}"
        )

    def test_blended_positionals_are_all_required(self) -> None:
        """Guard the other half: the framework rejects a positional default."""
        blended = {f.Meta.name for f in BLENDED}
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            if function.Meta.name not in blended:
                continue
            for parameter in function.get_metadata().parameters:
                assert parameter.required, f"{function.Meta.name}.{parameter.name}"

    def test_the_scans_may_still_take_named_arguments(self) -> None:
        """The restriction is specific to table-in-out functions."""
        named = {
            p.name
            for f in _POLYMARKET_CATALOG.schemas[0].functions
            if f.Meta.name == "markets"
            for p in f.get_metadata().parameters
            if isinstance(p.position, str)
        }
        assert "tag_id" in named and "closed" in named

    def test_constrained_positionals_still_advertise_their_choices(self) -> None:
        """A required positional keeps its discoverability."""
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            if function.Meta.name != "price_history":
                continue
            period = next(p for p in function.get_metadata().parameters if p.name == "period")
            assert period.constraints and "1w" in period.constraints["choices"]


class TestCatalogShape:
    def test_function_names_are_unqualified(self) -> None:
        names = {f.Meta.name for f in _POLYMARKET_CATALOG.schemas[0].functions}
        assert names == {
            "markets",
            "market",
            "events",
            "event",
            "event_markets",
            "orderbook",
            "quote",
            "price_history",
            "trades",
            "holders",
            "positions",
            "activity",
            "fill_price",
            "search",
            "all_tags",
            "all_series",
        }
        assert not any(n.startswith("polymarket_") for n in names)

    def test_unkeyed_scans_are_exposed_as_tables(self) -> None:
        tables = {t.name: t.function for t in _POLYMARKET_CATALOG.schemas[0].tables}
        assert tables == {"tags": AllTagsFunction, "series": AllSeriesFunction}

    def test_table_names_do_not_collide_with_functions(self) -> None:
        """`tags` the table is backed by `all_tags` the function, so both coexist."""
        schema = _POLYMARKET_CATALOG.schemas[0]
        function_names = {f.Meta.name for f in schema.functions}
        assert {t.name for t in schema.tables}.isdisjoint(function_names)

    def test_every_table_declares_a_primary_key(self) -> None:
        for table in _POLYMARKET_CATALOG.schemas[0].tables:
            assert table.primary_key, table.name


def _shipped_sql() -> list[str]:
    """Every SQL string this worker advertises, from every tag that can carry one.

    Examples reach a client through several independent channels — the schema's
    `vgi.example_queries`, each object's own, the framework's native
    `Meta.examples`, and the catalog's executable suite — so the checks below
    sweep all of them or they would pass while a whole channel rots.
    """
    schema = _POLYMARKET_CATALOG.schemas[0]
    carriers: list[dict[str, str]] = [
        _POLYMARKET_CATALOG.tags,
        schema.tags,
        *(table.tags for table in schema.tables),
        *(getattr(function.Meta, "tags", {}) for function in schema.functions),
    ]
    statements: list[str] = []
    for tags in carriers:
        for key in ("vgi.example_queries", "vgi.executable_examples"):
            for entry in json.loads(tags.get(key) or "[]"):
                sql = entry["sql"]
                statements.extend(sql if isinstance(sql, list) else [sql])
    for function in schema.functions:
        statements.extend(example.sql for example in getattr(function.Meta, "examples", []))
    return statements


def _agent_task_sql() -> list[str]:
    """Every reference query in the agent-suitability suite."""
    out: list[str] = []
    for task in json.loads(_POLYMARKET_CATALOG.tags["vgi.agent_test_tasks"]):
        if sql := task.get("reference_sql"):
            out.extend(sql if isinstance(sql, list) else [sql])
        if check := task.get("check_sql"):
            out.append(check)
    return out


#: Functions keyed on a market. Passing a token id to one of these returns an
#: empty result with HTTP 200 — no error at all.
CONDITION_ID_FUNCTIONS = ("market", "trades", "holders")

#: Keyed on a wallet address — a third key, distinct from both market
#: identifiers, and the only thing `positions()` accepts.
WALLET_FUNCTIONS = ("positions", "activity")

#: Functions keyed on a single outcome token. Passing a condition id is a 404 on
#: `orderbook` and a silent empty series on `price_history`.
TOKEN_ID_FUNCTIONS = ("orderbook", "quote", "price_history", "fill_price")


class TestShippedExamples:
    """The examples are the discovery surface, so a wrong one is a wrong product.

    The trap they must stay clear of is the identifier split: `condition_id` and
    `token_id` are both spelled `market` in Polymarket's own URLs, and passing
    the wrong one is silent in two of three directions.
    """

    def test_there_are_examples_to_check(self) -> None:
        """Guard the guard: these checks are vacuous if nothing is collected."""
        assert len(_shipped_sql()) >= 25

    def test_examples_are_catalog_qualified(self) -> None:
        """An unqualified example does not run for a client that attached the catalog."""
        offenders = [sql for sql in _shipped_sql() if "polymarket.main." not in sql]
        assert offenders == [], f"examples not qualified as polymarket.main.*: {offenders}"

    def test_examples_reference_only_registered_objects(self) -> None:
        schema = _POLYMARKET_CATALOG.schemas[0]
        names = {f.Meta.name for f in schema.functions} | {t.name for t in schema.tables}
        called = {
            m
            for sql in _shipped_sql() + _agent_task_sql()
            for m in re.findall(r"polymarket(?:\.main)?\.(\w+)", sql)
        }
        assert called, "no example names any object"
        assert called <= names, f"examples reference unknown objects: {sorted(called - names)}"

    def test_every_object_is_demonstrated(self) -> None:
        schema = _POLYMARKET_CATALOG.schemas[0]
        objects = {f.Meta.name for f in schema.functions} | {t.name for t in schema.tables}
        called = {m for sql in _shipped_sql() for m in re.findall(r"polymarket(?:\.main)?\.(\w+)", sql)}
        assert objects <= called, f"never demonstrated: {sorted(objects - called)}"

    def test_token_keyed_functions_are_never_passed_a_condition_id(self) -> None:
        """The headline trap, checked mechanically across every shipped example."""
        offenders: list[tuple[str, str]] = []
        for sql in _shipped_sql() + _agent_task_sql():
            for name in TOKEN_ID_FUNCTIONS:
                for arg in re.findall(rf"polymarket(?:\.main)?\.{name}\(\s*([\w.]+)", sql):
                    leaf = arg.split(".")[-1]
                    if leaf == "condition_id":
                        offenders.append((name, sql))
        assert offenders == [], f"these pass a condition_id to a token-keyed function: {offenders}"

    def test_market_keyed_functions_are_never_passed_a_token_id(self) -> None:
        offenders: list[tuple[str, str]] = []
        for sql in _shipped_sql() + _agent_task_sql():
            for name in CONDITION_ID_FUNCTIONS:
                for arg in re.findall(rf"polymarket(?:\.main)?\.{name}\(\s*([\w.]+)", sql):
                    leaf = arg.split(".")[-1]
                    if leaf == "token_id":
                        offenders.append((name, sql))
        assert offenders == [], f"these pass a token_id to a market-keyed function: {offenders}"

    def test_wallet_keyed_functions_are_never_passed_a_market_key(self) -> None:
        """`positions()` takes a wallet — the third key, and easy to confuse."""
        offenders: list[tuple[str, str]] = []
        for sql in _shipped_sql() + _agent_task_sql():
            for name in WALLET_FUNCTIONS:
                for arg in re.findall(rf"polymarket(?:\.main)?\.{name}\(\s*([\w.]+)", sql):
                    leaf = arg.split(".")[-1]
                    if leaf in ("condition_id", "token_id"):
                        offenders.append((name, sql))
        assert offenders == [], f"these pass a market key to a wallet-keyed function: {offenders}"

    def test_wallet_keyed_examples_reach_their_wallet_from_a_holder_or_trade(self) -> None:
        """A wallet address only comes from holders() or the trade tape."""
        for sql in _shipped_sql():
            for name in WALLET_FUNCTIONS:
                if re.search(rf"polymarket(?:\.main)?\.{name}\(", sql):
                    assert "proxy_wallet" in sql, sql

    def test_the_three_key_sets_are_disjoint(self) -> None:
        assert not set(WALLET_FUNCTIONS) & set(CONDITION_ID_FUNCTIONS)
        assert not set(WALLET_FUNCTIONS) & set(TOKEN_ID_FUNCTIONS)

    def test_the_two_identifier_sets_are_disjoint(self) -> None:
        """If they ever converge, the checks above stop being worth their weight."""
        assert not set(CONDITION_ID_FUNCTIONS) & set(TOKEN_ID_FUNCTIONS)

    def test_token_keyed_examples_reach_their_token_by_unnesting(self) -> None:
        """A token_id can only come from a market's outcomes list."""
        for sql in _shipped_sql():
            for name in TOKEN_ID_FUNCTIONS:
                if re.search(rf"polymarket(?:\.main)?\.{name}\(", sql):
                    assert "unnest" in sql or "token_id" in sql, sql

    def test_no_example_uses_the_reserved_word_order(self) -> None:
        """`order` is a DuckDB reserved word; the argument is `order_by`."""
        offenders = [sql for sql in _shipped_sql() + _agent_task_sql() if re.search(r"[^_]order\s*=>", sql)]
        assert offenders == [], f"`order =>` does not parse in DuckDB: {offenders}"

    def test_unnest_of_a_struct_list_uses_a_binding_form(self) -> None:
        """`LATERAL unnest(x) o` yields a column named `unnest`, not o.field.

        The form that binds is `LATERAL (SELECT unnest(x, recursive := true)) o`,
        or `(unnest(x)).field` in a projection. A plain `LATERAL unnest(x) o`
        followed by `o.field` is a binder error.
        """
        offenders = [
            sql
            for sql in _shipped_sql() + _agent_task_sql()
            if re.search(r"LATERAL\s+unnest\(", sql, re.IGNORECASE)
        ]
        assert offenders == [], (
            f"use `LATERAL (SELECT unnest(x, recursive := true)) alias` instead: {offenders}"
        )

    def test_no_example_passes_a_named_argument_to_a_blended_function(self) -> None:
        """DuckDB cannot bind a named argument on a table-in-out function.

        The framework supports declaring one — `vgi-python`'s own blended tests
        assert exactly this shape resolves — but the DuckDB extension's binder
        rejects the call site, with or without LATERAL and with or without an
        explicit cast:

            Binder Error: No function matches the given name and argument types
            'trades(VARCHAR, INTEGER)'
            Candidate functions:
            trades(VARCHAR, max_rows : BIGINT, side : VARCHAR, cache_ttl : BIGINT)

        The arguments therefore stay declared — their defaults are what every
        call actually gets, and they will work unchanged once the binder catches
        up — but nothing this worker *advertises* may depend on them, or the
        example does not run. Scans are unaffected: `markets(closed => 'false')`
        binds fine, because a TableFunctionGenerator is an ordinary table
        function.
        """
        blended = {f.Meta.name for f in BLENDED}
        offenders: list[tuple[str, str]] = []
        for sql in _shipped_sql() + _agent_task_sql():
            for name in blended:
                for call in re.finditer(rf"polymarket(?:\.main)?\.{name}\(", sql):
                    start = call.end()
                    depth, i = 1, start
                    while i < len(sql) and depth:
                        depth += (sql[i] == "(") - (sql[i] == ")")
                        i += 1
                    if re.search(r"\w+\s*=>", sql[start : i - 1]):
                        offenders.append((name, sql))
        assert offenders == [], (
            f"these pass a named argument to a blended function, which does not bind: {offenders}"
        )

    def test_scans_may_still_use_named_arguments(self) -> None:
        """Guard the guard: the restriction above is specific to blended functions."""
        assert any("closed => " in sql for sql in _shipped_sql())

    def test_no_example_hardcodes_a_market_identifier(self) -> None:
        """Markets resolve. An example naming one works until it settles.

        A condition id is 0x + 64 hex; a token id is a long decimal literal.
        Both are derived from a subquery in every shipped example instead.
        """
        condition = re.compile(r"'0x[0-9a-fA-F]{64}'")
        token = re.compile(r"'\d{20,}'")
        offenders = [
            sql for sql in _shipped_sql() + _agent_task_sql() if condition.search(sql) or token.search(sql)
        ]
        assert offenders == [], f"these name an identifier that will expire: {offenders}"


class TestAgentTasks:
    """Every task must be gradable, and graded by something that can pass.

    `vgi-lint`'s simulator grades a task with `reference_sql` by comparing the
    analyst's answer result-set to the reference's, for exact equality. Every
    interesting question here is about live prices, volumes and books that move
    between those two executions, so a reference comparison fails even for a
    perfect answer — the suite scored 8% until it moved to `check_sql`.
    """

    @staticmethod
    def _tasks() -> list[dict]:
        return json.loads(_POLYMARKET_CATALOG.tags["vgi.agent_test_tasks"])

    def test_there_are_tasks(self) -> None:
        assert len(self._tasks()) >= 10

    def test_every_task_is_graded_by_an_invariant(self) -> None:
        missing = [t["name"] for t in self._tasks() if not t.get("check_sql")]
        assert missing == [], f"tasks with no check_sql cannot be graded reliably: {missing}"

    def test_no_task_grades_on_live_data_equality(self) -> None:
        """A reference_sql over moving data can never match the analyst's answer."""
        offenders = [t["name"] for t in self._tasks() if t.get("reference_sql")]
        assert offenders == [], (
            "these grade by exact result-set equality against live market data, which "
            f"cannot pass: {offenders}"
        )

    def test_every_task_states_its_success_criteria(self) -> None:
        missing = [t["name"] for t in self._tasks() if not t.get("success_criteria")]
        assert missing == [], missing

    def test_every_object_is_exercised_by_a_task(self) -> None:
        """This is also what vgi-lint's VGI520 requires."""
        schema = _POLYMARKET_CATALOG.schemas[0]
        objects = {f.Meta.name for f in schema.functions} | {t.name for t in schema.tables}
        called = {m for sql in _agent_task_sql() for m in re.findall(r"polymarket(?:\.main)?\.(\w+)", sql)}
        assert objects <= called, f"no agent task exercises: {sorted(objects - called)}"


class TestProbabilityClaimIsQualified:
    """A price is a probability only where the book is tight.

    Polymarket normalises a market's displayed outcome prices to sum to 1, so
    they always look like a coherent forecast. A live rain market quoted 0.020
    bid against 0.990 ask — a midpoint of 0.505 on a $61 book with one order a
    side. Documentation that calls that "a 50.5% chance" produces confidently
    wrong analysis, and an agent reading the catalog is exactly the consumer
    that would.
    """

    @staticmethod
    def _docs() -> dict[str, str]:
        schema = _POLYMARKET_CATALOG.schemas[0]
        out = {
            "catalog.doc_md": _POLYMARKET_CATALOG.tags["vgi.doc_md"],
            "catalog.doc_llm": _POLYMARKET_CATALOG.tags["vgi.doc_llm"],
        }
        for function in schema.functions:
            for key in ("vgi.doc_llm", "vgi.doc_md"):
                out[f"{function.Meta.name}.{key}"] = getattr(function.Meta, "tags", {}).get(key, "")
        return out

    def test_no_doc_calls_a_price_a_probability_unconditionally(self) -> None:
        """`reads directly as` was the exact phrasing that made the claim absolute."""
        offenders = [
            name
            for name, text in self._docs().items()
            if re.search(r"directly as (a |an )?\d*%? ?implied probabilit", text)
            or "reads directly as the market's implied probability" in text
        ]
        assert offenders == [], (
            f"these promise a price is a probability with no liquidity caveat: {offenders}"
        )

    def test_the_places_that_quote_a_price_mention_the_spread(self) -> None:
        """Wherever the docs teach the probability reading, they must teach the check."""
        for name in ("markets.vgi.doc_md", "quote.vgi.doc_md", "catalog.doc_md"):
            text = self._docs()[name]
            assert "implied probabilit" in text, f"{name} no longer explains the reading"
            assert "spread" in text.lower(), f"{name} teaches the reading without the caveat"

    def test_quote_still_returns_the_spread_that_check_depends_on(self) -> None:
        """The advice is only actionable because the column exists."""
        from vgi_polymarket.schemas import QUOTE_SCHEMA

        assert "spread" in QUOTE_SCHEMA.names
        assert "best_bid" in QUOTE_SCHEMA.names and "best_ask" in QUOTE_SCHEMA.names


class TestExampleChannels:
    """The native `Meta.examples` and the `vgi.example_queries` tag must agree."""

    def test_native_examples_are_tagged_verbatim(self) -> None:
        offenders: list[tuple[str, str]] = []
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            tags = getattr(function.Meta, "tags", {})
            tagged = {e["sql"] for e in json.loads(tags.get("vgi.example_queries") or "[]")}
            offenders += [
                (function.Meta.name, example.sql)
                for example in getattr(function.Meta, "examples", [])
                if example.sql not in tagged
            ]
        assert offenders == [], f"native examples missing from vgi.example_queries: {offenders}"

    def test_every_function_declares_its_result_columns(self) -> None:
        """A table function's columns are invisible until it binds."""
        for function in _POLYMARKET_CATALOG.schemas[0].functions:
            tags = getattr(function.Meta, "tags", {})
            declared = json.loads(tags["vgi.result_columns_schema"])
            assert [c["name"] for c in declared] == function.FIXED_SCHEMA.names, function.Meta.name
            assert all(c["description"] for c in declared), function.Meta.name


class TestNoDeadApiSurface:
    """Every endpoint wrapper must be reachable, from SQL or from a test.

    A wrapper that no SQL function calls and no test exercises is either a
    missing feature or dead weight, and both are worth failing the build over.
    """

    SURFACE = ("markets.py", "trading.py", "reference.py", "paging.py")

    @staticmethod
    def _public_api_functions() -> set[str]:
        import ast

        tree = ast.parse((PACKAGE / "polymarket_api.py").read_text())
        return {
            node.name
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
        }

    def test_every_endpoint_wrapper_is_reachable(self) -> None:
        api_source = (PACKAGE / "polymarket_api.py").read_text()
        surface = "".join((PACKAGE / name).read_text() for name in self.SURFACE)
        tests = "".join(p.read_text() for p in PACKAGE.parent.glob("tests/test_*.py"))
        plumbing = {"open_client", "gamma_base_url", "clob_base_url", "data_base_url"}
        candidates = self._public_api_functions() - plumbing
        orphaned = [
            name
            for name in sorted(candidates)
            if f"api.{name}(" not in surface
            and f"api.{name}(" not in tests
            and f"polymarket_api.{name}(" not in tests
            and f" {name}(" not in api_source.split(f"def {name}(")[0]
        ]
        assert orphaned == [], (
            f"api functions reachable from neither SQL nor a test: {orphaned}. "
            "Expose them as table functions or delete them."
        )

    def test_the_check_can_actually_fail(self) -> None:
        surface = "".join((PACKAGE / name).read_text() for name in self.SURFACE)
        assert "api.definitely_not_a_real_endpoint(" not in surface
