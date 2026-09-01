"""Reference data: the ``tags`` and ``series`` catalogs.

Unlike the market-data functions, these have no required key and change slowly,
which makes them the natural fit for *real catalog tables* rather than
functions. Both are backed by offset-paged scans that stream a page per tick, so
a ``LIMIT`` over either stops early rather than pulling the whole catalog.

The tables are declared in :mod:`vgi_polymarket.worker` as
``Table(name="tags", function=AllTagsFunction)``; VGI's
``ReadOnlyCatalogInterface.table_scan_function_get`` auto-wires the scan, so no
scan code is needed here. The backing functions take an ``all_`` prefix because
a function and the table it backs cannot share one name in a schema.
"""

from __future__ import annotations

from typing import ClassVar

import pyarrow as pa
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc.rpc import OutputCollector

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.meta import docs, examples
from vgi_polymarket.paging import OffsetScanState, emit_offset_page
from vgi_polymarket.schemas import SERIES_SCHEMA, TAG_SCHEMA, series_row, tag_row


@init_single_worker
class AllTagsFunction(TableFunctionGenerator[None, OffsetScanState]):
    """Every topic tag — the scan behind the ``tags`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TAG_SCHEMA

    class Meta:
        name = "all_tags"
        description = "Every Polymarket topic tag (the scan backing the `tags` table)"
        categories = ["reference"]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=TAG_SCHEMA,
            llm=(
                "Every topic tag on Polymarket, as a function. Prefer the `tags` table, which "
                "scans this and reads as plain SQL; this form exists for the rare case you want "
                "the scan itself. Either way it is the discovery starting point: `tag_id` is the "
                "filter both `markets()` and `events()` accept, and it is the only way to ask "
                "for 'everything about crypto' rather than naming markets one at a time."
            ),
            md=(
                "Polymarket's topic axis — roughly 7,300 tags, streamed a page at a time.\n\n"
                "### Prefer the table\n\n"
                "The `tags` catalog table is backed by this function, so it can be selected from "
                "directly, without parentheses. This function returns exactly the same rows; it "
                "is named `all_tags` only so that the function and the table it backs can "
                "coexist in one schema.\n\n"
                "### The long tail is real\n\n"
                "A few dozen tags carry nearly all the volume (`Crypto`, `Politics`, `Sports`), "
                "and the rest are narrow one-off labels left over from individual events. Match "
                "on `label` or `slug` when you know the topic you want; scanning for a tag by "
                "eye is not rewarding.\n\n"
                "### Where to go next\n\n"
                "Take a `tag_id` and pass it to `markets()` or `events()`."
            ),
            example_queries=examples(
                (
                    "Find the tag id for a topic, read from the scan function",
                    (
                        "SELECT tag_id, label, slug FROM polymarket.main.all_tags() "
                        "WHERE label ILIKE '%crypto%' ORDER BY label"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT tag_id, label, slug FROM polymarket.main.all_tags() "
                    "WHERE label ILIKE '%crypto%' ORDER BY label"
                ),
                description="Find the tag id for a topic, read from the scan function",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> OffsetScanState:
        return OffsetScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: OffsetScanState, out: OutputCollector) -> None:
        """Emit one page of tags, forwarding Gamma's own freshness policy.

        ``/tags`` declares ``Cache-Control: public, max-age=1800`` — the longest
        TTL Polymarket publishes anywhere, and appropriate for a vocabulary that
        changes when someone adds a topic.
        """
        emit_offset_page(
            params,
            state,
            out,
            fetch=api.tags_page,
            shape=tag_row,
            page_limit=api.GAMMA_PAGE_LIMIT,
        )


@init_single_worker
class AllSeriesFunction(TableFunctionGenerator[None, OffsetScanState]):
    """Every recurring series — the scan behind the ``series`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SERIES_SCHEMA

    class Meta:
        name = "all_series"
        description = "Every Polymarket series (the scan backing the `series` table)"
        categories = ["reference"]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=SERIES_SCHEMA,
            llm=(
                "Every recurring question series on Polymarket, as a function. Prefer the "
                "`series` table, which scans this. A series is a question that opens a new event "
                "each occurrence — 'NFL' opens one per game — so this is the level to start from "
                "for anything periodic. Take a `slug` and pass it to `events()` as `series_slug`. "
                "Note that most one-off markets belong to no series at all, so this is a view of "
                "the recurring part of the site rather than of all of it."
            ),
            md=(
                "Polymarket's recurring question series — about 2,400 rows, streamed a page at "
                "a time.\n\n"
                "### Prefer the table\n\n"
                "The `series` catalog table is backed by this function and reads as plain SQL. "
                "This function returns exactly the same rows; the `all_` prefix exists only so "
                "that the function and the table can coexist in one schema.\n\n"
                "### What a series is, and is not\n\n"
                "A series groups events that recur: `nfl` opens an event per game, and each of "
                "those events carries a market per outcome. Plenty of Polymarket's biggest "
                "markets are one-offs that belong to no series, so this does **not** enumerate "
                "the site — `tags` is the axis that covers everything.\n\n"
                "### Cost\n\n"
                "Each series inlines every event beneath it, and those events inline their "
                "markets: 50 series come to 1.73 MB with the events and 31.6 KB without. This "
                "scan asks Gamma to exclude them, which is the difference between ~83 MB and "
                "~1.5 MB for the whole catalog."
            ),
            example_queries=examples(
                (
                    "Active recurring series by recent volume, read from the scan function",
                    (
                        "SELECT slug, title, recurrence, volume_24hr "
                        "FROM polymarket.main.all_series() WHERE active AND NOT closed "
                        "ORDER BY volume_24hr DESC NULLS LAST LIMIT 20"
                    ),
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT slug, title, recurrence, volume_24hr "
                    "FROM polymarket.main.all_series() WHERE active AND NOT closed "
                    "ORDER BY volume_24hr DESC NULLS LAST LIMIT 20"
                ),
                description="Active recurring series by recent volume, read from the scan function",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> OffsetScanState:
        return OffsetScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: OffsetScanState, out: OutputCollector) -> None:
        """Emit one page of series, with the inlined events excluded.

        ``/series`` clamps its page size to 50 rather than the 100 the rest of
        Gamma allows — and clamps silently, so asking for more is not an error,
        just a shorter page than the caller thinks it got.
        """
        emit_offset_page(
            params,
            state,
            out,
            fetch=api.series_page,
            shape=series_row,
            page_limit=api.SERIES_PAGE_LIMIT,
        )


REFERENCE_FUNCTIONS: list[type] = [AllTagsFunction, AllSeriesFunction]
