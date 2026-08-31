"""VGI worker exposing Polymarket prediction-market data to DuckDB/SQL (read-only).

    ATTACH 'polymarket' (TYPE vgi, LOCATION 'uv run polymarket_worker.py');
    SELECT tag_id, label FROM polymarket.tags WHERE label = 'Crypto';
    SELECT question, outcomes FROM polymarket.markets(tag_id => '21', closed => 'false');

No credentials are required, and none are accepted. Polymarket's entire
market-data surface — the Gamma catalog, the CLOB's books and price history, and
the Data API's tape and holder lists — is public. Only order placement needs
authentication, and that is an EIP-712 wallet signature this worker cannot
produce and never constructs.

Function names are bare (``markets``, not ``polymarket_markets``) because they
are already qualified by the catalog they live in.
"""

from __future__ import annotations

import json
import sys

from vgi import Worker
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.catalog.catalog_interface import CatalogInfo
from vgi.catalog.descriptors import Table

from vgi_polymarket import __version__
from vgi_polymarket.markets import MARKET_FUNCTIONS
from vgi_polymarket.meta import column_comments, docs, examples, keywords
from vgi_polymarket.reference import REFERENCE_FUNCTIONS, AllSeriesFunction, AllTagsFunction
from vgi_polymarket.schemas import SERIES_SCHEMA, TAG_SCHEMA
from vgi_polymarket.trading import TRADING_FUNCTIONS

IMPLEMENTATION_VERSION = __version__
DATA_VERSION_SPEC = f"=={__version__}"
SOURCE_URL = "https://github.com/Query-farm/vgi-polymarket"

_FUNCTIONS = [*MARKET_FUNCTIONS, *TRADING_FUNCTIONS, *REFERENCE_FUNCTIONS]

#: The Crypto tag. Used in examples because it is stable, busy at all hours, and
#: not seasonal the way the sports tags are.
CRYPTO_TAG = "21"

_EXAMPLE_QUERIES = examples(
    (
        "Find the topic you want, then its id",
        "SELECT tag_id, label, slug FROM polymarket.main.tags WHERE label ILIKE '%crypto%' ORDER BY label",
    ),
    (
        "The busiest open markets right now, with their implied probabilities",
        "SELECT question, outcomes, volume_24hr FROM polymarket.main.markets("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 20",
    ),
    (
        "Every outcome of every crypto market, one row per outcome",
        f"SELECT m.question, o.outcome, o.price, o.token_id FROM polymarket.main.markets("
        f"tag_id => '{CRYPTO_TAG}', closed => 'false') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o "
        "ORDER BY m.volume_24hr DESC LIMIT 50",
    ),
    (
        "Live top-of-book for every outcome in a topic, via LATERAL",
        f"SELECT m.question, o.outcome, q.midpoint, q.spread FROM polymarket.main.markets("
        f"tag_id => '{CRYPTO_TAG}', closed => 'false', order_by => 'volume24hr', "
        "ascending => 'false') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
        "LATERAL polymarket.main.quote(o.token_id) q LIMIT 40",
    ),
    (
        "Total resting size on each side of a topic's books",
        f"SELECT b.side, sum(b.size) AS resting FROM polymarket.main.markets("
        f"tag_id => '{CRYPTO_TAG}', closed => 'false') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o, "
        "LATERAL polymarket.main.orderbook(o.token_id) b GROUP BY b.side",
    ),
    (
        "A week of hourly implied probability for the busiest open market",
        "SELECT h.ts, h.price FROM (SELECT o.token_id FROM polymarket.main.markets("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
        "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
        "LATERAL polymarket.main.price_history(t.token_id) h "
        "ORDER BY h.ts",
    ),
    (
        "The most recent prints on the busiest open market, with who traded",
        "SELECT t.ts, t.side, t.outcome, t.price, t.size, t.trader_name FROM ("
        "SELECT condition_id FROM polymarket.main.markets("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
        "LATERAL polymarket.main.trades(m.condition_id) t ORDER BY t.ts DESC",
    ),
    (
        "Who holds the biggest positions in the busiest open market",
        "SELECT h.outcome_index, h.holder_name, h.amount FROM ("
        "SELECT condition_id FROM polymarket.main.markets("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
        "LATERAL polymarket.main.holders(m.condition_id) h ORDER BY h.amount DESC",
    ),
    (
        "Follow the biggest holder in a busy market to everything else they hold",
        "SELECT p.title, p.outcome, p.size, p.cash_pnl FROM (SELECT h.proxy_wallet FROM ("
        "SELECT condition_id FROM polymarket.main.markets(closed => 'false', "
        "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
        "LATERAL polymarket.main.holders(m.condition_id) h ORDER BY h.amount DESC LIMIT 1) w, "
        "LATERAL polymarket.main.positions(w.proxy_wallet) p ORDER BY p.current_value DESC",
    ),
    (
        "The biggest open events, and the markets under one of them",
        "SELECT m.question, m.volume_24hr FROM (SELECT event_id FROM polymarket.main.events("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) e, "
        "LATERAL polymarket.main.event_markets(e.event_id) m ORDER BY m.volume_24hr DESC NULLS LAST",
    ),
    (
        "Find events by subject, then the markets under the best match",
        "SELECT m.question, m.outcomes FROM (SELECT event_id FROM "
        "polymarket.main.search('bitcoin') LIMIT 1) e, "
        "LATERAL polymarket.main.event_markets(e.event_id) m",
    ),
    (
        "How the biggest holder in a busy market built their position",
        "SELECT a.ts, a.activity_type, a.side, a.price, a.size FROM (SELECT h.proxy_wallet FROM ("
        "SELECT condition_id FROM polymarket.main.markets(closed => 'false', "
        "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
        "LATERAL polymarket.main.holders(m.condition_id) h ORDER BY h.amount DESC LIMIT 1) w, "
        "LATERAL polymarket.main.activity(w.proxy_wallet) a ORDER BY a.ts DESC",
    ),
    (
        "Active recurring series by recent volume",
        "SELECT slug, title, recurrence, volume_24hr FROM polymarket.main.series "
        "WHERE active AND NOT closed ORDER BY volume_24hr DESC NULLS LAST LIMIT 20",
    ),
    (
        "Topics read from the scan function behind the tags table",
        "SELECT tag_id, label FROM polymarket.main.all_tags() WHERE label ILIKE '%politics%'",
    ),
    (
        "Series read from the scan function behind the series table",
        "SELECT slug, title FROM polymarket.main.all_series() WHERE recurrence = 'daily' LIMIT 20",
    ),
    (
        "The event behind the busiest open market",
        "SELECT e.title, e.volume_24hr, e.end_date FROM (SELECT event_id FROM "
        "polymarket.main.markets(closed => 'false', order_by => 'volume24hr', "
        "ascending => 'false') LIMIT 1) m, LATERAL polymarket.main.event(m.event_id) e",
    ),
    (
        "The question behind a market, by its on-chain id",
        "SELECT m.question, m.volume_24hr FROM (SELECT condition_id FROM polymarket.main.markets("
        "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) t, "
        "LATERAL polymarket.main.market(t.condition_id) m",
    ),
)

#: Examples the linter actually runs against a live worker, so every one of them
#: has to be true right now — no market id (which settles), no row-count
#: assumption about a market that may not be trading today.
_EXECUTABLE_EXAMPLES = json.dumps(
    [
        {
            "name": "tags_catalog_is_populated",
            "description": "The tag catalog streams thousands of topics.",
            "sql": "SELECT count(*) > 500 FROM (SELECT tag_id FROM polymarket.main.tags LIMIT 600)",
            "expected_result": [[True]],
        },
        {
            "name": "tags_and_scan_function_agree",
            "description": "The `tags` table and the `all_tags` function behind it are the same rows.",
            "sql": (
                "SELECT (SELECT count(*) FROM (SELECT tag_id FROM polymarket.main.tags LIMIT 200)) "
                "= (SELECT count(*) FROM (SELECT tag_id FROM polymarket.main.all_tags() LIMIT 200))"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "series_and_scan_function_agree",
            "description": "The `series` table and the `all_series` function behind it are the same rows.",
            "sql": (
                "SELECT (SELECT count(*) FROM (SELECT series_id FROM polymarket.main.series LIMIT 100)) "
                "= (SELECT count(*) FROM (SELECT series_id FROM polymarket.main.all_series() LIMIT 100))"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "a_limit_stops_the_scan_early",
            "description": (
                "A paged scan yields its first rows without walking every page — the property "
                "that keeps a LIMIT over a large catalog from wedging the client."
            ),
            "sql": "SELECT count(*) FROM (SELECT question FROM polymarket.main.markets() LIMIT 5)",
            "expected_result": [[5]],
        },
        {
            "name": "prices_are_exact_decimals",
            "description": "Money comes back as DECIMAL, never a float.",
            "sql": "SELECT typeof(volume_24hr) FROM polymarket.main.markets() LIMIT 1",
            "expected_result": [["DECIMAL(28,6)"]],
        },
        {
            "name": "outcomes_carry_their_tokens",
            "description": (
                "Every outcome is zipped with the CLOB token that trades it — the join key "
                "the order book needs."
            ),
            "sql": (
                "SELECT bool_and(o.token_id IS NOT NULL) FROM ("
                "SELECT outcomes FROM polymarket.main.markets(closed => 'false') LIMIT 20) m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "closed_filter_is_applied_by_the_origin",
            "description": "The closed filter reaches Gamma rather than being applied after the fact.",
            "sql": (
                "SELECT bool_and(NOT closed) FROM ("
                "SELECT closed FROM polymarket.main.markets(closed => 'false') LIMIT 100)"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "orderbook_sides_are_bid_or_ask",
            "description": "A book flattens to labelled sides, or to nothing when it is empty.",
            "sql": (
                "SELECT bool_and(side IN ('bid', 'ask')) IS NOT FALSE FROM ("
                "SELECT b.side FROM (SELECT o.token_id FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 5) t, "
                "LATERAL polymarket.main.orderbook(t.token_id) b LIMIT 50)"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "a_quote_is_ordered_bid_then_ask",
            "description": "best_ask is never below best_bid, which pins the BUY/SELL naming.",
            "sql": (
                "SELECT bool_and(q.best_ask >= q.best_bid) IS NOT FALSE FROM ("
                "SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 5) t, "
                "LATERAL polymarket.main.quote(t.token_id) q"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "prices_read_as_probabilities",
            "description": "Every outcome price sits between 0 and 1, so it reads as a probability.",
            "sql": (
                "SELECT bool_and(o.price BETWEEN 0 AND 1) IS NOT FALSE FROM ("
                "SELECT outcomes FROM polymarket.main.markets(closed => 'false') LIMIT 50) m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o WHERE o.price IS NOT NULL"
            ),
            "expected_result": [[True]],
        },
    ]
)

#: The agent-suitability suite: tasks an analyst should be able to complete from
#: the catalog metadata alone. They are written to exercise the things this
#: worker is easy to get wrong — above all, which of the two identifiers a given
#: function takes.
#: The agent-suitability suite: tasks an analyst should be able to complete from
#: the catalog metadata alone. They are written to exercise the things this
#: worker is easy to get wrong — above all, which of the two identifiers a given
#: function takes.
#:
#: Each task is graded by ``check_sql``, an invariant that must hold over the
#: object the task is about, rather than by ``reference_sql``. That is a
#: deliberate choice forced by the data: a reference query is graded by exact
#: result-set equality against the analyst's answer, and every interesting
#: question here is about live prices, volumes and order books that move between
#: the two executions — so a reference comparison would fail even for a perfect
#: answer. The invariants below are real properties of this worker (prices are
#: probabilities, an ask is never below a bid, a book side is labelled), so they
#: test something rather than merely passing.
_AGENT_TEST_TASKS = json.dumps(
    [
        {
            "name": "find_a_topic",
            "prompt": "What topics does Polymarket cover for crypto? Give me a tag id I can filter on.",
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT tag_id FROM polymarket.main.tags "
                "WHERE label ILIKE '%crypto%' LIMIT 5)"
            ),
            "success_criteria": "Finds tag ids by searching the tags table, rather than guessing one.",
            "unordered": True,
        },
        {
            "name": "busiest_markets",
            "prompt": (
                "Which Polymarket markets are busiest right now, and what probability does each "
                "outcome imply?"
            ),
            "check_sql": (
                "SELECT bool_and(o.price BETWEEN 0 AND 1) IS NOT FALSE FROM ("
                "SELECT outcomes FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') LIMIT 20) m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o WHERE o.price IS NOT NULL"
            ),
            "success_criteria": (
                "Uses markets() with an order_by argument rather than scanning and sorting the "
                "whole catalog, and reads prices as probabilities between 0 and 1."
            ),
            "unordered": True,
        },
        {
            "name": "order_book_for_an_outcome",
            "prompt": ("Show me the resting order book for one outcome of any busy open Polymarket market."),
            "check_sql": (
                "SELECT bool_and(b.side IN ('bid', 'ask')) IS NOT FALSE FROM ("
                "SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 3) t, "
                "LATERAL polymarket.main.orderbook(t.token_id) b"
            ),
            "success_criteria": (
                "Passes a token_id from the outcomes list to orderbook(). Passing a condition_id "
                "is wrong — the order book keys on the outcome token, not the market."
            ),
            "unordered": True,
        },
        {
            "name": "price_history_for_an_outcome",
            "prompt": (
                "Chart how the probability of one outcome of a busy Polymarket market moved over "
                "the last week."
            ),
            "check_sql": (
                "SELECT bool_and(h.price BETWEEN 0 AND 1) IS NOT FALSE FROM ("
                "SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 1) t, "
                "LATERAL polymarket.main.price_history(t.token_id) h"
            ),
            "success_criteria": (
                "Passes a token_id to price_history(). A condition_id returns an empty series "
                "with no error, so getting this wrong looks like a market that never traded."
            ),
        },
        {
            "name": "recent_prints",
            "prompt": (
                "What were the last few trades on the busiest open Polymarket market, and who "
                "was on the other side?"
            ),
            "check_sql": (
                "SELECT bool_and(t.price BETWEEN 0 AND 1 AND t.side IN ('BUY','SELL')) IS NOT FALSE "
                "FROM (SELECT condition_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                "LATERAL polymarket.main.trades(m.condition_id) t"
            ),
            "success_criteria": (
                "Passes a condition_id to trades() — NOT a token_id, which returns an empty "
                "list — and reads the trader columns for who traded."
            ),
            "unordered": True,
        },
        {
            "name": "who_holds_the_position",
            "prompt": "Who holds the largest positions in the busiest open Polymarket market?",
            "check_sql": (
                "SELECT bool_and(h.amount > 0) IS NOT FALSE FROM ("
                "SELECT condition_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                "LATERAL polymarket.main.holders(m.condition_id) h"
            ),
            "success_criteria": "Uses holders() with a condition_id and ranks by amount.",
            "unordered": True,
        },
        {
            "name": "live_quote",
            "prompt": (
                "What is the current bid, ask and midpoint for one outcome of a busy open Polymarket market?"
            ),
            "check_sql": (
                "SELECT bool_and(q.best_ask >= q.best_bid) IS NOT FALSE FROM ("
                "SELECT o.token_id FROM polymarket.main.markets(closed => 'false', "
                "order_by => 'volume24hr', ascending => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 3) t, "
                "LATERAL polymarket.main.quote(t.token_id) q"
            ),
            "success_criteria": (
                "Uses quote() for the point read rather than aggregating the whole order book."
            ),
        },
        {
            "name": "markets_under_an_event",
            "prompt": ("Take the biggest open event on Polymarket. What individual markets sit under it?"),
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT event_id FROM polymarket.main.events("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) e, "
                "LATERAL polymarket.main.event_markets(e.event_id) m"
            ),
            "success_criteria": (
                "Uses event_markets(). markets() has no event filter, so trying to pass one "
                "there would silently return unrelated markets."
            ),
            "unordered": True,
        },
        {
            "name": "the_event_behind_a_market",
            "prompt": (
                "Pick the busiest open market on Polymarket. What event does it belong to, and "
                "when does that event resolve?"
            ),
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT event_id FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                "LATERAL polymarket.main.event(m.event_id) e"
            ),
            "success_criteria": (
                "Uses event() with the event_id carried on the market row, rather than trying to "
                "filter events() by a correlated column."
            ),
        },
        {
            "name": "the_question_behind_an_id",
            "prompt": (
                "Given only a Polymarket condition id, what question does that market ask and how "
                "much has it traded?"
            ),
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT condition_id FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) t, "
                "LATERAL polymarket.main.market(t.condition_id) m WHERE m.question IS NOT NULL"
            ),
            "success_criteria": (
                "Uses the point lookup market() for a known condition_id rather than scanning "
                "the catalog again with markets()."
            ),
        },
        {
            "name": "recurring_series",
            "prompt": "Which recurring question series does Polymarket run, and how often do they recur?",
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT slug FROM polymarket.main.series "
                "WHERE recurrence IS NOT NULL LIMIT 20)"
            ),
            "success_criteria": (
                "Uses the series table and reads the recurrence column, rather than inferring "
                "cadence from market titles."
            ),
            "unordered": True,
        },
        {
            "name": "count_the_catalogs",
            "prompt": (
                "How many topics and how many recurring series does Polymarket list? A sample of "
                "each is fine."
            ),
            "check_sql": (
                "SELECT (SELECT count(*) FROM (SELECT tag_id FROM polymarket.main.all_tags() "
                "LIMIT 100)) = (SELECT count(*) FROM (SELECT series_id FROM "
                "polymarket.main.all_series() LIMIT 100))"
            ),
            "success_criteria": (
                "Counts through the tags/series tables or the all_tags()/all_series() scans "
                "behind them, and bounds the scan rather than walking the whole catalog."
            ),
            "unordered": True,
        },
        {
            "name": "follow_a_trader",
            "prompt": (
                "Find the largest holder in the busiest open Polymarket market, then show me "
                "everything else that wallet is holding and how it is doing."
            ),
            "check_sql": (
                "SELECT bool_and(p.avg_price BETWEEN 0 AND 1) IS NOT FALSE FROM ("
                "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                "LATERAL polymarket.main.holders(m.condition_id) h ORDER BY h.amount DESC LIMIT 1) w, "
                "LATERAL polymarket.main.positions(w.proxy_wallet) p"
            ),
            "success_criteria": (
                "Chains holders() to positions(): holders() takes a condition_id and yields "
                "wallets, positions() takes a wallet address — a third key, distinct from both "
                "market identifiers."
            ),
            "unordered": True,
        },
        {
            "name": "find_markets_by_subject",
            "prompt": (
                "I want Polymarket markets about the Federal Reserve. Find them and show me what "
                "is tradeable."
            ),
            "check_sql": (
                "SELECT count(*) > 0 FROM (SELECT event_id FROM "
                "polymarket.main.search('federal reserve') LIMIT 5)"
            ),
            "success_criteria": (
                "Uses search() for a subject given in words. Filtering markets() or events() on "
                "a title predicate cannot be pushed down and pages the whole catalog instead."
            ),
            "unordered": True,
        },
        {
            "name": "how_a_position_was_built",
            "prompt": (
                "Take the largest holder in the busiest open Polymarket market. How did they "
                "build that position over time?"
            ),
            "check_sql": (
                "SELECT bool_and(a.activity_type IS NOT NULL) IS NOT FALSE FROM ("
                "SELECT h.proxy_wallet FROM (SELECT condition_id FROM polymarket.main.markets("
                "closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m, "
                "LATERAL polymarket.main.holders(m.condition_id) h ORDER BY h.amount DESC LIMIT 1) w, "
                "LATERAL polymarket.main.activity(w.proxy_wallet) a"
            ),
            "success_criteria": (
                "Uses activity() for history, not positions() (which is only the current "
                "holding) and not trades() (which is keyed on a market, not a wallet). Filters "
                "to TRADE entries, since most of a long-lived wallet's feed is yield accrual."
            ),
            "unordered": True,
        },
        {
            "name": "topic_wide_liquidity",
            "prompt": (
                "Across open crypto markets on Polymarket, how much size is resting on each "
                "side of the books?"
            ),
            "check_sql": (
                "SELECT count(DISTINCT b.side) <= 2 FROM (SELECT o.token_id FROM "
                "polymarket.main.markets(tag_id => '21', closed => 'false') m, "
                "LATERAL (SELECT unnest(m.outcomes, recursive := true)) o LIMIT 10) t, "
                "LATERAL polymarket.main.orderbook(t.token_id) b"
            ),
            "success_criteria": (
                "Drives orderbook() from markets() through the outcomes list, filtering by "
                "tag_id rather than scanning the whole catalog."
            ),
            "unordered": True,
        },
    ]
)

_CATEGORIES = json.dumps(
    [
        {
            "name": "reference",
            "title": "Topics & Discovery",
            "description": (
                "The tag and series catalogs — where you find the ids everything else filters on."
            ),
            "keywords": ["tags", "series", "topics", "catalog", "discovery"],
        },
        {
            "name": "markets",
            "title": "Markets & Questions",
            "description": "Tradeable questions with their outcomes, prices, volume and lifecycle state.",
            "keywords": ["markets", "questions", "prices", "outcomes", "probability"],
        },
        {
            "name": "market-depth",
            "title": "Order Book & Quotes",
            "description": "Live top-of-book and the resting liquidity behind it, level by level.",
            "keywords": ["orderbook", "quote", "depth", "liquidity", "spread", "midpoint"],
        },
        {
            "name": "history",
            "title": "Price History & Tape",
            "description": "How an outcome's probability moved over time, and the raw trade prints.",
            "keywords": ["price history", "trades", "tape", "history", "backtest"],
        },
        {
            "name": "positions",
            "title": "Holders & Positions",
            "description": (
                "Who holds what, in both directions: the wallets in a market, and the markets in "
                "a wallet. Polymarket settles on a public chain, so positions are public data."
            ),
            "keywords": ["holders", "positions", "wallets", "concentration", "onchain", "pnl"],
        },
    ]
)

_CATALOG_TAGS = {
    "provider": "polymarket",
    "domain": "prediction-markets",
    "vgi.title": "Polymarket Prediction Markets",
    "vgi.source_url": SOURCE_URL,
    "vgi.author": "Query Farm LLC <hello@query.farm> - https://query.farm",
    "vgi.copyright": (
        "Copyright (c) 2026 Query Farm LLC - https://query.farm. Market data (c) Polymarket, "
        "redistributed under Polymarket's terms of use."
    ),
    "vgi.license": "MIT",
    "vgi.support_contact": "https://github.com/Query-farm/vgi-polymarket/issues",
    "vgi.support_policy_url": "https://github.com/Query-farm/vgi-polymarket/blob/main/README.md",
    "vgi.icon_url": "https://raw.githubusercontent.com/Query-farm/vgi-polymarket/main/docs/vgi-logo.png",
    "vgi.keywords": keywords(
        "polymarket",
        "prediction markets",
        "event contracts",
        "orderbook",
        "price history",
        "trades",
        "forecasting",
        "onchain",
        "probability",
    ),
    "vgi.executable_examples": _EXECUTABLE_EXAMPLES,
    "vgi.agent_test_tasks": _AGENT_TEST_TASKS,
    "vgi.doc_llm": (
        "Live and historical data from Polymarket, the largest prediction market, where shares in "
        "an outcome pay $1 if it happens and $0 if it does not — so a price reads directly as a "
        "probability. Reach for this catalog to answer what the crowd currently thinks about a "
        "future event, how that view has moved, how much liquidity stands behind it, and — "
        "uniquely, because Polymarket settles on a public chain — who is holding the position. "
        "Start at the `tags` table, take a tag id, and go to `markets()` from there.\n\n"
        "CRITICAL — there are two identifiers, and Polymarket's own URLs call both of them "
        "'market'. A `condition_id` (0x and 64 hex characters) names a whole market; a "
        "`token_id` (a long decimal string) names one outcome of it. Anything about resting "
        "liquidity, live quotes or price over time is keyed on the outcome, so it takes a "
        "`token_id`; anything about who traded or who holds a position is keyed on the market, "
        "so it takes a `condition_id`. Every market row carries an `outcomes` list holding each "
        "outcome's `token_id`, so unnesting that column is how you convert between the two. "
        "Passing the wrong one usually returns an empty result rather than an error, so check "
        "the identifier before concluding that nothing traded. Each function's own "
        "documentation names which of the two it takes.\n\n"
        "Read-only and public: no credentials, no account, no order entry."
    ),
    "vgi.doc_md": (
        "Polymarket is a prediction market where each outcome of a question trades as its own "
        "token. A token pays $1 if its outcome happens and $0 otherwise, so its price is the "
        "market's implied probability of that outcome.\n\n"
        "### The two identifiers — read this first\n\n"
        "Nearly every mistake against this catalog is the same mistake, and it is usually "
        "silent. There are two keys, and Polymarket's own URLs call both of them `market`:\n\n"
        "| Key | Looks like | Names | Used by |\n"
        "|---|---|---|---|\n"
        "| `condition_id` | `0x` + 64 hex | a whole market | the trade tape, the holder list, "
        "the market lookup |\n"
        "| `token_id` | a long decimal string | one outcome of it | resting depth, live quotes, "
        "price history |\n\n"
        "Every market row carries an `outcomes` list holding each outcome's `token_id`, so "
        "unnesting that column is how you get from one key to the other. Each function's own "
        "docs say which it takes.\n\n"
        "Getting it wrong fails differently by direction: a market key passed where an outcome "
        "key belongs is an HTTP 404 in one place and an empty price series in another, and an "
        "outcome key passed to the trade tape returns an empty list. Two of the three are "
        "indistinguishable from 'nothing traded'.\n\n"
        "### How the data is shaped\n\n"
        "- **Tag** — a topic such as Crypto or Politics. The filter almost everything accepts, "
        "and the only axis that covers the whole site.\n"
        "- **Series** — a recurring question that opens a new event each occurrence. Many "
        "markets belong to no series.\n"
        "- **Event** — the markets that resolve together: one game, or one election.\n"
        "- **Market** — one question, named by its `condition_id`.\n"
        "- **Outcome** — one answer to that question, trading as its own token and named by its "
        "`token_id`.\n\n"
        "### Reading prices\n\n"
        "Every price column is USDC per share between 0 and 1, so a midpoint of 0.62 is a 62% "
        "implied probability. They are `DECIMAL`, never `DOUBLE`. Polymarket sends money three "
        "different ways — decimal strings, JSON floats, and JSON arrays nested inside strings — "
        "and all three are recovered exactly rather than rounded through a float.\n\n"
        "### Access\n\n"
        "No credentials are needed and none are accepted. The whole market-data surface is "
        "public; only order placement requires a wallet signature, and this worker exposes no "
        "such endpoint. It is read-only by construction, not by configuration.\n\n"
        "### Rate limits and cost\n\n"
        "Order books are the expensive thing — about 147 ms each — so they are fetched in "
        "batches of up to 200 outcomes per request, measured ~81x faster than one at a time. "
        "Everything else runs at roughly 20 requests a second without throttling. The per-row "
        "functions take a `cache_ttl` argument that turns repeated lookups into cache hits when "
        "a few seconds of staleness is acceptable."
    ),
}

_SCHEMA_TAGS = {
    "provider": "polymarket",
    "domain": "prediction-markets",
    "vgi.title": "Polymarket Market Data",
    "vgi.categories": _CATEGORIES,
    "vgi.keywords": keywords(
        "polymarket",
        "prediction markets",
        "orderbook",
        "price history",
        "trades",
        "holders",
        "probability",
    ),
    "vgi.example_queries": _EXAMPLE_QUERIES,
    "vgi.doc_llm": (
        "The whole read-only Polymarket surface, in one schema. Work top-down: the `tags` table "
        "lists every topic and needs no arguments, so start there and take a tag id. That id "
        "filters the market catalog. Each market then leads two different ways with two "
        "different keys — its outcomes' token ids for anything about price or resting "
        "liquidity, and its own condition id for anything about who traded or who holds. Both "
        "keys live on the same row, which is what lets a single query span the catalog, the "
        "order book and the chain."
    ),
    "vgi.doc_md": (
        "One schema holding the whole read-only Polymarket surface.\n\n"
        "### Finding your way in\n\n"
        "The unkeyed `tags` table is the way in: it lists every topic, and a tag id is the "
        "filter the two catalog browsers accept. From a market you reach two different things "
        "with two different keys — its outcomes' token ids for anything about price, and its "
        "own condition id for anything about who traded.\n\n"
        "### What composes under LATERAL, and what does not\n\n"
        "The point-lookup functions are blended: one registration serves both a literal call "
        "and a correlated `LATERAL`, so a per-row lookup becomes a set-based query without a "
        "second registration. The two catalog browsers are streaming scans instead — a `LIMIT` "
        "over them stops early, but they cannot be the inner side of a LATERAL, so drive a join "
        "from them rather than into them. The point lookups exist for that direction.\n\n"
        "### Unnesting outcomes\n\n"
        "A market's outcomes arrive as a list of structs, so reaching one outcome's token needs "
        "`LATERAL (SELECT unnest(m.outcomes, recursive := true)) o`. The `recursive := true` is "
        "what exposes the struct's fields as columns; without it the alias carries a single "
        "column named `unnest` and `o.token_id` will not bind.\n\n"
        "### The one real trap\n\n"
        "Two identifiers, both called `market` in Polymarket's own URLs. A condition id names "
        "the whole market; a token id names one outcome. Unnesting the outcomes column converts "
        "between them. See the catalog notes for how the wrong one fails."
    ),
}

_TAGS_DOCS = docs(
    category="reference",
    llm=(
        "The catalog of every topic on Polymarket — around 7,300 rows, small and slow-changing. "
        "This is the discovery entry point: `tag_id` is the filter `markets()` and `events()` "
        "both accept, and this is where you find one. Match on `label` or `slug` for a topic like "
        "Crypto, Politics or Sports."
    ),
    md=(
        "Every topic tag Polymarket uses, as an ordinary table — no argument required.\n\n"
        "### Why this is a table and most things are functions\n\n"
        "It has no required key and changes slowly. The market-data functions are neither, which "
        "is why they take a key.\n\n"
        "### The long tail is real\n\n"
        "A few dozen tags carry nearly all the volume, and the rest are narrow one-off labels "
        "left over from individual events. Search `label` for the topic you want rather than "
        "browsing.\n\n"
        "### Where to go next\n\n"
        "Take a `tag_id` and pass it to `markets()` for the tradeable questions, or to `events()` "
        "for the groupings above them."
    ),
    example_queries=examples(
        (
            "Find the tag id for a topic",
            "SELECT tag_id, label, slug FROM polymarket.main.tags "
            "WHERE label ILIKE '%crypto%' ORDER BY label",
        ),
        (
            "Topics whose tags were published most recently",
            "SELECT label, slug, published_at FROM polymarket.main.tags "
            "WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 20",
        ),
    ),
    extra={
        "provider": "polymarket",
        "domain": "prediction-markets",
        "vgi.title": "Polymarket Topic Tags",
        "vgi.keywords": keywords("tags", "topics", "categories", "catalog", "discovery"),
    },
)

_SERIES_DOCS = docs(
    category="reference",
    llm=(
        "The catalog of recurring question series — about 2,400 rows. A series opens a new event "
        "each occurrence, like `nfl` opening one per game, so this is the level to start from for "
        "anything periodic. Take a `slug` and pass it to `events()` as `series_slug`. Note that "
        "many of Polymarket's biggest markets are one-offs belonging to no series, so use `tags` "
        "when you want coverage of the whole site."
    ),
    md=(
        "Every recurring series Polymarket lists, as an ordinary table — no argument required.\n\n"
        "### What a series is, and is not\n\n"
        "A series groups events that recur: `nfl` opens an event per game, and each of those "
        "events carries a market per outcome. Plenty of the biggest markets on the site are "
        "one-offs that belong to no series at all, so this is a view of the recurring part of "
        "Polymarket rather than of all of it — `tags` is the axis that covers everything.\n\n"
        "### Where to go next\n\n"
        "Take a `slug` and pass it to `events()` as `series_slug` for that series' occurrences."
    ),
    example_queries=examples(
        (
            "Active recurring series by recent volume",
            "SELECT slug, title, recurrence, volume_24hr FROM polymarket.main.series "
            "WHERE active AND NOT closed ORDER BY volume_24hr DESC NULLS LAST LIMIT 20",
        ),
        (
            "How many series run at each cadence",
            "SELECT recurrence, count(*) AS series_count FROM polymarket.main.series "
            "GROUP BY recurrence ORDER BY series_count DESC",
        ),
    ),
    extra={
        "provider": "polymarket",
        "domain": "prediction-markets",
        "vgi.title": "Polymarket Recurring Series",
        "vgi.keywords": keywords("series", "recurring", "catalog", "discovery", "schedule"),
    },
)

_POLYMARKET_CATALOG = Catalog(
    name="polymarket",
    default_schema="main",
    comment=(
        "What the crowd thinks a future event is worth, priced as a probability — read-only "
        "Polymarket data, including who holds each position"
    ),
    tags=_CATALOG_TAGS,
    schemas=[
        Schema(
            name="main",
            comment="Polymarket public market data — no credentials required",
            tags=_SCHEMA_TAGS,
            functions=list(_FUNCTIONS),
            tables=[
                Table(
                    name="tags",
                    function=AllTagsFunction,
                    comment="Every topic on Polymarket; the tag_id everything else filters on",
                    tags=_TAGS_DOCS,
                    column_comments=column_comments(TAG_SCHEMA),
                    primary_key=(("tag_id",),),
                    not_null=("tag_id",),
                    # Inlined so the planner does not have to ask. The catalog
                    # grows slowly — it was ~7,300 rows when measured — so an
                    # estimate stale by a few hundred still beats no estimate.
                    cardinality_estimate=7_300,
                    statistics_cache_max_age_seconds=3_600,
                ),
                Table(
                    name="series",
                    function=AllSeriesFunction,
                    comment="Recurring question series, each opening a new event per occurrence",
                    tags=_SERIES_DOCS,
                    column_comments=column_comments(SERIES_SCHEMA),
                    primary_key=(("series_id",),),
                    not_null=("series_id",),
                    cardinality_estimate=2_400,
                    statistics_cache_max_age_seconds=3_600,
                ),
            ],
        ),
    ],
)


class PolymarketCatalog(ReadOnlyCatalogInterface):
    """Advertises the worker's versions and its single read-only catalog.

    There is no secret type and no ``auth`` attach option, unlike the sibling
    Kalshi worker. That is not an omission: Polymarket's entire market-data
    surface is public, and the only thing authentication buys is the ability to
    place orders, which requires an EIP-712 wallet signature and is not
    something a read-only worker should be able to do. Offering an unused
    credential slot would imply a capability that does not exist.
    """

    catalog = _POLYMARKET_CATALOG
    catalog_name = _POLYMARKET_CATALOG.name

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise the single read-only Polymarket catalog and its provenance."""
        return [
            CatalogInfo(
                name=self._effective_catalog_name,
                implementation_version=IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
                source_url=SOURCE_URL,
            )
        ]


class PolymarketWorker(Worker):
    """Worker process hosting the read-only Polymarket catalog."""

    catalog = _POLYMARKET_CATALOG
    catalog_interface = PolymarketCatalog


def main() -> None:
    """Run the worker (stdio by default; pass ``--http`` for the HTTP server)."""
    PolymarketWorker.main()


def main_http() -> None:
    """Run the worker over HTTP."""
    argv = sys.argv[1:]
    if "--http" not in argv:
        argv = ["--http", *argv]
    sys.argv = [sys.argv[0], *argv]
    PolymarketWorker.main()
