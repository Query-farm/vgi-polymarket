<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-polymarket/main/docs/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
</p>

<h1 align="center">vgi-polymarket</h1>

<p align="center">
  Polymarket prediction-market data in DuckDB/SQL — a read-only <a href="https://query.farm/vgi/">VGI</a> connector.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
  <a href="https://query.farm/vgi/"><img src="https://img.shields.io/badge/protocol-VGI-orange.svg" alt="VGI"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.13%2B-blue.svg" alt="Python 3.13+"></a>
</p>

---

A **read-only** [VGI](https://query.farm/vgi/) worker exposing
[Polymarket](https://polymarket.com) prediction-market data to DuckDB/SQL —
markets, outcomes, order books, quotes, price history, trades, and — because
Polymarket settles on a public chain — who holds each position and what else
they hold, as ordinary tables and table functions.

> **No credentials required, and none accepted.** Polymarket's entire
> market-data surface is public. The only thing authentication buys is order
> placement, which needs an EIP-712 wallet signature this worker cannot produce
> and never constructs. Read-only is enforced structurally (two chokepoints in
> `polymarket_api.py`, the only module in the package that so much as imports
> `httpx`) and by a CI guard (`tests/test_readonly_guard.py`).

## Run

```bash
uv run polymarket_worker.py         # stdio
uv run serve.py --port 8000         # HTTP
```

```sql
ATTACH 'polymarket' (TYPE vgi, LOCATION 'uv run polymarket_worker.py');
```

Both scripts carry PEP-723 headers that resolve `vgi-python` and `vgi-rpc` from
sibling checkouts (`../vgi-python`, `../vgi-rpc`), matching this project's
`[tool.uv.sources]`. Installing the package instead (`pip install .`) uses the
published versions from `[project.dependencies]`.

## The identifiers

Polymarket identifies three different things, and its own URLs call two of them
`market`. They are not interchangeable, and passing one where another belongs
usually returns an empty result rather than an error — so it is worth settling
before anything else:

| Identifier | Looks like | Identifies | Functions that take it |
|---|---|---|---|
| `condition_id` | `0x` + 64 hex | the **market** (the question) | `market`, `trades`, `holders` |
| `token_id` | a 76-digit decimal string | **one outcome** of it | `orderbook`, `quote`, `price_history` |
| `proxy_wallet` | `0x` + 40 hex | **a trader** | `positions`, `activity` |

The third key is less confusable — nothing calls it `market` — and it comes
from `holders()` or from the `proxy_wallet` column of `trades()`.

If you know the subject but none of the three, start at `search()` — it is the
only text search here.

Every market row carries an `outcomes` list holding each outcome's `token_id`,
so `unnest` is how you convert between them:

```sql
SELECT m.question, o.outcome, q.midpoint
FROM polymarket.markets(tag_id => '21', closed => 'false') m,
     LATERAL (SELECT unnest(m.outcomes, recursive := true)) o,
     LATERAL polymarket.quote(o.token_id) q;
```

**Getting it wrong fails three different ways, and two of them are silent:**

| Mistake | What happens |
|---|---|
| `condition_id` → `orderbook()` | HTTP 404 — loud |
| `condition_id` → `price_history()` | HTTP 200, `{"history": []}` — **silent** |
| `token_id` → `trades()` | HTTP 200, `[]` — **silent** |

An empty result is therefore not evidence that nothing traded. That is why the
argument names here say which key they want, why the column comments repeat it,
and why `tests/test_catalog.py` checks every shipped example passes the right
one.

## Surface

Names are bare — they are already qualified by the `polymarket` catalog.

### Tables

| Table | |
|---|---|
| `tags` | Every topic on the site (~7,300 rows). `tag_id` is the filter everything else accepts |
| `series` | Recurring question series (~2,400 rows), each opening a new event per occurrence |

Both are real catalog tables, not functions: no required key, slow-changing.
They are backed by the `all_tags` and `all_series` functions via
`Table(function=…)`, which VGI auto-wires into a scan. Neither is small enough
to fetch in one response, so both stream a page per tick — a `LIMIT` over either
stops early.

`series` is a view of the *recurring* part of Polymarket, not of all of it: many
of the biggest markets are one-offs belonging to no series. `tags` is the axis
that covers everything.

### Functions

Functions come in two shapes, and which one an object gets is decided by the
endpoint behind it: **if it pages, the function pages.**

A **paged scan** (`markets`, `events`) sits on a cursor-paged endpoint. It emits
one API page per tick, holding the cursor in VGI scan state, so `LIMIT` stops
early and rows arrive immediately. It cannot be the inner side of a correlated
LATERAL — drive a join *from* it, not into it.

Everything else is **blended** (`RowTransformFunction`): the fetch is bounded by
construction — one market, one book, one price curve — so a single registration
serves both a literal call and a LATERAL.

The distinction is not stylistic. A blended function must emit everything for
its input in one `process()` call, so on a paged endpoint it walks every page
before DuckDB sees a row: a `LIMIT 10` still pays for the whole catalog, and
because a scan blocked inside its first batch cannot be cancelled, the query
*wedges the client* rather than merely running slowly.

| Function | Positional (per-row) | Named |
|---|---|---|
| `search(query)` *(scan)* | — | `events_status` |
| `markets()` *(scan)* | — | `tag_id`, `slug`, `condition_id`, `closed`, `active`, `order_by`, `ascending` |
| `market(condition_id)` | condition_id | `cache_ttl` |
| `events()` *(scan)* | — | `tag_id`, `series_slug`, `slug`, `closed`, `featured`, `title_search`, `order_by`, `ascending` |
| `event(event_id)` | event_id | `cache_ttl` |
| `event_markets(event_id)` | event_id | `cache_ttl` |
| `orderbook(token_id)` | token_id | `depth`, `cache_ttl` |
| `quote(token_id)` | token_id | `cache_ttl` |
| `price_history(token_id)` | token_id | `period`, `fidelity`, `start_ts`, `end_ts` |
| `trades(condition_id)` | condition_id | `side`, `max_rows`, `cache_ttl` |
| `holders(condition_id)` | condition_id | `max_rows`, `cache_ttl` |
| `positions(proxy_wallet)` | proxy_wallet | `max_rows`, `cache_ttl` |
| `activity(proxy_wallet)` | proxy_wallet | `activity_type`, `max_rows`, `cache_ttl` |

### Named arguments bind on the scans, not on the rest

`markets()` and `events()` are ordinary table functions, and their named
arguments work:

```sql
SELECT question FROM polymarket.markets(closed => 'false', order_by => 'volume24hr');
```

Every other function is a *table-in-out* function, and **DuckDB's binder cannot
currently bind a named argument on one** — with or without `LATERAL`, with a
literal or an explicit cast:

```
Binder Error: No function matches the given name and argument types 'trades(VARCHAR, INTEGER)'
Candidate functions:
    trades(VARCHAR, max_rows : BIGINT, side : VARCHAR, cache_ttl : BIGINT)
```

The arguments are still declared, because the framework supports the shape
(`vgi-python`'s own blended tests assert it resolves) and the defaults are what
every call receives. They are chosen to be the right answer untuned:
`max_rows` 1000 on the tape and 100 on holders, `period` `'1w'` at hourly
`fidelity`, `depth` unlimited, `cache_ttl` off. Nothing this worker advertises
depends on passing one, and `tests/test_catalog.py` fails the build if an
example starts to — so when the binder catches up, these become live knobs with
no change here. `Arg` also rejects a positional argument with a default, so
"make them positional" is not an alternative; the framework directs optional
arguments to the named form.

### Why there is no `markets(event_slug => …)`

`/markets/keyset` has no event parameter — and **Gamma silently ignores a query
parameter it does not recognise**, answering with the unfiltered first page and
HTTP 200. An `event_slug` argument would therefore return unrelated markets with
no error at all. `event_markets(event_id)` reads them from the event's own
payload instead, which is the only correct route.

This is the same hazard as the identifier mix-up, one level up: a filter that
does not exist is not an error here, it is a wrong answer. Only parameters
verified against both the live API and Gamma's published OpenAPI document are
ever sent, and that list is pinned in `_MARKET_PUSHDOWN` / `_EVENT_PUSHDOWN`.

## Three APIs, one catalog

Polymarket does not have *an* API. It has three, on three hosts, and Gamma alone
pages three different ways:

| Host | Serves | Pages by |
|---|---|---|
| `gamma-api.polymarket.com` | tags, series, events, markets | opaque `after_cursor` (keyset) |
| `gamma-api.polymarket.com` | search | 1-based `page` number |
| `clob.polymarket.com` | books, quotes, price history | not paged |
| `data-api.polymarket.com` | trades, holders, positions, activity | `offset` |

The split is also the identifier boundary: the CLOB keys on `token_id`, and the
Data API keys on `condition_id` — except `positions`, which keys on a wallet.

**Use `/markets/keyset`, not `/markets`.** The latter still answers, but with
`Deprecation: true`, `Sunset: Fri, 01 May 2026` and
`Warning: 299 - "use /markets/keyset"`. It also offset-pages, which skips and
repeats rows when the catalog shifts under a walk.

**The cursor parameter is `after_cursor`, and the response field it comes from
is `next_cursor`.** Passing it back under its own name is accepted with HTTP 200
and silently ignored, so the walk re-serves page one indefinitely. Nothing in the
response distinguishes that from real progress, which is why
`tests/test_paging.py` asserts that ids actually advance across pages.

**Filter pushdown is a promise, not a hint.** `markets()` and `events()` declare
`filter_pushdown`, which makes the DuckDB extension drop its own filter above the
scan — it now believes the worker applies the predicate. The filters are only
*delivered*, though, when `auto_apply_filters` is declared alongside it. Declaring
one without the other discards every predicate silently: the engine has stopped
re-checking, and the worker never receives anything to check. The three flags go
together:

| Flag | Why it is required |
|---|---|
| `filter_pushdown` | Accepts the predicate; the engine stops re-checking it |
| `auto_apply_filters` | Actually delivers and applies it |
| `projection_pushdown` | A pushed filter's column index is relative to the projection |

`tests/test_catalog.py::TestPushdownFlags` fails the build if any function
declares one without the others. `tests/test_end_to_end.py` — which drives a real
`ATTACH` — is the only kind of test that can see a fault of this shape at all: a
worker that ignores its filters still passes every test written against its API
layer.

Two consequences worth knowing. A predicate on a column Gamma *can* filter
(`slug`, `condition_id`, `market_id`) is translated into a query parameter and
answered in well under a second. A selective predicate on a column it cannot
(`volume_24hr`, say) can only be evaluated after fetching, so proving that
nothing matches means walking the catalog — see the next note.

**An unfiltered aggregate is capped.** A `LIMIT` stops a keyset scan early, but
an aggregate over a bare `markets()` has no reason to stop before the end of the
catalog — and a predicate DuckDB cannot fold to a constant, such as
`WHERE condition_id = (SELECT …)`, is exactly that. So is a selective predicate
on a column Gamma cannot filter. The market catalog was measured at **over
120,000 rows** — still not exhausted after 1,200 pages and 155 seconds — so this
is reachable in ordinary use, not a pathological case.

Past `MAX_PAGES` the scan raises `PolymarketPageLimitError` rather than returning
a prefix that reads like a complete result, and the message names the ways out:
narrow with an argument the API understands (`tag_id`, `closed`, `active`,
`slug`, `condition_id`), use `search()` for a text query, or add a `LIMIT`. That
is a deliberate trade — `WHERE volume_24hr > 999999999999` now fails loudly
where it used to return five unfiltered rows instantly.

**Page sizes clamp silently.** Ask `/markets/keyset` for 500 and you get 100;
ask `/series` for 100 and you get 50. There is no error and no indication in the
response that more was requested, so a caller that assumes it got what it asked
for will quietly under-read. Measured caps:

| Endpoint | Max page |
|---|---|
| `/markets/keyset`, `/events/keyset`, `/tags` | 100 |
| `/series` | 50 |

## Money on the wire

Polymarket encodes numbers three ways, and two of them lose precision if parsed
naively:

| Encoding | Example | Where |
|---|---|---|
| Decimal string | `"0.0445"`, `"189399.21791"` | Gamma market level, every CLOB price and size |
| JSON float | `0.955`, `12858502.949756002` | Gamma **event** level, every Data API price and size |
| JSON array inside a string | `'["Yes", "No"]'` | `outcomes`, `outcomePrices`, `clobTokenIds` |

The same quantity is a string at the market level and a float at the event
level, so both go through one converter. Floats are recovered via
`Decimal(str(value))` — Python prints the shortest decimal string that
round-trips, so `0.955` comes back as `0.955` rather than
`0.954999999999999960…`. That is exact for every price Polymarket can quote
(the finest tick is `0.001`) and for any sane size.

Money is `DECIMAL`, never `DOUBLE`: prices are `DECIMAL(18,6)` and amounts
`DECIMAL(28,6)`. Nothing ever raises — one odd value must not fail every other
row in its batch — but the two column families handle an unrepresentable value
differently, and the difference is deliberate:

- **Prices are exact or NULL.** Polymarket's finest tick is `0.001` and a
  midpoint splits it to `0.0005`, so nothing legitimate needs more than the six
  declared decimal places. A price that will not fit is a misread, and NULL is
  the honest report of one.
- **Amounts round, half-even.** A 24-hour volume arrives as
  `2306265.4030640004` — sixteen decimal places, of which the last ten are
  float representation noise the origin itself introduced, not USDC. Refusing
  to round would NULL almost every volume and liquidity figure on the site, so
  the exact-or-NULL rule is applied only where the precision is real. A value
  too *large* for the column is still NULL, since that is genuine overflow
  rather than noise.

### The `outcomes` column

`outcomes`, `outcomePrices` and `clobTokenIds` arrive as three independent JSON
documents nested in string fields, whose only relationship is **positional**:
element *i* of each describes the same outcome. They are zipped once, in
`schemas.zip_outcomes`, into a single `LIST(STRUCT(outcome_index, outcome,
token_id, price))` column, so a consumer cannot get the correspondence wrong —
and a market whose three arrays disagree in length yields the outcomes they
agree on rather than a misaligned row.

```sql
SELECT m.question, o.outcome, o.price
FROM polymarket.markets(tag_id => '21') m, LATERAL (SELECT unnest(m.outcomes, recursive := true)) o;
```

## Finding things by name

`search(query)` is the only text search in the catalog, and it is the way in
when you know the subject but not the tag or the slug. Gamma's other filters
match ids, slugs and tags — never free text — so a `WHERE title ILIKE '%…%'`
on `events()` cannot be pushed down and pages the entire catalog over the
network to filter locally.

```sql
SELECT m.question, m.outcomes
FROM (SELECT event_id FROM polymarket.search('federal reserve') LIMIT 1) e,
     LATERAL polymarket.event_markets(e.event_id) m;
```

Results come back as events, in the same columns `events()` returns, ranked by
relevance — so a `LIMIT` takes the best matches rather than an arbitrary slice.

## The whole chain, in both directions

Because Polymarket settles on a public chain, positions and history are public
data, and the catalog traverses the relationship in every direction:

- `holders(condition_id)` — market → the wallets in it
- `positions(proxy_wallet)` — wallet → the markets it is in *now*, with entry
  price, current value and unrealised P&L
- `activity(proxy_wallet)` — wallet → everything it has *ever* done: trades,
  redemptions, splits and merges, yield and rebates

Chaining them answers a question no conventional exchange's API can:

```sql
-- The largest holder in the busiest market, and everything else they hold
SELECT p.title, p.outcome, p.size, p.cash_pnl
FROM (SELECT h.proxy_wallet
      FROM (SELECT condition_id FROM polymarket.markets(
              closed => 'false', order_by => 'volume24hr', ascending => 'false') LIMIT 1) m,
           LATERAL polymarket.holders(m.condition_id) h
      ORDER BY h.amount DESC LIMIT 1) w,
     LATERAL polymarket.positions(w.proxy_wallet) p
ORDER BY p.current_value DESC;
```

`percent_pnl` is a percentage, not a fraction: `-44.5` means down 44.5%. It is
carried as Polymarket sends it rather than rescaled, so it agrees with the
site's own display.

The activity feed is wider than a trade tape — across a sample of active wallets
trades were ~92% of entries, the rest being redemptions, yield and rebates — so
filter on `activity_type` when you want executions. Only `TRADE` rows carry a
price and a side; the others leave them NULL rather than reporting a misleading
zero. The declared `activity_type` values are the ones observed live, and
`tests/test_live.py` fails if the API starts emitting one this worker does not
declare.

## Caching

Polymarket's own headers drive the policy — nothing is invented. Its
`Cache-Control` is forwarded to DuckDB's result cache as `vgi.cache.*` metadata,
so if Polymarket changes a TTL, this follows automatically.

| Endpoint | Polymarket sends | We advertise |
|---|---|---|
| `/tags` | `public, max-age=1800` | `ttl=1800` |
| `/markets/keyset`, `/events/keyset`, `/series` | `public, max-age=300` | `ttl=300` |
| `/trades`, `/holders` | `public, max-age=300` | `ttl=300` |
| `/positions`, `/activity` | `public, max-age=5` | `ttl=5` |
| `/prices-history` | `public, max-age=90` | `ttl=90` |
| `/book`, `/midpoints`, `/prices` | *(nothing)* | uncached unless `cache_ttl` |

A response counts as cacheable only if it declares a non-zero `max-age` *and*
declares nothing that forbids reuse. `no-store`, `no-cache` and `private` beat
any `max-age` beside them, and `max-age=0` is treated as uncacheable rather than
as a zero-lifetime entry — storing one would pair an already-stale result with
`stale_if_error` and quietly license five minutes of staleness the origin never
offered.

Polymarket sends **no `ETag` and no `Last-Modified`** on anything, so a cached
result can only expire — it can never be cheaply revalidated. Every cacheable
result carries `stale_if_error=300`.

**Price history is cached by immutability where it can be.** A sampled point can
never change once its bar has closed, so an explicit window ending in the past
is cached for a day. A window running up to `now` contains a point that is still
moving and takes the origin's 90 seconds instead.

**Opt-in caching for live data.** `orderbook()`, `quote()`, `market()`,
`event()`, `event_markets()`, `trades()` and `holders()` declare a `cache_ttl`
argument, default `0` (off), which would also enable `per_value` memoization.
It is not reachable from SQL today — see the note on named arguments above — so
the live endpoints are simply uncached, and the catalog endpoints run on
Polymarket's own declared TTLs, which is the safe default either way.

## Performance

**Order books are the expensive thing, and they are batched.** A single `/book`
costs ~147 ms. `orderbook()` and `quote()` instead call `POST /books`, which
takes up to 200 token ids at once:

| | 100 books |
|---|---|
| one request each | 12.85 s |
| one batched request | 0.16 s |
| | **~81× faster** |

This happens automatically; no query changes. A token with no book is *absent
from the response* rather than present and empty (100 tokens returned 98
entries), so the caller tolerates a missing key — absent and empty mean the same
thing here.

**Measured throughput, unauthenticated, no 429s observed at these rates:**

| Endpoint | sustained | ms/request |
|---|---|---|
| `/prices-history` | ~22.6 req/s | 44 |
| `/trades` | ~22.3 req/s | 45 |
| `/markets/keyset` (limit=500) | ~19.6 req/s | 51 |
| `/events/keyset` | ~19.2 req/s | 52 |
| `/book` (single) | **~6.8 req/s** | **147** |

`/book` is the one worth batching, and it is the one that is batched.

`_send` retries five times with exponential backoff (~0.5s → 8s) on a 429, on a
transient 5xx, and on a dropped connection — a read is idempotent, and retrying
only the rate limiter would let one reset connection abort a fan-out that had
already made 300 successful calls. A 4xx is not retried, and a 429 is never
folded into the freshness hint, since it carries the CDN's error policy rather
than the resource's.

**Transport.** Responses are gzipped. `brotli` is deliberately **not** a
dependency: Polymarket's CDN answers `Accept-Encoding: br, gzip, zstd` with gzip
on all three hosts, so the codec would be installed and never negotiated.
`tests/test_packaging.py::TestTransport` pins what is actually offered, so the
reasoning cannot go stale unnoticed.

**The two catalog tables fetch their pages concurrently.** This is the one
thing offset paging is good for: page N+1's address does not depend on page N's
response, so unlike a cursor walk the pages can be issued at once. It matters —
`tags` is ~7,300 rows at 100 a page, so a serial walk is 73 round trips, and a
`WHERE label ILIKE '%crypto%'` has to reach the end of it:

| Query | serial | 8 pages at once |
|---|---|---|
| `tags WHERE label ILIKE '%crypto%'` | 8.5 s | **1.6 s** |
| full `tags` scan (7,279 rows) | — | 1.5 s |
| full `series` scan (2,392 rows) | — | 1.0 s |

Eight is sized against the measured ~20 req/s ceiling, and a 429 is retried
anyway. The keyset scans (`markets`, `events`) stay serial because a cursor
gives them no choice — they stream instead, so a `LIMIT` stops them early.

**`exclude_events` on `/series` is not optional.** Each series inlines every
event beneath it, and those events inline their markets: 50 series come to
1.73 MB with them and 31.6 KB without — a 55× difference, or ~83 MB against
~1.5 MB across the whole catalog, for columns the table does not expose either
way.

## Read-only, with a POST chokepoint

Polymarket's batch read endpoints (`/books`, `/prices`, `/midpoints`) are
**POST-with-a-body even though they are pure reads** — the `GET` spellings
answer HTTP 400. Since batching is an 81× lever, refusing to POST would mean
refusing to batch.

So the guard here is about **paths, not verbs**. `_post_read` refuses any path
outside an explicit allowlist, checked at call time rather than left to review:

```python
READ_ONLY_POST_PATHS = frozenset({"/books", "/prices", "/midpoints"})
```

`tests/test_readonly_guard.py` asserts that the allowlist is exactly those
three, that `httpx` is imported in exactly one module, that no write verb is
called anywhere outside the two chokepoints, and that no order-entry or
credential path appears anywhere in the package. Order placement lives behind
`/order` and requires a wallet signature this worker has no code to produce.

## Catalog metadata

Everything a client sees on `ATTACH` — object descriptions, column comments,
result schemas, examples, categories — is published as `vgi.*` tags and checked
by [vgi-lint](https://github.com/Query-farm/vgi-lint-check):

```bash
vgi-lint lint                     # config lives in vgi-lint.toml
vgi-lint lint --audit-waivers     # prove the waiver still buys something
```

Column documentation has a single source: `vgi_polymarket/schemas.py` attaches a
comment to every Arrow field via `meta.field()`, and `meta.result_columns_schema()`
reads those same strings back out to build each function's declared result
schema. A column documented once therefore shows up in `DESCRIBE`, in
`duckdb_columns()`, and in the function's `vgi.result_columns_schema` — and
cannot drift between them.

## Tests

```bash
pytest              # 163 offline tests
pytest -m live      # 42 live tests, plus 14 end-to-end through a real ATTACH
```

`tests/test_catalog.py` asserts the metadata the linter reads: every shipped
example is catalog-qualified, calls a real object, and passes the right
identifier to the right function.

`tests/test_paging.py` pins the cursor mechanics — that `after_cursor` is the
parameter sent, and that a scan which stops advancing is caught. That failure
mode returns HTTP 200 forever, so nothing but an explicit test notices it.

`tests/test_end_to_end.py` drives a real `ATTACH` and is the only place a
pushdown fault is visible: every other test calls the API layer or the schema
helpers directly, so all of them passed throughout the period when `markets()`
was ignoring every `WHERE`. It needs `duckdb` and `pytz` (DuckDB requires the
latter to materialize `TIMESTAMP WITH TIME ZONE`, which most columns here are),
and `allow_unsigned_extensions`, because the vgi extension ships unsigned from
the community repository. It is marked `live`, so `pytest` alone does not run it.

`tests/test_packaging.py` checks the two entry-point scripts' PEP-723 headers
still cover every runtime dependency — they resolve independently of
`pyproject.toml`, so they drift silently and only an end-to-end `ATTACH`
notices.

The `vgi.agent_test_tasks` suite is graded by `check_sql` invariants rather than
by `reference_sql`. That is forced by the data: vgi-lint grades a reference task
by comparing the analyst's answer result-set to the reference's for exact
equality, and every interesting question here is about prices, volumes and books
that move between the two executions — so a reference comparison fails even for a
correct answer. The invariants assert real properties instead (prices are
probabilities, an ask is never below a bid, a book side is labelled `bid` or
`ask`), so they still test something. `tests/test_catalog.py::TestAgentTasks`
keeps it that way.

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) 2026 Query Farm LLC — <https://query.farm>

Market data is Polymarket's, redistributed under their terms of use. This
connector reads their public APIs and adds nothing to them but a SQL interface.

## Links

- [Query Farm](https://query.farm) — who builds this
- [VGI](https://query.farm/vgi/) — the protocol this connector speaks
- [vgi-python](https://github.com/Query-farm/vgi-python) — the reference implementation it is built on
- [vgi-lint](https://github.com/Query-farm/vgi-lint-check) — the catalog-metadata linter it is checked by
