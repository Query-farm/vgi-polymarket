# Changelog

## 0.1.1

### Fixed

- `market(condition_id)` returned no row for any resolved market. Gamma's
  `/markets/keyset` applies `closed=false` when the parameter is omitted, even
  to an explicit `condition_ids` filter, so a market that had closed was
  invisible to its own id. Ids the open pass misses are now looked up again
  with `closed=true` (one extra request, only when something is missing). The
  same lookup backs every correlated `LATERAL polymarket.main.market(...)`.
- The first `orderbook()` example took the single busiest open market's first
  outcome, which is often a game that has just ended with an empty book. It
  now takes one outcome among the ten busiest that has resting orders, so the
  example shows real depth.

### Documentation

- The catalog docs link to the upstream service and its API documentation
  (VGI183): polymarket.com, the Polymarket API reference, the Gamma and CLOB
  OpenAPI specs, the Data API v2 docs and the market-data overview, and state
  the Polymarket Terms of Use (also linked from the README).
- Per-function detail in the catalog overview (the spread check behind
  `quote()`, order-book batching costs) is shortened to a pointer; those
  functions' own docs already carry it. This also keeps `vgi.doc_md` inside the
  4,000-character agent context limit (VGI417).
- `vgi-lint.toml` waives VGI417 for the 33-column market row's
  `vgi.result_columns_schema` on `markets`, `market` and `event_markets`, as
  the Kalshi worker does for its market row.
- `vgi-lint.toml` raises the slow-example warning bar to 10 s: the live
  examples run concurrently under `--execute` and land at 5-8 s together
  against 1-2.5 s alone.
- README's pinned-install example now pins v0.1.1.

## 0.1.0

- First release.
