# Polymarket Prediction Markets — SQL notebooks

[Worker](https://github.com/Query-farm/vgi-polymarket) · [Shared standard](https://github.com/Query-farm/vgi-example-notebooks/blob/main/docs/standard.md)

Read the executed notebooks on GitHub or use each **Open in Colab** button. These public-data examples send no orders, accept no credentials and read no wallets or account positions. A quoted price is not a calibrated probability or recommendation.

| Notebook | Question | Experiment |
|---|---|---|
| [01 · The best ask does not price every share](01_price_the_displayed_depth_at_size.ipynb) | What can the recorded displayed depth cover, and what does a partial average price mean? | Increase independent hypothetical size from 1,000 to 200,000 shares. |
| [02 · Finding an event does not enumerate its questions](02_keep_events_markets_and_outcomes_distinct.ipynb) | How do topic labels, recurring series, search events, markets and outcome tokens differ? | Switch from the annual to November Bitcoin event while retaining discovery inputs. |

## Setup

The installation cell uses only the base shared package **v0.3.49**. `prepare("polymarket")` downloads the 46 immutable companions with SHA256 verification: one domain helper, the original four fixture/provenance JSON files, and 41 discovery response/provenance files. No manual checkout is needed in Colab. Compatible imported data/plot/kernel libraries stay in place. For local publication, download `notebooks/`, install [uv](https://docs.astral.sh/uv/) and Git, and run from this directory:

```sh
uv sync --locked --extra runner
uv run --locked --extra runner vgi-notebooks
```

Notebook Python supports 3.11–3.14. The independent worker requires 3.13 or newer, which uv can provision. Initial shared-package, companion, worker and VGI extension installation requires network access; replay does not contact Polymarket. The literal expanded SQL is:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'polymarket' (TYPE vgi,
 LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-polymarket@v0.1.0 vgi-polymarket');
```

The release owns its dependencies; no root checkout, local worker binary, build or transitive `--with` overrides are needed. The local notebook lock is separate from the worker and root locks. Shared `connect(globals())` uses an empty temporary SQL-secret directory with persistence disabled, prevents a saved JupySQL INI connection from loading, and establishes UTC on connection creation. Cleanup verifies the empty inventory, closes the session and restores the helper's previous base-URL environment.

## Original depth story and recorded evidence

Book 01 retains its substantive SQL, financial interpretation and independent Decimal checks. It uses one actual public esports condition, “Counter-Strike: Spirit vs MOUZ (BO3) — BLAST Rivals Playoffs,” and its two outcome books recorded on **14 November 2026, 09:27:18–09:27:19 UTC**. [fixtures/manifest.json](fixtures/manifest.json) preserves complete original response bytes, source URLs, methods, request bodies, timestamps, headers, byte sizes and hashes. Those original four files are unchanged.

Condition IDs identify questions; token IDs identify outcomes. One condition lookup and two single-token POST `/books` reads are observed in this execution. POST is the API's batch-read spelling, not an order submission. The SQL materializes all returned levels and walks asks in ascending price order. Missing asks produce an unavailable average, not zero. Partial averages price only filled shares. Requested = filled + unfilled, price bounds and exact Decimal notional reconcile against the raw responses. Source prices, sizes and notional are decimals; SQL weighted-average division is a floating-point ratio.

Book timestamps and run clocks are distinct; separate responses are not an atomic snapshot. The price ladder uses clearly labeled separate vertical scales around prices within its displayed share range. The calculation retains all levels. It does not model queue position, latency, cancellations, hidden liquidity, replenishment, fees or future execution. The two outcomes are independent hypothetical buys, not a combined portfolio.

## Discovery and read-ahead

Book 02 uses complete unmodified public Gamma responses acquired on **15 November 2026 UTC**. [fixtures/discovery/manifest.json](fixtures/discovery/manifest.json) records each response's exact request, timing and SHA256. Search uses `Bitcoin` with active-event filtering; separate Crypto topic scans use tag ID 21. The annual and November event IDs are the first two recorded search results. This is a bounded illustrative selection, not a representative or complete market sample.

The reference tables and their HTTP pagination are deliberately distinguished. SQL retains 20 topic labels, 20 recurring series and eight rows from each of three discovery scans. The released reference scanner fetches eight pages per batch, and engine read-ahead can fetch another batch before LIMIT cancels. Actual response rows, HTTP reads and final SQL rows are reported separately. The companion set includes the actual additional read-ahead responses, with original cursor URLs; it never fabricates a terminal page. Retrieval may vary with the execution plan. Any request outside that recorded set fails explicitly.

`event(event_id)` and `event_markets(event_id)` follow one event's real child relationship. Inventing an unsupported filter on `markets()` could return unrelated rows. The notebook reconciles declared and returned child counts, distinct condition IDs, outcome-token identities and a complete closed/open/status-missing partition. An open event can retain closed questions. Accepting-order flags do not guarantee a book or execution. Topic labels and recurring-series records are separate reference populations; absence from their bounded slices says nothing about a full catalog.

## Experiments and optional live reads

```sh
uv run --locked --extra runner vgi-notebooks 01_price_the_displayed_depth_at_size.ipynb --parameters '{"SIZE_SHARES":200000}' --output-dir validation/size-200000
uv run --locked --extra runner vgi-notebooks 02_keep_events_markets_and_outcomes_distinct.ipynb --parameters '{"EVENT_CHOICE":2}' --output-dir validation/second-event
```

The shared runner requires a separate destination for overrides, rejects unknown parameters and saves only a successful execution. Failure preserves the prior default and experiment. SVG charts remain native outputs; defaults also contain verified GitHub image links. One fresh preloaded Python 3.12 notebook-only bootstrap covers both stories' shared launcher/companion path. Hosted Colab execution is not claimed.

`DATA_MODE="live"` explicitly forwards only the same registered public-read requests through the helper, clears inherited base-URL overrides for the child, records actual response times/hashes/bytes and restores the prior environment at cleanup. There is no replay fallback. An explicit live event lookup was verified separately. The acquisition of every new raw recording also used the public API. Full live reruns are optional and can fail: old cursor URLs expire, the fixed match may close, and public endpoints can rate-limit. Use `--parameters '{"DATA_MODE":"live"}' --output-dir validation/live` for a chosen book and read failures as failures.

## Coverage and source baseline

[coverage.json](coverage.json) records native released/current catalogs, successful raw-JSON execution receipts and editorial mappings. Both catalogs expose **18 logical names**: 16 functions plus `tags` and `series` tables. These stories cover **9/18**, meeting the required nine. Different backing function names (`all_tags`, `all_series`) remain separate objects; neither is credited merely because its table ran. Also uncovered: `quote`, `fill_price`, `price_history`, `trades`, `holders`, `positions`, `activity`. None is removed from the denominator because it needs a different story.

The v0.1.0 tag dereferences to the captured public-main source **4b7daf67140d31db972e16188407d8bd37367902**; release/current source and dependencies are identical. Analytical receipts use the release label; current-interface credit is reconciled separately. Source CI [43615441551](https://github.com/Query-farm/vgi-polymarket/actions/runs/43615441551) has passing lint/format and **223 offline tests**. Its live job has **45 passes and one HTTP 429 failure** on `/v2/positions`; that is an existing upstream rate-limit failure, not a notebook regression. No worker builds, full suites or source CI dispatches were repeated for this notebook-only change. Historical metadata findings from the previous notebook runtime are not represented as a clean current metadata gate.

[Official order-book response documentation](https://docs.polymarket.com/api-reference/market-data/get-order-book) and the [worker identifier guide](https://github.com/Query-farm/vgi-polymarket#the-identifiers) explain the domain contracts. Presentation, runner and coverage rules live in the shared standard; the duplicated local runner is removed.
