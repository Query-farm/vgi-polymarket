# Polymarket notebooks

[01 · Price the displayed depth at size](01_price_the_displayed_depth_at_size.ipynb)
asks how a hypothetical buy's size changes its displayed-book average price and
coverage. One actual public esports market supplies two outcome books. The SQL
walks asks in ascending price order and keeps unfilled shares explicit.

## Run from notebooks alone

Install [uv](https://docs.astral.sh/uv/) and Git, then use Python 3.13 or 3.14:

```sh
cd notebooks
uv sync --locked
uv run --locked python execute.py
uv run --locked python -m ipykernel install --user --name vgi-polymarket-notebooks --display-name 'Python (vgi-polymarket notebooks)'
```

Select that kernel in Jupyter, with this directory as its working directory.
The directory is self-contained: no root worker checkout or root environment is
needed. Initial dependency, VGI-extension and Git-package installation needs
internet. Replay then serves recorded API responses on a temporary loopback port.
The tested platform is macOS ARM64 with Haybarn 2.0.0rc11.

The expanded SQL shows the literal startup command:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'polymarket' (
 TYPE vgi,
 LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-polymarket.git@c89399e51206d7406c03530e8f1ff367650fc923 --with vgi-python[http,haybarn]==0.51.0 --with vgi-rpc==0.53.1 --with httpx==0.28.1 --with httpx2==2.16.0 --with pyarrow==25.0.0 --with numpy==2.5.4 vgi-polymarket'
);
```

There is no published worker release/package to assume here. `uvx` fetches the
immutable Git commit and caches its installed environment. The notebook pins the
tested worker runtime independently of its own environment and the unchanged root
lock. The SDK's `haybarn` extra resolves Haybarn 1.5.5rc1 inside the worker for
filter evaluation; the notebook SQL engine is Haybarn 2.0.0rc11. NumPy is supplied
explicitly because filter evaluation otherwise fails when creating its UDF. To
upgrade, change the literal revision/runtime pins and rerun all validation.

## Recorded responses and live reads

`fixtures/manifest.json` records the complete unmodified bodies, request method,
URL and JSON body, UTC start/receive times, source Date header, byte size and SHA256.
`market.json` identifies one condition; `book-0.json` and `book-1.json` contain one
outcome book each. The helper verifies every hash and matches requests exactly,
normalizing query order and JSON token order. Unknown requests fail; there is no
synthetic response or replay fallback. Polymarket owns the public market data;
source terms apply. No wallet positions or identities are collected.

The notebook uses one condition-ID lookup plus two single-token POST `/books`
reads in the tested SQL execution. POST is the API's batch-read spelling, not an
order submission. This plan bounds the population by identifiers and materializes
all levels. It does not claim that a SQL LIMIT bounds requests or that a one-row
function call guarantees a single request in every execution plan.

`DATA_MODE="live"` forwards only those same bounded reads to the public origins
through the helper, records response times/hashes/bytes and feeds them to the real
worker. It clears inherited base-URL overrides for the run and restores them at
cleanup. Live results can change, including empty or missing books after the match;
failures remain failures. To execute live without replacing replay outputs:

```sh
uv run --locked python execute.py --parameters '{"DATA_MODE":"live"}' --output-dir validation/live
```

Book timestamps and the notebook run clock are separate. Separate API responses
are not an atomic snapshot. No trading endpoint, credentials or wallet is used.

## Size experiment and output protection

```sh
uv run --locked python execute.py --parameters '{"SIZE_SHARES":200000}' --output-dir validation/size-200000
```

Keep replay fixed. The default is 1,000 shares per independent outcome scenario;
reset to that value afterwards. The experiment increases only size. The input
fingerprint, source timestamps, level population and three-request shape must stay
fixed. The runner requires a separate destination for overrides, rejects unknown
parameters, and replaces a saved notebook only after successful execution. Separate
copies include assets, fixtures, helper, documentation and locked environment.
`validation/` and `artifacts/` are ignored; default embedded outputs are committed.

## What the model can establish

The SQL counts displayed shares and pre-fee notional. It does not model queue
position, latency, cancellations, hidden liquidity, replenishment or fees, and does
not guarantee any execution. Missing asks produce an unavailable average price,
not a zero price. A partial fill prices only its covered portion. Outcomes are
independent hypothetical buys, not a combined portfolio. Prices are not calibrated
probabilities or recommendations.

The depth plot clips its horizontal display using requested size; SQL uses all
returned levels. Input prices/sizes and notional use decimal columns; weighted
average division yields a floating-point ratio. Independent Decimal calculations
against the complete JSON responses reconcile every level and each fill. Additional
checks cover identifiers, availability, spread ordering, conservation of requested
shares, level limits, average-price bounds and exact request counts. See the
[official book response documentation](https://docs.polymarket.com/api-reference/market-data/get-order-book)
and [STANDARD.md](STANDARD.md).

## Source validation baseline

The unchanged source passes its 219 offline tests, 46 live API tests, Ruff lint and
format checks. The published Git runtime is also exercised through real SQL and
20 original stdio/HTTP test cases, with the explicit NumPy addition described above.
A temporary test harness changes only worker launch locations to the pinned Git
entrypoints; source tests and assertions remain unchanged.

Metadata validation with `vgi-lint-check==2.5.0 --audit-waivers --fail-on error`
reports 46/100 and exits 2: 17 VGI416 errors from legacy grader fields embedded in
agent tasks, and 23 warnings (VGI148 subtitle, VGI417 context length, VGI520 task
coverage). The two declared waivers still suppress findings. These are unchanged
source metadata findings; no worker metadata or root dependency files are edited.
