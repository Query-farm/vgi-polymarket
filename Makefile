# Common tasks. `make help` lists them. Requires uv (https://docs.astral.sh/uv/).
# Adapted from the vgi-starter's Makefile.

HAYBARN_VERSION ?= 2.0.0rc11
# vgi-lint is deliberately not pinned: the gate tracks the current quality bar.
VGI_LINT        := uvx --prerelease=allow --from 'vgi-lint-check>=2,<3' vgi-lint lint
LINT_ARGS       ?=
WORKER          := $(CURDIR)/.venv/bin/vgi-polymarket
TOOLS           := $(CURDIR)/.tools
HAYBARN_UNITTEST := $(TOOLS)/bin/haybarn-unittest
TRANSPORT       ?= stdio

.DEFAULT_GOAL := help
.PHONY: help build tools lint fmt test-unit test-sql test-sql-all test test-live lint-explain check clean

help: ## List the targets
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  %-14s %s\n",$$1,$$2}' $(MAKEFILE_LIST)

build: ## Install the worker and dev tools into .venv
	uv sync --locked --all-extras --group dev

tools: ## Install the pinned SQL test runner into .tools
	@test -x $(HAYBARN_UNITTEST) || { uv venv -q $(TOOLS) && uv pip install -q -p $(TOOLS) haybarn-unittest==$(HAYBARN_VERSION); }

fmt: ## Format the code
	uv run ruff format .
	uv run ruff check --fix .

lint: build ## Static checks: ruff format + ruff check
	uv run ruff format --check .
	uv run ruff check .

test-unit: build ## Offline unit tests (pytest; live tests are opt-in)
	uv run pytest -q

test-sql: build tools ## SQL tests through DuckDB against the mock API: make test-sql TRANSPORT=stdio|launch|http
	WORKER=$(WORKER) HAYBARN_UNITTEST=$(HAYBARN_UNITTEST) TRANSPORT=$(TRANSPORT) PYTHON="uv run --no-project python" ci/run-integration.sh

test-sql-all: ## SQL tests over every transport
	$(MAKE) test-sql TRANSPORT=stdio
	$(MAKE) test-sql TRANSPORT=launch
	$(MAKE) test-sql TRANSPORT=http

test: test-unit test-sql-all ## Unit + SQL tests (offline)

test-live: build ## Tests against the real Polymarket API (network)
	uv run pytest -q -m live

lint-explain: build ## Metadata quality report (vgi-lint, settings in vgi-lint.toml)
	$(VGI_LINT) "$(WORKER)" --no-execute $(LINT_ARGS)

check: lint test-unit test-sql-all lint-explain ## Everything CI runs offline. Done = this passes.

clean: ## Remove build outputs and tool caches
	rm -rf .venv .tools dist build .ruff_cache .pytest_cache
