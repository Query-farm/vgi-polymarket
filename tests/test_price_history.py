"""Price-history normalization through the native VGI SDK emit contract.

Only HTTP is replaced. The worker process method, Arrow conversion, SDK output
collector and parent-row metadata encoding/decoding are exercised together.
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import httpx
import pyarrow as pa
import pytest
from vgi.protocol import _decode_parent_rows, _TrackingOutputCollector
from vgi.table_function import ProcessParams, ResolvedSecrets
from vgi_rpc.rpc import OutputCollector

from vgi_polymarket import polymarket_api as api
from vgi_polymarket.schemas import PRICE_HISTORY_SCHEMA
from vgi_polymarket.trading import PriceHistoryArgs, PriceHistoryFunction


def _params() -> ProcessParams[PriceHistoryArgs]:
    return ProcessParams(
        args=PriceHistoryArgs(token_id="unused", period="1d", fidelity=1),
        init_call=None,
        init_response=None,
        output_schema=PRICE_HISTORY_SCHEMA,
        settings={},
        secrets=ResolvedSecrets(),
        storage=None,  # type: ignore[arg-type] - this worker uses no storage
    )


def _input(tokens: list[str | None]) -> pa.RecordBatch:
    return pa.record_batch(
        {"token_id": tokens, "period": ["1d"] * len(tokens), "fidelity": [1] * len(tokens)},
        schema=pa.schema([("token_id", pa.string()), ("period", pa.string()), ("fidelity", pa.int64())]),
    )


def test_deduplicated_rows_keep_exact_parent_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    histories = [
        [{"t": 60, "p": 0.1}, {"t": 120, "p": 0.2}, {"t": 120, "p": 0.2}],
        [{"t": 60, "p": "0.3"}, {"t": 60, "p": "0.300"}],
        [],
        [{"t": 60, "p": 0.4}, {"t": 60, "p": 0.4}],
    ]
    requested: list[str] = []

    def response(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/prices-history"
        assert request.url.params["interval"] == "1d"
        assert request.url.params["fidelity"] == "1"
        history = histories[len(requested)]
        requested.append(request.url.params["market"])
        return httpx.Response(200, json={"history": history}, headers={"cache-control": "max-age=90"})

    monkeypatch.setattr(api, "open_client", lambda: httpx.Client(transport=httpx.MockTransport(response)))
    batch = _input(["a", "b", "empty", "a", None])
    output = OutputCollector(PRICE_HISTORY_SCHEMA, producer_mode=False)
    PriceHistoryFunction.process(
        _params(), None, batch, cast("OutputCollector", _TrackingOutputCollector(output))
    )

    emitted = output.data_batch
    parents = _decode_parent_rows(
        emitted.custom_metadata, output_rows=emitted.batch.num_rows, input_rows=batch.num_rows
    )
    assert requested == ["a", "b", "empty", "a"]
    assert parents == [0, 0, 1, 3]
    assert emitted.batch.column("token_id").to_pylist() == ["a", "a", "b", "a"]
    assert emitted.batch.column("price").to_pylist() == [
        Decimal("0.1"),
        Decimal("0.2"),
        Decimal("0.3"),
        Decimal("0.4"),
    ]
    assert len(parents) == emitted.batch.num_rows


def test_conflicting_response_fails_before_emit(monkeypatch: pytest.MonkeyPatch) -> None:
    def response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"history": [{"t": 60, "p": 0.1}, {"t": 60, "p": 0.2}]})

    monkeypatch.setattr(api, "open_client", lambda: httpx.Client(transport=httpx.MockTransport(response)))
    output = OutputCollector(PRICE_HISTORY_SCHEMA, producer_mode=False)
    with pytest.raises(ValueError, match="Conflicting price-history observations.*'a'"):
        PriceHistoryFunction.process(
            _params(), None, _input(["a"]), cast("OutputCollector", _TrackingOutputCollector(output))
        )
    with pytest.raises(RuntimeError, match="No data batch was emitted"):
        _ = output.data_batch
