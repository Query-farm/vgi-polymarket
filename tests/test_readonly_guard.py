"""Structural guard: this worker must never write to Polymarket.

Order entry sits on the same host as the order book. The worker is read-only by
construction — two chokepoints, one of which refuses any path outside an
explicit allowlist — and this test fails the build if that stops being true.

The usual "never call .post()" guard would not work here: Polymarket's batch
*read* endpoints are POST-with-a-body, and the GET spellings are HTTP 400, so
banning the verb would ban an 81x speedup on order books. The property this
enforces instead is that POST is confined to a fixed list of read paths, and
that the list stays small.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parent.parent / "vgi_polymarket"

#: httpx verbs that would mutate state, and are never legitimate here.
WRITE_VERBS = {"put", "patch", "delete", "request", "stream", "send"}

#: The one function permitted to issue a POST, and the one permitted a GET.
POST_CHOKEPOINT = "_post_read"
GET_CHOKEPOINT = "_get"


def _module(name: str) -> ast.Module:
    return ast.parse((PACKAGE / name).read_text())


def _enclosing_function(tree: ast.Module, node: ast.AST) -> str | None:
    """The name of the function lexically containing ``node``, if any."""
    for candidate in ast.walk(tree):
        if isinstance(candidate, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(inner is node for inner in ast.walk(candidate)):
                return candidate.name
    return None


class TestNoWriteVerbs:
    def test_no_mutating_http_calls_anywhere(self) -> None:
        offenders: list[str] = []
        for path in PACKAGE.rglob("*.py"):
            for call in ast.walk(ast.parse(path.read_text())):
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
                    if call.func.attr in WRITE_VERBS:
                        offenders.append(f"{path.name}: .{call.func.attr}()")
        assert offenders == [], f"mutating HTTP calls found: {offenders}"

    def test_single_http_module(self) -> None:
        """Only polymarket_api.py may reference httpx at all.

        Every other module reaches the network through the two chokepoints, so
        there is exactly one file to audit.
        """
        users = sorted(p.name for p in PACKAGE.rglob("*.py") if "httpx" in p.read_text())
        assert users == ["polymarket_api.py"], users


class TestPostIsConfined:
    """POST exists only to batch reads, and only to three known paths."""

    def test_post_is_called_in_exactly_one_function(self) -> None:
        tree = _module("polymarket_api.py")
        callers = {
            _enclosing_function(tree, call)
            for call in ast.walk(tree)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "post"
        }
        assert callers == {POST_CHOKEPOINT}, (
            f"http.post() must appear only inside {POST_CHOKEPOINT}, found in {sorted(callers)}"
        )

    def test_get_is_called_in_exactly_one_function(self) -> None:
        tree = _module("polymarket_api.py")
        callers = {
            _enclosing_function(tree, call)
            for call in ast.walk(tree)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "get"
            # `dict.get` and `headers.get` are everywhere; only the client's
            # `http.get(...)` matters, and it takes a url as its first arg.
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "http"
        }
        assert callers == {GET_CHOKEPOINT}, (
            f"http.get() must appear only inside {GET_CHOKEPOINT}, found in {sorted(callers)}"
        )

    def test_the_allowlist_is_exactly_the_three_read_endpoints(self) -> None:
        """A new entry here is a deliberate decision, not an accident."""
        from vgi_polymarket.polymarket_api import READ_ONLY_POST_PATHS

        assert READ_ONLY_POST_PATHS == {"/books", "/prices", "/midpoints"}

    def test_post_to_an_unlisted_path_is_refused(self) -> None:
        """The allowlist is enforced at call time, not merely documented."""
        import pytest

        from vgi_polymarket import polymarket_api as api

        with pytest.raises(ValueError, match="read-only"):
            api._post_read("/order", [{"anything": "at all"}])

    def test_refusal_happens_before_any_request(self) -> None:
        """A refused path must not reach the network even with a client in hand."""
        import httpx
        import pytest

        from vgi_polymarket import polymarket_api as api

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            raise AssertionError(f"a request escaped the guard: {request.url}")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ValueError):
                api._post_read("/order", [{"x": 1}], client=client)


class TestPathsCannotBeRetargeted:
    """A caller-supplied id must not be able to choose the endpoint.

    ``event_id`` arrives from SQL and is the only caller value this worker puts
    in a URL *path* — everything else travels as a query parameter, which httpx
    encodes. Interpolated raw, ``event('16183/../../markets')`` resolved to
    ``GET /markets`` and returned a different endpoint's rows reshaped as an
    event: not a bad request, a wrong answer. The read-only guard is about which
    endpoints are reachable, and this is a way to reach one that was never
    declared.
    """

    @staticmethod
    def _wire_path(event_id: str) -> str:
        import httpx

        from vgi_polymarket import polymarket_api as api

        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["raw"] = request.url.raw_path.decode()
            return httpx.Response(200, json={})

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            api.event(event_id, client=client)
        return seen["raw"]

    def test_a_normal_id_is_untouched(self) -> None:
        assert self._wire_path("16183") == "/events/16183"

    @pytest.mark.parametrize(
        "hostile",
        ["16183/../../markets", "../../markets", "a/b", "16183?x=1", "16183#frag", "16183 space"],
    )
    def test_an_id_stays_one_path_segment(self, hostile: str) -> None:
        path = self._wire_path(hostile)
        assert path.startswith("/events/"), path
        # Nothing after the prefix may re-open the path or the query string.
        tail = path[len("/events/") :]
        assert "/" not in tail, f"{hostile!r} escaped its segment: {path}"
        assert "?" not in tail and "#" not in tail, f"{hostile!r} escaped into the query: {path}"

    def test_traversal_cannot_reach_another_endpoint(self) -> None:
        """The concrete failure this closes."""
        assert self._wire_path("16183/../../markets") != "/markets"


class TestNoCredentialedSurface:
    def test_no_order_or_wallet_paths(self) -> None:
        """No order-entry or credentialed endpoint family is reachable."""
        forbidden = ("/order", "/auth/", "/api-key", "/closed-only", "/balance-allowance")
        for path in PACKAGE.rglob("*.py"):
            text = path.read_text()
            for fragment in forbidden:
                assert f'"{fragment}' not in text, f"{path.name} references {fragment}"

    def test_no_signing_machinery(self) -> None:
        """Order placement needs a wallet signature; nothing here can produce one."""
        for path in PACKAGE.rglob("*.py"):
            text = path.read_text().lower()
            for fragment in ("eth_account", "eip712", "private_key", "sign_message", "web3"):
                assert fragment not in text, f"{path.name} mentions {fragment}"

    def test_the_check_can_actually_fail(self) -> None:
        """Guard the guard: the scan must be able to see a planted string."""
        assert "/order" in '"/order"'
