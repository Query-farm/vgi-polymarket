"""The two entry-point scripts must be runnable on their own.

`uv run polymarket_worker.py` resolves dependencies from the script's PEP-723
header, not from `pyproject.toml`, so the two lists drift silently: the package
imports fine under `pytest` while the worker a client actually launches dies at
import. Only an end-to-end ATTACH would otherwise notice.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ("polymarket_worker.py", "serve.py")

#: PEP 723 inline script metadata: a `# /// script` ... `# ///` comment block.
_BLOCK = re.compile(r"^# /// script$(.+?)^# ///$", re.MULTILINE | re.DOTALL)


def _script_dependencies(path: Path) -> set[str]:
    match = _BLOCK.search(path.read_text())
    assert match is not None, f"{path.name} has no PEP-723 script header"
    body = "".join(
        line.removeprefix("# ").removeprefix("#") for line in match.group(1).splitlines(keepends=True)
    )
    return {_name(spec) for spec in tomllib.loads(body)["dependencies"]}


def _name(requirement: str) -> str:
    return re.split(r"[\[><=!~;\s]", requirement, maxsplit=1)[0].strip().lower()


@pytest.fixture(scope="module")
def project_dependencies() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {_name(spec) for spec in data["project"]["dependencies"]}


class TestScriptHeaders:
    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_covers_every_runtime_dependency(
        self, script: str, project_dependencies: set[str]
    ) -> None:
        declared = _script_dependencies(ROOT / script)
        missing = project_dependencies - declared
        assert missing == set(), (
            f"{script} would fail at import: its PEP-723 header is missing {sorted(missing)}"
        )

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_declares_nothing_unknown(self, script: str, project_dependencies: set[str]) -> None:
        """The scripts also pull vgi-rpc directly, but nothing beyond that."""
        extra = _script_dependencies(ROOT / script) - project_dependencies - {"vgi-rpc"}
        assert extra == set(), f"{script} declares dependencies the project does not: {sorted(extra)}"

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_the_script_imports_the_worker(self, script: str) -> None:
        assert "vgi_polymarket.worker" in (ROOT / script).read_text()


class TestTransport:
    """Content encoding is negotiated by what is installed, not by our code.

    httpx builds `Accept-Encoding` from the codecs it can find at import time.
    Polymarket's CDN answers with gzip on all three hosts even when brotli and
    zstd are offered, which is why `brotli` is deliberately NOT a dependency
    here — unlike the sibling Kalshi worker, where it saves 35% on a 15 MB
    catalog. A behavioural check is the only thing that would notice if that
    reasoning went stale.
    """

    @staticmethod
    def _request_headers() -> httpx.Headers:
        seen: dict[str, httpx.Headers] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["headers"] = request.headers
            return httpx.Response(200, json={})

        from vgi_polymarket import polymarket_api

        with httpx.Client(transport=httpx.MockTransport(handler), timeout=polymarket_api.TIMEOUT) as client:
            client.get("https://example.invalid/x")
        return seen["headers"]

    def test_gzip_is_offered(self) -> None:
        """The only encoding Polymarket actually serves."""
        assert "gzip" in self._request_headers()["accept-encoding"]

    def test_brotli_is_not_a_declared_dependency(self) -> None:
        """It would be installed and never negotiated; see the class docstring."""
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        assert not any("brotli" in d.lower() for d in data["project"]["dependencies"])

    def test_no_credential_dependency(self) -> None:
        """There is no read credential to sign with, so nothing to sign it."""
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        assert not any("cryptography" in d.lower() for d in data["project"]["dependencies"])


class TestEntryPoints:
    def test_console_scripts_resolve(self) -> None:
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        scripts = data["project"]["scripts"]
        assert scripts["vgi-polymarket"] == "vgi_polymarket.worker:main"
        assert scripts["vgi-polymarket-http"] == "vgi_polymarket.worker:main_http"

    def test_the_version_is_the_single_source(self) -> None:
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        assert data["tool"]["hatch"]["version"]["path"] == "vgi_polymarket/__init__.py"
