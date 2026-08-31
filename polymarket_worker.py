# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python[http]", "vgi-rpc", "httpx"]
#
# [tool.uv.sources]
# vgi-python = { path = "../vgi-python" }
# vgi-rpc = { path = "../vgi-rpc" }
# ///
"""Stdio entry point for the Polymarket VGI worker (``uv run``).

ATTACH 'polymarket' (TYPE vgi, LOCATION 'uv run polymarket_worker.py');
"""

from __future__ import annotations

from vgi_polymarket.worker import main

if __name__ == "__main__":
    main()
