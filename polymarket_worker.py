# /// script
# requires-python = ">=3.14"
# dependencies = ["vgi-python[http,haybarn]>=0.55.2,<0.56", "vgi-rpc>=0.53.1", "httpx>=0.28"]
# ///
"""Stdio entry point for the Polymarket VGI worker (``uv run``).

ATTACH 'polymarket' (TYPE vgi, LOCATION 'uv run polymarket_worker.py');
"""

from __future__ import annotations

from vgi_polymarket.worker import main

if __name__ == "__main__":
    main()
