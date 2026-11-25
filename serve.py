# /// script
# requires-python = ">=3.14"
# dependencies = ["vgi-python[http,haybarn]>=0.55.2,<0.56", "vgi-rpc>=0.53.1", "httpx>=0.28"]
# ///
"""HTTP entry point for the Polymarket VGI worker (``uv run serve.py --port 8000``)."""

from __future__ import annotations

from vgi_polymarket.worker import main_http

if __name__ == "__main__":
    main_http()
