"""Serve exact recorded responses or forward the same bounded public reads."""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import requests


def request_key(method, path, body):
    parsed = urlsplit(path)
    payload = json.loads(body) if body else None
    if isinstance(payload, list):
        payload = sorted(payload, key=lambda item: item["token_id"])
    return method, parsed.path, tuple(sorted(parse_qsl(parsed.query))), json.dumps(payload, sort_keys=True)


class RecordedAPI:
    def __init__(self, directory: Path, mode: str, artifact_dir: Path):
        if mode not in {"replay", "live"}:
            raise ValueError("DATA_MODE must be replay or live")
        self.requests = []
        self.mode = mode
        self.artifact_dir = artifact_dir
        artifact_dir.mkdir(exist_ok=True)
        (artifact_dir / "request-audit.json").write_text("[]\n")
        manifest = json.loads((directory / "manifest.json").read_text())
        entries = {}
        for entry in manifest["entries"]:
            body = (directory / entry["filename"]).read_bytes()
            if hashlib.sha256(body).hexdigest() != entry["sha256"]:
                raise ValueError(f"Recorded response hash mismatch: {entry['filename']}")
            prefix = "/gamma" if "gamma-api" in entry["url"] else "/clob"
            url = urlsplit(entry["url"])
            path = prefix + url.path + ("?" + url.query if url.query else "")
            raw = json.dumps(entry["request_json"]).encode() if entry["request_json"] else b""
            entries[request_key(entry["method"], path, raw)] = (entry, body)
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_GET(self):
                self.respond()

            def do_POST(self):
                self.respond()

            def respond(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                record = {
                    "method": self.command,
                    "path": self.path,
                    "started_utc": datetime.now(UTC).isoformat(),
                }
                owner.requests.append(record)
                try:
                    key = request_key(self.command, self.path, raw)
                    if key not in entries:
                        raise ValueError(f"Request outside recorded reads; no fallback: {key}")
                    entry, body = entries[key]
                    if mode == "live":
                        response = requests.request(
                            entry["method"], entry["url"], json=entry["request_json"], timeout=35
                        )
                        response.raise_for_status()
                        body = response.content
                        record["source_date"] = response.headers.get("date")
                    record.update(
                        status=200,
                        bytes=len(body),
                        sha256=hashlib.sha256(body).hexdigest(),
                        received_utc=datetime.now(UTC).isoformat(),
                        source_url=entry["url"],
                    )
                    (artifact_dir / entry["filename"]).write_bytes(body)
                    (artifact_dir / "request-audit.json").write_text(json.dumps(owner.requests, indent=2))
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as exc:
                    record.update(status=400, error=str(exc))
                    (artifact_dir / "request-audit.json").write_text(json.dumps(owner.requests, indent=2))
                    self.send_error(400, str(exc))

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
