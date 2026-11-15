"""Serve exact recorded responses or forward the same bounded public reads."""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from urllib.request import Request, urlopen


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
        self._audit_lock = threading.Lock()
        self.mode = mode
        self.artifact_dir = artifact_dir
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "request-audit.json").write_text("[]\n")
        manifest = json.loads((directory / "manifest.json").read_text())
        entries = {}
        for entry in manifest["entries"]:
            file = Path(entry["filename"])
            if file.is_absolute() or ".." in file.parts:
                raise ValueError("Fixture filenames must be relative and contained")
            origin = urlsplit(entry["url"])
            allowed = origin.scheme == "https" and origin.netloc in {
                "gamma-api.polymarket.com",
                "clob.polymarket.com",
            }
            if not allowed or not (
                entry["method"] == "GET"
                or (
                    entry["method"] == "POST"
                    and origin.netloc == "clob.polymarket.com"
                    and origin.path == "/books"
                )
            ):
                raise ValueError("Manifest contains an unsupported public read")
            body = (directory / file).read_bytes()
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
                with owner._audit_lock:
                    owner.requests.append(record)
                try:
                    key = request_key(self.command, self.path, raw)
                    if key not in entries:
                        raise ValueError(f"Request outside recorded reads; no fallback: {key}")
                    entry, body = entries[key]
                    if mode == "live":
                        payload = (
                            json.dumps(entry["request_json"]).encode() if entry["request_json"] else None
                        )
                        request = Request(
                            entry["url"],
                            data=payload,
                            method=entry["method"],
                            headers={"Content-Type": "application/json"},
                        )
                        with urlopen(request, timeout=35) as response:
                            body = response.read()
                            record["source_date"] = response.headers.get("date")
                    owner._save_audit(
                        record,
                        status=200,
                        bytes=len(body),
                        sha256=hashlib.sha256(body).hexdigest(),
                        received_utc=datetime.now(UTC).isoformat(),
                        source_url=entry["url"],
                    )
                    (artifact_dir / entry["filename"]).write_bytes(body)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as exc:
                    owner._save_audit(record, status=400, error=str(exc))
                    self.send_error(400, str(exc))

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self._closed = False
        names = ["POLYMARKET_GAMMA_URL", "POLYMARKET_CLOB_URL", "POLYMARKET_DATA_URL"]
        self.previous_env = {name: os.environ.get(name) for name in names}
        for name, path in zip(names, ["gamma", "clob", "unavailable"], strict=True):
            os.environ[name] = self.url + "/" + path
        atexit.register(self.close)

    def _save_audit(self, record, **changes):
        with self._audit_lock:
            record.update(changes)
            (self.artifact_dir / "request-audit.json").write_text(json.dumps(self.requests, indent=2))

    def close(self):
        if self._closed:
            return
        self._closed = True
        for name, value in self.previous_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
