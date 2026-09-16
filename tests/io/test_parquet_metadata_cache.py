from __future__ import annotations

import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft


@pytest.fixture
def range_server(tmp_path):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.respond(head=True)

        def do_GET(self):
            self.respond(head=False)

        def respond(self, head):
            path = (tmp_path / unquote(self.path.lstrip("/"))).resolve()
            if not path.is_relative_to(tmp_path) or not path.is_file():
                self.send_error(404)
                return
            size = path.stat().st_size
            start, end = 0, size
            requested = self.headers.get("Range")
            if requested:
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", requested)
                if not match or not any(match.groups()):
                    self.send_error(416)
                    return
                first, last = match.groups()
                if first:
                    start = int(first)
                    end = min(size, int(last) + 1) if last else size
                else:
                    start = max(0, size - int(last))
                if not 0 <= start < end <= size:
                    self.send_error(416)
                    return
            self.send_response(206 if requested else 200)
            self.send_header("Content-Length", str(end - start))
            self.send_header("Accept-Ranges", "bytes")
            if requested:
                self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
            self.end_headers()
            if not head:
                requests.append((path.name, start, end))
                with path.open("rb") as source:
                    source.seek(start)
                    self.wfile.write(source.read(end - start))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_schema_inference_metadata_reused_for_http_scan(tmp_path, range_server):
    page_table = pa.table({"id": range(8192), "payload": [f"value-{i}" for i in range(8192)]})
    path = tmp_path / "cached-footer.parquet"
    pq.write_table(page_table, path, row_group_size=128, write_page_index=True)
    df = daft.read_parquet(f"{range_server[0]}/{path.name}").select("payload")
    before = list(range_server[1])
    assert before  # Schema inference has fetched the footer.
    actual = df.to_arrow()
    expected = page_table.select(["payload"])
    assert actual.cast(expected.schema).equals(expected)
    footer_requests_before = [(start, end) for _, start, end in before if end == path.stat().st_size]
    footer_requests_after = [(start, end) for _, start, end in range_server[1] if end == path.stat().st_size]
    assert footer_requests_before
    assert footer_requests_after == footer_requests_before
