"""
Integration tests for ``arequest``'s error-diagnostics logging.

These spin up a real local HTTP server returning ``403 Forbidden`` with a
GitHub-secondary-rate-limit-shaped body and ``retry-after`` /
``x-ratelimit-*`` headers, then drive ``arequest`` against it with httpx so
that the full path through ``_describe_http_error`` is exercised against
real ``httpx.Response`` objects — not a mocked-out response — to catch
breakage if httpx changes how headers/body are surfaced.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import threading

import httpx
import pytest

from backups2datalad.aioutil import arequest

pytestmark = pytest.mark.anyio


# A canned body that mimics what GitHub returns when the secondary
# (abuse) rate limit fires.  We assert substrings of this in the logs.
_RATE_LIMIT_BODY = (
    b'{"message":"You have exceeded a secondary rate limit. '
    b'Please wait a few minutes before you try again.",'
    b'"documentation_url":"https://docs.github.com/rest/overview/'
    b'resources-in-the-rest-api#secondary-rate-limits"}'
)


def _make_handler(
    responses: list[tuple[int, bytes]],
    counter: dict[str, int],
) -> type[BaseHTTPRequestHandler]:
    """
    Build a one-shot HTTP handler class that returns each response in
    ``responses`` in order, cycling on the last entry, recording the
    number of requests served in ``counter['count']``.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            n = counter["count"]
            counter["count"] = n + 1
            status, body = responses[min(n, len(responses) - 1)]
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            if status == 403:
                self.send_header("Retry-After", "1")
                self.send_header("X-RateLimit-Remaining", "0")
                self.send_header("X-RateLimit-Reset", "1234567890")
                self.send_header("X-RateLimit-Resource", "core")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003
            pass  # silence default stderr access log

    return Handler


@contextmanager
def _serve(
    handler_cls: type[BaseHTTPRequestHandler],
) -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.ai_generated
async def test_arequest_403_no_retry_logs_body_and_headers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    With ``retry_on=()`` a 403 must propagate immediately, and an ERROR-level
    log emitted just before the raise must include the response body excerpt
    and the rate-limit / retry-after headers.
    """
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler([(403, _RATE_LIMIT_BODY)], counter)
    with caplog.at_level(logging.DEBUG, logger="backups2datalad"):
        with _serve(handler_cls) as base_url:
            async with httpx.AsyncClient() as client:
                with pytest.raises(httpx.HTTPStatusError):
                    await arequest(client, "GET", f"{base_url}/repos/dandisets/000005")

    assert counter["count"] == 1, "Should have made exactly one request (no retry)"

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, (
        "Expected an ERROR log emitted before the final raise; "
        f"got records: {[(r.levelno, r.getMessage()) for r in caplog.records]}"
    )
    msg = "\n".join(r.getMessage() for r in error_records)
    assert "secondary rate limit" in msg, msg
    assert "retry-after=1" in msg, msg
    assert "x-ratelimit-remaining=0" in msg, msg
    assert "x-ratelimit-reset=1234567890" in msg, msg
    assert "x-ratelimit-resource=core" in msg, msg


@pytest.mark.ai_generated
async def test_arequest_403_retries_and_warning_carries_details(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    With ``retry_on=[403]`` (matching ``GitHub.get_repo`` and
    ``GitHub.edit_repo``) the request must retry; the WARNING emitted before
    the sleep must include the same body + header diagnostics.
    """
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler(
        [(403, _RATE_LIMIT_BODY), (200, b'{"ok": true}')],
        counter,
    )
    with caplog.at_level(logging.DEBUG, logger="backups2datalad"):
        with _serve(handler_cls) as base_url:
            async with httpx.AsyncClient() as client:
                r = await arequest(
                    client,
                    "GET",
                    f"{base_url}/repos/dandisets/000005",
                    retry_on=[403],
                )
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert counter["count"] == 2, "Expected one 403 retry then one 200"

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, (
        "Expected a WARNING log on retry; "
        f"got records: {[(r.levelno, r.getMessage()) for r in caplog.records]}"
    )
    msg = "\n".join(r.getMessage() for r in warnings)
    assert "Retrying GET request" in msg, msg
    assert "secondary rate limit" in msg, msg
    assert "retry-after=1" in msg, msg
    assert "x-ratelimit-remaining=0" in msg, msg


@pytest.mark.ai_generated
async def test_arequest_2xx_emits_no_error_diagnostics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Sanity check: a successful response must not invoke the diagnostics path
    or emit any ERROR / WARNING records.  Guards against the helper being
    accidentally wired into the happy path.
    """
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler([(200, b'{"ok": true}')], counter)
    with caplog.at_level(logging.DEBUG, logger="backups2datalad"):
        with _serve(handler_cls) as base_url:
            async with httpx.AsyncClient() as client:
                r = await arequest(client, "GET", f"{base_url}/whatever")
    assert r.status_code == 200
    assert counter["count"] == 1
    for record in caplog.records:
        assert record.levelno < logging.WARNING, record.getMessage()
