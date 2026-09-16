"""
Integration tests for ``arequest``'s error-diagnostics logging and its
GitHub rate-limit handling.

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

from backups2datalad.aioutil import (
    GitHubGate,
    GitHubRateLimited,
    arequest,
    is_rate_limited,
)

pytestmark = pytest.mark.anyio


# A canned body that mimics what GitHub returns when the secondary
# (abuse) rate limit fires.  We assert substrings of this in the logs.
_RATE_LIMIT_BODY = (
    b'{"message":"You have exceeded a secondary rate limit. '
    b'Please wait a few minutes before you try again.",'
    b'"documentation_url":"https://docs.github.com/rest/overview/'
    b'resources-in-the-rest-api#secondary-rate-limits"}'
)


# Default rate-limit headers sent with a 403 when a response gives none
_DEFAULT_403_HEADERS = {
    "Retry-After": "1",
    "X-RateLimit-Remaining": "0",
    "X-RateLimit-Reset": "1234567890",
    "X-RateLimit-Resource": "core",
}


def _make_handler(
    responses: list[tuple[int, bytes] | tuple[int, bytes, dict[str, str]]],
    counter: dict[str, int],
) -> type[BaseHTTPRequestHandler]:
    """
    Build a one-shot HTTP handler class that returns each response in
    ``responses`` in order, cycling on the last entry, recording the
    number of requests served in ``counter['count']``.  A response is
    ``(status, body)`` -- a 403 then gets `_DEFAULT_403_HEADERS` -- or
    ``(status, body, headers)``.
    """

    class Handler(BaseHTTPRequestHandler):
        def _respond(self) -> None:
            n = counter["count"]
            counter["count"] = n + 1
            resp = responses[min(n, len(responses) - 1)]
            status, body = resp[0], resp[1]
            headers = (
                resp[2]
                if len(resp) == 3
                else (_DEFAULT_403_HEADERS if status == 403 else {})
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_PATCH = _respond

        def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003
            pass  # silence default stderr access log

    return Handler


class FakeClock:
    """
    Deterministic stand-in for a `GitHubGate`'s ``clock``/``wall`` and
    ``sleep``: sleeping advances the clock and records the requested delay
    """

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.slept.append(delay)
        self.now += delay


def make_gate(clock: FakeClock, attempts: int | None = None) -> GitHubGate:
    gate = GitHubGate(clock=clock, wall=clock, sleep=clock.sleep)
    if attempts is not None:
        gate.attempts = attempts
    return gate


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


@pytest.mark.ai_generated
async def test_arequest_429_with_gate_retries_after_retry_after(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    With a `GitHubGate`, a 429 is retried after exactly the ``Retry-After``
    GitHub asked for, not after the local exponential backoff, and the
    retry is logged as a rate-limit event.
    """
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler(
        [
            (429, _RATE_LIMIT_BODY, {"Retry-After": "7"}),
            (200, b'{"ok": true}'),
        ],
        counter,
    )
    clock = FakeClock()
    gate = make_gate(clock)
    with caplog.at_level(logging.DEBUG, logger="backups2datalad"):
        with _serve(handler_cls) as base_url:
            async with httpx.AsyncClient() as client:
                r = await arequest(
                    client,
                    "PATCH",
                    f"{base_url}/repos/dandizarrs/abc",
                    json={"description": "x"},
                    gate=gate,
                )
    assert r.status_code == 200
    assert counter["count"] == 2
    assert clock.slept == [7.0]
    assert gate.consecutive == 0, "successful mutation must reset the count"
    msg = "\n".join(
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    )
    assert "RATELIMIT" in msg, msg
    assert "retry-after=7" in msg, msg


@pytest.mark.ai_generated
async def test_arequest_403_remaining_zero_with_gate_sleeps_until_reset() -> None:
    """
    A 403 with ``x-ratelimit-remaining: 0`` and no ``Retry-After`` sleeps
    until ``x-ratelimit-reset`` (relative to the gate's wall clock).
    """
    counter: dict[str, int] = {"count": 0}
    clock = FakeClock(start=1_000_000.0)
    handler_cls = _make_handler(
        [
            (
                403,
                _RATE_LIMIT_BODY,
                {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1000030"},
            ),
            (200, b'{"ok": true}'),
        ],
        counter,
    )
    gate = make_gate(clock)
    with _serve(handler_cls) as base_url:
        async with httpx.AsyncClient() as client:
            r = await arequest(client, "GET", f"{base_url}/repos/x/y", gate=gate)
    assert r.status_code == 200
    assert counter["count"] == 2
    assert clock.slept == [30.0]


@pytest.mark.ai_generated
async def test_arequest_429_without_gate_raises() -> None:
    """Without a gate (DANDI API, S3) a 429 is fatal, exactly as before."""
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler([(429, _RATE_LIMIT_BODY)], counter)
    with _serve(handler_cls) as base_url:
        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.HTTPStatusError):
                await arequest(client, "GET", f"{base_url}/whatever")
    assert counter["count"] == 1


@pytest.mark.ai_generated
def test_is_rate_limited() -> None:
    assert is_rate_limited(429, {}, "")
    assert is_rate_limited(403, {"Retry-After": "5"}, "")
    assert is_rate_limited(403, {"x-ratelimit-remaining": "0"}, "")
    assert is_rate_limited(403, {}, _RATE_LIMIT_BODY.decode())
    assert is_rate_limited(
        403, {}, "You have been temporarily blocked from content creation."
    )
    assert not is_rate_limited(403, {"x-ratelimit-remaining": "42"}, "Bad credentials")
    assert not is_rate_limited(404, {"Retry-After": "5"}, "")


@pytest.mark.ai_generated
async def test_github_gate_fallback_escalates_and_resets() -> None:
    """
    Without usable headers the gate waits GitHub's documented minimum of a
    minute, doubling per consecutive hit; hits during a running cooldown are
    the same incident; a successful mutation resets the escalation.
    """
    clock = FakeClock()
    gate = make_gate(clock)
    await gate.wait()
    assert clock.slept == [], "no cooldown -> no sleep"
    gate.note_rate_limited({}, "first")
    assert (gate.consecutive, gate.cooldown_until - clock()) == (1, 60)
    gate.note_rate_limited({}, "raced with the first")
    assert gate.consecutive == 1, "a hit inside a running cooldown must not escalate"
    await gate.wait()
    assert clock.slept == [60.0]
    gate.note_rate_limited({}, "second")
    assert (gate.consecutive, gate.cooldown_until - clock()) == (2, 120)
    await gate.wait()
    gate.note_rate_limited({}, "third")
    assert gate.cooldown_until - clock() == 240
    gate.note_success()
    assert gate.consecutive == 0
    await gate.wait()
    gate.note_rate_limited({}, "after success")
    assert gate.cooldown_until - clock() == 60


@pytest.mark.ai_generated
async def test_github_gate_header_precedence() -> None:
    """``Retry-After`` wins over ``x-ratelimit-reset``, which wins over the fallback."""
    clock = FakeClock(start=1_000.0)
    gate = make_gate(clock)
    gate.note_rate_limited(
        {"Retry-After": "5", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1300"},
        "both",
    )
    assert gate.cooldown_until - clock() == 5
    await gate.wait()
    gate.note_rate_limited(
        {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1300"}, "reset only"
    )
    assert gate.cooldown_until - clock() == 1300 - 1005
    await gate.wait()
    gate.note_rate_limited({"X-RateLimit-Remaining": "7"}, "quota left")
    assert gate.cooldown_until - clock() == 240, "third consecutive hit -> fallback"


@pytest.mark.ai_generated
async def test_github_gate_gives_up_and_spaces_mutations() -> None:
    clock = FakeClock()
    gate = make_gate(clock, attempts=2)
    async with gate.mutation():
        pass
    async with gate.mutation():
        pass
    assert clock.slept == [1.0], "second mutation waits out the spacing"
    for what in ("one", "two"):
        gate.note_rate_limited({}, what)
        await gate.wait()
        assert not gate.gave_up
    gate.note_rate_limited({}, "three")
    assert gate.gave_up
    with pytest.raises(GitHubRateLimited):
        async with gate.mutation():
            pass
    # Reads still sleep out the cooldown but are not refused by the gate
    slept_before = len(clock.slept)
    await gate.wait()
    assert clock.slept[slept_before:] == [240.0]
    assert not clock.slept[slept_before + 1 :]


@pytest.mark.ai_generated
async def test_arequest_get_stays_bounded_after_give_up() -> None:
    """
    A read that keeps being rate-limited is retried through the gate only
    until the gate gives up; then it falls back to the ordinary handling
    (here: not in ``retry_on``, so it raises) instead of looping forever.
    """
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler(
        [(403, _RATE_LIMIT_BODY, {"Retry-After": "0"})], counter
    )
    clock = FakeClock()
    gate = make_gate(clock, attempts=2)
    with _serve(handler_cls) as base_url:
        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.HTTPStatusError):
                await arequest(client, "GET", f"{base_url}/repos/x/y", gate=gate)
    assert counter["count"] == 3, "two slept-out hits, then the third gives up"
    assert clock.slept == [1.0, 1.0], "Retry-After: 0 is clamped to 1 s"
    assert gate.gave_up


@pytest.mark.ai_generated
async def test_arequest_mutation_raises_once_gate_gave_up() -> None:
    counter: dict[str, int] = {"count": 0}
    handler_cls = _make_handler(
        [(429, _RATE_LIMIT_BODY, {"Retry-After": "5"})], counter
    )
    clock = FakeClock()
    gate = make_gate(clock, attempts=1)
    with _serve(handler_cls) as base_url:
        async with httpx.AsyncClient() as client:
            with pytest.raises(GitHubRateLimited):
                await arequest(
                    client,
                    "PATCH",
                    f"{base_url}/repos/x/y",
                    json={},
                    retry_on=[403],
                    gate=gate,
                )
    assert counter["count"] == 2
    assert clock.slept == [5.0]
