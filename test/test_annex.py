"""
Tests for `AsyncAnnex`'s pipelined ``--batch`` request handling.

These don't need git-annex: they drive `AsyncAnnex` against a stand-in
subprocess that speaks the same one-response-line-per-request-line protocol, so
that the properties that actually matter -- that a whole chunk of requests can
be written while the responses are read concurrently without deadlocking on a
full pipe, that responses stay paired with their requests across chunk and
process-restart boundaries, and that a desynchronised stream is caught rather
than silently misattributed -- are exercised against real OS pipes.
"""

from __future__ import annotations

import fcntl
from pathlib import Path
import subprocess
import sys

import anyio
from anyio.streams.text import TextReceiveStream
import pytest

from backups2datalad.aioutil import LineReceiveStream, TextProcess
from backups2datalad.annex import AsyncAnnex, render_request
from backups2datalad.consts import BATCH_CHUNK_SIZE

pytestmark = pytest.mark.anyio

# `F_GETPIPE_SZ`; the default is 65536 on Linux.
F_GETPIPE_SZ = 1032

# Reads lines and, for each, writes back a line padded out to `padding` bytes so
# that the responses to a chunk of requests can be made far larger than a pipe
# buffer.
ECHO_SCRIPT = """\
import sys
padding = int(sys.argv[1])
for line in sys.stdin:
    sys.stdout.write("resp:" + line.rstrip("\\n").ljust(padding) + "\\n")
    sys.stdout.flush()
"""


def pipe_buf_size() -> int:
    r, w = None, None
    try:
        r, w = __import__("os").pipe()
        return int(fcntl.fcntl(w, F_GETPIPE_SZ))
    finally:
        for fd in (r, w):
            if fd is not None:
                __import__("os").close(fd)


async def open_echo(padding: int = 0) -> TextProcess:
    p = await anyio.open_process(
        [sys.executable, "-c", ECHO_SCRIPT, str(padding)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    assert p.stdout is not None
    return TextProcess(p, LineReceiveStream(TextReceiveStream(p.stdout)), "echo")


class EchoAnnex(AsyncAnnex):
    """
    An `AsyncAnnex` whose ``--batch`` subprocesses are the echo script above.

    ``opened`` counts how many subprocesses have been spawned, so that
    ``restart_after`` behaviour can be asserted.
    """

    def __init__(self, repo: Path, padding: int = 0) -> None:
        super().__init__(repo)
        self.padding = padding
        self.opened = 0

    async def _get_proc(self, name: str) -> TextProcess:
        try:
            return self.procs[name]
        except KeyError:
            self.opened += 1
            p = await open_echo(self.padding)
            self.procs[name] = p
            return p


@pytest.mark.ai_generated
async def test_pipeline_preserves_order(tmp_path: Path) -> None:
    annex = AsyncAnnex(tmp_path)
    p = await open_echo()
    try:
        requests = [f"req{i}\n" for i in range(500)]
        responses = await annex._pipeline(p, requests)
        assert [r.rstrip("\n") for r in responses] == [
            f"resp:req{i}" for i in range(500)
        ]
    finally:
        await p.aclose()


@pytest.mark.ai_generated
async def test_pipeline_does_not_deadlock_on_full_pipes(tmp_path: Path) -> None:
    """
    Both the request payload and the response stream are made larger than the
    OS pipe buffer, so this hangs if requests are written without reading
    responses concurrently, or vice versa.
    """
    bufsize = pipe_buf_size()
    # Long enough that the requests alone overflow the pipe buffer several
    # times over:
    reqlen = max(200, (bufsize * 4) // BATCH_CHUNK_SIZE)
    requests = [f"req{i:06d}".ljust(reqlen) + "\n" for i in range(BATCH_CHUNK_SIZE)]
    assert len("".join(requests)) > bufsize
    annex = AsyncAnnex(tmp_path)
    p = await open_echo(padding=bufsize // 16)
    try:
        with anyio.fail_after(60):
            responses = await annex._pipeline(p, requests)
        assert len(responses) == BATCH_CHUNK_SIZE
        assert responses[0].startswith("resp:req000000")
        assert responses[-1].startswith(f"resp:req{BATCH_CHUNK_SIZE - 1:06d}")
        # The responses alone also exceed the buffer:
        assert sum(map(len, responses)) > bufsize
    finally:
        await p.aclose()


@pytest.mark.ai_generated
async def test_pipeline_empty(tmp_path: Path) -> None:
    annex = AsyncAnnex(tmp_path)
    p = await open_echo()
    try:
        assert await annex._pipeline(p, []) == []
    finally:
        await p.aclose()


@pytest.mark.ai_generated
async def test_batch_pairs_responses_across_chunks(tmp_path: Path) -> None:
    """Items must stay paired with their responses across chunk boundaries."""
    n = BATCH_CHUNK_SIZE * 2 + 7
    items = [f"item{i:06d}" for i in range(n)]
    seen: list[tuple[str, str]] = []
    progress: list[int] = []
    annex = EchoAnnex(tmp_path)
    async with annex:
        await annex._batch(
            "registerurl",
            items,
            lambda it: f"{it}\n",
            lambda it, resp: seen.append((it, resp.rstrip("\n"))),
            progress=progress.append,
        )
    assert seen == [(it, f"resp:{it}") for it in items]
    assert progress == [BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 2, n]
    assert annex.opened == 1


@pytest.mark.ai_generated
async def test_batch_discards_process_after_failure(tmp_path: Path) -> None:
    """
    A subprocess that dies mid-chunk must not be reused: its stdout could
    still hold unread responses, so a later request would read a stale one.
    The next call must get a fresh process and stay in sync on it.
    """
    annex = EchoAnnex(tmp_path)
    async with annex:
        p = await annex._get_proc("fromkey")
        await p.aclose()
        # anyio reports failures inside `_pipeline`'s task group as a group
        # (here wrapping the reader's EndOfStream and the writer's
        # "suddenly exited" RuntimeError).
        with pytest.raises(Exception, match="unhandled errors in a TaskGroup"):
            await annex._batch("fromkey", ["a", "b"], lambda it: f"{it}\n")
        assert "fromkey" not in annex.procs
        assert annex.opened == 1
        # A later call reopens and pairs responses correctly on the new process
        seen: list[tuple[str, str]] = []
        items = [f"item{i:04d}" for i in range(BATCH_CHUNK_SIZE + 5)]
        await annex._batch(
            "fromkey",
            items,
            lambda it: f"{it}\n",
            lambda it, resp: seen.append((it, resp.rstrip("\n"))),
        )
        assert annex.opened == 2
        assert seen == [(it, f"resp:{it}") for it in items]


@pytest.mark.ai_generated
def test_render_request_rejects_embedded_newlines() -> None:
    assert render_request(lambda s: f"{s}\n", "ok") == "ok\n"
    with pytest.raises(ValueError, match="not a single line"):
        render_request(lambda s: f"{s}\n", "two\nlines")
    with pytest.raises(ValueError, match="not a single line"):
        render_request(lambda s: s, "no-newline")


@pytest.mark.ai_generated
async def test_mkkeys_rejects_desynchronised_response(tmp_path: Path) -> None:
    """
    `examinekey` output is a bare key, so a shifted response stream would
    otherwise annex files under the wrong keys without any error.
    """
    annex = EchoAnnex(tmp_path)
    async with annex:
        with pytest.raises(RuntimeError, match="does not start with the expected"):
            await annex.mkkeys([("foo.txt", 12, "0" * 32)])
