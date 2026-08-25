"""
Tests for `AsyncAnnex`'s pipelined ``--batch`` request handling.

These don't need git-annex: they drive `AsyncAnnex._pipeline` against a stand-in
subprocess that speaks the same one-response-line-per-request-line protocol, so
that the property that actually matters -- that a whole chunk of requests can be
written while the responses are read concurrently, without deadlocking on a full
pipe -- is exercised against real OS pipes.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import anyio
from anyio.streams.text import TextReceiveStream
import pytest

from backups2datalad.aioutil import LineReceiveStream, TextProcess
from backups2datalad.annex import AsyncAnnex

pytestmark = pytest.mark.anyio

# Reads lines and, for each, writes back a line padded out to `padding` bytes so
# that the responses to a chunk of requests are far larger than a pipe buffer.
ECHO_SCRIPT = """\
import sys
padding = int(sys.argv[1])
for line in sys.stdin:
    sys.stdout.write("resp:" + line.rstrip("\\n").ljust(padding) + "\\n")
    sys.stdout.flush()
"""


async def open_echo(padding: int = 0) -> TextProcess:
    p = await anyio.open_process(
        [sys.executable, "-c", ECHO_SCRIPT, str(padding)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    assert p.stdout is not None
    return TextProcess(p, LineReceiveStream(TextReceiveStream(p.stdout)), "echo")


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
    Both the request payload and the response stream are made much larger than
    the OS pipe buffer (typically 64 KiB), so this fails if requests are written
    without reading responses concurrently, or vice versa.
    """
    annex = AsyncAnnex(tmp_path)
    p = await open_echo(padding=4096)
    try:
        requests = [f"req{i:06d}\n" for i in range(1000)]
        with anyio.fail_after(60):
            responses = await annex._pipeline(p, requests)
        assert len(responses) == 1000
        assert responses[0].startswith("resp:req000000")
        assert responses[-1].startswith("resp:req000999")
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
