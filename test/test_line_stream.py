"""
Tests for ``LineReceiveStream`` and ``iter_null_separated``.

``LineReceiveStream`` is checked against `io.StringIO` in universal-newlines
mode, which is the same `io.IncrementalNewlineDecoder` the implementation is
built on but reached through an independent, well-known API: for any text,
``io.StringIO(text, newline=None).readlines()`` is exactly the sequence of
lines the stream must produce, however the text is chopped into chunks.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import io
import random
from typing import Any, Callable

import anyio
import pytest

from backups2datalad.aioutil import LineReceiveStream, iter_null_separated

pytestmark = pytest.mark.anyio


class Chunks(anyio.abc.ObjectReceiveStream[str]):
    """A minimal ``str`` receive stream yielding the given chunks."""

    def __init__(self, chunks: Iterable[str]) -> None:
        self.chunks = list(chunks)
        self.closed = False
        self._attrs: Mapping[Any, Callable[[], Any]] = {}

    async def receive(self) -> str:
        if not self.chunks:
            raise anyio.EndOfStream()
        return self.chunks.pop(0)

    async def aclose(self) -> None:
        self.closed = True

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._attrs


async def read_all(stream: anyio.abc.ObjectReceiveStream[str]) -> list[str]:
    lines = []
    while True:
        try:
            lines.append(await stream.receive())
        except anyio.EndOfStream:
            return lines


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "chunks,expected",
    [
        (["a\nb\n"], ["a\n", "b\n"]),
        (["a\r\nb\r\n"], ["a\n", "b\n"]),
        (["a\rb\r"], ["a\n", "b\n"]),
        # A CRLF straddling a chunk boundary is still one terminator
        (["a\r", "\nb\n"], ["a\n", "b\n"]),
        (["a\r", "b\n"], ["a\n", "b\n"]),
        (["no-terminator"], ["no-terminator"]),
        (["a\n\n\nb"], ["a\n", "\n", "\n", "b"]),
        (["xxxxx\r"], ["xxxxx\n"]),
        ([], []),
        (["", "", "a\n", ""], ["a\n"]),
        # A line assembled from many chunks comes back whole
        (["ab"] * 10 + ["\n"], ["ab" * 10 + "\n"]),
    ],
)
async def test_line_receive_stream(chunks: list[str], expected: list[str]) -> None:
    assert await read_all(LineReceiveStream(Chunks(chunks))) == expected


@pytest.mark.ai_generated
async def test_line_receive_stream_arbitrary_chunkings() -> None:
    """
    Any chunking of any text must give the lines `io.StringIO` gives, so no
    terminator may be lost, duplicated, or split across a chunk boundary.
    """
    alphabet = ["a", "b", "\n", "\r", "\r\n", "", "zz", "\n\n", "c" * 40]
    rnd = random.Random(0xC0FFEE)
    for _ in range(2000):
        text = "".join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 16)))
        chunks = []
        i = 0
        while i < len(text):
            j = min(len(text), i + rnd.randint(1, 6))
            chunks.append(text[i:j])
            i = j
        expected = io.StringIO(text, newline=None).readlines()
        assert await read_all(LineReceiveStream(Chunks(chunks))) == expected, (
            f"chunking {chunks!r}"
        )


@pytest.mark.ai_generated
async def test_line_receive_stream_explicit_newline() -> None:
    """An explicit ``newline`` splits on exactly that string, untranslated."""
    stream = LineReceiveStream(Chunks(["a\r", "\nb\r\nc\nd"]), newline="\r\n")
    assert await read_all(stream) == ["a\r\n", "b\r\n", "c\nd"]


@pytest.mark.ai_generated
async def test_line_receive_stream_rejects_long_newline() -> None:
    """
    A terminator longer than ``"\\r\\n"`` could straddle a chunk boundary, which
    this implementation relies on being impossible, so it is refused up front.
    """
    with pytest.raises(ValueError, match="Unsupported 'newline' value"):
        LineReceiveStream(Chunks([]), newline="END")


@pytest.mark.ai_generated
async def test_line_receive_stream_aclose_propagates() -> None:
    transport = Chunks(["x\n"])
    await LineReceiveStream(transport).aclose()
    assert transport.closed


@pytest.mark.ai_generated
async def test_line_receive_stream_extra_attributes() -> None:
    transport = Chunks([])
    assert LineReceiveStream(transport).extra_attributes is transport.extra_attributes


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "chunks,expected",
    [
        (["a\0b\0"], ["a", "b"]),
        # A record with no trailing NUL is still yielded
        (["a\0b"], ["a", "b"]),
        # Empty records between NULs are preserved
        (["a\0\0b\0"], ["a", "", "b"]),
        # CR and LF are data, not separators
        (["a\r\nb\0c\0"], ["a\r\nb", "c"]),
        # A record split across chunks is reassembled
        (["ab", "cd", "\0"], ["abcd"]),
        ([], []),
        (["\0"], [""]),
    ],
)
async def test_iter_null_separated(chunks: list[str], expected: list[str]) -> None:
    assert [rec async for rec in iter_null_separated(Chunks(chunks))] == expected
