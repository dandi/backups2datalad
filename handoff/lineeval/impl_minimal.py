r"""
A ``linesep``-free ``LineReceiveStream``, built as a pipeline of four tiny
async generators::

    reads -> hold back a trailing "\r" -> translate newlines -> split on "\n"

Each stage looks at each chunk exactly once and never re-scans what it has
already looked at, and the fragments of a line are joined only once, when the
line is complete; so reading N characters costs O(N) however the reads happen
to be chunked and however long a single line is.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from typing import Any

import anyio


async def _reads(
    stream: anyio.abc.ObjectReceiveStream[str],
) -> AsyncGenerator[str, None]:
    """Yield the text read from ``stream``, one read at a time."""
    while True:
        try:
            yield await stream.receive()
        except anyio.EndOfStream:
            return


async def _hold_cr(chunks: AsyncIterator[str]) -> AsyncGenerator[str, None]:
    r"""
    Re-chunk ``chunks`` so that no chunk ends with ``"\r"``: such a ``"\r"`` is
    held back until the next chunk (or the end of the stream) shows whether a
    ``"\n"`` follows it.  Every ``"\r\n"`` is thus contained in a single chunk,
    and a ``"\r"`` at the end of a chunk really is a line ending.
    """
    held = ""
    async for chunk in chunks:
        chunk = held + chunk
        chunk, held = (chunk[:-1], "\r") if chunk.endswith("\r") else (chunk, "")
        yield chunk
    yield held


async def _translate(chunks: AsyncIterator[str]) -> AsyncGenerator[str, None]:
    r"""Convert the line endings in ``_hold_cr()``-ed ``chunks`` to ``"\n"``."""
    async for chunk in chunks:
        yield chunk.replace("\r\n", "\n").replace("\r", "\n")


async def _split(
    chunks: AsyncIterator[str], sep: str, retain: bool
) -> AsyncGenerator[str, None]:
    """
    Split the concatenation of ``chunks`` into the segments terminated by
    ``sep``, keeping the terminator if ``retain`` is true.  A trailing
    unterminated segment is yielded as-is; a trailing empty one is not.
    """
    term = sep if retain else ""
    parts: list[str] = []  # the pieces of the segment being built
    async for chunk in chunks:
        head, *rest = chunk.split(sep)
        parts.append(head)
        for nxt in rest:
            yield "".join(parts) + term
            parts = [nxt]
    if last := "".join(parts):
        yield last


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits the strings from ``transport_stream`` on
    newlines and returns each line individually, terminator included.

    `receive()` is not atomic with respect to cancellation: cancelling it
    discards any partially-read line, after which the stream is spent.
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        r"""
        :param transport_stream: any `str`-based receive stream
        :param newline:
            `None` (the default) for universal newlines mode, in which
            ``"\n"``, ``"\r\n"`` and ``"\r"`` all end a line and are returned
            as ``"\n"``; or one of ``"\n"``, ``"\r\n"`` or ``"\r"``, which then
            ends a line all by itself and is returned untranslated.  (`open()`
            also accepts ``""``; that is not supported here.)
        """
        if newline not in (None, "\n", "\r\n", "\r"):
            raise ValueError(f"Invalid 'newline' value: {newline!r}")
        chunks = _hold_cr(_reads(transport_stream))
        if newline is None:
            chunks, newline = _translate(chunks), "\n"
        self._stream = transport_stream
        self._lines = _split(chunks, newline, retain=True)

    async def receive(self) -> str:
        try:
            return await anext(self._lines)
        except StopAsyncIteration:
            raise anyio.EndOfStream() from None

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes


async def iter_null_separated(
    transport_stream: anyio.abc.ObjectReceiveStream[str],
) -> AsyncGenerator[str, None]:
    r"""
    Yield the NUL-terminated records read from ``transport_stream``, without
    the NULs.  Replaces ``TerminatedSplitter("\0", retain=False)``, so the body
    of ``stream_null_command()`` becomes::

        stream = TextReceiveStream(p.stdout)
        async for record in iter_null_separated(stream):
            yield record
    """
    async for record in _split(_reads(transport_stream), "\0", retain=False):
        yield record
