"""
Line splitting for `anyio` ``str`` receive streams, built on the standard
library's incremental newline decoder.

The whole newline problem is delegated to `io.IncrementalNewlineDecoder`, the
object `io.TextIOWrapper` uses to implement universal newlines: it translates
``"\\r\\n"`` and ``"\\r"`` to ``"\\n"`` and -- the part that matters here --
holds back a ``"\\r"`` that ends a chunk until it can see whether the next
chunk starts with ``"\\n"``.  It does that even when translation is off.

Consequently the decoder's output never ends with ``"\\r"`` (except on the
final flush, when nothing can follow anyway), so none of the terminators
supported here can straddle a chunk boundary, and each decoded chunk can be
split on its own with plain `str.split` and never looked at again.  That is
what makes this linear: no data is ever re-scanned.
"""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator, Callable, Mapping
import io
from typing import Any

import anyio


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits the strings received from ``transport_stream``
    into lines and returns one line at a time, terminators included.
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        """
        :param transport_stream: any `str`-based receive stream
        :param newline:
            `None` (the default) selects universal newlines: ``"\\n"``,
            ``"\\r\\n"`` and ``"\\r"`` all terminate a line and are translated
            to ``"\\n"``.  Any single character, or ``"\\r\\n"``, may be given
            instead to split on exactly that string, retained untranslated.
            Longer terminators are rejected: they could straddle a chunk
            boundary, which this implementation relies on being impossible.
        """
        if newline is not None and len(newline) != 1 and newline != "\r\n":
            raise ValueError(
                f"Unsupported 'newline' value {newline!r}: expected None, a"
                " single character, or '\\r\\n'"
            )
        self._stream = transport_stream
        self._decoder = io.IncrementalNewlineDecoder(None, translate=newline is None)
        #: The string that terminates a line in the decoder's output
        self._terminator = "\n" if newline is None else newline
        #: Completed lines, ready to be handed out
        self._lines: deque[str] = deque()
        #: Pieces of the line currently being assembled
        self._tail: list[str] = []
        #: Whether the transport has been exhausted
        self._eof = False

    async def receive(self) -> str:
        while not self._lines:
            if self._eof:
                raise anyio.EndOfStream()
            try:
                chunk = await self._stream.receive()
            except anyio.EndOfStream:
                self._close_input()
            else:
                self._feed(chunk)
        return self._lines.popleft()

    def _feed(self, chunk: str) -> None:
        text = self._decoder.decode(chunk)
        if not text:
            return
        term = self._terminator
        pieces = text.split(term)
        # Every piece but the last one was followed by `term` in `text`.
        if len(pieces) == 1:
            self._tail.append(text)
        else:
            self._tail.append(pieces[0] + term)
            self._lines.append("".join(self._tail))
            self._lines.extend(piece + term for piece in pieces[1:-1])
            self._tail = [pieces[-1]]

    def _close_input(self) -> None:
        # The flush emits the "\r" (translated or not) that the decoder was
        # holding back, if any; it terminates the last line.
        last = "".join(self._tail) + self._decoder.decode("", final=True)
        self._tail = []
        self._eof = True
        if last:
            self._lines.append(last)

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes


async def iter_null_separated(
    transport_stream: anyio.abc.ObjectReceiveStream[str],
) -> AsyncGenerator[str, None]:
    """
    Yield the NUL-terminated records received from ``transport_stream``, with
    the NULs stripped.  A final record without a trailing NUL is yielded as-is;
    a trailing NUL does not produce an extra empty record.
    """
    async for record in LineReceiveStream(transport_stream, newline="\0"):
        yield record.removesuffix("\0")
