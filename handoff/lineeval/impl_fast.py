r"""
A standard-library-only replacement for the two uses of ``linesep`` in
``backups2datalad.aioutil``: `LineReceiveStream` and `iter_null_separated`.

Both are linear in the size of their input no matter how it is chunked,
because neither ever re-examines a character it has already looked at: every
incoming chunk is split exactly once by `str.split` (in C), and the at most one
unterminated piece left over at the end of a chunk is stashed in a list that is
joined only when the line it belongs to is finally completed.
"""

from collections import deque
from collections.abc import AsyncGenerator, Callable, Mapping
from typing import Any

import anyio


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits the strings received from ``transport_stream``
    into lines and returns them one at a time, with their terminators retained.
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        r"""
        :param transport_stream: any `str`-based receive stream
        :param newline:
            `None` (the default) selects universal newlines mode: each of
            ``"\n"``, ``"\r\n"`` and ``"\r"`` ends a line and is translated to
            ``"\n"``.  Passing ``"\n"``, ``"\r\n"`` or ``"\r"`` instead splits
            on exactly that string, retained untranslated.  Unlike `open()`,
            ``""`` is not accepted: nothing in this codebase asks for
            untranslated universal newlines, and supporting them would mean a
            second, slower splitting path.
        """
        if newline is not None and newline not in ("\n", "\r\n", "\r"):
            raise ValueError(f"Invalid 'newline' value: {newline!r}")
        self._stream = transport_stream
        self._sep = newline or "\n"
        self._universal = newline is None
        self._ready: deque[str] = deque()  # complete lines, not yet returned
        self._partial: list[str] = []  # pieces of the line still being read
        self._held_cr = False  # chunk ended in CR that may yet turn out to be CRLF
        self._eof = False

    def _feed(self, chunk: str, last: bool = False) -> None:
        """
        Split one chunk, appending whatever lines it completes to ``_ready``.
        ``last`` says this is the end of the stream, so a held-back CR is now
        known to be a line ending (universal mode) or plain data (otherwise).
        """
        if self._held_cr:
            chunk = "\r" + chunk
            self._held_cr = False
        if not last and chunk.endswith("\r"):
            # Hold it back: the next chunk may start with the LF of a CRLF.
            chunk = chunk[:-1]
            self._held_cr = True
        if self._universal and "\r" in chunk:
            chunk = chunk.replace("\r\n", "\n").replace("\r", "\n")
        sep = self._sep
        parts = chunk.split(sep)
        tail = parts.pop()  # text after the last terminator; may be ""
        if parts:
            if self._partial:  # finish the line carried over from earlier chunks
                self._partial.append(parts[0])
                parts[0] = "".join(self._partial)
                self._partial.clear()
            self._ready.extend([p + sep for p in parts])
        if tail:
            self._partial.append(tail)

    async def receive(self) -> str:
        while True:
            try:
                return self._ready.popleft()
            except IndexError:
                pass
            if self._eof:
                raise anyio.EndOfStream()
            try:
                self._feed(await self._stream.receive())
            except anyio.EndOfStream:
                self._eof = True
                self._feed("", last=True)
                if self._partial:  # trailing line with no terminator
                    self._ready.append("".join(self._partial))
                    self._partial.clear()

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes


async def iter_null_separated(
    transport_stream: anyio.abc.ObjectReceiveStream[str],
) -> AsyncGenerator[str, None]:
    """
    Yield the NUL-terminated records read from ``transport_stream``, without
    the NULs.  A trailing record with no NUL after it is yielded too; a final
    empty one (the normal case for e.g. ``git ls-tree -z``) is not.
    """
    partial: list[str] = []
    while True:
        try:
            chunk = await transport_stream.receive()
        except anyio.EndOfStream:
            break
        records = chunk.split("\0")
        tail = records.pop()
        if records:
            if partial:
                partial.append(records[0])
                records[0] = "".join(partial)
                partial.clear()
            for record in records:
                yield record
        if tail:
            partial.append(tail)
    if partial:
        yield "".join(partial)
