"""
A ``linesep``-free ``LineReceiveStream`` for backups2datalad.

The only genuinely fiddly part of universal-newline splitting is the
translation of ``\\r\\n``/``\\r`` to ``\\n`` when a ``\\r\\n`` straddles a chunk
boundary.  The standard library already ships an incremental, C-implemented
solution for exactly that -- `io.IncrementalNewlineDecoder`, the machinery
behind `io.TextIOWrapper`'s ``newline=None`` mode -- so we delegate to it and
are left with the trivial problem of splitting on a single fixed separator.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Mapping
from io import IncrementalNewlineDecoder
from typing import Any

import anyio


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits strings from ``transport_stream`` on newlines
    and returns each line individually, with the terminator retained.
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        """
        :param transport_stream: any `str`-based receive stream
        :param newline:
            If `None` (the default), universal newlines mode is used: each of
            ``"\\n"``, ``"\\r\\n"`` and ``"\\r"`` terminates a line and is
            translated to ``"\\n"``.  Otherwise, ``newline`` is a nonempty
            string that terminates a line and is retained untranslated.

            Unlike the ``newline`` argument to `open()`, ``""`` (untranslated
            universal newlines) is not supported; nothing needs it, and it
            would require reimplementing the translation logic that
            `io.IncrementalNewlineDecoder` gives us for free.
        """
        if newline == "":
            raise ValueError("newline='' is not supported; use None or a nonempty str")
        self._stream = transport_stream
        # In universal mode the decoder turns every line ending into "\n"
        # (buffering a trailing "\r" until it knows whether "\n" follows), so
        # all that is left to split on is "\n":
        self._decoder = (
            IncrementalNewlineDecoder(None, True) if newline is None else None
        )
        self._sep = newline or "\n"
        self._chunk = ""  # text received but not yet returned
        self._pos = 0  # how far into `_chunk` we have already scanned
        self._parts: list[str] = []  # earlier chunks' share of the current line
        self._eof = False

    async def receive(self) -> str:
        # Invariant: the line under construction is
        # ``"".join(self._parts) + self._chunk[self._pos:]``, and no separator
        # occurs in it except possibly one starting in the last len(sep)-1
        # characters of `_chunk`.
        seplen = len(self._sep)
        while True:
            i = self._chunk.find(self._sep, self._pos)
            if i >= 0:
                line = self._chunk[self._pos : i + seplen]
                self._pos = i + seplen
                if not self._parts:
                    return line
                self._parts.append(line)
                line = "".join(self._parts)
                self._parts.clear()
                return line
            if self._eof:
                # Return whatever is left as a final, unterminated line.
                self._parts.append(self._chunk[self._pos :])
                line = "".join(self._parts)
                self._chunk, self._pos = "", 0
                self._parts.clear()
                if not line:
                    raise anyio.EndOfStream
                return line
            # No separator in `_chunk[_pos:]`: set it aside rather than
            # rescanning it next time round (that rescanning is what made the
            # old implementation quadratic).  Keep back the last seplen-1
            # characters, which are all a separator split across the chunk
            # boundary could start in.
            rest = self._chunk[self._pos :]
            cut = max(len(rest) - seplen + 1, 0)
            if cut:
                self._parts.append(rest[:cut])
            try:
                text = await self._stream.receive()
            except anyio.EndOfStream:
                self._eof = True
                text = ""
            if self._decoder is not None:
                text = self._decoder.decode(text, self._eof)
            self._chunk = rest[cut:] + text
            self._pos = 0

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
    the NULs.  Equivalent to ``linesep.TerminatedSplitter("\\0", retain=False)``:
    a trailing NUL at end of input does not produce an extra empty record, but
    an unterminated final record is still yielded.
    """
    async for record in LineReceiveStream(transport_stream, "\0"):
        yield record[:-1] if record.endswith("\0") else record
