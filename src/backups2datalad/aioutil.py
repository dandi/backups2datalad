from __future__ import annotations

from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Container,
    Mapping,
)
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
import logging
import math
from pathlib import Path
import shlex
import ssl
import subprocess
import textwrap
from typing import Any, Generic, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from anyio.streams.text import TextReceiveStream
import httpx
from linesep import TerminatedSplitter

from .consts import DEFAULT_WORKERS, GIT_OPTIONS
from .logging import log
from .util import exp_wait

T = TypeVar("T")
InT = TypeVar("InT")
OutT = TypeVar("OutT")


@dataclass
class TextProcess(anyio.abc.ObjectStream[str]):
    p: anyio.abc.Process
    stdout: LineReceiveStream
    desc: str
    warn_on_fail: bool = True

    async def aclose(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()
        log.debug("Waiting for %s to terminate", self.desc)
        rc = await self.p.wait()
        log.log(
            logging.WARNING if rc != 0 and self.warn_on_fail else logging.DEBUG,
            "Command %s exited with return code %d",
            self.desc,
            rc,
        )

    async def force_aclose(self, timeout: float = 5) -> None:
        try:
            with anyio.fail_after(timeout):
                await self.aclose()
                return
        except TimeoutError:
            log.debug(
                "Command %s did not terminate in time; sending SIGTERM", self.desc
            )
            self.p.terminate()
            try:
                with anyio.fail_after(timeout):
                    await self.p.wait()
                    log.debug("Command %s successfully terminated", self.desc)
            except TimeoutError:
                log.warning("Command %s did not terminate in time; killing", self.desc)
                self.p.kill()

    async def send(self, s: str) -> None:
        if self.p.returncode is not None:
            raise RuntimeError(
                f"Command {self.desc} suddenly exited with return code"
                f" {self.p.returncode}!"
            )
        assert self.p.stdin is not None
        await self.p.stdin.send(s.encode("utf-8"))

    async def receive(self) -> str:
        return await self.stdout.receive()

    async def send_eof(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()


async def open_git_annex(
    *args: str,
    path: Path | None = None,
    warn_on_fail: bool = True,
    env: dict[str, str] | None = None,
) -> TextProcess:
    # This is strictly for spawning git-annex processes that data will be both
    # sent to and received from.  To open a process solely for receiving data,
    # use `stream_lines_command()` or `stream_null_command()`.
    allargs = ["git", *GIT_OPTIONS, "annex", *args]
    desc = f"`{shlex.join(allargs)}`"
    if path is not None:
        desc += f" [cwd={path}]"
    log.debug("Opening pipe to %s", desc)
    p = await anyio.open_process(
        allargs,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=path,
        env=env,
    )
    assert p.stdout is not None
    stdout = LineReceiveStream(TextReceiveStream(p.stdout))
    return TextProcess(p, stdout, desc, warn_on_fail=warn_on_fail)


async def arequest(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    retry_on: Container[int] = (),
    **kwargs: Any,
) -> httpx.Response:
    waits = exp_wait(attempts=15, base=2)
    # custom timeout if was not specified to wait longer  in hope to overcome
    # https://github.com/dandi/dandisets/issues/298 and alike
    kwargs.setdefault("timeout", 60)
    while True:
        try:
            r = await client.request(method, url, follow_redirects=True, **kwargs)
            r.raise_for_status()
        except (httpx.HTTPError, ssl.SSLError) as e:
            # For HTTP status errors, capture body + rate-limit/retry headers
            # so 403s (esp. GitHub secondary rate limits) can be distinguished
            # from auth/permission failures in the logs.
            err_detail = (
                _describe_http_error(e.response)
                if isinstance(e, httpx.HTTPStatusError)
                else ""
            )
            if isinstance(e, (httpx.RequestError, ssl.SSLError)) or (
                isinstance(e, httpx.HTTPStatusError)
                and (
                    e.response.status_code >= 500 or e.response.status_code in retry_on
                )
            ):
                try:
                    delay = next(waits)
                except StopIteration:
                    if err_detail:
                        log.error(
                            "Giving up on %s request to %s after retries; %s",
                            method.upper(),
                            url,
                            err_detail,
                        )
                    raise e
                log.warning(
                    "Retrying %s request to %s in %f seconds as it raised %s: %s%s",
                    method.upper(),
                    url,
                    delay,
                    type(e).__name__,
                    str(e),
                    f"; {err_detail}" if err_detail else "",
                )
                await anyio.sleep(delay)
                continue
            else:
                if err_detail:
                    log.error(
                        "%s request to %s failed: %s; %s",
                        method.upper(),
                        url,
                        str(e),
                        err_detail,
                    )
                raise
        return r


def _describe_http_error(response: httpx.Response) -> str:
    parts = []
    for hdr in (
        "retry-after",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-ratelimit-resource",
    ):
        value = response.headers.get(hdr)
        if value is not None:
            parts.append(f"{hdr}={value}")
    body = response.text
    if body:
        body = textwrap.shorten(body.replace("\n", " "), width=500, placeholder="...")
        parts.append(f"body={body!r}")
    return "; ".join(parts)


@dataclass
class PoolReport(Generic[InT, OutT]):
    results: list[tuple[InT, OutT]] = field(default_factory=list)
    failed: list[InT] = field(default_factory=list)


async def pool_amap(
    func: Callable[[InT], Awaitable[OutT]],
    inputs: AsyncGenerator[InT, None],
    workers: int = DEFAULT_WORKERS,
) -> PoolReport[InT, OutT]:
    report: PoolReport[InT, OutT] = PoolReport()

    async def dowork(rec: MemoryObjectReceiveStream[InT]) -> None:
        async with rec:
            async for inp in rec:
                try:
                    outp = await func(inp)
                except Exception:
                    log.exception("Job failed on input %r:", inp)
                    report.failed.append(inp)
                else:
                    report.results.append((inp, outp))

    async with anyio.create_task_group() as tg:
        sender, receiver = anyio.create_memory_object_stream[InT](math.inf)
        async with receiver:
            for _ in range(max(1, workers)):
                tg.start_soon(dowork, receiver.clone())
        async with sender, aclosing(inputs):
            async for item in inputs:
                await sender.send(item)
    return report


async def aruncmd(
    *args: str | Path, quiet_rcs: Collection[int] = (), **kwargs: Any
) -> subprocess.CompletedProcess[bytes]:
    argstrs = [str(a) for a in args]
    desc = shlex.join(argstrs)
    if (cwd := kwargs.get("cwd")) is not None:
        desc += f" [cwd={cwd}]"
    log.debug("Running: %s", desc)
    kwargs["stdout"] = subprocess.PIPE
    kwargs.setdefault("stderr", subprocess.PIPE)
    try:
        r = await anyio.run_process(argstrs, **kwargs)
    except subprocess.CalledProcessError as e:
        if e.returncode not in quiet_rcs:
            label = "Stdout" if e.stderr is not None else "Output"
            stdout = e.stdout.decode("utf-8", "surrogateescape")
            if stdout:
                output = f"{label}:\n\n" + textwrap.indent(stdout, " " * 4)
            else:
                output = f"{label}: <empty>"
            if e.stderr is not None:
                stderr = e.stderr.decode("utf-8", "surrogateescape")
                if stderr:
                    output += "\n\nStderr:\n\n" + textwrap.indent(stderr, " " * 4)
                else:
                    output += "\n\nStderr: <empty>"
            log.warning("Failed [rc=%d]: %s\n\n%s", e.returncode, desc, output)
        else:
            log.debug("Finished [rc=%d]: %s", e.returncode, desc)
        raise e
    else:
        log.debug("Finished [rc=%d]: %s", r.returncode, desc)
        return r


async def areadcmd(*args: str | Path, strip: bool = True, **kwargs: Any) -> str:
    kwargs["stdout"] = subprocess.PIPE
    kwargs.setdefault("stderr", None)
    r = await aruncmd(*args, **kwargs)
    s = r.stdout.decode("utf-8")
    if strip:
        s = s.strip()
    return s


async def stream_null_command(
    *args: str | Path, cwd: Path | None = None
) -> AsyncGenerator[str, None]:
    argstrs = [str(a) for a in args]
    desc = f"`{shlex.join(argstrs)}`"
    if cwd is not None:
        desc += f" [cwd={cwd}]"
    log.debug("Opening pipe to %s", desc)
    async with kill_on_error(
        await anyio.open_process(argstrs, cwd=cwd, stderr=None), desc
    ) as p:
        assert p.stdout is not None
        try:
            stream = TextReceiveStream(p.stdout)
            splitter = TerminatedSplitter("\0", retain=False)
            async for chunk in splitter.aitersplit(stream):
                yield chunk
        except BaseException:
            log.exception("Exception raised while handling output from %s", desc)
            raise
    log.log(
        logging.DEBUG if p.returncode == 0 else logging.WARNING,
        "Command %s exited with return code %d",
        desc,
        p.returncode,
    )
    ### TODO: Raise an exception if p.returncode is nonzero?


async def stream_lines_command(
    *args: str | Path, cwd: Path | None = None
) -> AsyncGenerator[str, None]:
    argstrs = [str(a) for a in args]
    desc = f"`{shlex.join(argstrs)}`"
    if cwd is not None:
        desc += f" [cwd={cwd}]"
    log.debug("Opening pipe to %s", desc)
    async with kill_on_error(
        await anyio.open_process(argstrs, cwd=cwd, stderr=None), desc
    ) as p:
        assert p.stdout is not None
        async for line in LineReceiveStream(TextReceiveStream(p.stdout)):
            yield line
    log.log(
        logging.DEBUG if p.returncode == 0 else logging.WARNING,
        "Command %s exited with return code %d",
        desc,
        p.returncode,
    )
    ### TODO: Raise an exception if p.returncode is nonzero?


@asynccontextmanager
async def kill_on_error(
    p: anyio.abc.Process, desc: str, timeout: float = 5
) -> AsyncIterator[anyio.abc.Process]:
    """
    When used like so::

        async with kill_on_error(
            await anyio.open_process(...),
            "command args ...",
            timeout=...
        ) as p:
            ...

    then the subprocess ``p``, in addition to being waited for on normal
    context manager exit, will be terminated if an error (including
    cancellation) occurs in the body of the ``async with:`` block; if it
    doesn't exit after ``timeout`` seconds, it will instead be killed.
    """

    async with p:
        try:
            yield p
        except BaseException:
            with anyio.CancelScope(shield=True):
                log.debug("Forcing command %s to terminate", desc)
                p.terminate()
                try:
                    with anyio.fail_after(timeout):
                        await p.wait()
                        log.debug("Command %s successfully terminated", desc)
                except TimeoutError:
                    log.warning("Command %s did not terminate in time; killing", desc)
                    p.kill()
            raise


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits strings from ``transport_stream`` on newlines
    and returns each line individually, with its terminator retained.

    With ``newline=None`` (the default) this follows universal-newlines rules:
    ``\n``, ``\r\n``, and a lone ``\r`` all terminate a line and are
    translated to ``\n`` in the returned string.  A final line with no
    terminator is returned as-is.

    The splitting is incremental: each chunk received from the transport is
    scanned once, and the pieces of a line are joined only when that line is
    complete.  That matters because a single line can be very large -- ``git
    annex whereis --json`` for a key with many registered URLs runs to tens of
    megabytes -- and the obvious implementation (append to a buffer, re-scan
    the buffer from the start) is quadratic in the length of the line.
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        """
        :param transport_stream: any `str`-based receive stream
        :param newline:
            ``None`` for universal-newlines mode (see above); otherwise the
            exact string that terminates a line, retained untranslated
        """
        self._stream = transport_stream
        self._newline = newline
        #: Pieces of the line currently being accumulated, none of which
        #: contain a terminator
        self._parts: list[str] = []
        #: The chunk currently being scanned, and how far into it we have got
        self._cur: str | None = None
        self._pos: int = 0
        #: Set when a chunk ended with a ``\r`` that might yet turn out to be
        #: the first half of a ``\r\n``
        self._carry_cr: bool = False
        self._eof: bool = False

    def _flush(self, tail: str = "") -> str:
        if self._parts:
            self._parts.append(tail)
            line = "".join(self._parts)
            self._parts.clear()
            return line
        return tail

    def _find(self, s: str, pos: int) -> tuple[int, int] | None:
        """
        Locate the next terminator in ``s`` at or after ``pos``, returning its
        span, or `None` if there is none.  Returns ``(-1, -1)`` to mean "``s``
        ends with a ``\r`` whose meaning depends on the next chunk".
        """
        if self._newline is not None:
            i = s.find(self._newline, pos)
            return None if i < 0 else (i, i + len(self._newline))
        i_n = s.find("\n", pos)
        # Only look for a CR *before* the next LF, so that scanning a chunk of
        # many LF-terminated lines stays linear in the chunk's length rather
        # than re-scanning the tail once per line.
        i_r = s.find("\r", pos) if i_n < 0 else s.find("\r", pos, i_n)
        if i_r < 0:
            return None if i_n < 0 else (i_n, i_n + 1)
        if i_r == len(s) - 1:
            return (-1, -1)
        return (i_r, i_r + 2 if s[i_r + 1] == "\n" else i_r + 1)

    def _take_line(self) -> str | None:
        assert self._cur is not None
        s, pos = self._cur, self._pos
        if pos >= len(s):
            return None
        span = self._find(s, pos)
        if span is None:
            self._parts.append(s[pos:])
            self._pos = len(s)
            return None
        start, end = span
        if start < 0:
            # Trailing CR: hold it back until we know whether an LF follows.
            self._parts.append(s[pos : len(s) - 1])
            self._carry_cr = True
            self._pos = len(s)
            return None
        self._pos = end
        sep = "\n" if self._newline is None else s[start:end]
        return self._flush(s[pos:start] + sep)

    async def receive(self) -> str:
        while True:
            if self._cur is not None:
                line = self._take_line()
                if line is not None:
                    return line
                self._cur = None
            if self._eof:
                if self._carry_cr:
                    self._carry_cr = False
                    return self._flush("\n")
                rest = self._flush()
                if rest:
                    return rest
                raise anyio.EndOfStream()
            try:
                data = await self._stream.receive()
            except anyio.EndOfStream:
                self._eof = True
                continue
            if not data:
                continue
            if self._carry_cr:
                # The held CR terminates the pending line either way; an LF
                # immediately after it is the rest of a CRLF and is consumed.
                self._carry_cr = False
                line = self._flush("\n")
                self._cur = data
                self._pos = 1 if data[0] == "\n" else 0
                return line
            self._cur = data
            self._pos = 0

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes
