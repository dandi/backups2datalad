from __future__ import annotations

from collections import deque
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
import io
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
            async for chunk in iter_null_separated(TextReceiveStream(p.stdout)):
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
    Stream wrapper that splits the strings received from ``transport_stream``
    into lines and returns one line at a time, terminators included.

    The whole newline problem is delegated to `io.IncrementalNewlineDecoder`,
    the object `io.TextIOWrapper` uses to implement universal newlines: it
    translates ``"\\r\\n"`` and ``"\\r"`` to ``"\\n"`` and -- the part that
    matters here -- holds back a ``"\\r"`` that ends a chunk until it can see
    whether the next chunk starts with ``"\\n"``.  It does that even when
    translation is off.

    Consequently the decoder's output never ends with ``"\\r"`` (except on the
    final flush, when nothing can follow anyway), so none of the terminators
    supported here can straddle a chunk boundary, and each decoded chunk can be
    split on its own with plain `str.split` and never looked at again.  That is
    what makes reading a line linear in its length: no data is ever re-scanned.
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
