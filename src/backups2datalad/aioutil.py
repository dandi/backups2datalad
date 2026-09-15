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
import re
import shlex
import ssl
import subprocess
import textwrap
import time
from typing import Any, Generic, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from anyio.streams.text import TextReceiveStream
import httpx
from linesep import SplitterEmptyError, TerminatedSplitter, get_newline_splitter

from .consts import (
    DEFAULT_WORKERS,
    GIT_OPTIONS,
    GITHUB_MUTATION_SPACING,
    GITHUB_RATE_LIMIT_ATTEMPTS,
    GITHUB_RATE_LIMIT_FALLBACK,
)
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


class GitHubRateLimited(Exception):
    """
    Raised for a GitHub mutation once the `GitHubGate` has given up on this
    process's run after too many consecutive rate-limited responses
    """


_RATE_LIMIT_BODY_RGX = re.compile(
    r"secondary rate limit|abuse detection|rate limit exceeded"
    r"|blocked from content creation",
    flags=re.IGNORECASE,
)


def is_rate_limited(status: int, headers: Mapping[str, str], body: str) -> bool:
    """
    Whether an HTTP response is GitHub telling us to slow down: a 429, or a
    403 that carries `retry-after`, has exhausted the primary quota
    (`x-ratelimit-remaining: 0`), or reads like a secondary-limit message.
    Any other 403 is a genuine permission problem and must not be retried as
    a rate limit.
    """
    if status == 429:
        return True
    if status != 403:
        return False
    hdrs = {k.lower(): v for k, v in headers.items()}
    return (
        "retry-after" in hdrs
        or hdrs.get("x-ratelimit-remaining") == "0"
        or _RATE_LIMIT_BODY_RGX.search(body) is not None
    )


@dataclass
class GitHubGate:
    """
    Per-process pacing of GitHub API mutations, driven by what GitHub tells
    us rather than by local quota numbers.

    - Mutations (repo creation, PATCH, POST) are serialised and spaced at
      least ``spacing`` seconds apart, as GitHub's guidelines ask.
    - A rate-limited response (`is_rate_limited()`) starts a cooldown shared
      by every worker: as long as `retry-after` / `x-ratelimit-reset` say,
      or else ``fallback`` seconds doubling per consecutive hit.  Hits that
      arrive while a cooldown is already running are the same incident and
      do not escalate.
    - After ``attempts`` consecutive rate-limited responses the gate gives up
      for the rest of the process: further mutations raise
      `GitHubRateLimited` at once, so the run ends with a clear failure
      instead of every worker sleeping in turn.  A successful mutation
      resets the count.

    ``clock`` (monotonic), ``wall`` (epoch, for ``x-ratelimit-reset``) and
    ``sleep`` are injectable for tests.  All state is only ever touched from
    the event loop.
    """

    attempts: int = GITHUB_RATE_LIMIT_ATTEMPTS
    spacing: float = GITHUB_MUTATION_SPACING
    fallback: float = GITHUB_RATE_LIMIT_FALLBACK
    clock: Callable[[], float] = time.monotonic
    wall: Callable[[], float] = time.time
    sleep: Callable[[float], Awaitable[None]] = anyio.sleep
    cooldown_until: float = field(init=False, default=0.0)
    consecutive: int = field(init=False, default=0)
    gave_up: bool = field(init=False, default=False)
    _last_mutation_end: float | None = field(init=False, default=None)
    _lock: anyio.Lock | None = field(init=False, default=None)

    @property
    def lock(self) -> anyio.Lock:
        # Created lazily so that a gate can be constructed outside of an
        # event loop (e.g. as a dataclass default)
        if self._lock is None:
            self._lock = anyio.Lock()
        return self._lock

    def note_rate_limited(self, headers: Mapping[str, str], what: str) -> None:
        """
        Record a rate-limited response described by ``what`` and extend the
        cooldown accordingly
        """
        now = self.clock()
        hdrs = {k.lower(): v for k, v in headers.items()}
        if now >= self.cooldown_until:
            # A fresh hit, not one that raced with an ongoing cooldown
            self.consecutive += 1
        retry_after = hdrs.get("retry-after", "").strip()
        reset = hdrs.get("x-ratelimit-reset", "").strip()
        if retry_after.isdigit():
            delay = float(retry_after)
            source = "retry-after"
        elif hdrs.get("x-ratelimit-remaining") == "0" and reset.isdigit():
            delay = max(float(reset) - self.wall(), 1.0)
            source = "x-ratelimit-reset"
        else:
            delay = self.fallback * 2 ** (self.consecutive - 1)
            source = "fallback"
        self.cooldown_until = max(self.cooldown_until, now + delay)
        if self.consecutive > self.attempts:
            self.gave_up = True
            log.warning(
                "GAVE-UP: %d consecutive rate-limited GitHub responses; failing"
                " all further GitHub mutations in this run.  Last: %s",
                self.consecutive,
                what,
            )
        else:
            log.warning(
                "RATELIMIT: %s; hit %d/%d; cooling down for %.0f s (from %s)",
                what,
                self.consecutive,
                self.attempts,
                self.cooldown_until - now,
                source,
            )

    def note_success(self) -> None:
        """Record a successful mutation (GETs do not count)"""
        self.consecutive = 0

    def raise_if_gave_up(self) -> None:
        if self.gave_up:
            raise GitHubRateLimited(
                "GitHub kept rate-limiting us; giving up on further GitHub"
                " mutations in this run"
            )

    async def wait(self) -> None:
        """Sleep out the current cooldown, if any (never escalates)"""
        while (remaining := self.cooldown_until - self.clock()) > 0:
            await self.sleep(remaining)

    @asynccontextmanager
    async def mutation(self) -> AsyncIterator[None]:
        """
        Context for one mutating GitHub request: waits for the cooldown (so
        that only one waiter probes GitHub once it ends) and for the minimum
        spacing since the previous mutation, and raises `GitHubRateLimited`
        if the gate has given up.  Not re-entrant: never enter it while
        already inside one.
        """
        self.raise_if_gave_up()
        async with self.lock:
            self.raise_if_gave_up()
            await self.wait()
            if self._last_mutation_end is not None:
                pause = self._last_mutation_end + self.spacing - self.clock()
                if pause > 0:
                    await self.sleep(pause)
            try:
                yield
            finally:
                self._last_mutation_end = self.clock()


async def arequest(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    retry_on: Container[int] = (),
    gate: GitHubGate | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """
    Perform an HTTP request, retrying on transport errors, 5xx responses, and
    the status codes in ``retry_on`` with exponential backoff.

    If a `GitHubGate` is given (GitHub API calls only), mutating requests are
    serialised and spaced through it, and a rate-limited response is retried
    after the cooldown GitHub asked for instead of the local backoff.  Without
    a gate the behavior is exactly as before, for the DANDI API and S3.
    """
    waits = exp_wait(attempts=15, base=2)
    # custom timeout if was not specified to wait longer  in hope to overcome
    # https://github.com/dandi/dandisets/issues/298 and alike
    kwargs.setdefault("timeout", 60)
    mutating = method.upper() not in ("GET", "HEAD")
    while True:
        try:
            if gate is None:
                r = await client.request(
                    method, url, follow_redirects=True, **kwargs
                )
            elif mutating:
                # Only the request itself is held under the gate's lock; the
                # backoff sleeps below happen with it released.
                async with gate.mutation():
                    r = await client.request(
                        method, url, follow_redirects=True, **kwargs
                    )
            else:
                await gate.wait()
                r = await client.request(
                    method, url, follow_redirects=True, **kwargs
                )
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
            if (
                gate is not None
                and isinstance(e, httpx.HTTPStatusError)
                and is_rate_limited(
                    e.response.status_code, e.response.headers, e.response.text
                )
            ):
                # The next attempt sleeps out the cooldown (under the lock,
                # for mutations) or raises GitHubRateLimited once the gate
                # has given up.
                gate.note_rate_limited(
                    e.response.headers, f"{method.upper()} {url}: {err_detail}"
                )
                continue
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
        if gate is not None and mutating:
            gate.note_success()
        return r


def _describe_http_error(response: httpx.Response) -> str:
    parts = []
    for hdr in (
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-used",
        "x-ratelimit-reset",
        "x-ratelimit-resource",
        # needed when asking GitHub support about a rate-limit block
        "x-github-request-id",
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
    *args: str | Path, cwd: Path | None = None, check: bool = False
) -> AsyncGenerator[str, None]:
    """
    If ``check`` is true, raise `subprocess.CalledProcessError` when the
    command exits nonzero.  Note that, as for any generator, this can only
    happen if the caller iterates to exhaustion.
    """
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
    if check and (rc := p.returncode) is not None and rc != 0:
        raise subprocess.CalledProcessError(rc, argstrs)
    ### TODO: Should `check` be the default?


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
    and returns each line individually.  Requires the linesep_ package.

    .. _linesep: https://github.com/jwodder/linesep
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: str | None = None,
    ) -> None:
        """
        :param transport_stream: any `str`-based receive stream
        :param newline:
            controls how universal newlines mode works; has the same set of
            allowed values and semantics as the ``newline`` argument to
            `open()`
        """
        self._stream = transport_stream
        self._splitter = get_newline_splitter(newline, retain=True)

    async def receive(self) -> str:
        while not self._splitter.nonempty and not self._splitter.closed:
            try:
                self._splitter.feed(await self._stream.receive())
            except anyio.EndOfStream:
                self._splitter.close()
        try:
            return self._splitter.get()
        except SplitterEmptyError:
            raise anyio.EndOfStream()

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes
