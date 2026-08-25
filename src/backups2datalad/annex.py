from __future__ import annotations

from collections import defaultdict
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import aclosing
from dataclasses import dataclass, field
import json
from pathlib import Path
from types import TracebackType
from typing import TypeVar

import anyio

from .aioutil import TextProcess, open_git_annex, stream_null_command
from .consts import BATCH_CHUNK_SIZE, GIT_OPTIONS, JOURNAL_FLUSH_INTERVAL
from .logging import log
from .util import format_errors

T = TypeVar("T")

#: Command lines (sans the leading ``git annex``) for the ``--batch``
#: subprocesses managed by `AsyncAnnex`, keyed by the name used to refer to
#: each one
BATCH_COMMANDS: dict[str, tuple[str, ...]] = {
    "fromkey": ("fromkey", "--force", "--batch", "--json", "--json-error-messages"),
    "whereis": ("whereis", "--batch-keys", "--json", "--json-error-messages"),
    "registerurl": (
        "registerurl",
        "-c",
        "annex.alwayscompact=false",
        "--batch",
        "--json",
        "--json-error-messages",
    ),
}


def render_request(render: Callable[[T], str], item: T) -> str:
    """
    Render ``item`` as a single request line for a ``--batch`` subprocess.

    Batch protocols are one line in, one line out, so an embedded newline
    would turn one request into two and leave every later response in the
    chunk attributed to the wrong request.  git-annex takes raw file paths for
    `examinekey` and `fromkey`, and an S3 object key may legally contain a
    newline, so refuse rather than silently corrupt the backup.
    """
    line = render(item)
    if not line.endswith("\n") or "\n" in line[:-1]:
        raise ValueError(f"Batch request for {item!r} is not a single line: {line!r}")
    return line


@dataclass
class AsyncAnnex:
    repo: Path
    digest_type: str = "SHA256"
    #: Open ``git annex --batch`` subprocesses, keyed as in `BATCH_COMMANDS`
    #: (plus ``"examinekey"``, whose command line depends on `digest_type`)
    procs: dict[str, TextProcess] = field(init=False, default_factory=dict)
    #: Number of requests sent to each subprocess since it was last (re)started
    sent: dict[str, int] = field(init=False, default_factory=lambda: defaultdict(int))
    locks: dict[str, anyio.Lock] = field(
        init=False, default_factory=lambda: defaultdict(anyio.Lock)
    )

    async def __aenter__(self) -> AsyncAnnex:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        procs = list(self.procs.values())
        self.procs.clear()
        self.sent.clear()
        if exc_type is None:
            for p in procs:
                await p.aclose()
        else:
            with anyio.CancelScope(shield=True):
                for p in procs:
                    await p.force_aclose()

    def _command(self, name: str) -> tuple[str, ...]:
        if name == "examinekey":
            return (
                "examinekey",
                "--batch",
                f"--migrate-to-backend={self.digest_type}E",
            )
        return BATCH_COMMANDS[name]

    async def _get_proc(self, name: str) -> TextProcess:
        try:
            return self.procs[name]
        except KeyError:
            p = await open_git_annex(
                *self._command(name),
                path=self.repo,
                # `whereis` legitimately fails for keys git-annex doesn't know
                # about, so a nonzero exit code is not worth warning about:
                warn_on_fail=(name != "whereis"),
            )
            self.procs[name] = p
            return p

    async def _close_proc(self, name: str, force: bool = False) -> None:
        p = self.procs.pop(name, None)
        self.sent.pop(name, None)
        if p is not None:
            if force:
                await p.force_aclose()
            else:
                await p.aclose()

    async def _pipeline(self, p: TextProcess, requests: Sequence[str]) -> list[str]:
        """
        Send ``requests`` to the ``--batch`` subprocess ``p`` and collect one
        response line per request.

        The requests are written by a separate task while the responses are
        read concurrently, so git-annex can start working on the next request
        while we are still reading the response to the previous one.  Doing a
        blocking send-then-receive per request instead (which is what a naive
        use of ``--batch`` amounts to) costs a full event-loop round trip and a
        subprocess wakeup per item, which dominates the runtime once there are
        tens of thousands of items.
        """
        payload = "".join(requests)

        async def feed() -> None:
            await p.send(payload)

        responses: list[str] = []
        async with anyio.create_task_group() as tg:
            tg.start_soon(feed)
            for _ in requests:
                responses.append(await p.receive())
        return responses

    async def _batch(
        self,
        name: str,
        items: Sequence[T],
        render: Callable[[T], str],
        handle: Callable[[T, str], None] | None = None,
        *,
        restart_after: int | None = None,
        progress: Callable[[int], None] | None = None,
    ) -> None:
        """
        Feed ``items`` (rendered as request lines by ``render``) to the
        ``--batch`` subprocess ``name`` in chunks, passing each item and its
        response line to ``handle``.  `_batch` itself only holds one chunk's
        worth of requests and responses at a time, though callers that collect
        results (e.g. `mkkeys()`) still accumulate one entry per item.
        """
        if not items:
            return
        async with self.locks[name]:
            done = 0
            for i in range(0, len(items), BATCH_CHUNK_SIZE):
                chunk = items[i : i + BATCH_CHUNK_SIZE]
                requests = [render_request(render, it) for it in chunk]
                p = await self._get_proc(name)
                try:
                    responses = await self._pipeline(p, requests)
                except BaseException:
                    # The subprocess is now in an unknown state: some requests
                    # may have been written without their responses being read
                    # (or a request may have been written only partially), so
                    # reusing it would misattribute every later response.
                    with anyio.CancelScope(shield=True):
                        await self._close_proc(name, force=True)
                    raise
                done += len(chunk)
                self.sent[name] += len(chunk)
                if handle is not None:
                    for it, resp in zip(chunk, responses):
                        handle(it, resp)
                if restart_after is not None and self.sent[name] >= restart_after:
                    await self._close_proc(name)
                if progress is not None:
                    progress(done)

    async def from_keys(
        self,
        keyfiles: Sequence[tuple[str, str]],
        progress: Callable[[int], None] | None = None,
    ) -> None:
        def handle(keyfile: tuple[str, str], response: str) -> None:
            r = json.loads(response)
            if not r["success"]:
                log.error(
                    "`git annex fromkey %s %s` [cwd=%s] call failed:%s",
                    keyfile[0],
                    keyfile[1],
                    self.repo,
                    format_errors(r["error-messages"]),
                )
                ### TODO: Raise an exception?

        await self._batch(
            "fromkey",
            keyfiles,
            lambda kf: f"{kf[0]} {kf[1]}\n",
            handle,
            progress=progress,
        )

    async def from_key(self, key: str, path: str) -> None:
        await self.from_keys([(key, path)])

    async def mkkeys(self, files: Sequence[tuple[str, int, str]]) -> list[str]:
        keys: list[str] = []

        def handle(f: tuple[str, int, str], response: str) -> None:
            filename, size, digest = f
            key = response.strip()
            # `examinekey` output is a bare key rather than JSON, so -- unlike
            # the other batch commands -- a response stream that got out of
            # step with the requests would not fail to parse; it would just
            # silently annex every file under the previous file's key.  The
            # key is fully determined by the request apart from the extension,
            # so check it:
            expected = f"{self.digest_type}E-s{size}--{digest}"
            if not key.startswith(expected):
                raise RuntimeError(
                    f"`git annex examinekey` [cwd={self.repo}] returned"
                    f" {key!r} for {filename!r}, which does not start with the"
                    f" expected {expected!r}"
                )
            keys.append(key)

        await self._batch(
            "examinekey",
            files,
            lambda f: f"{self.digest_type}-s{f[1]}--{f[2]} {f[0]}\n",
            handle,
        )
        return keys

    async def mkkey(self, filename: str, size: int, digest: str) -> str:
        (key,) = await self.mkkeys([(filename, size, digest)])
        return key

    async def get_keys_remotes(self, keys: Sequence[str]) -> list[list[str] | None]:
        # An entry is None if the corresponding key is not known to git-annex
        remotes: list[list[str] | None] = []

        def handle(_key: str, response: str) -> None:
            whereis = json.loads(response)
            if whereis["success"]:
                remotes.append(
                    [
                        w["description"].strip("[]")
                        for w in whereis["whereis"] + whereis["untrusted"]
                    ]
                )
            else:
                remotes.append(None)

        await self._batch("whereis", keys, lambda key: f"{key}\n", handle)
        return remotes

    async def get_key_remotes(self, key: str) -> list[str] | None:
        # Returns None if key is not known to git-annex
        (remotes,) = await self.get_keys_remotes([key])
        return remotes

    async def register_urls(
        self,
        keyurls: Sequence[tuple[str, str]],
        progress: Callable[[int], None] | None = None,
    ) -> None:
        def handle(keyurl: tuple[str, str], response: str) -> None:
            r = json.loads(response)
            if not r["success"]:
                log.error(
                    "`git annex registerurl %s %s` [cwd=%s] call failed:%s",
                    keyurl[0],
                    keyurl[1],
                    self.repo,
                    format_errors(r["error-messages"]),
                )
                ### TODO: Raise an exception?

        await self._batch(
            "registerurl",
            keyurls,
            lambda ku: f"{ku[0]} {ku[1]}\n",
            handle,
            restart_after=JOURNAL_FLUSH_INTERVAL,
            progress=progress,
        )

    async def register_url(self, key: str, url: str) -> None:
        await self.register_urls([(key, url)])

    async def list_files(self, path: Path | None = None) -> AsyncGenerator[str, None]:
        async with aclosing(
            stream_null_command(
                "git",
                *GIT_OPTIONS,
                "ls-tree",
                "-r",
                "--name-only",
                "-z",
                "HEAD",
                *([str(path)] if path is not None else []),
                cwd=self.repo,
            )
        ) as p:
            async for fname in p:
                if path is not None:
                    yield (path / fname).as_posix()
                else:
                    yield fname
