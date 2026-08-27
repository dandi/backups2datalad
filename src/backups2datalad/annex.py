from __future__ import annotations

from collections import defaultdict
from collections.abc import AsyncGenerator
from contextlib import aclosing
from dataclasses import dataclass, field
import json
from pathlib import Path
from types import TracebackType

import anyio

from .aioutil import (
    TextProcess,
    open_git_annex,
    stream_lines_command,
    stream_null_command,
)
from .consts import GIT_OPTIONS
from .logging import log
from .util import format_errors


@dataclass
class AsyncAnnex:
    repo: Path
    digest_type: str = "SHA256"
    pfromkey: TextProcess | None = None
    pexaminekey: TextProcess | None = None
    pregisterurl: TextProcess | None = None
    locks: dict[str, anyio.Lock] = field(
        init=False, default_factory=lambda: defaultdict(anyio.Lock)
    )
    missing_from: dict[str, set[str]] = field(init=False, default_factory=dict)

    async def __aenter__(self) -> AsyncAnnex:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            for p in [
                self.pfromkey,
                self.pexaminekey,
                self.pregisterurl,
            ]:
                if p is not None:
                    await p.aclose()
        else:
            with anyio.CancelScope(shield=True):
                for p in [
                    self.pfromkey,
                    self.pexaminekey,
                    self.pregisterurl,
                ]:
                    if p is not None:
                        await p.force_aclose()

    async def from_key(self, key: str, path: str) -> None:
        async with self.locks["fromkey"]:
            if self.pfromkey is None:
                self.pfromkey = await open_git_annex(
                    "fromkey",
                    "--force",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pfromkey.send(f"{key} {path}\n")
            r = json.loads(await self.pfromkey.receive())
        if not r["success"]:
            log.error(
                "`git annex fromkey %s %s` [cwd=%s] call failed:%s",
                key,
                path,
                self.repo,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

    async def mkkey(self, filename: str, size: int, digest: str) -> str:
        async with self.locks["examinekey"]:
            if self.pexaminekey is None:
                self.pexaminekey = await open_git_annex(
                    "examinekey",
                    "--batch",
                    f"--migrate-to-backend={self.digest_type}E",
                    path=self.repo,
                )
            await self.pexaminekey.send(
                f"{self.digest_type}-s{size}--{digest} {filename}\n"
            )
            return (await self.pexaminekey.receive()).strip()

    async def get_keys_missing_from(self, remote: str | None) -> set[str]:
        """
        Return the keys that git-annex records as present somewhere but not in
        ``remote``, or an empty set if ``remote`` is `None`.

        This is a single pass over the repository rather than a lookup per
        key.  The obvious alternative, ``whereis``, also serialises every URL
        registered on a key, and a Zarr key shared by many identical chunks
        accumulates enough of those to make its output tens of megabytes.

        The result is computed on first use and cached for the lifetime of
        this object, i.e. for one Dandiset or Zarr sync.
        """
        if remote is None:
            return set()
        # Checked before locking so that the common case — a hit, once per
        # asset — doesn't pay for the two event loop checkpoints that even an
        # uncontended `anyio.Lock` acquisition performs.  Safe to double-check
        # because there is no `await` between the lookup and the return.
        if (keys := self.missing_from.get(remote)) is not None:
            return keys
        async with self.locks[f"findkeys {remote}"]:
            if (keys := self.missing_from.get(remote)) is None:
                keys = set()
                async with aclosing(
                    stream_lines_command(
                        "git",
                        *GIT_OPTIONS,
                        "annex",
                        "findkeys",
                        "--copies=1",
                        "--not",
                        "--in",
                        remote,
                        cwd=self.repo,
                    )
                ) as p:
                    async for key in p:
                        keys.add(key.rstrip("\n"))
                self.missing_from[remote] = keys
            return keys

    async def register_url(self, key: str, url: str) -> None:
        async with self.locks["registerurl"]:
            if self.pregisterurl is None:
                self.pregisterurl = await open_git_annex(
                    "registerurl",
                    "-c",
                    "annex.alwayscompact=false",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pregisterurl.send(f"{key} {url}\n")
            r = json.loads(await self.pregisterurl.receive())
        if not r["success"]:
            log.error(
                "`git annex registerurl %s %s` [cwd=%s] call failed:%s",
                key,
                url,
                self.repo,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

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
