from __future__ import annotations

import hashlib
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from typing import IO, Any, cast
from unittest.mock import MagicMock

import anyio
from dandi.consts import EmbargoStatus
import httpx
import pytest

from backups2datalad import asyncer
from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import Downloader
from backups2datalad.manager import Manager
from backups2datalad.util import AssetTracker

pytestmark = pytest.mark.anyio


@pytest.mark.ai_generated
async def test_asha256_holds_files_open_only_while_hashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Regression test for #130: `asha256()` used to open a file in one worker
    thread call and read it in others, so with thousands of hashes queued for
    the thread pool nearly all of them held their file open while waiting,
    running out of file descriptors.  No more files may be open at once than
    there are worker threads.
    """
    lock = threading.Lock()
    open_now = 0
    peak = 0

    class CountingFile:
        def __init__(self, fp: IO[bytes]) -> None:
            self.fp = fp

        def __enter__(self) -> IO[bytes]:
            return self.fp

        def __exit__(self, *_exc: object) -> None:
            nonlocal open_now
            with lock:
                open_now -= 1
            self.fp.close()

    def counting_open(*args: Any, **kwargs: Any) -> CountingFile:
        nonlocal open_now, peak
        fp = open(*args, **kwargs)
        with lock:
            open_now += 1
            peak = max(peak, open_now)
        # Give every hash that could start the chance to do so
        time.sleep(0.01)
        return CountingFile(fp)

    monkeypatch.setattr(asyncer, "open", counting_open, raising=False)
    dm = Downloader(
        dandiset_id="000001",
        embargoed=False,
        embargo_status=EmbargoStatus.OPEN,
        ds=cast(AsyncDataset, SimpleNamespace(pathobj=tmp_path)),
        manager=cast(Manager, SimpleNamespace(log=MagicMock())),
        tracker=cast(AssetTracker, MagicMock()),
        s3client=cast(httpx.AsyncClient, MagicMock()),
        annex=cast(AsyncAnnex, MagicMock()),
    )
    threads = 2
    digests: dict[int, str] = {}

    async def hash_one(i: int) -> None:
        path = tmp_path / f"f{i}.txt"
        path.write_bytes(f"{i}\n".encode())
        digests[i] = await dm.asha256(path)

    anyio.to_thread.current_default_thread_limiter().total_tokens = threads
    async with anyio.create_task_group() as tg:
        for i in range(20):
            tg.start_soon(hash_one, i)
    assert 0 < peak <= threads
    assert open_now == 0
    assert digests == {
        i: hashlib.sha256(f"{i}\n".encode()).hexdigest() for i in range(20)
    }
