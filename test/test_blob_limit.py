from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import anyio
from dandi.consts import EmbargoStatus
from dandi.dandiapi import AssetType
import httpx
import pytest

from backups2datalad import asyncer
from backups2datalad.adandi import RemoteAsset, RemoteBlobAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import Downloader, run_downloader
from backups2datalad.manager import Manager
from backups2datalad.util import AssetTracker

pytestmark = pytest.mark.anyio

CREATED = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_asset(path: str, digest: str) -> RemoteBlobAsset:
    asset = MagicMock(spec=RemoteBlobAsset)
    asset.path = path
    asset.asset_type = AssetType.BLOB
    asset.created = CREATED
    asset.get_digest_value.return_value = digest
    return cast(RemoteBlobAsset, asset)


@pytest.mark.ai_generated
async def test_asset_loop_bounds_concurrent_blobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Regression test for #130: with the asset listing fetched upfront,
    `asset_loop()` turned every asset into a `process_blob()` task at once,
    and hashing the (unannexed) text files among them concurrently ran out
    of file descriptors.  At most `BLOB_LIMIT` may now be in flight.
    """
    limit = 3
    monkeypatch.setattr(asyncer, "BLOB_LIMIT", limit)
    assets: list[RemoteAsset] = []
    for i in range(limit * 5):
        content = f"log {i}\n".encode()
        (tmp_path / f"trace{i}.txt").write_bytes(content)
        assets.append(
            make_asset(f"trace{i}.txt", hashlib.sha256(content).hexdigest())
        )
    config = SimpleNamespace(
        force=None,
        dandisets=SimpleNamespace(remote=None),
        match_asset=lambda _path: True,
        hash_limit=anyio.CapacityLimiter(100),
    )
    manager = SimpleNamespace(config=config, log=MagicMock())
    manager.with_sublogger = lambda _name: manager
    dm = Downloader(
        dandiset_id="000001",
        embargoed=False,
        embargo_status=EmbargoStatus.OPEN,
        ds=cast(AsyncDataset, SimpleNamespace(pathobj=tmp_path)),
        manager=cast(Manager, manager),
        tracker=cast(AssetTracker, MagicMock()),
        s3client=cast(httpx.AsyncClient, MagicMock()),
        annex=cast(AsyncAnnex, SimpleNamespace(get_keys_missing_from=AsyncMock())),
    )
    in_flight = 0
    peak = 0
    hashed = 0
    real_asha256 = dm.asha256

    async def counting_asha256(path: Path) -> str:
        nonlocal in_flight, peak, hashed
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            # Give every task that could start the chance to do so
            await anyio.sleep(0.01)
            return await real_asha256(path)
        finally:
            in_flight -= 1
            hashed += 1

    dm.asha256 = counting_asha256  # type: ignore[method-assign]

    async def aia() -> AsyncIterator[RemoteAsset | None]:
        for a in assets:
            yield a

    await run_downloader(dm, aia())
    assert hashed == len(assets)
    assert peak == limit
    assert dm.report.added == dm.report.updated == 0


@pytest.mark.ai_generated
async def test_asha256_bounds_open_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Hashing -- the part of `process_blob()` and `check_unannexed_hash()`
    that holds a file open -- is bounded by the process-wide `hash_limit`,
    which `blob_limit`, being per Dandiset, is not (#130).
    """
    open_now = 0
    peak = 0
    real_open = anyio.Path.open

    async def counting_open(self: anyio.Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal open_now, peak
        fp = await real_open(self, *args, **kwargs)
        open_now += 1
        peak = max(peak, open_now)
        real_aclose = fp.aclose

        async def aclose() -> None:
            nonlocal open_now
            open_now -= 1
            await real_aclose()

        fp.aclose = aclose  # type: ignore[method-assign]
        # Give every hash that could start the chance to do so
        await anyio.sleep(0.01)
        return fp

    monkeypatch.setattr(anyio.Path, "open", counting_open)
    manager = SimpleNamespace(
        config=SimpleNamespace(hash_limit=anyio.CapacityLimiter(2)),
        log=MagicMock(),
    )
    dm = Downloader(
        dandiset_id="000001",
        embargoed=False,
        embargo_status=EmbargoStatus.OPEN,
        ds=cast(AsyncDataset, SimpleNamespace(pathobj=tmp_path)),
        manager=cast(Manager, manager),
        tracker=cast(AssetTracker, MagicMock()),
        s3client=cast(httpx.AsyncClient, MagicMock()),
        annex=cast(AsyncAnnex, MagicMock()),
    )
    digests: dict[int, str] = {}

    async def hash_one(i: int) -> None:
        path = tmp_path / f"f{i}.txt"
        path.write_bytes(f"{i}\n".encode())
        digests[i] = await dm.asha256(path)

    async with anyio.create_task_group() as tg:
        for i in range(10):
            tg.start_soon(hash_one, i)
    assert peak == 2
    assert open_now == 0
    assert digests == {
        i: hashlib.sha256(f"{i}\n".encode()).hexdigest() for i in range(10)
    }
