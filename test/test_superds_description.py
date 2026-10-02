"""
Tests for the GitHub description of the superdataset, which is to say how many
of the archive's Dandisets are mirrored and how much data the mirrors hold
next to the archive's own total.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

from datalad.api import Dataset
import httpx
import pytest

from backups2datalad.adandi import ArchiveStats, AsyncDandiClient, RemoteDandiset
from backups2datalad.adataset import AsyncDataset, DatasetStats
from backups2datalad.config import BackupConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.manager import Manager
from backups2datalad.util import describe_superdataset

pytestmark = pytest.mark.anyio

TAIL = "  DataLad super-dataset of all Dandisets from https://github.com/dandisets"


@pytest.mark.ai_generated
def test_describe_superdataset() -> None:
    assert describe_superdataset(
        mirrored=1190,
        on_archive=1191,
        size=2_300_000_000_000_000,
        archive_size=2_395_165_137_428_611,
    ) == ("1190 of 1191 Dandisets mirrored (2.3 PB of the archive's 2.4 PB)." + TAIL)


@pytest.mark.ai_generated
def test_describe_superdataset_singular() -> None:
    assert describe_superdataset(
        mirrored=1, on_archive=1, size=2_000_000, archive_size=3_000_000
    ) == ("1 of 1 Dandiset mirrored (2.0 MB of the archive's 3.0 MB)." + TAIL)


def make_datasetter(
    tmp_path: Path,
    on_archive: list[str],
    archive_stats: ArchiveStats | Exception,
    list_error: Exception | None = None,
) -> tuple[DandiDatasetter, AsyncMock]:
    async def get_dandisets() -> AsyncGenerator[RemoteDandiset, None]:
        if list_error is not None:
            raise list_error
        for did in on_archive:
            d = MagicMock()
            d.identifier = did
            yield cast(RemoteDandiset, d)

    client = MagicMock()
    client.get_dandisets = get_dandisets
    if isinstance(archive_stats, Exception):
        client.get_archive_stats = AsyncMock(side_effect=archive_stats)
    else:
        client.get_archive_stats = AsyncMock(return_value=archive_stats)
    datasetter = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, client),
        config=BackupConfig(backup_root=tmp_path),
    )
    manager = MagicMock()
    manager.edit_github_repo = AsyncMock()
    datasetter._manager = cast(Manager, manager)
    return datasetter, manager.edit_github_repo


def make_superds(
    tmp_path: Path, submodules: list[str], installed: list[str]
) -> AsyncDataset:
    for did in installed:
        Dataset(tmp_path / did).create(annex=False, result_renderer="disabled")
    superds = MagicMock()
    superds.has_github_remote = AsyncMock(return_value=True)
    superds.get_ghrepo = AsyncMock(return_value="dandi/dandisets")
    superds.get_subdatasets = AsyncMock(
        return_value=[
            {"gitmodule_path": did, "path": str(tmp_path / did)} for did in submodules
        ]
    )
    return cast(AsyncDataset, superds)


SIZES = {"000001": 1_000_000, "000002": 2_000_000}


@pytest.fixture
def fake_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    async def get_stats(
        self: AsyncDataset, config: BackupConfig  # noqa: U100
    ) -> DatasetStats:
        return DatasetStats(files=1, size=SIZES[self.pathobj.name])

    monkeypatch.setattr(AsyncDataset, "get_stats", get_stats)


@pytest.mark.ai_generated
@pytest.mark.usefixtures("fake_stats")
async def test_set_superds_description(tmp_path: Path) -> None:
    # 000001 & 000002 are mirrored; 000003 has been deleted from the archive
    # (its submodule lingers, but is not even looked at); 000004 is on the
    # archive but not mirrored.
    datasetter, edit = make_datasetter(
        tmp_path,
        ["000001", "000002", "000004"],
        ArchiveStats(dandiset_count=3, size=10_000_000),
    )
    superds = make_superds(
        tmp_path, ["000001", "000002", "000003"], installed=["000001", "000002"]
    )
    await datasetter.set_superds_description(superds)
    edit.assert_awaited_once_with(
        "dandi/dandisets",
        description=(
            "2 of 3 Dandisets mirrored (3.0 MB of the archive's 10.0 MB)." + TAIL
        ),
    )


@pytest.mark.ai_generated
@pytest.mark.usefixtures("fake_stats")
@pytest.mark.parametrize(
    "archive_stats,list_error",
    [
        (
            ArchiveStats(dandiset_count=1, size=1),
            httpx.ConnectError("no route to host"),
        ),
        (httpx.ConnectError("no route to host"), None),
    ],
)
async def test_set_superds_description_api_fails(
    tmp_path: Path,
    archive_stats: ArchiveStats | Exception,
    list_error: Exception | None,
) -> None:
    """
    A DANDI API request that fails even after `arequest()`'s retries fails the
    run rather than leave the description silently out of date.
    """
    datasetter, edit = make_datasetter(
        tmp_path, ["000001"], archive_stats, list_error=list_error
    )
    superds = make_superds(tmp_path, ["000001"], installed=["000001"])
    with pytest.raises(httpx.ConnectError):
        await datasetter.set_superds_description(superds)
    edit.assert_not_awaited()


@pytest.mark.ai_generated
@pytest.mark.usefixtures("fake_stats")
async def test_set_superds_description_not_installed(tmp_path: Path) -> None:
    datasetter, edit = make_datasetter(
        tmp_path, ["000001"], ArchiveStats(dandiset_count=1, size=1)
    )
    # An uninstalled submodule leaves an empty directory
    (tmp_path / "000001").mkdir()
    superds = make_superds(tmp_path, ["000001"], installed=[])
    with pytest.raises(RuntimeError, match="000001.* is not installed"):
        await datasetter.set_superds_description(superds)
    edit.assert_not_awaited()


@pytest.mark.ai_generated
async def test_set_superds_description_uncountable(tmp_path: Path) -> None:
    """
    A mirror with a Zarr cannot be counted without `zarr_root`; that fails the
    run rather than leave the mirror out of the total.
    """
    datasetter, edit = make_datasetter(
        tmp_path, ["000001"], ArchiveStats(dandiset_count=1, size=1)
    )
    assert datasetter.config.zarr_root is None
    path = tmp_path / "000001"
    Dataset(path).create(result_renderer="disabled")
    Dataset(path).create(path / "sample.zarr", annex=False, result_renderer="disabled")
    superds = make_superds(tmp_path, ["000001"], installed=[])
    with pytest.raises(AssertionError):
        await datasetter.set_superds_description(superds)
    edit.assert_not_awaited()


@pytest.mark.ai_generated
async def test_get_stats_recounts_stale(tmp_path: Path) -> None:
    """
    Stats cached for an older commit are recounted, not counted as 0 as the
    description used to do, silently dropping the mirror from the total.
    """
    config = BackupConfig(backup_root=tmp_path)
    path = tmp_path / "000001"
    Dataset(path).create(annex=False, result_renderer="disabled")
    (path / "data.txt").write_text("0123456789")
    ds = AsyncDataset(path)
    await ds.save(message="Add data", path=["data.txt"])
    await ds.store_stats(DatasetStats(files=1, size=10))
    assert await ds.get_stats(config=config) == DatasetStats(files=1, size=10)
    (path / "more.txt").write_text("01234")
    await ds.save(message="Add more", path=["more.txt"])
    assert await ds.get_stored_stats() is None
    stats = await ds.get_stats(config=config)
    assert stats.size == 15
    # ... and caches the recount
    assert await ds.get_stored_stats() == stats
