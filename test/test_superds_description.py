"""
Tests for the GitHub description of the superdataset, which is to say how many
of the archive's Dandisets are mirrored and how much data the mirrors hold
next to the archive's own total.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
import json
from pathlib import Path
from typing import Any, cast
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
        mirrored=1191,
        on_archive=1191,
        size=1_080_000_000_000_000,
        archive_size=2_395_165_137_428_611,
    ) == ("1191 of 1191 Dandisets mirrored (1.1 PB of the archive's 2.4 PB)." + TAIL)


@pytest.mark.ai_generated
def test_describe_superdataset_no_archive_size() -> None:
    assert describe_superdataset(
        mirrored=1, on_archive=1, size=2_000_000, archive_size=None
    ) == ("1 of 1 Dandiset mirrored (2.0 MB)." + TAIL)


def make_datasetter(
    tmp_path: Path,
    on_archive: list[str],
    archive_stats: Any,
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


def make_superds(tmp_path: Path, submodules: list[str]) -> AsyncDataset:
    superds = MagicMock()
    superds.has_github_remote = AsyncMock(return_value=True)
    superds.get_ghrepo = AsyncMock(return_value="dandi/dandisets")
    superds.get_subdatasets = AsyncMock(
        return_value=[
            {"gitmodule_path": did, "path": str(tmp_path / did)} for did in submodules
        ]
    )
    return cast(AsyncDataset, superds)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "archive_stats,sizes",
    [
        (
            ArchiveStats(dandiset_count=3, size=10_000_000),
            "3.0 MB of the archive's 10.0 MB",
        ),
        (httpx.ConnectError("no route to host"), "3.0 MB"),
        # A 200 with a non-JSON body, as from a misbehaving proxy
        (json.JSONDecodeError("Expecting value", "<html>", 0), "3.0 MB"),
    ],
)
async def test_set_superds_description(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    archive_stats: Any,
    sizes: str,
) -> None:
    # 000001 & 000002 are mirrored; 000003 has been deleted from the archive
    # (but its mirror lingers); 000004 is on the archive but not mirrored.
    datasetter, edit = make_datasetter(
        tmp_path, ["000001", "000002", "000004"], archive_stats
    )
    superds = make_superds(tmp_path, ["000001", "000002", "000003"])
    sizes_by_id = {"000001": 1_000_000, "000002": 2_000_000, "000003": 4_000_000}

    async def get_mirror_stats(_self: Any, ds: AsyncDataset) -> DatasetStats:
        return DatasetStats(files=1, size=sizes_by_id[ds.pathobj.name])

    monkeypatch.setattr(DandiDatasetter, "get_mirror_stats", get_mirror_stats)
    await datasetter.set_superds_description(superds)
    edit.assert_awaited_once_with(
        "dandi/dandisets",
        description=f"2 of 3 Dandisets mirrored ({sizes})." + TAIL,
    )


@pytest.mark.ai_generated
async def test_set_superds_description_listing_fails(tmp_path: Path) -> None:
    """A failure to list the archive skips the update without failing the run"""
    datasetter, edit = make_datasetter(
        tmp_path,
        [],
        ArchiveStats(dandiset_count=1, size=1),
        list_error=httpx.ConnectError("no route to host"),
    )
    await datasetter.set_superds_description(make_superds(tmp_path, ["000001"]))
    edit.assert_not_awaited()


@pytest.mark.ai_generated
async def test_get_mirror_stats_recounts_stale(tmp_path: Path) -> None:
    """
    Stats cached for an older commit are recounted rather than counted as 0,
    which is what used to make a mirror silently vanish from the total.
    """
    datasetter, _ = make_datasetter(tmp_path, [], None)
    path = tmp_path / "000001"
    Dataset(path).create(annex=False, result_renderer="disabled")
    (path / "data.txt").write_text("0123456789")
    ds = AsyncDataset(path)
    await ds.save(message="Add data", path=["data.txt"])
    await ds.store_stats(DatasetStats(files=1, size=10))
    assert await datasetter.get_mirror_stats(ds) == DatasetStats(files=1, size=10)
    (path / "more.txt").write_text("01234")
    await ds.save(message="Add more", path=["more.txt"])
    assert await ds.get_stored_stats() is None
    stats = await datasetter.get_mirror_stats(ds)
    assert stats is not None
    assert stats.size == 15
    # ... and caches the recount
    assert await ds.get_stored_stats() == stats


@pytest.mark.ai_generated
async def test_get_mirror_stats_uncountable(tmp_path: Path) -> None:
    """
    A mirror with a Zarr cannot be counted without `zarr_root`; that is
    logged, not raised, as the description is best-effort.
    """
    datasetter, _ = make_datasetter(tmp_path, [], None)
    assert datasetter.config.zarr_root is None
    path = tmp_path / "000001"
    Dataset(path).create(result_renderer="disabled")
    Dataset(path).create(path / "sample.zarr", annex=False, result_renderer="disabled")
    assert await datasetter.get_mirror_stats(AsyncDataset(path)) is None


@pytest.mark.ai_generated
@pytest.mark.parametrize("make_dir", [False, True])
async def test_get_mirror_stats_missing(tmp_path: Path, make_dir: bool) -> None:
    datasetter, _ = make_datasetter(tmp_path, [], None)
    if make_dir:
        # An uninstalled submodule leaves an empty directory
        (tmp_path / "000001").mkdir()
    assert await datasetter.get_mirror_stats(AsyncDataset(tmp_path / "000001")) is None
