"""
Tests for `AsyncDataset.get_stats()`, which counts the files of a mirror's
``HEAD`` -- never of its working tree, which a concurrent or interrupted run
may have left disagreeing with it (#139).
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import subprocess

import pytest

from backups2datalad.adataset import AsyncDataset, DatasetStats
from backups2datalad.config import BackupConfig, ResourceConfig

pytestmark = pytest.mark.anyio


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=path, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


def init_repo(path: Path) -> None:
    path.mkdir(parents=True)
    git(path, "init", "-q")
    git(path, "config", "user.name", "Test")
    git(path, "config", "user.email", "test@example.nil")
    git(path, "annex", "init", "-q")
    # As `cfg_dandiset` does, so that `.dandi/` content can be annexed
    git(path, "annex", "config", "--set", "annex.dotfiles", "true")


def write(path: Path, content: bytes | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content)
    else:
        path.write_bytes(content)


@pytest.mark.ai_generated
async def test_get_stats_counts_head_not_worktree(tmp_path: Path) -> None:
    path = tmp_path / "000001"
    init_repo(path)
    write(path / "data.bin", b"x" * 1000)
    write(path / "sub-1" / "more.bin", b"y" * 500)
    write(path / ".dandi" / "assets.json", b"[]" * 5000)
    git(path, "annex", "add", "-q", "data.bin", "sub-1", ".dandi")
    write(path / "small.txt", "hello\n")
    write(path / "dandiset.yaml", "identifier: '000001'\n")
    git(path, "annex", "add", "-q", "--force-small", "small.txt", "dandiset.yaml")
    git(path, "commit", "-q", "-m", "Add files")
    # Annexed files are counted by their keys' sizes, the rest by their blobs'
    # sizes; metadata files are not counted, whether in git (dandiset.yaml) or
    # annexed (.dandi/)
    expected = DatasetStats(files=3, size=1000 + 500 + 6)

    # What an interrupted or concurrent run may leave behind (#139): an
    # annexed file not in HEAD made `get_file_stats()` raise a KeyError
    write(path / "new.bin", b"z" * 300)
    git(path, "annex", "add", "-q", "new.bin")
    git(path, "rm", "-q", "data.bin")
    write(path / "uncommitted.bin", b"u" * 200)
    ds = AsyncDataset(path)
    assert await ds.is_dirty()

    config = BackupConfig(backup_root=tmp_path)
    assert await ds.get_stats(config=config) == expected
    assert await ds.get_stored_stats() == expected


@pytest.mark.ai_generated
async def test_get_stats_refuses_unannexed_symlink(tmp_path: Path) -> None:
    """
    Annexed files are told by being symlinks, so one that is not annexed
    fails the count rather than skewing it
    """
    path = tmp_path / "000001"
    init_repo(path)
    write(path / "data.bin", b"x" * 100)
    git(path, "annex", "add", "-q", "data.bin")
    (path / "link").symlink_to("data.bin")
    git(path, "annex", "add", "-q", "--force-small", "link")
    git(path, "commit", "-q", "-m", "Add files")
    ds = AsyncDataset(path)
    with pytest.raises(RuntimeError, match="counts 1 annexed files .* 2 symlinks"):
        await ds.get_stats(config=BackupConfig(backup_root=tmp_path))


@pytest.mark.ai_generated
async def test_get_stats_cached_for_counted_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Stats are cached for the commit they were counted for, even if HEAD moves
    on while they are being counted
    """
    path = tmp_path / "000001"
    init_repo(path)
    write(path / "data.bin", b"x" * 100)
    git(path, "annex", "add", "-q", "data.bin")
    git(path, "commit", "-q", "-m", "Add data")
    counted = git(path, "rev-parse", "HEAD")

    get_annexed_tree_stats = AsyncDataset.get_annexed_tree_stats

    async def count_then_commit(
        self: AsyncDataset, commit: str, exclude: Iterable[str] = ()
    ) -> DatasetStats:
        r = await get_annexed_tree_stats(self, commit, exclude)
        write(path / "more.bin", b"y" * 50)
        git(path, "annex", "add", "-q", "more.bin")
        git(path, "commit", "-q", "-m", "Add more")
        return r

    monkeypatch.setattr(AsyncDataset, "get_annexed_tree_stats", count_then_commit)
    ds = AsyncDataset(path)
    stats = await ds.get_stats(config=BackupConfig(backup_root=tmp_path))
    assert stats == DatasetStats(files=1, size=100)
    assert await ds.get_stored_stats(counted) == stats
    assert await ds.get_stored_stats() is None


@pytest.mark.ai_generated
async def test_get_stats_zarr_from_committed_gitmodules(tmp_path: Path) -> None:
    """
    A Zarr submodule is looked up in the ``.gitmodules`` of the commit being
    counted, not in the working tree's
    """
    config = BackupConfig(backup_root=tmp_path, zarrs=ResourceConfig(path="zarrs"))
    assert config.zarr_root is not None
    zarr_id = "4c6ed7b6-7b4e-4b5f-9a30-1d4a4d7e3f39"
    zpath = config.zarr_root / zarr_id
    init_repo(zpath)
    write(zpath / ".zgroup", b"g" * 24)
    write(zpath / "0" / "0", b"c" * 1000)
    git(zpath, "annex", "add", "-q", ".zgroup", "0")
    write(zpath / ".dandi" / "zarr-checksum", "abc-2--1024\n")
    git(zpath, "annex", "add", "-q", "--force-small", ".dandi")
    git(zpath, "commit", "-q", "-m", "Add Zarr")
    zcommit = git(zpath, "rev-parse", "HEAD")

    path = config.dandiset_root / "000001"
    init_repo(path)
    write(path / "data.bin", b"x" * 100)
    git(path, "annex", "add", "-q", "data.bin")
    zarr_path = "sub-1/sample.zarr"
    git(path, "update-index", "--add", "--cacheinfo", f"160000,{zcommit},{zarr_path}")
    gitmodules = str(path / ".gitmodules")
    git(path, "config", "-f", gitmodules, f"submodule.{zarr_path}.path", zarr_path)
    git(
        path,
        "config",
        "-f",
        gitmodules,
        f"submodule.{zarr_path}.url",
        f"https://github.com/dandizarrs/{zarr_id}",
    )
    git(path, "annex", "add", "-q", "--force-small", ".gitmodules")
    git(path, "commit", "-q", "-m", "Add Zarr submodule")
    # Lose the submodule from the working tree's .gitmodules
    git(path, "config", "-f", gitmodules, "--remove-section", f"submodule.{zarr_path}")

    ds = AsyncDataset(path)
    assert await ds.get_stats(config=config) == DatasetStats(
        files=1 + 2, size=100 + 24 + 1000
    )
    assert await AsyncDataset(zpath).get_stored_stats() == DatasetStats(
        files=2, size=1024
    )
