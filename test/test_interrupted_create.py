"""
Completing a dataset whose `datalad create` was interrupted

`datalad create` stages ``.gitattributes`` well before its first commit.  Killed
in between -- as cancellation does to whatever command is running when one Zarr
of a Dandiset fails and the others in flight are cancelled -- it leaves a
repository that `Dataset.is_installed()` accepts but that has no dataset ID,
annex backend or special remotes.  Later runs used to build on that and then
fail it as dirty ("A  .gitattributes"), under ``--mode verify`` or without
``--zarr-dirty=reset+clean``; with that, the dirt was "cleaned" away, leaving
the mirror broken for good.  001412's Zarr accbedc4-6c4b-4202-b652-c97bb335042e
was found that way.
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
import logging
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock

import anyio
from datalad.tests.utils_pytest import assert_repo_status
import pytest
from test_util import GitRepo, gitattributes_policy

from backups2datalad.adandi import RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.config import BackupConfig, Mode, ResourceConfig, ZarrDirty
from backups2datalad.manager import Manager
from backups2datalad.procedures.cfg_dandiset import policy_lines
from backups2datalad.zarr import sync_zarr

pytestmark = pytest.mark.anyio

CREATED = datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc)


def git(path: Path, *args: str) -> None:
    # Identity comes from conftest's autouse `tmp_home`
    subprocess.run(["git", *args], cwd=path, check=True, capture_output=True)


def half_create(path: Path, backend: str) -> None:
    """
    Leave ``path`` as `datalad create` does when killed right after staging
    the annex backend: git-annex initialized, ``.gitattributes`` staged, and
    neither a commit nor ``.datalad/``.
    """
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "draft")
    git(path, "annex", "init", "-q")
    (path / ".gitattributes").write_text(f"* annex.backend={backend}\n")
    git(path, "add", ".gitattributes")


def build_upon(path: Path) -> None:
    """
    What `sync_zarr()` then did to such a repository: commit
    ``.dandi/.gitattributes`` -- by itself, so as the root commit, leaving
    ``.gitattributes`` staged.
    """
    (path / ".dandi").mkdir()
    (path / ".dandi" / ".gitattributes").write_text("* annex.largefiles=nothing\n")
    git(path, "add", ".dandi/.gitattributes")
    git(path, "commit", "-q", "-m", "Exclude .dandi/", "--", ".dandi/.gitattributes")


def has_dandiapi_remote(path: Path) -> bool:
    r = GitRepo(path).runcmd(
        "config", "--get", "remote.dandiapi.annex-config-uuid", capture_output=True
    )
    return r.returncode == 0


async def refuse() -> None:
    raise AssertionError("before_create consulted for a dataset being completed")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "backend,cfg_proc",
    [("MD5E", None), ("SHA256E", "dandiset")],
    ids=["zarr", "dandiset"],
)
async def test_interrupted_create_is_completed(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    backend: str,
    cfg_proc: str | None,
) -> None:
    path = tmp_path / "ds"
    half_create(path, backend)
    ds = AsyncDataset(path)
    assert ds.ds.is_installed()
    assert not await ds.is_created()

    # `before_create` guards against creating a mirror anew where one exists
    # elsewhere; this is not a new mirror, and its GitHub repository may well
    # exist, so the guard must not get a say.
    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        assert await ds.ensure_installed(
            "Test dataset",
            commit_date=CREATED,
            backend=backend,
            cfg_proc=cfg_proc,
            before_create=refuse,
        )
    assert any(
        "creation of dataset" in r.getMessage() and "interrupted" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )
    assert_repo_status(ds.path)
    assert await ds.is_created()
    attrs = (path / ".gitattributes").read_text().splitlines()
    assert attrs.count(f"* annex.backend={backend}") == 1
    assert "**/.git* annex.largefiles=nothing" in attrs
    if cfg_proc is not None:
        assert gitattributes_policy(path) == policy_lines()
    assert has_dandiapi_remote(path)
    repo = GitRepo(path)
    assert repo.get_commit_date("HEAD") == "2021-06-01T12:34:56+00:00"

    # It is complete now, and is left alone from here on:
    commits = repo.get_commit_count()
    assert not await ds.ensure_installed(
        "Test dataset", backend=backend, cfg_proc=cfg_proc, before_create=refuse
    )
    assert repo.get_commit_count() == commits


@pytest.mark.ai_generated
async def test_interrupted_create_built_upon_is_completed(tmp_path: Path) -> None:
    """Completing it keeps what a later run committed on top."""
    path = tmp_path / "zarr"
    half_create(path, "MD5E")
    build_upon(path)
    repo = GitRepo(path)
    old_head = repo.get_commitish_hash("HEAD")
    ds = AsyncDataset(path)
    assert not await ds.is_created()
    assert await ds.ensure_installed(
        "Test Zarr", backend="MD5E", cfg_proc=None, before_create=refuse
    )
    assert_repo_status(ds.path)
    assert await ds.is_created()
    assert repo.is_ancestor(old_head, "HEAD")
    assert (
        repo.get_blob("HEAD", ".dandi/.gitattributes") == "* annex.largefiles=nothing"
    )
    assert has_dandiapi_remote(path)


@pytest.mark.ai_generated
@pytest.mark.parametrize("zarr_dirty", list(ZarrDirty))
async def test_sync_zarr_completes_interrupted_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    zarr_dirty: ZarrDirty,
) -> None:
    """
    The reported failure: `update-from-backup --mode verify --zarr-dirty
    reset+clean 001412` failed on accbedc4 with "is dirty ...  A  .gitattributes"
    -- verify never discards.  Neither verify nor the default `error` may
    fail it now, and nothing is discarded to get there.
    """
    config = BackupConfig(
        backup_root=tmp_path,
        zarrs=ResourceConfig(path="zarrs"),
        dandisets=ResourceConfig(path="dandisets"),
        mode=Mode.VERIFY,
        zarr_dirty=zarr_dirty,
    )
    log = MagicMock()
    manager = Manager(config=config, gh=None, log=log, token="dummy")
    asset = MagicMock(spec=RemoteZarrAsset)
    asset.zarr = "accbedc4-6c4b-4202-b652-c97bb335042e"
    asset.dandiset_id = "001412"
    asset.path = "sample.ome.zarr"
    asset.created = CREATED
    dsdir = tmp_path / "zarrs" / asset.zarr
    half_create(dsdir, "MD5E")
    build_upon(dsdir)
    monkeypatch.setattr("backups2datalad.zarr.ZarrSyncer.run", AsyncMock())

    await sync_zarr(asset, None, dsdir, manager)

    ds = AsyncDataset(dsdir)
    assert_repo_status(ds.path)
    assert await ds.is_created()
    assert has_dandiapi_remote(dsdir)
    assert not [c for c in log.warning.call_args_list if "ZARR-RESET" in str(c)]


@pytest.mark.ai_generated
async def test_create_is_not_cut_short_by_cancellation(tmp_path: Path) -> None:
    """
    How such repositories come about: one failed Zarr cancels the other Zarr
    syncs in flight, and cancellation kills the command each is running --
    `datalad create`, for a new Zarr.  Creation has to see itself through.
    """
    path = tmp_path / "zarr"
    ds = AsyncDataset(path)
    async with anyio.create_task_group() as tg:
        tg.start_soon(
            partial(ds.ensure_installed, "Test Zarr", backend="MD5E", cfg_proc=None)
        )
        # Cancel once `datalad create` is underway:
        with anyio.fail_after(60):
            while not (path / ".git").exists():
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()
    assert await ds.is_created()
    assert_repo_status(ds.path)
    assert has_dandiapi_remote(path)
