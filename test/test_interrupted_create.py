"""
Redoing or completing a dataset whose `datalad create` was interrupted

`datalad create` stages ``.gitattributes`` well before its first commit.  Killed
in between -- as cancellation does to whatever command is running when one Zarr
of a Dandiset fails and the others in flight are cancelled -- it leaves a
repository that `Dataset.is_installed()` accepts but that has no dataset ID,
annex backend or special remotes.  Later runs used to build on that and then
fail it as dirty ("A  .gitattributes"), under ``--mode verify`` or without
``--zarr-dirty=reset+clean``; with that, the dirt was "cleaned" away, leaving
the mirror broken for good.  001412's Zarr accbedc4-6c4b-4202-b652-c97bb335042e
was found that way.

Such a mirror is removed and created anew, unless it was published in the
meantime, in which case its creation is completed in place.
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
from backups2datalad.util import MirrorMissingError
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


def build_upon(path: Path) -> str:
    """
    What `sync_zarr()` then did to such a repository: commit
    ``.dandi/.gitattributes`` -- by itself, so as the root commit, leaving
    ``.gitattributes`` staged.  And, as a run with ``reset+clean`` could have
    gone on to do, annex some content, whose read-only objects removal has to
    cope with.  Returns the resulting ``HEAD``.
    """
    (path / ".dandi").mkdir()
    (path / ".dandi" / ".gitattributes").write_text("* annex.largefiles=nothing\n")
    git(path, "add", ".dandi/.gitattributes")
    git(path, "commit", "-q", "-m", "Exclude .dandi/", "--", ".dandi/.gitattributes")
    (path / "data.bin").write_bytes(b"\0" * 64)
    git(path, "annex", "add", "-q", "--", "data.bin")
    git(path, "commit", "-q", "-m", "Add data", "--", "data.bin")
    return GitRepo(path).get_commitish_hash("HEAD")


def has_dandiapi_remote(path: Path) -> bool:
    r = GitRepo(path).runcmd(
        "config", "--get", "remote.dandiapi.annex-config-uuid", capture_output=True
    )
    return r.returncode == 0


def has_commit(path: Path, commit: str) -> bool:
    r = GitRepo(path).runcmd("cat-file", "-e", f"{commit}^{{commit}}")
    return r.returncode == 0


def warnings_of(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


async def assert_datalad_sees_it(ds: AsyncDataset) -> None:
    """
    DataLad's in-process view of the dataset is that of the repository now on
    disk, not what it cached before `ensure_installed()` replaced it
    """
    repo = GitRepo(ds.pathobj)
    assert ds.ds.id == await ds.get_datalad_id()
    assert ds.ds.config.get("datalad.dataset.id") == await ds.get_datalad_id()
    assert ds.ds.repo.uuid == repo.readcmd("config", "annex.uuid")


async def refuse() -> None:
    raise AssertionError("before_create consulted although a remote says enough")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "backend,cfg_proc",
    [("MD5E", None), ("SHA256E", "dandiset")],
    ids=["zarr", "dandiset"],
)
async def test_unpublished_interrupted_create_starts_over(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    backend: str,
    cfg_proc: str | None,
) -> None:
    path = tmp_path / "ds"
    half_create(path, backend)
    old_head = build_upon(path)
    ds = AsyncDataset(path)
    assert ds.ds.is_installed()
    assert not await ds.is_created()
    consulted = []

    async def before_create() -> None:
        consulted.append(True)

    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        assert await ds.ensure_installed(
            "Test dataset",
            commit_date=CREATED,
            backend=backend,
            cfg_proc=cfg_proc,
            before_create=before_create,
        )
    # Only `before_create` can say whether a mirror of it exists elsewhere:
    assert consulted == [True]
    (warning,) = warnings_of(caplog)
    assert "never published" in warning
    assert "creating it anew" in warning
    assert not has_commit(path, old_head)
    assert not (path / "data.bin").exists()
    assert not (path / ".dandi").exists()
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
    await assert_datalad_sees_it(ds)

    # It is complete now, and is left alone from here on:
    commits = repo.get_commit_count()
    assert not await ds.ensure_installed(
        "Test dataset", backend=backend, cfg_proc=cfg_proc, before_create=refuse
    )
    assert repo.get_commit_count() == commits


@pytest.mark.ai_generated
async def test_interrupted_create_with_github_remote_is_completed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    accbedc4's case: a later run built on it and created its GitHub
    repository.  Made anew, it could not be pushed over that, so its creation
    is completed in place, keeping what was committed on top.
    """
    path = tmp_path / "zarr"
    half_create(path, "MD5E")
    old_head = build_upon(path)
    git(path, "remote", "add", "github", "https://github.com/dandizarrs/accbedc4")
    ds = AsyncDataset(path)
    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        assert await ds.ensure_installed(
            "Test Zarr", backend="MD5E", cfg_proc=None, before_create=refuse
        )
    (warning,) = warnings_of(caplog)
    assert "published since" in warning
    assert "`github` remote" in warning
    assert_repo_status(ds.path)
    assert await ds.is_created()
    repo = GitRepo(path)
    assert repo.is_ancestor(old_head, "HEAD")
    assert repo.get_blob("HEAD", ".dandi/.gitattributes") == (
        "* annex.largefiles=nothing"
    )
    attrs = (path / ".gitattributes").read_text().splitlines()
    assert attrs.count("* annex.backend=MD5E") == 1
    assert has_dandiapi_remote(path)
    await assert_datalad_sees_it(ds)


@pytest.mark.ai_generated
async def test_interrupted_create_vetoed_by_before_create_is_completed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Without a `github` remote -- e.g. a run cancelled while creating the
    GitHub repository -- the mirror is still completed in place if
    `before_create` says a mirror of it exists elsewhere.
    """
    path = tmp_path / "zarr"
    half_create(path, "MD5E")
    old_head = build_upon(path)

    async def on_github() -> None:
        raise MirrorMissingError("GitHub repository dandizarrs/z exists already")

    ds = AsyncDataset(path)
    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        assert await ds.ensure_installed(
            "Test Zarr", backend="MD5E", cfg_proc=None, before_create=on_github
        )
    (warning,) = warnings_of(caplog)
    assert "published since" in warning
    assert "dandizarrs/z exists already" in warning
    assert_repo_status(ds.path)
    assert await ds.is_created()
    assert GitRepo(path).is_ancestor(old_head, "HEAD")


@pytest.mark.ai_generated
@pytest.mark.parametrize("zarr_dirty", list(ZarrDirty))
async def test_sync_zarr_redoes_interrupted_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    zarr_dirty: ZarrDirty,
) -> None:
    """
    The reported failure: `update-from-backup --mode verify --zarr-dirty
    reset+clean 001412` failed on accbedc4 with "is dirty ...  A  .gitattributes"
    -- verify never discards.  Neither verify nor the default `error` may
    fail it now, and `reset+clean` is not what gets it there.
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
    old_head = build_upon(dsdir)
    monkeypatch.setattr("backups2datalad.zarr.ZarrSyncer.run", AsyncMock())

    await sync_zarr(asset, None, dsdir, manager)

    ds = AsyncDataset(dsdir)
    assert_repo_status(ds.path)
    assert await ds.is_created()
    assert not has_commit(dsdir, old_head)
    assert (dsdir / ".dandi" / ".gitattributes").exists()
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


@pytest.mark.ai_generated
async def test_find_interrupted_create_script(tmp_path: Path) -> None:
    """`tools/find-interrupted-create` lists exactly what `is_created()` rejects"""
    script = Path(__file__).parent.parent / "tools" / "find-interrupted-create"
    dandisets = tmp_path / "dandisets"
    dandisets.mkdir()
    # A superdataset without a dataset ID, so that a check that wandered up
    # into it from an uninstalled submodule would list that:
    git(dandisets, "init", "-q", "-b", "draft")
    assert await AsyncDataset(dandisets / "000001").ensure_installed(
        "Dandiset 000001", cfg_proc=None
    )
    (dandisets / "000002").mkdir()  # uninstalled
    half_create(dandisets / "000003", "SHA256E")
    zarrs = tmp_path / "dandizarrs"
    assert await AsyncDataset(zarrs / "good").ensure_installed(
        "Zarr good", backend="MD5E", cfg_proc=None
    )
    half_create(zarrs / "half", "MD5E")
    half_create(zarrs / "built-upon", "MD5E")
    build_upon(zarrs / "built-upon")
    (zarrs / "not-a-repo").mkdir()

    r = subprocess.run(
        [str(script)], cwd=tmp_path, capture_output=True, text=True, check=True
    )
    assert sorted(r.stdout.splitlines()) == [
        "dandisets/000003",
        "dandizarrs/built-upon",
        "dandizarrs/half",
    ]
    assert r.stderr == ""
    r = subprocess.run(
        [str(script), str(zarrs)], capture_output=True, text=True, check=True
    )
    assert sorted(r.stdout.splitlines()) == [
        str(zarrs / "built-upon"),
        str(zarrs / "half"),
    ]
