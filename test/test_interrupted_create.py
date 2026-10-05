"""
Completing a dataset whose `datalad create` was interrupted

`datalad create` stages ``.gitattributes`` well before its first commit.  Killed
in between -- e.g. by the cancellation of the other Zarr syncs in flight when
one Zarr of a Dandiset fails -- it leaves a repository that
`Dataset.is_installed()` accepts but that has no dataset ID, annex backend or
special remotes.  Later runs used to build on that and then fail it as dirty
("A  .gitattributes"), under ``--mode verify`` or without
``--zarr-dirty=reset+clean``; with that, the dirt was "cleaned" away, leaving
the mirror broken for good.  001412's Zarr accbedc4-6c4b-4202-b652-c97bb335042e
was found that way.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock

from dandi.consts import EmbargoStatus
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


def half_create(path: Path, backend: str, config_written: bool = False) -> None:
    """
    Leave ``path`` as `datalad create` does when killed right after staging
    the annex backend: git-annex initialized, ``.gitattributes`` staged, and
    no commit.  With ``config_written``, killed a little later: after it wrote
    the dataset ID to ``.datalad/config``, which is thus uncommitted.
    """
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "draft")
    git(path, "annex", "init", "-q")
    (path / ".gitattributes").write_text(f"* annex.backend={backend}\n")
    git(path, "add", ".gitattributes")
    if config_written:
        (path / ".datalad").mkdir()
        git(path, "config", "-f", ".datalad/config", "datalad.dataset.id", "x")


def build_upon(path: Path) -> str:
    """
    What `sync_zarr()` then did to such a repository: commit
    ``.dandi/.gitattributes`` -- by itself, so as the root commit, leaving
    ``.gitattributes`` staged.  Returns the resulting ``HEAD``.
    """
    (path / ".dandi").mkdir()
    (path / ".dandi" / ".gitattributes").write_text("* annex.largefiles=nothing\n")
    git(path, "add", ".dandi/.gitattributes")
    git(path, "commit", "-q", "-m", "Exclude .dandi/", "--", ".dandi/.gitattributes")
    return GitRepo(path).get_commitish_hash("HEAD")


def has_dandiapi_remote(path: Path) -> bool:
    r = GitRepo(path).runcmd(
        "config", "--get", "remote.dandiapi.annex-config-uuid", capture_output=True
    )
    return r.returncode == 0


async def assert_completed(ds: AsyncDataset, backend: str) -> None:
    assert_repo_status(ds.path)
    assert await ds.is_created()
    attrs = (ds.pathobj / ".gitattributes").read_text().splitlines()
    assert attrs.count(f"* annex.backend={backend}") == 1
    assert "**/.git* annex.largefiles=nothing" in attrs
    assert has_dandiapi_remote(ds.pathobj)
    # DataLad's in-process view is of the dataset now on disk, not of what it
    # cached before `datalad create --force`:
    assert ds.ds.id == await ds.get_datalad_id()


async def refuse() -> None:
    raise AssertionError("before_create consulted for a dataset being completed")


@pytest.mark.ai_generated
@pytest.mark.parametrize("config_written", [False, True], ids=["staged", "written"])
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
    config_written: bool,
) -> None:
    path = tmp_path / "ds"
    half_create(path, backend, config_written=config_written)
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
        "interrupted; completing it" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )
    await assert_completed(ds, backend)
    if cfg_proc is not None:
        assert gitattributes_policy(path) == policy_lines()
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
    """
    accbedc4's state: a later run committed on top and created its GitHub
    sibling.  Completing it keeps what was committed.
    """
    path = tmp_path / "zarr"
    half_create(path, "MD5E")
    old_head = build_upon(path)
    git(path, "remote", "add", "github", "https://github.com/dandizarrs/accbedc4")
    ds = AsyncDataset(path)
    assert not await ds.is_created()
    assert await ds.ensure_installed(
        "Test Zarr", backend="MD5E", cfg_proc=None, before_create=refuse
    )
    await assert_completed(ds, "MD5E")
    repo = GitRepo(path)
    assert repo.is_ancestor(old_head, "HEAD")
    assert repo.get_blob("HEAD", ".dandi/.gitattributes") == (
        "* annex.largefiles=nothing"
    )


@pytest.mark.ai_generated
async def test_completing_embargoed_commits_only_its_own_changes(
    tmp_path: Path,
) -> None:
    """
    Recording the embargo status commits `.datalad/config` alone, not whatever
    else a half-made mirror has lying around, which would then pass every
    dirtiness check.
    """
    path = tmp_path / "zarr"
    half_create(path, "MD5E")
    (path / "stray.txt").write_text("junk\n")
    ds = AsyncDataset(path)
    assert await ds.ensure_installed(
        "Test Zarr",
        backend="MD5E",
        cfg_proc=None,
        embargo_status=EmbargoStatus.EMBARGOED,
    )
    repo = GitRepo(path)
    assert "stray.txt" not in repo.readcmd_z(
        "ls-tree", "-r", "-z", "--name-only", "HEAD"
    )
    assert (
        repo.readcmd(
            "config", "--blob", "HEAD:.datalad/config", "dandi.dandiset.embargo-status"
        )
        == EmbargoStatus.EMBARGOED.value
    )
    assert await ds.is_dirty()


@pytest.mark.ai_generated
async def test_sync_zarr_completes_interrupted_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The reported failure: `update-from-backup --mode verify --zarr-dirty
    reset+clean 001412` failed on accbedc4 with "is dirty ...  A  .gitattributes",
    `reset+clean` being ignored under verify.  The Zarr is now completed
    instead, so nothing is left to discard.
    """
    config = BackupConfig(
        backup_root=tmp_path,
        zarrs=ResourceConfig(path="zarrs"),
        dandisets=ResourceConfig(path="dandisets"),
        mode=Mode.VERIFY,
        zarr_dirty=ZarrDirty.RESET_CLEAN,
    )
    manager = Manager(config=config, gh=None, log=MagicMock(), token="dummy")
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
    await assert_completed(ds, "MD5E")
    assert GitRepo(dsdir).is_ancestor(old_head, "HEAD")


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
    half_create(zarrs / "built-upon", "MD5E", config_written=True)
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
