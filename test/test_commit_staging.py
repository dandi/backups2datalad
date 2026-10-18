"""
What may be left out of a commit that records the backup state.

git-annex stages the files its `fromkey`/`addurl` create itself and, when
`.git/index.lock` is taken at that moment, drops the whole queued batch while
reporting success all the same.  Such files were left out of the commit that
records the backup state, which then failed with an uninformative "still dirty
after committing" (000571, #72).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
import logging
from pathlib import Path
import subprocess
from typing import Any
from unittest.mock import MagicMock

from dandi.consts import EmbargoStatus
from obscure import OBSCURE_NAMES
import pytest

from backups2datalad import asyncer
from backups2datalad.adataset import AsyncDataset
from backups2datalad.config import BackupConfig
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager

pytestmark = pytest.mark.anyio

KEY = "SHA256E-s5--" + "0" * 64 + ".nwb"


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, text=True, capture_output=True
    ).stdout


def lose_fromkey(repo: Path, key: str, path: str) -> None:
    """What `fromkey` leaves when its flush finds `.git/index.lock` taken"""
    git(repo, "annex", "fromkey", "--force", key, path)
    git(repo, "rm", "-q", "--cached", "--", path)


def init_annex_repo(repo: Path) -> None:
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "annex", "init", "-q")
    # Like the dandiset policy: binary files to git-annex, text to Git
    (repo / ".gitattributes").write_text(
        "* annex.largefiles=(mimeencoding=binary)and(largerthan=0)\n"
    )


def unstaged(repo: Path) -> list[str]:
    status = git(
        repo,
        "status",
        "--porcelain",
        "-z",
        "--no-renames",
        "--untracked-files=all",
        "--ignore-submodules=all",
    ).split("\0")
    return [f for f in status if f and f[1] != " "]


@pytest.mark.ai_generated
async def test_stage_unstaged(tmp_path: Path) -> None:
    """
    Files git-annex left untracked or modified are staged, whatever their
    names, with git-annex's policy; renames, other staged changes and
    submodules are not touched.
    """
    repo = tmp_path / "ds"
    init_annex_repo(repo)
    for name in ["replaced.txt", "renamed.txt", "a1.txt"]:
        (repo / name).write_text(f"{name}\n")
    # A submodule whose checkout has moved on from what is recorded
    sub = repo / "sub.zarr"
    sub.mkdir()
    git(sub, "init", "-q")
    git(sub, "commit", "-q", "--allow-empty", "-m", "One")
    recorded = git(sub, "rev-parse", "HEAD").strip()
    git(sub, "commit", "-q", "--allow-empty", "-m", "Two")
    git(repo, "add", "--", "replaced.txt", "renamed.txt", "a1.txt", ".gitattributes")
    git(repo, "update-index", "--add", "--cacheinfo", f"160000,{recorded},sub.zarr")
    git(repo, "commit", "-q", "-m", "Initial")

    git(repo, "mv", "renamed.txt", "moved.txt")
    # In a new directory, which `git status` would show collapsed unless told
    # to list every untracked file
    (repo / "sub-01").mkdir()
    lose_fromkey(repo, KEY, "sub-01/new.nwb")
    # A binary regular file, which should end up annexed and locked
    (repo / "sub-02").mkdir()
    (repo / "sub-02" / "bin.dat").write_bytes(bytes(range(256)) * 4)
    # A replaced file: `git rm`, then re-added by git-annex
    git(repo, "rm", "-q", "--", "replaced.txt")
    (repo / "replaced.txt").write_text("replacement\n")
    (repo / "a[1].txt").write_text("glob\n")
    for name in OBSCURE_NAMES:
        (repo / name).write_text("obscure\n")
    wanted = [
        "sub-01/new.nwb",
        "sub-02/bin.dat",
        "replaced.txt",
        "a[1].txt",
        *OBSCURE_NAMES,
    ]

    ds = AsyncDataset(repo)
    staged = await ds.stage_unstaged(set(wanted))
    assert sorted(staged) == sorted(wanted)
    assert unstaged(repo) == []
    modes = {}
    for line in git(repo, "ls-files", "-s", "-z").split("\0"):
        if line:
            meta, _, path = line.partition("\t")
            modes[path] = meta.split()[0]
    assert modes["sub-01/new.nwb"] == "120000"
    assert modes["sub-02/bin.dat"] == "120000"
    assert git(repo, "annex", "find", "--unlocked") == ""
    assert modes["replaced.txt"] == "100644"
    assert modes["sub.zarr"] == "160000"
    assert all(modes[name] == "100644" for name in OBSCURE_NAMES)
    # Staged changes are left as they were, and so is the glob's sibling.
    assert "moved.txt" in modes and "renamed.txt" not in modes
    assert git(repo, "diff", "--cached", "--name-only", "--", "a1.txt") == ""
    assert git(sub, "rev-parse", "HEAD").strip() != recorded
    assert await ds.stage_unstaged(set(wanted)) == []


@pytest.mark.ai_generated
@pytest.mark.parametrize("dirt", ["stray file", "deletion", "nested repository"])
async def test_stage_unstaged_refuses_other_dirt(tmp_path: Path, dirt: str) -> None:
    """
    Anything unstaged besides the files git-annex was given is reported, and
    nothing is staged, so the backup state is not committed past it.
    """
    repo = tmp_path / "ds"
    init_annex_repo(repo)
    (repo / "tracked.txt").write_text("tracked\n")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "Initial")
    lose_fromkey(repo, KEY, "new.nwb")
    if dirt == "stray file":
        (repo / "notes.txt").write_text("not ours\n")
        culprit = "?? notes.txt"
    elif dirt == "deletion":
        (repo / "tracked.txt").unlink()
        culprit = " D tracked.txt"
    else:
        (repo / "nested").mkdir()
        git(repo / "nested", "init", "-q")
        (repo / "nested" / "file").write_text("not ours\n")
        culprit = "?? nested/"
    before = unstaged(repo)
    with pytest.raises(RuntimeError, match="besides files git-annex failed") as excinfo:
        await AsyncDataset(repo).stage_unstaged({"new.nwb"})
    assert culprit in str(excinfo.value)
    assert "new.nwb" not in str(excinfo.value)
    assert unstaged(repo) == before


@pytest.mark.ai_generated
async def test_stage_unstaged_checks_its_own_staging(tmp_path: Path) -> None:
    """
    `git annex add` too gives up silently when `.git/index.lock` is taken, so
    files it could not stage are reported rather than claimed staged.
    """
    repo = tmp_path / "ds"
    init_annex_repo(repo)
    git(repo, "add", ".gitattributes")
    git(repo, "commit", "-q", "-m", "Initial")
    lose_fromkey(repo, KEY, "new.nwb")
    lock = repo / ".git" / "index.lock"
    lock.touch()
    try:
        with pytest.raises(RuntimeError, match=r"(?s)could not stage.*new\.nwb"):
            await AsyncDataset(repo).stage_unstaged({"new.nwb"})
    finally:
        lock.unlink()
    assert unstaged(repo) == ["?? new.nwb"]


async def no_versions(**_kwargs: Any) -> AsyncIterator[Any]:
    return
    yield


@pytest.mark.ai_generated
@pytest.mark.parametrize("stray", [False, True])
async def test_async_assets_stages_what_annex_left(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stray: bool,
) -> None:
    """
    `async_assets()` stages what git-annex failed to stage before committing
    the backup state, and refuses to commit it past other unstaged changes.
    """
    created = datetime(2026, 9, 10, 16, 0, 0, tzinfo=timezone.utc)
    modified = datetime(2026, 9, 10, 19, 22, 44, 983059, tzinfo=timezone.utc)
    ds = AsyncDataset(tmp_path / "ds")
    await ds.ensure_installed("test")
    (ds.pathobj / ".dandi").mkdir()
    (ds.pathobj / ".dandi" / "assets.json").write_text("[]\n")

    async def fake_run_downloader(
        dm: asyncer.Downloader, aia: AsyncIterator[Any]
    ) -> None:
        async for _ in aia:
            pass
        # `fromkey` made the symlink and `addurl` the text file, and both
        # lost their staging
        lose_fromkey(ds.pathobj, KEY, "a.nwb")
        (ds.pathobj / "notes.txt").write_text("text\n")
        if stray:
            (ds.pathobj / "stray.txt").write_text("not from the sync\n")
        dm.annex_paths |= {"a.nwb", "notes.txt"}
        dm.report.registered = 1
        dm.report.downloaded = 1
        dm.last_timestamp = created

    monkeypatch.setattr(asyncer, "run_downloader", fake_run_downloader)
    dandiset = MagicMock()
    dandiset.identifier = "000001"
    dandiset.version_id = "draft"
    dandiset.embargo_status = EmbargoStatus.OPEN
    dandiset.version.modified = modified
    dandiset.aget_versions = no_versions
    manager = Manager(
        config=BackupConfig(backup_root=tmp_path), gh=None, log=plog, token="token"
    )
    head = git(ds.pathobj, "rev-parse", "HEAD")
    if stray:
        with pytest.raises(RuntimeError, match=r"(?s)besides files.*stray\.txt"):
            await asyncer.async_assets(dandiset, ds, manager, MagicMock(), [])
        assert git(ds.pathobj, "rev-parse", "HEAD") == head
        assert not (ds.pathobj / ".dandi" / "assets-state.json").exists()
        return
    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        report = await asyncer.async_assets(dandiset, ds, manager, MagicMock(), [])
    assert report.commits == 1
    assert not await ds.is_dirty()
    tree = git(ds.pathobj, "ls-tree", "-r", "--name-only", "HEAD").split()
    assert {"a.nwb", "notes.txt", ".dandi/assets-state.json"} <= set(tree)
    assert any(
        r.getMessage().startswith("ANNEX-UNSTAGED: 2 files left unstaged")
        for r in caplog.records
    )


@pytest.mark.ai_generated
async def test_commit_reports_what_is_still_dirty(tmp_path: Path) -> None:
    repo = tmp_path / "ds"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "staged.txt").write_text("staged\n")
    (repo / "stray.txt").write_text("stray\n")
    git(repo, "add", "staged.txt")
    ds = AsyncDataset(repo)
    with pytest.raises(
        RuntimeError,
        match=r"(?s)still dirty after committing.*1 dirty path.*stray\.txt",
    ):
        await ds.commit("Commit staged.txt only")
    assert git(repo, "log", "-1", "--format=%s").strip() == "Commit staged.txt only"
