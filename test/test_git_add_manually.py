"""
`git_add_manually()` re-adds the files git-annex's batch processes added.

git-annex stages those files itself, a batch at a time, and if
`.git/index.lock` is taken when it does, the batch is left unstaged though
every file was reported added and the process exits 0 (#72).  001550 lost
files that way while our `git rm`s held the lock, and its commit recorded them
as deleted.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess

import anyio
from obscure import obscure_asset_path
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import git_add_manually
from backups2datalad.logging import log as plog

pytestmark = pytest.mark.anyio

REPLACED = obscure_asset_path("replaced.nwb")
NEW = obscure_asset_path("new.nwb")


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, text=True, capture_output=True
    ).stdout


def status(repo: Path) -> set[str]:
    return set(git(repo, "status", "--porcelain", "-z").split("\0")) - {""}


def key(content: bytes) -> str:
    return f"SHA256E-s{len(content)}--{hashlib.sha256(content).hexdigest()}.nwb"


async def replace_and_add(repo: Path, lose_batch: bool) -> AsyncDataset:
    """
    Replace one annexed file and add another as `Downloader.process_blob()`
    does, taking `.git/index.lock` while `fromkey` stages them if
    ``lose_batch``
    """
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "annex", "init", "-q")
    async with AsyncAnnex(repo) as annex:
        assert await annex.from_key(key(b"old"), REPLACED)
    git(repo, "commit", "-q", "-m", "Initial")
    (repo / REPLACED).unlink()
    lock = repo / ".git" / "index.lock"
    async with AsyncAnnex(repo) as annex:
        assert await annex.from_key(key(b"replacement"), REPLACED)
        assert await annex.from_key(key(b"new"), NEW)
        if lose_batch:
            # Taken when `fromkey` stages its batch, on exit
            lock.touch()
    if lose_batch:
        lock.unlink()
    return AsyncDataset(repo)


@pytest.mark.ai_generated
async def test_git_add_manually_restages_lost_batch(tmp_path: Path) -> None:
    """
    Files of a batch git-annex failed to stage end up staged as it would have
    staged them, and re-adding files it did stage changes nothing
    """
    staged = await replace_and_add(tmp_path / "staged", lose_batch=False)
    assert status(staged.pathobj) == {f"M  {REPLACED}", f"A  {NEW}"}
    index = git(staged.pathobj, "ls-files", "-s", "-z")
    assert await git_add_manually(staged, [REPLACED, NEW], plog) == 0
    assert git(staged.pathobj, "ls-files", "-s", "-z") == index

    lost = await replace_and_add(tmp_path / "lost", lose_batch=True)
    (lost.pathobj / "stray.txt").write_text("stray\n")
    assert status(lost.pathobj) == {f" M {REPLACED}", f"?? {NEW}", "?? stray.txt"}
    assert await git_add_manually(lost, [REPLACED, NEW], plog) == 0
    assert git(lost.pathobj, "ls-files", "-s", "-z") == index
    assert status(lost.pathobj) == {f"M  {REPLACED}", f"A  {NEW}", "?? stray.txt"}


@pytest.mark.ai_generated
async def test_git_add_manually_waits_for_lock(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ds = await replace_and_add(tmp_path / "ds", lose_batch=True)
    lock = ds.pathobj / ".git" / "index.lock"
    lock.touch()

    async def release() -> None:
        await anyio.sleep(0.5)
        lock.unlink()

    async with anyio.create_task_group() as tg:
        tg.start_soon(release)
        assert await git_add_manually(ds, [REPLACED, NEW], plog) == 0
    assert "Retrying stage 2 files" in caplog.text
    assert status(ds.pathobj) == {f"M  {REPLACED}", f"A  {NEW}"}
