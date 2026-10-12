"""
Tests for how a Dandiset sync touches the Git index (000571, #72, #62).

The sync used to `git rm` every new file before adding it: 177k index rewrites
took 15.5 h for 000571, and raced git-annex's own index updates, which do not
retry on a held lock.  Files git-annex thereby failed to stage were left out of
the commit that records the backup as complete, which then failed with an
uninformative "still dirty after committing".
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock

import anyio
from dandi.consts import EmbargoStatus
from obscure import OBSCURE_NAMES
import pytest
from test_util import GitRepo

from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import Downloader, ToDownload
from backups2datalad.blob import BlobBackup
from backups2datalad.config import BackupConfig
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager

pytestmark = pytest.mark.anyio


def init_repo(path: Path) -> GitRepo:
    repo = GitRepo(path)
    path.mkdir(parents=True, exist_ok=True)
    repo.runcmd("init", "-q", check=True)
    return repo


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "existing,remove_calls",
    [
        # New file: nothing to remove, so no `git rm` (and no index rewrite)
        (None, 0),
        # File whose content changed: the old one has to go first
        (b"old content\n", 1),
    ],
)
async def test_process_blob_removes_only_replaced_files(
    tmp_path: Path, existing: bytes | None, remove_calls: int
) -> None:
    path = "sub-01/sub-01_ecephys.nwb"
    if existing is not None:
        (tmp_path / path).parent.mkdir(parents=True)
        (tmp_path / path).write_bytes(existing)
    ds = MagicMock(spec=AsyncDataset)
    ds.pathobj = tmp_path
    ds.remove = AsyncMock()
    annex = MagicMock(spec=AsyncAnnex)
    annex.mkkey = AsyncMock(return_value="SHA256E-s5--0123.nwb")
    annex.from_key = AsyncMock()
    annex.register_url = AsyncMock()
    tracker = MagicMock()
    tracker.register_asset.return_value = "metadata diff"
    manager = Manager(
        config=BackupConfig(backup_root=tmp_path), gh=None, log=plog, token="token"
    )
    dm = Downloader(
        dandiset_id="000001",
        # Embargoed, so that no S3 bucket URL is looked up
        embargoed=True,
        embargo_status=EmbargoStatus.EMBARGOED,
        ds=ds,
        manager=manager,
        tracker=tracker,
        s3client=MagicMock(),
        annex=annex,
    )
    blob = MagicMock(spec=BlobBackup)
    blob.path = path
    blob.asset = MagicMock(
        size=5,
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        base_download_url="https://api.example/assets/1/download/",
    )
    blob.sha256_digest = "0123"
    blob.is_binary.return_value = True
    blob.register_url = AsyncMock()
    blob.log = plog
    sender, receiver = anyio.create_memory_object_stream[ToDownload](1)
    async with receiver:
        await dm.process_blob(blob, sender)
    assert ds.remove.await_count == remove_calls
    if remove_calls:
        ds.remove.assert_awaited_with(path)
    annex.from_key.assert_awaited_once_with("SHA256E-s5--0123.nwb", path)


@pytest.mark.ai_generated
async def test_stage_unstaged_files(tmp_path: Path) -> None:
    """
    Untracked files and unstaged modifications get staged, under whatever
    names; deletions and nested repositories are left for the dirtiness
    check to report.
    """
    repo = init_repo(tmp_path)
    for name in ["modified.txt", "deleted.txt", "renamed.txt"]:
        (tmp_path / name).write_text(f"{name}\n")
    repo.runcmd("add", ".", check=True)
    repo.runcmd("commit", "-q", "-m", "Initial", check=True)
    (tmp_path / "modified.txt").write_text("changed\n")
    (tmp_path / "deleted.txt").unlink()
    repo.runcmd("mv", "renamed.txt", "moved.txt", check=True)
    (tmp_path / "new.txt").write_text("new\n")
    (tmp_path / "dir" / "deeper").mkdir(parents=True)
    (tmp_path / "dir" / "deeper" / "file.txt").write_text("deep\n")
    for name in OBSCURE_NAMES:
        (tmp_path / name).write_text("obscure\n")
    nested = tmp_path / "nested"
    nested.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=nested, check=True)
    (nested / "payload").write_text("not ours\n")

    ds = AsyncDataset(tmp_path)
    staged = await ds.stage_unstaged_files()
    assert sorted(staged) == sorted(
        ["modified.txt", "new.txt", "dir/deeper/file.txt", *OBSCURE_NAMES]
    )
    left = repo.readcmd_z(
        "status", "--porcelain", "-z", "--untracked-files=all"
    )
    assert sorted(left) == sorted(
        [
            " D deleted.txt",
            "?? nested/",
            "M  modified.txt",
            "A  new.txt",
            "A  dir/deeper/file.txt",
            "R  moved.txt",
            "renamed.txt",
            *(f"A  {name}" for name in OBSCURE_NAMES),
        ]
    )
    assert await ds.stage_unstaged_files() == []


@pytest.mark.ai_generated
async def test_commit_reports_what_is_still_dirty(tmp_path: Path) -> None:
    repo = init_repo(tmp_path)
    (tmp_path / "staged.txt").write_text("staged\n")
    (tmp_path / "stray.txt").write_text("stray\n")
    repo.runcmd("add", "staged.txt", check=True)
    ds = AsyncDataset(tmp_path)
    with pytest.raises(
        RuntimeError, match=r"(?s)still dirty after committing.*1 dirty path.*stray\.txt"
    ):
        await ds.commit("Commit staged.txt only")
    assert repo.readcmd("log", "-1", "--format=%s") == "Commit staged.txt only"
