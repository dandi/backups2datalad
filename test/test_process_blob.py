"""
`Downloader.process_blob()` runs `git rm` only for a file it replaces.

It used to run one before adding *every* asset, new ones included, each
holding `.git/index.lock` while reading the whole index: 000571's 177k new
files took 15.5 h of that (#62), and git-annex's own index updates, which are
not retried, left files unstaged while we held the lock, so the sync failed
"still dirty after committing" (#72).
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock

import anyio
from dandi.consts import EmbargoStatus
from obscure import obscure_asset_path
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import Downloader, ToDownload
from backups2datalad.blob import BlobBackup
from backups2datalad.config import BackupConfig
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager
from backups2datalad.util import AssetTracker

pytestmark = pytest.mark.anyio


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, text=True, capture_output=True
    ).stdout


@pytest.mark.ai_generated
@pytest.mark.parametrize("case", ["new", "modified", "unchanged"])
async def test_process_blob_removes_only_replaced_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """
    A new asset is registered without a `git rm` before it, a modified one
    with one, and an unchanged one is left alone -- in a real git-annex
    repository, so that the resulting index is checked too.
    """
    repo = tmp_path / "ds"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "annex", "init", "-q")
    (repo / ".gitattributes").write_text("* annex.largefiles=anything\n")
    path = obscure_asset_path("data.nwb")
    content = b"new content\n"
    if case != "new":
        (repo / path).parent.mkdir(parents=True)
        (repo / path).write_bytes(content if case == "unchanged" else b"old content\n")
    git(repo, "annex", "add", "-q", "--", ".")
    git(repo, "commit", "-q", "-m", "Initial")

    ds = AsyncDataset(repo)
    real_remove = ds.remove
    removed: list[str] = []

    async def spy_remove(p: str) -> None:
        removed.append(p)
        await real_remove(p)

    monkeypatch.setattr(ds, "remove", spy_remove)
    tracker = MagicMock(spec=AssetTracker)
    tracker.register_asset.return_value = "metadata diff"
    blob = MagicMock(spec=BlobBackup)
    blob.path = path
    blob.asset = MagicMock(
        path=path,
        size=len(content),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        base_download_url="https://api.example/assets/1/download/",
    )
    blob.sha256_digest = hashlib.sha256(content).hexdigest()
    blob.is_binary.return_value = True
    blob.register_url = AsyncMock()
    blob.log = plog
    manager = Manager(
        config=BackupConfig(backup_root=tmp_path), gh=None, log=plog, token="token"
    )
    async with AsyncAnnex(repo) as annex:
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
        sender, receiver = anyio.create_memory_object_stream[ToDownload](1)
        async with receiver:
            await dm.process_blob(blob, sender)
    # `fromkey` stages its files when it exits, i.e. by here.
    assert removed == ([path] if case == "modified" else [])
    status = git(repo, "status", "--porcelain", "-z")
    assert (
        status
        == {"new": f"A  {path}\0", "modified": f"M  {path}\0", "unchanged": ""}[case]
    )
    if case != "unchanged":
        assert (
            (repo / path)
            .readlink()
            .name.endswith(hashlib.sha256(content).hexdigest() + ".nwb")
        )
