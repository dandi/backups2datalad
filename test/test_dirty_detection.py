"""
Tests for the checks that keep a mirror left dirty by an interrupted run from
being skipped -- and therefore never reported -- on every subsequent run.

`set_assets_state()` writes and stages the new timestamp well before the commit
that seals it, so a run cut short in between leaves a working-tree
`.dandi/assets-state.json` claiming a backup that was never committed.  Gating
on that copy made `update_dandiset()` take the "not modified since last backup"
branch and skip `sync_dataset()`, which is where the only dirtiness check used
to live.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any, NoReturn, cast
from unittest.mock import MagicMock

from dandi.consts import EmbargoStatus
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset
from backups2datalad.adataset import AssetsState, AsyncDataset
from backups2datalad.config import BackupConfig
from backups2datalad.datasetter import DandiDatasetter

pytestmark = pytest.mark.anyio

#: What the server says about the Dandiset
MODIFIED = datetime(2026, 9, 10, 19, 22, 44, 983059, tzinfo=timezone.utc)

#: What the last *committed* backup recorded
BACKED_UP = datetime(2026, 9, 10, 16, 59, 42, 138257, tzinfo=timezone.utc)


class SyncCalled(Exception):
    """Raised in place of `sync_dataset()` to show the gate let us through."""


class Tagging(Exception):
    """Raised in place of `tag_releases()`, i.e. past the sync decision."""


def git(path: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Tester",
            "-c",
            "user.email=tester@example.com",
            *args,
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )


def write_state(path: Path, timestamp: datetime) -> None:
    statefile = path / AssetsState.PATH
    statefile.parent.mkdir(parents=True, exist_ok=True)
    statefile.write_text(
        AssetsState(timestamp=timestamp).model_dump_json(indent=4) + "\n"
    )


def make_mirror(path: Path, timestamp: datetime = BACKED_UP) -> AsyncDataset:
    """
    A committed, clean mirror whose recorded backup state is ``timestamp``.
    """
    path.mkdir(parents=True, exist_ok=True)
    git(path, "init", "-q", "-b", "draft")
    (path / "dandiset.yaml").write_text("identifier: DANDI:000571\n")
    (path / "sub-c02").mkdir()
    (path / "sub-c02" / "events.tsv").write_text("onset\tduration\n0\t1\n")
    write_state(path, timestamp)
    git(path, "add", "-A")
    git(path, "commit", "-qm", "[backups2datalad] initial")
    return AsyncDataset(path)


def mock_dandiset(
    modified: datetime = MODIFIED,
    embargo_status: EmbargoStatus = EmbargoStatus.OPEN,
) -> RemoteDandiset:
    d: Any = MagicMock()
    d.identifier = "000571"
    d.embargo_status = embargo_status
    d.version.modified = modified
    d.__str__.return_value = "Dandiset 000571/draft"
    return cast(RemoteDandiset, d)


def make_datasetter(backup_root: Path) -> DandiDatasetter:
    return DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, MagicMock()),
        config=BackupConfig(backup_root=backup_root, quiescent_period=0.0),
    )


@pytest.fixture
def no_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Make the two things `update_dandiset()` can do after the gate blow up in
    distinguishable ways, so that a test can tell "the Dandiset was synced"
    apart from "the Dandiset was skipped as up to date" without needing a
    server to sync it against.
    """

    async def sync_dataset(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise SyncCalled()

    async def tag_releases(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise Tagging()

    monkeypatch.setattr(DandiDatasetter, "sync_dataset", sync_dataset)
    monkeypatch.setattr(DandiDatasetter, "tag_releases", tag_releases)


# --- AsyncDataset.get_committed_assets_state() ------------------------------


@pytest.mark.ai_generated
async def test_committed_state_ignores_uncommitted_bump(tmp_path: Path) -> None:
    """
    The working-tree copy follows an interrupted run; HEAD's does not.
    """
    ds = make_mirror(tmp_path / "000571")
    write_state(ds.pathobj, MODIFIED)
    git(ds.pathobj, "add", str(AssetsState.PATH))
    worktree = ds.get_assets_state()
    assert worktree is not None and worktree.timestamp == MODIFIED
    committed = await ds.get_committed_assets_state()
    assert committed is not None and committed.timestamp == BACKED_UP


@pytest.mark.ai_generated
async def test_committed_state_without_state_file(tmp_path: Path) -> None:
    path = tmp_path / "000571"
    path.mkdir()
    git(path, "init", "-q", "-b", "draft")
    (path / "dandiset.yaml").write_text("identifier: DANDI:000571\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "no state here")
    assert await AsyncDataset(path).get_committed_assets_state() is None


@pytest.mark.ai_generated
async def test_committed_state_without_any_commits(tmp_path: Path) -> None:
    path = tmp_path / "000571"
    path.mkdir()
    git(path, "init", "-q", "-b", "draft")
    write_state(path, BACKED_UP)
    assert await AsyncDataset(path).get_committed_assets_state() is None


# --- AsyncDataset.has_changes(cached=True) ----------------------------------


@pytest.mark.ai_generated
async def test_staged_check_on_clean_mirror(tmp_path: Path) -> None:
    ds = make_mirror(tmp_path / "000571")
    assert await ds.has_changes(cached=True) is False
    assert await ds.is_dirty() is False


@pytest.mark.ai_generated
async def test_staged_check_sees_pruned_assets(tmp_path: Path) -> None:
    """What `prune_deleted()` leaves behind when the commit never happens."""
    ds = make_mirror(tmp_path / "000571")
    git(ds.pathobj, "rm", "-q", "-f", "sub-c02/events.tsv")
    assert await ds.has_changes(cached=True) is True


@pytest.mark.ai_generated
async def test_staged_check_ignores_unstaged_and_untracked(tmp_path: Path) -> None:
    """
    The cheap check deliberately does not stat the working tree; the full
    `is_dirty()` on the sync path is what covers this.
    """
    ds = make_mirror(tmp_path / "000571")
    (ds.pathobj / "dandiset.yaml").write_text("identifier: DANDI:000571 (edited)\n")
    (ds.pathobj / "stray.txt").write_text("not ours\n")
    assert await ds.has_changes(cached=True) is False
    assert await ds.is_dirty() is True


# --- update_dandiset() ------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.usefixtures("no_sync")
async def test_interrupted_run_is_reported_not_skipped(tmp_path: Path) -> None:
    """
    The regression: a mirror left mid-sync, with the state file already
    bumped to the server's timestamp, used to be skipped without a word.
    """
    ds = make_mirror(tmp_path / "000571")
    git(ds.pathobj, "rm", "-q", "-f", "sub-c02/events.tsv")
    write_state(ds.pathobj, MODIFIED)
    git(ds.pathobj, "add", str(AssetsState.PATH))
    di = make_datasetter(tmp_path)
    with pytest.raises(RuntimeError) as excinfo:
        await di.update_dandiset(mock_dandiset(), ds)
    msg = str(excinfo.value)
    assert "Dirty Dandiset 000571/draft" in msg
    # The message has to say what needs cleaning up, since the operator sees
    # only the log.
    assert "dirty path" in msg
    assert "sub-c02/events.tsv" in msg


@pytest.mark.ai_generated
@pytest.mark.usefixtures("no_sync")
async def test_unstaged_state_bump_reaches_the_sync_path(tmp_path: Path) -> None:
    """
    A run killed between writing the state file and staging it leaves nothing
    in the index, so the cheap check passes; gating on HEAD still routes the
    Dandiset to `sync_dataset()`, whose `is_dirty()` catches it.
    """
    ds = make_mirror(tmp_path / "000571")
    write_state(ds.pathobj, MODIFIED)
    assert await ds.has_changes(cached=True) is False
    assert await ds.is_dirty() is True
    di = make_datasetter(tmp_path)
    with pytest.raises(SyncCalled):
        await di.update_dandiset(mock_dandiset(), ds)


@pytest.mark.ai_generated
@pytest.mark.usefixtures("no_sync")
async def test_clean_up_to_date_mirror_is_still_skipped(tmp_path: Path) -> None:
    """The fast path is untouched for a mirror that is genuinely current."""
    ds = make_mirror(tmp_path / "000571", timestamp=MODIFIED)
    di = make_datasetter(tmp_path)
    # Getting as far as tagging means the sync was skipped.
    with pytest.raises(Tagging):
        await di.update_dandiset(mock_dandiset(), ds)


@pytest.mark.ai_generated
@pytest.mark.usefixtures("no_sync")
async def test_clean_stale_mirror_is_synced(tmp_path: Path) -> None:
    ds = make_mirror(tmp_path / "000571", timestamp=BACKED_UP)
    di = make_datasetter(tmp_path)
    with pytest.raises(SyncCalled):
        await di.update_dandiset(mock_dandiset(), ds)
