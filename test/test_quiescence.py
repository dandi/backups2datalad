"""
Tests for the quiescent-period gate that keeps `update_dandiset()` from
backing up a Dandiset that is still being changed on the server.

See <https://github.com/dandi/backups2datalad/issues/119>.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NoReturn, cast
from unittest.mock import MagicMock

from dandi.consts import EmbargoStatus
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset
from backups2datalad.config import BackupConfig
from backups2datalad.consts import DEFAULT_QUIESCENT_PERIOD
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.util import quiescence_wait

pytestmark = pytest.mark.anyio

NOW = datetime(2026, 9, 10, 17, 15, 12, tzinfo=timezone.utc)


class NotSkipped(Exception):
    """Raised in place of `init_dataset()` to show the gate let us through."""


def mock_dandiset(
    modified: datetime, embargo_status: EmbargoStatus = EmbargoStatus.OPEN
) -> RemoteDandiset:
    d = MagicMock()
    d.identifier = "000571"
    d.embargo_status = embargo_status
    d.version.modified = modified
    return cast(RemoteDandiset, d)


def make_datasetter(backup_root: Path, quiescent_period: float) -> DandiDatasetter:
    return DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, MagicMock()),
        config=BackupConfig(backup_root=backup_root, quiescent_period=quiescent_period),
    )


@pytest.fixture
def uninitializable(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Make `init_dataset()` blow up, so that a test can tell "the Dandiset was
    skipped" apart from "the Dandiset went on to be backed up" without needing
    a server to back it up from.
    """

    async def init_dataset(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise NotSkipped()

    monkeypatch.setattr(DandiDatasetter, "init_dataset", init_dataset)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "modified,period,expected",
    [
        # Nothing has elapsed yet: the full period remains.
        (NOW, 30.0, 30.0),
        (NOW - timedelta(seconds=10), 30.0, 20.0),
        # Exactly at the boundary: quiescent (a zero wait is not > 0).
        (NOW - timedelta(seconds=30), 30.0, 0.0),
        (NOW - timedelta(seconds=45), 30.0, -15.0),
        (NOW - timedelta(days=400), 30.0, -34559970.0),
        # A "modified" in the future (local clock lagging the server's) errs
        # towards waiting longer.
        (NOW + timedelta(seconds=10), 30.0, 40.0),
        # A zero period is never a wait, no matter how fresh the timestamp.
        (NOW, 0.0, 0.0),
    ],
)
def test_quiescence_wait(modified: datetime, period: float, expected: float) -> None:
    assert quiescence_wait(modified, period, now=NOW) == pytest.approx(expected)


@pytest.mark.ai_generated
def test_quiescence_wait_defaults_to_local_clock() -> None:
    # Ten seconds into a thirty-second period, ~twenty seconds remain.
    modified = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert quiescence_wait(modified, 30.0) == pytest.approx(20.0, abs=5.0)


@pytest.mark.ai_generated
async def test_skip_recently_modified_dandiset(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A Dandiset touched a moment ago is left alone entirely."""
    di = make_datasetter(tmp_path, quiescent_period=30.0)
    d = mock_dandiset(datetime.now(timezone.utc) - timedelta(seconds=5))
    assert await di.update_dandiset(d) is False
    # Not even a local dataset should have been brought into being.
    assert not (tmp_path / "dandisets" / "000571").exists()
    assert any(
        "was modified" in r.message and "not syncing" in r.message
        for r in caplog.records
    )


@pytest.mark.ai_generated
@pytest.mark.usefixtures("uninitializable")
async def test_settled_dandiset_is_not_skipped(tmp_path: Path) -> None:
    di = make_datasetter(tmp_path, quiescent_period=30.0)
    d = mock_dandiset(datetime.now(timezone.utc) - timedelta(seconds=45))
    with pytest.raises(NotSkipped):
        await di.update_dandiset(d)


@pytest.mark.ai_generated
@pytest.mark.usefixtures("uninitializable")
async def test_zero_quiescent_period_disables_gate(tmp_path: Path) -> None:
    """A period of 0 turns the gate off, even for a future timestamp."""
    di = make_datasetter(tmp_path, quiescent_period=0.0)
    d = mock_dandiset(datetime.now(timezone.utc) + timedelta(seconds=10))
    with pytest.raises(NotSkipped):
        await di.update_dandiset(d)


@pytest.mark.ai_generated
async def test_unembargoing_checked_before_quiescence(tmp_path: Path) -> None:
    """
    An unembargoing Dandiset is skipped for that reason, with the more
    specific message, rather than for being freshly modified.
    """
    di = make_datasetter(tmp_path, quiescent_period=30.0)
    d = mock_dandiset(
        datetime.now(timezone.utc), embargo_status=EmbargoStatus.UNEMBARGOING
    )
    assert await di.update_dandiset(d) is False


@pytest.mark.ai_generated
def test_quiescent_period_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # The suite-wide `no_quiescent_period` fixture patches the constant to 0;
    # put the shipped value back to check that it is what a config picks up.
    monkeypatch.setattr(
        "backups2datalad.config.DEFAULT_QUIESCENT_PERIOD",
        DEFAULT_QUIESCENT_PERIOD,
    )
    assert BackupConfig().quiescent_period == 30.0


@pytest.mark.ai_generated
def test_quiescent_period_must_be_nonnegative() -> None:
    with pytest.raises(ValueError):
        BackupConfig(quiescent_period=-1)


@pytest.mark.ai_generated
def test_quiescent_period_from_yaml(tmp_path: Path) -> None:
    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text("quiescent_period: 90\n")
    assert BackupConfig.load_yaml(cfgfile).quiescent_period == 90.0
