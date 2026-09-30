"""Tests for fetch_stable_assets() and its use in sync_dataset() (dandi-archive#2943)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock

from dandi.consts import EmbargoStatus
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteAsset, RemoteDandiset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.asyncer import fetch_stable_assets
from backups2datalad.config import BackupConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.manager import Manager
from backups2datalad.syncer import Syncer

pytestmark = pytest.mark.anyio

T0 = datetime(2026, 9, 23, 12, 0, 0, tzinfo=timezone.utc)


def make_asset(
    path: str, created: datetime, identifier: str | None = None
) -> RemoteAsset:
    return cast(
        RemoteAsset,
        SimpleNamespace(
            path=path,
            created=created,
            identifier=f"id-{path}" if identifier is None else identifier,
        ),
    )


def make_dandiset(
    assets: list[RemoteAsset],
    before: datetime,
    after: datetime,
    initial_modified: datetime | None = None,
) -> RemoteDandiset:
    """``before``/``after`` are what successive ``get_dandiset()`` calls see;
    ``initial_modified`` is ``dandiset.version.modified`` as of the run's start
    (defaults to ``before``)."""
    fetched = iter([before, after])

    async def aget_assets() -> object:
        for a in assets:
            yield a

    async def get_dandiset(_identifier: str) -> SimpleNamespace:
        return SimpleNamespace(version=SimpleNamespace(modified=next(fetched)))

    return cast(
        RemoteDandiset,
        SimpleNamespace(
            identifier="000001",
            version_id="draft",
            version=SimpleNamespace(
                modified=before if initial_modified is None else initial_modified
            ),
            aget_assets=aget_assets,
            aclient=SimpleNamespace(get_dandiset=get_dandiset),
        ),
    )


# --- fetch_stable_assets() ------------------------------------------------


@pytest.mark.ai_generated
async def test_fetch_stable_assets_keeps_newest_of_assets_sharing_a_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two distinct assets at one path, as left behind on the server by
    # concurrent uploads of the same file; only one can be mirrored.
    old = make_asset("code/submit.sh", T0, identifier="old")
    other = make_asset("b", T0 + timedelta(seconds=1))
    new = make_asset("code/submit.sh", T0 + timedelta(seconds=2), identifier="new")
    dandiset = make_dandiset([old, other, new], T0, T0)
    assert await fetch_stable_assets(dandiset) == [other, new]
    assert "multiple assets at path code/submit.sh" in caplog.text


@pytest.mark.ai_generated
async def test_fetch_stable_assets_returns_none_when_asset_listed_twice() -> None:
    # The same asset on two pages: pagination was inconsistent, so something
    # else may be missing from the listing.
    a = make_asset("a", T0)
    b = make_asset("b", T0 + timedelta(seconds=1))
    dandiset = make_dandiset([a, b, b], T0, T0)
    assert await fetch_stable_assets(dandiset) is None


@pytest.mark.ai_generated
async def test_fetch_stable_assets_returns_all_assets_when_unchanged() -> None:
    assets = [make_asset("a", T0), make_asset("b", T0 + timedelta(seconds=1))]
    dandiset = make_dandiset(assets, T0, T0)
    assert await fetch_stable_assets(dandiset) == assets


@pytest.mark.ai_generated
async def test_fetch_stable_assets_returns_none_when_dandiset_changed_mid_fetch() -> (
    None
):
    assets = [make_asset("a", T0)]
    dandiset = make_dandiset(assets, T0, T0 + timedelta(seconds=5))
    assert await fetch_stable_assets(dandiset) is None


@pytest.mark.ai_generated
async def test_fetch_stable_assets_ignores_change_before_listing_began() -> None:
    """A change between the run's start and the listing does not invalidate
    the listing: only the timestamps bracketing the fetch are compared."""
    assets = [make_asset("a", T0)]
    later = T0 + timedelta(seconds=3)
    dandiset = make_dandiset(assets, later, later, initial_modified=T0)
    assert await fetch_stable_assets(dandiset) == assets


@pytest.mark.ai_generated
async def test_fetch_stable_assets_asserts_non_decreasing_creation_order() -> None:
    assets = [make_asset("a", T0 + timedelta(seconds=5)), make_asset("b", T0)]
    dandiset = make_dandiset(assets, T0, T0)
    with pytest.raises(AssertionError, match="returned after an asset created at"):
        await fetch_stable_assets(dandiset)


@pytest.mark.ai_generated
async def test_fetch_stable_assets_skips_recheck_for_published_versions() -> None:
    """Published versions are immutable; no re-check needed (or performed)."""
    assets = [make_asset("a", T0)]

    async def aget_assets() -> object:
        for a in assets:
            yield a

    async def boom(_identifier: str) -> SimpleNamespace:
        raise AssertionError("must not re-check modified for a published version")

    dandiset = cast(
        RemoteDandiset,
        SimpleNamespace(
            identifier="000001",
            version_id="0.1.0",
            version=SimpleNamespace(modified=T0),
            aget_assets=aget_assets,
            aclient=SimpleNamespace(get_dandiset=boom),
        ),
    )
    assert await fetch_stable_assets(dandiset) == assets


# --- DandiDatasetter.sync_dataset() -----------------------------------------


def git(path: Path, *args: str) -> None:
    # Identity comes from conftest's autouse `tmp_home`
    subprocess.run(["git", *args], cwd=path, check=True, capture_output=True)


def make_clean_dataset(tmp_path: Path) -> AsyncDataset:
    ds_path = tmp_path / "000001"
    ds_path.mkdir()
    git(ds_path, "init", "-q", "-b", "draft")
    git(ds_path, "commit", "-q", "--allow-empty", "-m", "initial")
    return AsyncDataset(ds_path)


def make_datasetter(tmp_path: Path) -> DandiDatasetter:
    config = BackupConfig(backup_root=tmp_path, quiescent_period=0.0)
    di = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, MagicMock()), config=config
    )
    di._manager = Manager(config=config, gh=None, log=MagicMock(), token="dummy")
    return di


@pytest.mark.ai_generated
async def test_sync_dataset_skips_asset_sync_when_listing_is_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale listing must skip sync_assets/prune_deleted/dump_asset_metadata,
    but not the listing-independent embargo/metadata updates."""
    ds = make_clean_dataset(tmp_path)

    async def fake_fetch_stable_assets(_dandiset: object) -> None:
        return None

    monkeypatch.setattr(
        "backups2datalad.datasetter.fetch_stable_assets", fake_fetch_stable_assets
    )
    monkeypatch.setattr(
        "backups2datalad.datasetter.update_dandiset_metadata", AsyncMock()
    )

    async def explode(*_a: object, **_kw: object) -> None:
        raise AssertionError("asset-sync step must not run on a stale listing")

    monkeypatch.setattr(Syncer, "sync_assets", explode)
    monkeypatch.setattr(Syncer, "prune_deleted", explode)
    monkeypatch.setattr(Syncer, "dump_asset_metadata", explode)

    di = make_datasetter(tmp_path)
    dandiset = cast(RemoteDandiset, SimpleNamespace(embargo_status=EmbargoStatus.OPEN))
    changed = await di.sync_dataset(dandiset, ds, di.manager)
    assert changed is False
    assert not await ds.is_dirty()


@pytest.mark.ai_generated
async def test_sync_dataset_syncs_assets_when_listing_is_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sanity check: a stable listing does reach the asset-sync steps."""
    ds = make_clean_dataset(tmp_path)
    assets: list[RemoteAsset] = []

    async def fake_fetch_stable_assets(_dandiset: object) -> list[RemoteAsset]:
        return assets

    monkeypatch.setattr(
        "backups2datalad.datasetter.fetch_stable_assets", fake_fetch_stable_assets
    )
    monkeypatch.setattr(
        "backups2datalad.datasetter.update_dandiset_metadata", AsyncMock()
    )

    calls: list[str] = []

    async def record(name: str, *_a: object, **_kw: object) -> None:
        calls.append(name)

    monkeypatch.setattr(Syncer, "sync_assets", lambda self, a: record("sync_assets", a))
    monkeypatch.setattr(Syncer, "prune_deleted", lambda self: record("prune_deleted"))
    monkeypatch.setattr(
        Syncer, "dump_asset_metadata", lambda self: record("dump_asset_metadata")
    )

    di = make_datasetter(tmp_path)
    dandiset = cast(RemoteDandiset, SimpleNamespace(embargo_status=EmbargoStatus.OPEN))
    await di.sync_dataset(dandiset, ds, di.manager)
    assert calls == ["sync_assets", "prune_deleted", "dump_asset_metadata"]
