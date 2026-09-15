from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging
from pathlib import Path
from shutil import rmtree
import subprocess
from time import sleep
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from conftest import Archive, SampleDandiset
from datalad.api import Dataset
import numpy as np
import pytest
import requests
from test_aioutil import FakeClock, make_gate
from test_util import GitRepo, zarr_format_of

from backups2datalad.adandi import RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset, DatasetStats
from backups2datalad.aioutil import GitHubRateLimited, arequest
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager
from backups2datalad.zarr import ZarrLink, sync_zarr

log = logging.getLogger("test_backups2datalad.test_zarr")

pytestmark = pytest.mark.anyio


async def test_sync_zarr(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    assert isinstance(asset, RemoteZarrAsset)
    checksum = asset.get_digest_value()
    config = BackupConfig(
        s3bucket=docker_archive.s3bucket,
        s3endpoint=docker_archive.s3endpoint,
        content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
        zarrs=ResourceConfig(path="zarrs"),
    )
    await sync_zarr(
        asset,
        checksum,
        tmp_path,
        Manager(config=config, gh=None, log=plog, token=new_dandiset.client.token),
    )
    local_checksum = await AsyncDataset(tmp_path).compute_zarr_checksum()
    new_dandiset.check_zarr_backup(
        Dataset(tmp_path),
        new_dandiset.zarr_assets["sample.zarr"],
        checksum,
        local_checksum,
    )


async def test_backup_zarr(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.add_text("file.txt", "This is test text.\n")
    await new_dandiset.upload()
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    assert isinstance(asset, RemoteZarrAsset)

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance=docker_archive.instance_id,
            s3bucket=docker_archive.s3bucket,
            s3endpoint=docker_archive.s3endpoint,
            content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )
    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    ds = Dataset(tmp_path / "ds" / dandiset_id)
    await new_dandiset.check_backup(ds, tmp_path / "zarrs")

    zarrgit = GitRepo(tmp_path / "zarrs" / asset.zarr)
    assert zarrgit.get_commit_count() == 3

    gitrepo = GitRepo(ds.pathobj)
    assert gitrepo.get_commit_count() == 3
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 2 files added"

    # On-disk size of the Zarr store differs between zarr-python's V2 layout
    # (.zarray / .zgroup, default in zarr-python 2.x) and V3 layout
    # (zarr.json, default in 3.x); see dandi/dandi-cli#1858 for the same
    # accommodation upstream.
    expected_zarr_size = {"2": 1535, "3": 3954}[
        zarr_format_of(new_dandiset.zarr_assets["sample.zarr"])
    ]
    assert await AsyncDataset(ds.pathobj).get_stats(config=di.config) == DatasetStats(
        files=6, size=expected_zarr_size
    )

    log.info("test_backup_zarr: Syncing unmodified Zarr dandiset")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(ds, tmp_path / "zarrs")

    c = gitrepo.get_commit_count()
    if c == 4:
        # dandiset.yaml was updated again during the second backup because the
        # server took a while to incorporate the Zarr size data
        bump = 1
        assert (
            gitrepo.get_commit_subject("HEAD")
            == "[backups2datalad] Only some metadata updates"
        )
    else:
        bump = 0
        assert c == 3
        assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 2 files added"
    assert zarrgit.get_commit_count() == 3

    new_dandiset.add_zarr("sample.zarr", np.eye(5))
    await new_dandiset.upload()
    log.info("test_backup_zarr: Syncing modified Zarr dandiset")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(ds, tmp_path / "zarrs")

    assert gitrepo.get_commit_count() == 4 + bump
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file updated"
    assert zarrgit.get_commit_count() == 4


@pytest.mark.skip(
    reason=(
        "Checksum mismatch caused by https://github.com/minio/minio/issues/20167"
        " results in infinite upload loop"
    )
)
async def test_backup_zarr_entry_conflicts(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr_path.mkdir()
    (zarr_path / "changed01").mkdir()
    (zarr_path / "changed01" / "file.txt").write_text("This is test text.\n")
    (zarr_path / "changed02").write_text("This is also test text.\n")
    new_dandiset.zarr_assets["sample.zarr"] = {
        "changed01/file.txt": b"This is test text.\n",
        "changed02": b"This is also test text.\n",
    }
    await new_dandiset.upload()

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance=docker_archive.instance_id,
            s3bucket=docker_archive.s3bucket,
            s3endpoint=docker_archive.s3endpoint,
            content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )
    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr_entry_conflicts: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(
        Dataset(tmp_path / "ds" / dandiset_id), tmp_path / "zarrs"
    )

    rmtree(zarr_path)
    zarr_path.mkdir()
    (zarr_path / "changed01").write_text("This is now a file.\n")
    (zarr_path / "changed02").mkdir()
    (zarr_path / "changed02" / "file.txt").write_text(
        "The parent is now a directory.\n"
    )
    new_dandiset.zarr_assets["sample.zarr"] = {
        "changed01": b"This is now a file.\n",
        "changed02/file.txt": b"This is now a directory.\n",
    }
    await new_dandiset.upload()

    log.info("test_backup_zarr_entry_conflicts: Syncing modified Zarr dandiset")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(
        Dataset(tmp_path / "ds" / dandiset_id), tmp_path / "zarrs"
    )


async def test_backup_zarr_delete_zarr(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance=docker_archive.instance_id,
            s3bucket=docker_archive.s3bucket,
            s3endpoint=docker_archive.s3endpoint,
            content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )

    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr_delete_zarr: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    assert isinstance(asset, RemoteZarrAsset)
    await new_dandiset.client.delete(asset.api_path)
    new_dandiset.rmasset("sample.zarr")

    log.info("test_backup_zarr_delete_zarr: Syncing Zarr dandiset after deleting Zarr")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(Dataset(tmp_path / "ds" / dandiset_id))
    gitrepo = GitRepo(tmp_path / "ds" / dandiset_id)
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file deleted"


async def test_backup_zarr_pathological(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()

    client = new_dandiset.client
    dandiset_id = new_dandiset.dandiset_id
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    assert isinstance(asset, RemoteZarrAsset)
    sample_zarr_id = asset.zarr

    await client.post(
        f"{new_dandiset.dandiset.version_api_path}assets/",
        json={"metadata": {"path": "link.zarr"}, "zarr_id": sample_zarr_id},
    )
    new_dandiset.zarr_assets["link.zarr"] = new_dandiset.zarr_assets["sample.zarr"]

    r = await client.post(
        "/zarr/", json={"name": "empty.zarr", "dandiset": dandiset_id}
    )
    empty_zarr_id = r["zarr_id"]
    await client.post(
        f"{new_dandiset.dandiset.version_api_path}assets/",
        json={"metadata": {"path": "empty.zarr"}, "zarr_id": empty_zarr_id},
    )
    await arequest(client.session, "POST", f"/zarr/{empty_zarr_id}/finalize/")
    while True:
        sleep(2)
        r = await client.get(f"/zarr/{empty_zarr_id}/")
        if r["status"] == "Complete":
            break
    new_dandiset.zarr_assets["empty.zarr"] = {}

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance=docker_archive.instance_id,
            s3bucket=docker_archive.s3bucket,
            s3endpoint=docker_archive.s3endpoint,
            content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )

    log.info("test_backup_zarr_pathological: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])
    await new_dandiset.check_backup(
        Dataset(tmp_path / "ds" / dandiset_id), tmp_path / "zarrs"
    )


# ---- GitHub sibling creation & convergence (no docker, no network) -------

RATE_LIMIT_MSG = (
    "You have exceeded a secondary rate limit and have been temporarily"
    " blocked from content creation. Please retry your request again later."
)


async def _make_dataset(path: Path) -> AsyncDataset:
    ds = AsyncDataset(path)
    await ds.ensure_installed("test dataset", backend="MD5E", cfg_proc=None)
    return ds


def _make_bare(path: Path) -> Path:
    subprocess.run(["git", "init", "--bare", "--quiet", str(path)], check=True)
    return path


def _git(ds: AsyncDataset, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ds.path, check=True, capture_output=True, text=True
    ).stdout.strip()


def _add_github_remote(ds: AsyncDataset, bare: Path) -> None:
    """Configure ``bare`` as the ``github`` sibling the way DataLad would"""
    _git(ds, "remote", "add", "github", str(bare))
    _git(ds, "config", "remote.github.annex-ignore", "true")


def _fake_create_sibling(
    monkeypatch: pytest.MonkeyPatch,
    ds: AsyncDataset,
    bare: Path,
    outcomes: list[str | Exception],
) -> list[dict[str, Any]]:
    """
    Replace ``ds.ds.create_sibling_github`` with a fake that consumes one
    outcome per call: ``"ok"`` configures ``bare`` as the sibling and returns
    an ok record, another string becomes DataLad's 403 error record (``("%s"
    format, message)`` tuple), and an exception is raised.  Returns the list
    of keyword arguments of each call.
    """
    calls: list[dict[str, Any]] = []

    def fake(**kwargs: Any) -> list[dict[str, Any]]:
        calls.append(kwargs)
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "ok":
            _add_github_remote(ds, bare)
            return [
                {
                    "status": "ok",
                    "message": (
                        "sibling repository '%s' created at %s",
                        "github",
                        str(bare),
                    ),
                }
            ]
        return [{"status": "error", "message": ("unauthorized: %s", outcome)}]

    monkeypatch.setattr(ds.ds, "create_sibling_github", fake)
    return calls


def _http_error(status: int, headers: dict[str, str], body: str) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    resp.headers.update(headers)
    resp._content = body.encode()
    return requests.HTTPError(response=resp)


@pytest.mark.ai_generated
async def test_create_github_sibling_retries_rate_limited_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A 403 secondary-rate-limit that DataLad reports as an error record is
    retried after GitHub's documented fallback wait, the description is
    passed through, and our sibling config is set afterwards.
    """
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    calls = _fake_create_sibling(monkeypatch, ds, bare, [RATE_LIMIT_MSG, "ok"])
    clock = FakeClock()
    gate = make_gate(clock)
    created = await ds.create_github_sibling(
        owner="org", name="zarr1", backup_remote=None, description="d", gate=gate
    )
    assert created
    assert len(calls) == 2
    assert calls[0]["description"] == "d"
    assert calls[0]["result_renderer"] == "disabled"
    assert clock.slept == [60.0]
    assert gate.consecutive == 0
    assert await ds.has_github_remote()
    assert await ds.get_repo_config("remote.github.pushurl") == "git@github.com:org/zarr1"
    assert await ds.get_repo_config("branch.draft.remote") == "github"
    assert await ds.get_repo_config("branch.draft.merge") == "refs/heads/draft"


@pytest.mark.ai_generated
async def test_create_github_sibling_429_honours_retry_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 429 escapes DataLad as `requests.HTTPError`; its Retry-After is used."""
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    err = _http_error(429, {"Retry-After": "7"}, '{"message": "slow down"}')
    calls = _fake_create_sibling(monkeypatch, ds, bare, [err, "ok"])
    clock = FakeClock()
    await ds.create_github_sibling(
        owner="org", name="zarr1", backup_remote=None, gate=make_gate(clock)
    )
    assert len(calls) == 2
    assert clock.slept == [7.0]


@pytest.mark.ai_generated
async def test_create_github_sibling_other_error_not_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    calls = _fake_create_sibling(monkeypatch, ds, bare, ["Bad credentials"])
    clock = FakeClock()
    with pytest.raises(RuntimeError, match="Bad credentials"):
        await ds.create_github_sibling(
            owner="org", name="zarr1", backup_remote=None, gate=make_gate(clock)
        )
    assert len(calls) == 1
    assert clock.slept == []
    assert not await ds.has_github_remote()


@pytest.mark.ai_generated
async def test_create_github_sibling_gives_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    calls = _fake_create_sibling(monkeypatch, ds, bare, [RATE_LIMIT_MSG] * 3)
    clock = FakeClock()
    gate = make_gate(clock, attempts=2)
    with pytest.raises(GitHubRateLimited):
        await ds.create_github_sibling(
            owner="org", name="zarr1", backup_remote=None, gate=gate
        )
    assert len(calls) == 3
    assert clock.slept == [60.0, 120.0]
    assert gate.gave_up


@pytest.mark.ai_generated
async def test_create_github_sibling_restores_missing_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    When the sibling exists but our own config was lost (a run cancelled
    right after DataLad configured it), the next visit restores it without
    calling DataLad again.
    """
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    calls = _fake_create_sibling(monkeypatch, ds, bare, ["ok"])
    await ds.create_github_sibling(owner="org", name="zarr1", backup_remote=None)
    _git(ds, "config", "--unset", "remote.github.pushurl")
    _git(ds, "config", "--unset", "branch.draft.remote")
    created = await ds.create_github_sibling(
        owner="org", name="zarr1", backup_remote=None
    )
    assert not created
    assert len(calls) == 1
    assert await ds.get_repo_config("remote.github.pushurl") == "git@github.com:org/zarr1"
    assert await ds.get_repo_config("branch.draft.remote") == "github"


@pytest.mark.ai_generated
async def test_has_unpushed_commits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ds = await _make_dataset(tmp_path / "ds")
    bare = _make_bare(tmp_path / "bare.git")
    _add_github_remote(ds, bare)
    with caplog.at_level(logging.WARNING, logger="backups2datalad"):
        assert not await ds.has_unpushed_commits(), "no upstream -> cannot tell"
    assert "no upstream configured" in caplog.text
    _git(ds, "config", "branch.draft.remote", "github")
    _git(ds, "config", "branch.draft.merge", "refs/heads/draft")
    assert await ds.has_unpushed_commits(), "never pushed"
    _git(ds, "push", "--quiet", "github", "draft")
    assert not await ds.has_unpushed_commits()
    (ds.pathobj / "new.txt").write_text("hi\n")
    await ds.add("new.txt")
    await ds.commit("Add a file", paths=["new.txt"], check_dirty=False)
    assert await ds.has_unpushed_commits()


def _spy(monkeypatch: pytest.MonkeyPatch, cls: type, name: str) -> list[dict[str, Any]]:
    """Record the keyword arguments of each call to ``cls.name`` (still called)"""
    calls: list[dict[str, Any]] = []
    orig: Callable[..., Any] = getattr(cls, name)

    async def spy(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls.append(kwargs)
        return await orig(self, *args, **kwargs)

    monkeypatch.setattr(cls, name, spy)
    return calls


@pytest.mark.ai_generated
async def test_sync_zarr_converges_github_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    `sync_zarr` pushes commits the GitHub sibling lacks and records the
    description whenever it was never recorded -- not only on the visit that
    made a commit -- so a run cut short after creating the repository is
    healed by the next one; converged Zarrs make no push and no API call.
    """
    zarr_root = tmp_path / "zarrs"
    config = BackupConfig(
        backup_root=tmp_path,
        zarrs=ResourceConfig(path="zarrs", github_org="dandizarrs"),
        dandisets=ResourceConfig(path="dandisets", github_org="dandisets"),
    )
    gh = MagicMock()
    gh.edit_repo = AsyncMock()
    gh.gate = make_gate(FakeClock())
    manager = Manager(config=config, gh=gh, log=plog, token="token")
    asset = MagicMock(spec=RemoteZarrAsset)
    asset.zarr = "zarr1"
    asset.dandiset_id = "000001"
    asset.path = "sample.zarr"
    asset.created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dsdir = zarr_root / "zarr1"
    bare = _make_bare(tmp_path / "bare.git")

    async def fake_create(
        self: AsyncDataset, owner: str, name: str, *args: Any, **kwargs: Any
    ) -> bool:
        if await self.has_github_remote():
            return False
        _add_github_remote(self, bare)
        _git(self, "config", "branch.draft.remote", "github")
        _git(self, "config", "branch.draft.merge", "refs/heads/draft")
        return True

    monkeypatch.setattr(AsyncDataset, "create_github_sibling", fake_create)
    monkeypatch.setattr("backups2datalad.zarr.ZarrSyncer.run", AsyncMock())
    pushes = _spy(monkeypatch, AsyncDataset, "push")

    async def visit() -> None:
        link = ZarrLink(zarr_dspath=dsdir, timestamp=None, asset_paths=["sample.zarr"])
        await sync_zarr(asset, None, dsdir, manager, link=link)

    ds = AsyncDataset(dsdir)
    # First visit: fresh dataset, sibling created, nothing pushed yet
    await visit()
    assert len(pushes) == 1
    assert pushes[0]["force"] is False
    assert _git(ds, "rev-parse", "HEAD") == subprocess.run(
        ["git", "rev-parse", "refs/heads/draft"],
        cwd=bare, check=True, capture_output=True, text=True,
    ).stdout.strip()
    gh.edit_repo.assert_awaited_once()
    assert "description" in gh.edit_repo.await_args.kwargs
    assert await ds.get_repo_config("dandi.github-description") is not None
    # Second visit: converged -> no push, no API call
    await visit()
    assert len(pushes) == 1
    gh.edit_repo.assert_awaited_once()
    # As if an earlier run had committed but never pushed
    _git(ds, "update-ref", "-d", "refs/remotes/github/draft")
    await visit()
    assert len(pushes) == 2
    gh.edit_repo.assert_awaited_once()
    # As if an earlier run had never recorded the description (DataLad's
    # in-process config cache does not notice the out-of-band change)
    _git(ds, "config", "--unset", "dandi.github-description")
    ds.ds.config.reload(force=True)
    await visit()
    assert gh.edit_repo.await_count == 2
    assert len(pushes) == 2
    # A dirty Zarr is still refused, now with a digest of what is dirty
    (ds.pathobj / "stray.txt").write_text("x\n")
    with pytest.raises(RuntimeError, match=r"(?s)is dirty.*1 dirty path.*stray\.txt"):
        await visit()


@pytest.mark.ai_generated
async def test_set_zarr_description_uses_given_dataset(tmp_path: Path) -> None:
    """
    `set_zarr_description` records the description cache in the dataset it
    is given (under ``backup-zarrs`` that is a partial directory, not
    ``zarr_root/<id>``) and refuses to work without an installed dataset,
    instead of silently falling back to DataLad's global config.
    """
    config = BackupConfig(
        backup_root=tmp_path,
        zarrs=ResourceConfig(path="zarrs", github_org="dandizarrs"),
        dandisets=ResourceConfig(path="dandisets", github_org="dandisets"),
    )
    gh = MagicMock()
    gh.edit_repo = AsyncMock()
    manager = Manager(config=config, gh=gh, log=plog, token="token")
    ds = await _make_dataset(tmp_path / "partial" / "zarr1")
    stats = DatasetStats(files=1, size=10)
    await manager.set_zarr_description("zarr1", stats, ds=ds)
    gh.edit_repo.assert_awaited_once()
    assert await ds.get_repo_config("dandi.github-description") == "1 file, 10 Bytes"
    assert (
        subprocess.run(
            ["git", "config", "--global", "dandi.github-description"],
            capture_output=True, text=True,
        ).returncode != 0
    ), "must not leak into the global git config"
    with pytest.raises(RuntimeError, match="no dataset at"):
        await manager.set_zarr_description("missing", stats)
