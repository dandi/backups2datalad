"""Refusing to create a mirror that exists already but is not installed"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import anyio
from dandi.consts import EmbargoStatus
from ghrepo import GHRepo
import httpx
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.aioutil import TextProcess, areadcmd
from backups2datalad.annex import AsyncAnnex
from backups2datalad.asyncer import Downloader, ToDownload, run_downloader
from backups2datalad.blob import BlobBackup
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.consts import ZARR_LIMIT
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.manager import GitHub, Manager
from backups2datalad.util import AssetTracker, MirrorMissingError
from backups2datalad.zarr import sync_zarr

pytestmark = pytest.mark.anyio

CREATED = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
ZARR_ID = "0596fd61-17af-484f-aa1d-a6052949a950"


def git(path: Path, *args: str) -> None:
    # Identity comes from conftest's autouse `tmp_home`
    subprocess.run(["git", *args], cwd=path, check=True, capture_output=True)


def register_submodule(ds: Path, path: str, url: str) -> None:
    """Record a submodule in ``.gitmodules`` without installing it."""
    git(ds, "config", "--file", ".gitmodules", f"submodule.{path}.path", path)
    git(ds, "config", "--file", ".gitmodules", f"submodule.{path}.url", url)


def make_superds(root: Path) -> Path:
    """A bare-bones superdataset: an installed repository with .gitmodules."""
    superds = root / "dandisets"
    superds.mkdir(parents=True)
    git(superds, "init", "-q", "-b", "draft")
    git(superds, "commit", "-q", "--allow-empty", "-m", "initial")
    return superds


class FakeGitHub:
    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.asked: list[str] = []

    async def repo_exists(self, repo: GHRepo) -> bool:
        self.asked.append(str(repo))
        return str(repo) in self.existing


def make_datasetter(backup_root: Path, gh: FakeGitHub | None = None) -> DandiDatasetter:
    orgs: dict[str, Any] = {}
    if gh is not None:
        orgs = {
            "dandisets": ResourceConfig(path="dandisets", github_org="dandisets"),
            "zarrs": ResourceConfig(path="dandizarrs", github_org="dandizarrs"),
        }
    config = BackupConfig(backup_root=backup_root, quiescent_period=0.0, **orgs)
    di = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, MagicMock()), config=config
    )
    di._manager = Manager(
        config=config, gh=cast(GitHub, gh), log=MagicMock(), token="dummy"
    )
    return di


# --- Dandisets ----------------------------------------------------------------


@pytest.mark.ai_generated
async def test_registered_but_uninstalled_dandiset_is_not_recreated(
    tmp_path: Path,
) -> None:
    superds = make_superds(tmp_path)
    register_submodule(superds, "000026", "https://github.com/dandisets/000026")
    (superds / "000026").mkdir()  # what a clone leaves for a submodule
    di = make_datasetter(tmp_path)
    with pytest.raises(MirrorMissingError, match="registered in the superdataset"):
        await di.init_dataset(
            superds / "000026",
            dandiset_id="000026",
            create_time=CREATED,
            embargo_status=EmbargoStatus.OPEN,
        )
    assert not (superds / "000026" / ".git").exists()


@pytest.mark.ai_generated
async def test_dandiset_already_on_github_is_not_recreated(tmp_path: Path) -> None:
    superds = make_superds(tmp_path)
    gh = FakeGitHub({"dandisets/000026"})
    di = make_datasetter(tmp_path, gh)
    with pytest.raises(MirrorMissingError, match="exists already"):
        await di.init_dataset(
            superds / "000026",
            dandiset_id="000026",
            create_time=CREATED,
            embargo_status=EmbargoStatus.OPEN,
        )
    assert gh.asked == ["dandisets/000026"]
    assert not (superds / "000026").exists()


@pytest.mark.ai_generated
async def test_new_dandiset_passes_the_guard(tmp_path: Path) -> None:
    superds = make_superds(tmp_path)
    register_submodule(superds, "000003", "https://github.com/dandisets/000003")
    gh = FakeGitHub({"dandisets/000003"})
    di = make_datasetter(tmp_path, gh)
    # Neither registered nor on GitHub: must not raise
    await di.assert_dandiset_mirror_is_new(AsyncDataset(superds / "000999"), "000999")
    assert gh.asked == ["dandisets/000999"]


@pytest.mark.ai_generated
async def test_installed_dataset_is_not_checked(tmp_path: Path) -> None:
    """The guard is consulted only when a dataset is about to be created."""
    ds_path = tmp_path / "000026"
    ds_path.mkdir()
    git(ds_path, "init", "-q", "-b", "draft")
    git(ds_path, "commit", "-q", "--allow-empty", "-m", "initial")

    async def before_create() -> None:
        raise AssertionError("guard consulted for an installed dataset")

    created = await AsyncDataset(ds_path).ensure_installed(
        "Dandiset 000026", cfg_proc=None, before_create=before_create
    )
    assert not created


# --- Zarrs --------------------------------------------------------------------


@pytest.mark.ai_generated
async def test_zarr_registered_in_dandiset_is_not_recreated(tmp_path: Path) -> None:
    dandiset = tmp_path / "000108"
    dandiset.mkdir()
    git(dandiset, "init", "-q", "-b", "draft")
    register_submodule(
        dandiset, "sub-1/sample.ome.zarr", f"https://github.com/dandizarrs/{ZARR_ID}"
    )
    fake = SimpleNamespace(ds=AsyncDataset(dandiset), dandiset_id="000108")
    asset = SimpleNamespace(path="sub-1/sample.ome.zarr", zarr=ZARR_ID)
    with pytest.raises(MirrorMissingError, match="is a submodule of Dandiset"):
        await Downloader.assert_zarr_mirror_is_new(
            cast(Downloader, fake),
            cast(RemoteZarrAsset, asset),
            tmp_path / "dandizarrs" / ZARR_ID,
        )
    # A different Zarr at that path (i.e., the asset was replaced) is new:
    other = SimpleNamespace(path="sub-1/sample.ome.zarr", zarr="some-other-zarr")
    await Downloader.assert_zarr_mirror_is_new(
        cast(Downloader, fake),
        cast(RemoteZarrAsset, other),
        tmp_path / "dandizarrs" / "some-other-zarr",
    )


@pytest.mark.ai_generated
async def test_zarr_already_on_github_is_not_recreated(tmp_path: Path) -> None:
    gh = FakeGitHub({f"dandizarrs/{ZARR_ID}"})
    di = make_datasetter(tmp_path, gh)
    asset = SimpleNamespace(zarr=ZARR_ID, dandiset_id="000108", created=CREATED)
    dsdir = tmp_path / "dandizarrs" / ZARR_ID
    with pytest.raises(MirrorMissingError, match="exists already"):
        await sync_zarr(cast(RemoteZarrAsset, asset), None, dsdir, di.manager)
    assert gh.asked == [f"dandizarrs/{ZARR_ID}"]
    assert not dsdir.exists()


@pytest.mark.ai_generated
async def test_uninstalled_zarrs_are_checked_within_zarr_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Regression test for #133: `asset_loop()` starts a task for every asset at
    once, and each uninstalled Zarr runs a `git config` to look itself up in
    ``.gitmodules``; unbounded, a Dandiset with thousands of new Zarrs ran out
    of file descriptors.  No more of those may run at once than `ZARR_LIMIT`.
    """
    dandiset = tmp_path / "001412"
    dandiset.mkdir()
    git(dandiset, "init", "-q", "-b", "draft")
    running = peak = 0

    async def counting_areadcmd(*args: str | Path, **kwargs: Any) -> str:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            return await areadcmd(*args, **kwargs)
        finally:
            running -= 1

    synced: list[str] = []

    async def fake_sync_zarr(asset: RemoteZarrAsset, *_args: Any, **_kw: Any) -> None:
        synced.append(asset.zarr)

    monkeypatch.setattr("backups2datalad.adataset.areadcmd", counting_areadcmd)
    monkeypatch.setattr("backups2datalad.asyncer.sync_zarr", fake_sync_zarr)
    config = BackupConfig(backup_root=tmp_path, zarrs=ResourceConfig(path="dandizarrs"))
    dm = Downloader(
        dandiset_id="001412",
        embargoed=False,
        embargo_status=EmbargoStatus.OPEN,
        ds=AsyncDataset(dandiset),
        manager=Manager(config=config, gh=None, log=MagicMock(), token="dummy"),
        tracker=cast(AssetTracker, MagicMock()),
        s3client=cast(httpx.AsyncClient, MagicMock()),
        annex=cast(AsyncAnnex, MagicMock()),
    )
    zarr_ids = [f"zarr-{i}" for i in range(3 * ZARR_LIMIT)]
    async with anyio.create_task_group() as tg:
        dm.nursery = tg
        for i, zarr_id in enumerate(zarr_ids):
            asset = SimpleNamespace(
                path=f"sub-{i}/z{i}.ome.zarr", zarr=zarr_id, created=CREATED
            )
            tg.start_soon(dm.process_zarr, cast(RemoteZarrAsset, asset), None)
    assert 0 < peak <= ZARR_LIMIT
    assert sorted(synced) == sorted(zarr_ids)


# --- Downloader cleanup --------------------------------------------------------


class FakeAddurl:
    """Stand-in for a `TextProcess`, recording which close method ran."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def aclose(self) -> None:
        self.calls.append("aclose")

    async def force_aclose(self) -> None:
        self.calls.append("force_aclose")


def make_downloader() -> Downloader:
    annex = SimpleNamespace(get_keys_missing_from=AsyncMock())
    manager = SimpleNamespace(
        config=SimpleNamespace(dandisets=SimpleNamespace(remote=None))
    )
    return Downloader(
        dandiset_id="000001",
        embargoed=False,
        embargo_status=EmbargoStatus.OPEN,
        ds=cast(AsyncDataset, MagicMock()),
        manager=cast(Manager, manager),
        tracker=cast(AssetTracker, MagicMock()),
        s3client=cast(httpx.AsyncClient, MagicMock()),
        annex=cast(AsyncAnnex, annex),
    )


@pytest.mark.ai_generated
async def test_downloader_force_closes_addurl_after_task_group_fails() -> None:
    """
    Regression test for a leaked `git-annex addurl --batch` process. This
    calls the actual `run_downloader()` helper `async_assets()` uses (not a
    hand-copied nesting), so a regression back to the old ordering -- where
    `async with dm:` closed around nothing but `nursery.start_soon(...)`, so
    `__aexit__` ran immediately with `self.addurl` still `None` and a crash
    left the real subprocess orphaned -- gets caught here too.
    """
    dm = make_downloader()
    fake_addurl = FakeAddurl()

    async def flaky_asset_loop(_aia: object) -> None:
        dm.addurl = cast(TextProcess, fake_addurl)
        raise RuntimeError("boom")

    dm.asset_loop = flaky_asset_loop  # type: ignore[method-assign,assignment]
    with pytest.raises(ExceptionGroup) as excinfo:
        await run_downloader(dm, cast(Any, None))
    assert isinstance(excinfo.value.exceptions[0], RuntimeError)
    assert fake_addurl.calls == ["force_aclose"]


@pytest.mark.ai_generated
async def test_downloader_waits_for_sibling_task_before_force_closing_addurl() -> None:
    """
    `force_aclose()` must fire only once every task in the group --
    including one still running when a sibling crashes -- has actually
    finished, not merely been *told* to cancel. Models the real incident
    shape: `feed_addurl`/`read_addurl` (started via `self.nursery`, same as
    here) still in flight when another task raises.
    """
    dm = make_downloader()
    fake_addurl = FakeAddurl()
    long_task_done = False

    async def flaky_asset_loop(_aia: object) -> None:
        async def long_running() -> None:
            nonlocal long_task_done
            try:
                await anyio.sleep_forever()
            finally:
                long_task_done = True

        assert dm.nursery is not None
        dm.addurl = cast(TextProcess, fake_addurl)
        dm.nursery.start_soon(long_running)
        raise RuntimeError("boom")

    dm.asset_loop = flaky_asset_loop  # type: ignore[method-assign,assignment]
    with pytest.raises(ExceptionGroup):
        await run_downloader(dm, cast(Any, None))
    assert long_task_done, "addurl was closed before the sibling task finished"
    assert fake_addurl.calls == ["force_aclose"]


@pytest.mark.ai_generated
async def test_downloader_closes_addurl_gracefully_on_success() -> None:
    """Sanity check: the no-exception branch still calls the plain `aclose`."""
    dm = make_downloader()
    fake_addurl = FakeAddurl()

    async def quiet_asset_loop(_aia: object) -> None:
        dm.addurl = cast(TextProcess, fake_addurl)

    dm.asset_loop = quiet_asset_loop  # type: ignore[method-assign,assignment]
    await run_downloader(dm, cast(Any, None))
    assert fake_addurl.calls == ["aclose"]


class FakeStdin:
    async def __aenter__(self) -> FakeStdin:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass


@pytest.mark.ai_generated
async def test_feed_addurl_refuses_path_already_in_flight() -> None:
    """
    Feeding a path to `addurl` while a download of it is still in progress
    must fail right there, naming the path -- not later as a bare `KeyError`
    in `pop_in_progress()` once the second result for the path arrives
    (the 001873 crash, caused by the path being listed twice).
    """
    dm = make_downloader()
    dm.ds.lock = anyio.Lock()  # type: ignore[assignment]
    sent: list[str] = []

    async def send(line: str) -> None:
        sent.append(line)

    dm.addurl = cast(
        TextProcess, SimpleNamespace(p=SimpleNamespace(stdin=FakeStdin()), send=send)
    )
    path = "code/submit.sh"
    blob = SimpleNamespace(path=path, log=MagicMock())
    td = ToDownload(blob=cast(BlobBackup, blob), url="https://example.com/x")

    async def feed_twice() -> None:
        async with dm.download_sender:
            await dm.download_sender.send(td)
            await dm.download_sender.send(td)

    with pytest.raises(ExceptionGroup) as excinfo:
        async with anyio.create_task_group() as tg:
            tg.start_soon(dm.feed_addurl)
            tg.start_soon(feed_twice)
    (exc,) = excinfo.value.exceptions
    assert isinstance(exc, RuntimeError)
    assert f"{path} sent for download while a download of it is already" in str(exc)
    assert sent == [f"https://example.com/x {path}\n"]

# --- GitHub.repo_exists() -----------------------------------------------------


async def mock_github(
    status: int, headers: dict[str, str] | None = None, token: str = "dummy"
) -> tuple[GitHub, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, headers=headers, json={})

    gh = GitHub(token)
    await gh.client.aclose()
    gh.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return gh, requests


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "status,headers,expected",
    [
        (200, {}, True),
        (404, {}, False),  # fine-grained or App token: no scopes reported
        (404, {"x-oauth-scopes": "repo, workflow"}, False),
    ],
    ids=["exists", "missing", "missing-classic-token"],
)
async def test_repo_exists(
    status: int, headers: dict[str, str], expected: bool
) -> None:
    gh, requests = await mock_github(status, headers)
    async with gh:
        assert await gh.repo_exists(GHRepo("dandisets", "000026")) is expected
    assert [r.method for r in requests] == ["GET"]


@pytest.mark.ai_generated
async def test_repo_exists_distrusts_404_without_private_access() -> None:
    """A classic token without `repo` gets 404 for private repositories."""
    gh, _ = await mock_github(404, {"x-oauth-scopes": "public_repo, read:org"})
    async with gh:
        with pytest.raises(RuntimeError, match="lacks the 'repo' scope"):
            await gh.repo_exists(GHRepo("dandisets", "000026"))


@pytest.mark.ai_generated
async def test_repo_exists_needs_a_token() -> None:
    gh, requests = await mock_github(404, token="")
    async with gh:
        with pytest.raises(RuntimeError, match="without a token"):
            await gh.repo_exists(GHRepo("dandisets", "000026"))
    assert requests == []


@pytest.mark.ai_generated
@pytest.mark.parametrize("first", ["connect-error", 502], ids=str)
async def test_repo_exists_retries_what_is_not_an_answer(first: str | int) -> None:
    """Anything but 404/2xx goes through `get_repo()` and its retries."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            if first == "connect-error":
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(int(first))
        return httpx.Response(200, json={"full_name": "dandisets/000026"})

    gh = GitHub("dummy")
    await gh.client.aclose()
    gh.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with gh:
        assert await gh.repo_exists(GHRepo("dandisets", "000026")) is True
    assert len(requests) == 2
