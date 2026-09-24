"""
Tests for refusing to create a Dandiset or Zarr mirror from scratch when one
already exists elsewhere -- registered in the superdataset (or, for a Zarr, in
its Dandiset), or present on GitHub -- but is not installed where we look.

Creating it anyway starts a second, unrelated history under the same name: new
datalad-id, a one-commit history, and a push that collides with the real
repository.  This is what happens to an uninstalled submodule in a fresh clone
of the superdataset, or to a mirror directory removed by hand.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

from dandi.consts import EmbargoStatus
from ghrepo import GHRepo
import httpx
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.asyncer import Downloader
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.manager import GitHub, Manager
from backups2datalad.util import MirrorMissingError
from backups2datalad.zarr import sync_zarr

pytestmark = pytest.mark.anyio

CREATED = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
ZARR_ID = "0596fd61-17af-484f-aa1d-a6052949a950"


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


# --- GitHub.repo_exists() -----------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "status,expected", [(200, True), (404, False)], ids=["exists", "missing"]
)
async def test_repo_exists(status: int, expected: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json={"full_name": "dandisets/000026"})

    gh = GitHub("dummy")
    await gh.client.aclose()
    gh.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        assert await gh.repo_exists(GHRepo("dandisets", "000026")) is expected
    finally:
        await gh.aclose()
    assert len(requests) == 1
    assert requests[0].method == "GET"
