from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from functools import partial
import json
import logging
import os
from pathlib import Path
import re
from shutil import rmtree
import subprocess
from time import sleep
from typing import Any

import anyio
from dandi.consts import DandiInstance, dandiset_metadata_file, known_instances
from dandi.exceptions import NotFoundError
from dandi.upload import upload
from dandischema.consts import DANDI_SCHEMA_VERSION
from datalad.api import Dataset
from datalad.tests.utils_pytest import assert_repo_status
import pytest
import requests
from test_util import find_filepaths
import zarr

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset, RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.util import is_meta_file
from backups2datalad.zarr import CHECKSUM_FILE

LOCAL_DOCKER_DIR = Path(__file__).with_name("data") / "dandiarchive-docker"
LOCAL_DOCKER_ENV = LOCAL_DOCKER_DIR.name

TEST_INSTANCE = "dandi-api-local-docker-tests"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def capture_all_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(5, logger="backups2datalad")
    caplog.set_level(logging.DEBUG, logger="test_backups2datalad")


@pytest.fixture(autouse=True)
def tmp_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    home = tmp_path_factory.mktemp("tmp_home")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_DIRS", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_DATA_DIRS", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("LOCALAPPDATA", str(home))
    for key, value in [
        ("annex.security.allowed-ip-addresses", "127.0.0.1 ::1"),
        ("user.email", "git@test.nil"),
        ("user.name", "Test Gitter"),
    ]:
        subprocess.run(["git", "config", "--global", key, value], check=True)
    return home


@dataclass
class Archive:
    instance: DandiInstance
    api_token: str
    s3endpoint: str
    s3bucket: str

    @property
    def instance_id(self) -> str:
        iid = self.instance.name
        assert isinstance(iid, str)
        return iid

    @property
    def api_url(self) -> str:
        url = self.instance.api
        assert isinstance(url, str)
        return url


@pytest.fixture(scope="session")
def docker_archive() -> Iterator[Archive]:
    # Check that we're running on a Unix-based system (Linux or macOS), as the
    # Docker images don't work on Windows.
    if os.name != "posix":
        pytest.fail("Docker images require Unix host")
    persist = os.environ.get("BACKUPS2DATALAD_TESTS_PERSIST_DOCKER_COMPOSE")
    create = (
        persist is None
        or subprocess.run(
            ["docker", "inspect", f"{LOCAL_DOCKER_ENV}-django-1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        != 0
    )

    env = {**os.environ, "DJANGO_DANDI_SCHEMA_VERSION": DANDI_SCHEMA_VERSION}
    try:
        if create:
            if os.environ.get("DANDI_TESTS_PULL_DOCKER_COMPOSE", "1") not in ("", "0"):
                subprocess.run(
                    ["docker", "compose", "pull"], cwd=LOCAL_DOCKER_DIR, check=True
                )
            subprocess.run(
                ["docker", "compose", "run", "--rm", "createbuckets"],
                cwd=LOCAL_DOCKER_DIR,
                env=env,
                check=True,
            )
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "django",
                    "./manage.py",
                    "migrate",
                ],
                cwd=LOCAL_DOCKER_DIR,
                env=env,
                check=True,
            )
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "django",
                    "./manage.py",
                    "createcachetable",
                ],
                cwd=LOCAL_DOCKER_DIR,
                env=env,
                check=True,
            )
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "run",
                    "--rm",
                    "-e",
                    "DJANGO_SUPERUSER_PASSWORD=nsNc48DBiS",
                    "django",
                    "./manage.py",
                    "createsuperuser",
                    "--no-input",
                    "--email",
                    "admin@nil.nil",
                ],
                cwd=LOCAL_DOCKER_DIR,
                env=env,
                check=True,
            )

        r = subprocess.check_output(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "-T",
                "django",
                "./manage.py",
                "drf_create_token",
                "admin@nil.nil",
            ],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            text=True,
        )
        m = re.search(r"^Generated token (\w+) for user admin@nil.nil$", r, flags=re.M)
        if not m:
            raise RuntimeError(
                "Could not extract Django auth token from drf_create_token"
                f" output: {r!r}"
            )
        django_api_key = m[1]
        instance = known_instances[TEST_INSTANCE]

        if create:
            subprocess.run(
                ["docker", "compose", "up", "-d", "django", "celery"],
                cwd=str(LOCAL_DOCKER_DIR),
                env=env,
                check=True,
            )
            for _ in range(25):
                try:
                    requests.get(f"{instance.api}/dandisets/")
                except requests.ConnectionError:
                    sleep(1)
                else:
                    break
            else:
                raise RuntimeError("Django container did not start up in time")
        os.environ["DANDI_API_KEY"] = django_api_key  # For uploading
        yield Archive(
            instance=instance,
            api_token=django_api_key,
            s3endpoint="http://localhost:9000",
            s3bucket="dandi-dandisets",
        )
    finally:
        if persist in (None, "0"):
            subprocess.run(
                ["docker", "compose", "down", "-v"], cwd=LOCAL_DOCKER_DIR, check=True
            )


@pytest.fixture
async def dandi_client(docker_archive: Archive) -> AsyncIterator[AsyncDandiClient]:
    async with AsyncDandiClient.for_dandi_instance(
        docker_archive.instance_id, token=docker_archive.api_token
    ) as client:
        yield client


@dataclass
class SampleDandiset:
    client: AsyncDandiClient
    dspath: Path
    dandiset: RemoteDandiset
    dandiset_id: str
    #: Mapping from asset relative paths to their contents
    text_assets: dict[str, str] = field(default_factory=dict)
    #: Mapping from asset relative paths to their contents
    blob_assets: dict[str, bytes] = field(default_factory=dict)
    #: Mapping from asset relative paths to mappings from Zarr entry paths to
    #: their contents
    zarr_assets: dict[str, dict[str, bytes]] = field(default_factory=dict)

    def add_text(self, path: str, contents: str) -> None:
        self.rmasset(path)
        target = self.dspath / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents)
        self.text_assets[path] = contents

    def add_blob(self, path: str, contents: bytes) -> None:
        self.rmasset(path)
        target = self.dspath / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
        self.blob_assets[path] = contents

    def add_zarr(self, path: str, *args: Any, **kwargs: Any) -> None:
        self.rmasset(path)
        target = self.dspath / path
        target.parent.mkdir(parents=True, exist_ok=True)
        zarr.save(target, *args, **kwargs)
        self.zarr_assets[path] = {
            p.relative_to(target).as_posix(): p.read_bytes()
            for p in find_filepaths(target)
        }

    def rmasset(self, path: str) -> None:
        target = self.dspath / path
        if path in self.text_assets:
            del self.text_assets[path]
            target.unlink()
        if path in self.blob_assets:
            del self.blob_assets[path]
            target.unlink()
        elif path in self.zarr_assets:
            del self.zarr_assets[path]
            rmtree(target)
        else:
            return
        d = target.parent
        while d != self.dspath and not any(d.iterdir()):
            d.rmdir()
            d = d.parent

    async def upload(
        self, paths: list[str | Path] | None = None, **kwargs: Any
    ) -> None:
        await anyio.to_thread.run_sync(
            partial(
                upload,
                paths=paths or [self.dspath],
                dandi_instance=TEST_INSTANCE,
                devel_debug=True,
                allow_any_path=True,
                validation="skip",
                **kwargs,
            )
        )

    async def check_backup(
        self, backup_ds: Dataset, zarr_root: Path | None = None
    ) -> tuple[PopulateManifest, PopulateManifest]:
        # Returns a tuple of (blob assets populate manifest, Zarr populate manifest)
        assert backup_ds.is_installed()
        assert_repo_status(backup_ds.path)
        backup_files = {
            f
            for f in backup_ds.repo.get_files()
            if not is_meta_file(f) or f.startswith(".dandi/")
        }
        asset_set = (
            self.text_assets.keys() | self.blob_assets.keys() | self.zarr_assets.keys()
        )
        assert backup_files == (
            asset_set
            | {dandiset_metadata_file, ".dandi/assets.json", ".dandi/assets-state.json"}
        )
        with (backup_ds.pathobj / ".dandi" / "assets.json").open() as fp:
            assert {asset["path"] for asset in json.load(fp)} == asset_set

        assert not any(backup_ds.repo.is_under_annex(list(self.text_assets)))
        for path, contents in self.text_assets.items():
            p = backup_ds.pathobj / path
            assert p.is_file()
            assert p.read_text() == contents

        assert all(backup_ds.repo.is_under_annex(list(self.blob_assets)))
        keys2blobs: dict[str, bytes] = {}
        for path, blob in self.blob_assets.items():
            p = backup_ds.pathobj / path
            assert p.is_symlink() and not p.exists()
            keys2blobs[Path(os.readlink(p)).name] = blob

        zarr_manifest = await self.check_all_zarrs(backup_ds, zarr_root)
        return (PopulateManifest(keys2blobs), zarr_manifest)

    async def check_all_zarrs(
        self, backup_ds: Dataset, zarr_root: Path | None = None
    ) -> PopulateManifest:
        subdatasets = {
            Path(sds["path"]).relative_to(backup_ds.pathobj).as_posix(): sds
            for sds in backup_ds.subdatasets(state="any", result_renderer=None)
        }
        zarr_keys2blobs: dict[str, bytes] = {}
        if self.zarr_assets:
            assert zarr_root is not None
            for path, entries in self.zarr_assets.items():
                asset = await self.dandiset.aget_asset_by_path(path)
                assert isinstance(asset, RemoteZarrAsset)
                zarr_ds = Dataset(zarr_root / asset.zarr)
                try:
                    checksum = asset.get_digest_value()
                except NotFoundError:
                    # Happens when Zarr is empty?
                    checksum = None
                assert path in subdatasets
                subds = subdatasets.pop(path)
                assert subds["gitmodule_url"] == str(zarr_ds.pathobj)
                assert subds["type"] == "dataset"
                assert subds["gitshasum"] == zarr_ds.repo.format_commit("%H")
                assert (
                    subds["state"] == "absent"
                )  # we should have them uninstalled in the dataset
                local_checksum = await AsyncDataset(
                    zarr_ds.pathobj
                ).compute_zarr_checksum()
                zarr_keys2blobs.update(
                    self.check_zarr_backup(zarr_ds, entries, checksum, local_checksum)
                )
        assert not subdatasets
        return PopulateManifest(zarr_keys2blobs)

    def check_zarr_backup(
        self,
        zarr_ds: Dataset,
        entries: dict[str, bytes],
        checksum: str | None,
        local_checksum: str,
    ) -> dict[str, bytes]:
        assert zarr_ds.is_installed()
        assert_repo_status(zarr_ds.path)
        zarr_files = {f for f in zarr_ds.repo.get_files() if not is_meta_file(f)}
        assert zarr_files == entries.keys()
        assert all(zarr_ds.repo.is_under_annex(list(zarr_files)))
        keys2blobs: dict[str, bytes] = {}
        for path, blob in entries.items():
            p = zarr_ds.pathobj / path
            assert p.is_symlink() and not p.exists()
            keys2blobs[Path(os.readlink(p)).name] = blob
        stored_checksum = (zarr_ds.pathobj / CHECKSUM_FILE).read_text().strip()
        assert stored_checksum == local_checksum
        if checksum is not None:
            assert stored_checksum == checksum
        assert zarr_ds.repo.is_under_annex([str(CHECKSUM_FILE)]) == [False]
        return keys2blobs


@pytest.fixture()
async def new_dandiset(
    dandi_client: AsyncDandiClient, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[SampleDandiset]:
    d = await dandi_client.create_dandiset(
        "Dandiset for testing backups2datalad",
        {
            "name": "Dandiset for testing backups2datalad",
            "description": "A test text Dandiset",
            "contributor": [
                {
                    "schemaKey": "Person",
                    "name": "Wodder, John",
                    "email": "nemo@example.com",
                    "roleName": ["dcite:Author", "dcite:ContactPerson"],
                }
            ],
            "license": ["spdx:CC0-1.0"],
        },
    )
    dandiset_id = d.identifier
    dspath = tmp_path_factory.mktemp(f"new_dandiset_{dandiset_id}")
    (dspath / dandiset_metadata_file).write_text(f"identifier: '{dandiset_id}'\n")
    ds = SampleDandiset(
        client=dandi_client,
        dspath=dspath,
        dandiset=d,
        dandiset_id=d.identifier,
    )
    yield ds
    # Cleanup: delete the dandiset from the server after the test
    try:
        await dandi_client.delete(f"/dandisets/{dandiset_id}/")
    except Exception:
        # If deletion fails (e.g., dandiset doesn't exist or already deleted),
        # we can ignore it as the test has completed
        pass


@pytest.fixture()
async def embargoed_dandiset(
    dandi_client: AsyncDandiClient, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[SampleDandiset]:
    d = await dandi_client.create_dandiset(
        "Embargoed Dandiset for testing backups2datalad",
        {
            "name": "Embargoed Dandiset for testing backups2datalad",
            "description": "A test embargoed Dandiset",
            "contributor": [
                {
                    "schemaKey": "Person",
                    "name": "Wodder, John",
                    "email": "nemo@example.com",
                    "roleName": ["dcite:Author", "dcite:ContactPerson"],
                }
            ],
            "license": ["spdx:CC0-1.0"],
        },
        embargo=True,
    )
    dandiset_id = d.identifier
    dspath = tmp_path_factory.mktemp(f"embargoed_dandiset_{dandiset_id}")
    (dspath / dandiset_metadata_file).write_text(f"identifier: '{dandiset_id}'\n")
    ds = SampleDandiset(
        client=dandi_client,
        dspath=dspath,
        dandiset=d,
        dandiset_id=d.identifier,
    )
    yield ds
    # Cleanup: delete the dandiset from the server after the test
    try:
        await dandi_client.delete(f"/dandisets/{dandiset_id}/")
    except Exception:
        # If deletion fails (e.g., dandiset doesn't exist or already deleted),
        # we can ignore it as the test has completed
        pass


@dataclass
class PopulateManifest:
    keys2blobs: dict[str, bytes]

    def check(self, root: Path) -> None:
        files = {p.name: p.read_bytes() for p in find_filepaths(root)}
        assert files == self.keys2blobs


@pytest.fixture()
async def text_dandiset(new_dandiset: SampleDandiset) -> AsyncIterator[SampleDandiset]:
    for path, contents in [
        ("file.txt", "This is test text.\n"),
        ("v0.txt", "Version 0\n"),
        ("subdir1/apple.txt", "Apple\n"),
        ("subdir2/banana.txt", "Banana\n"),
        ("subdir2/coconut.txt", "Coconut\n"),
    ]:
        new_dandiset.add_text(path, contents)
    await new_dandiset.upload()
    yield new_dandiset
    # Cleanup is handled by the new_dandiset fixture
