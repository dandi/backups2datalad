"""
Test fixtures for backups2datalad.

Most of the heavy lifting (docker-compose orchestration, DRF token bootstrap,
RemoteDandiset creation) is delegated to ``dandi.pytest_plugin`` -- which is
auto-loaded via dandi-cli's ``pytest11`` entry point.  Locally we only define:

* an :class:`Archive` adapter so existing test imports
  (``from conftest import Archive, SampleDandiset``) keep working;
* an async-aware :class:`SampleDandiset` subclass that adds
  ``add_text`` / ``add_blob`` / ``add_zarr`` / ``rmasset`` / ``check_*`` helpers
  and an ``async upload()`` wrapper;
* a ``dandi_client`` fixture that wraps upstream's sync ``local_dandi_api``
  into our :class:`backups2datalad.adandi.AsyncDandiClient`;
* re-bound ``new_dandiset`` / ``text_dandiset`` / ``embargoed_dandiset``
  fixtures so they yield our subclass;
* small autouse fixtures for ``tmp_home`` (git config), session-wide
  ``DANDI_API_KEY`` export (needed by CLI subprocesses), and log capture.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from functools import partial
import json
import logging
import os
from pathlib import Path
from shutil import rmtree
import subprocess
from typing import Any

import anyio
from dandi.consts import dandiset_metadata_file
from dandi.exceptions import NotFoundError
from dandi.tests.fixtures import DandiAPI
from dandi.tests.fixtures import SampleDandiset as _UpstreamSampleDandiset
from dandi.tests.fixtures import SampleDandisetFactory
from dandi.upload import upload
from dandischema.models import DigestType
from datalad.api import Dataset
from datalad.tests.utils_pytest import assert_repo_status
import pytest
from test_util import find_filepaths
import zarr

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset, RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset
from backups2datalad.util import is_meta_file
from backups2datalad.zarr import CHECKSUM_FILE

# The S3 endpoint/bucket are not exposed by upstream's `local_dandi_api` -- they
# are part of the docker-compose stack's minio config, which is implementation
# detail of dandi-cli.  These constants match upstream's
# `dandi/tests/data/dandiarchive-docker/docker-compose.yml`.
#
# The S3 host is `127.0.0.1` (not `localhost`): upstream's compose sets
# `DJANGO_MINIO_STORAGE_MEDIA_URL: http://127.0.0.1:9000/...`, so the
# dandi-archive server returns asset `contentUrl`s using `127.0.0.1`
# rather than `localhost`.  Our tests use this endpoint to build
# `content_url_regex`, which must match those server-returned URLs.
LOCAL_S3_ENDPOINT = "http://127.0.0.1:9000"
LOCAL_S3_BUCKET = "dandi-dandisets"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def capture_all_logs(caplog: pytest.LogCaptureFixture) -> None:
    # Upstream's autouse `capture_all_logs` only sets the "dandi" logger.  We
    # override it (same name -> local wins) to also capture our own loggers.
    caplog.set_level(logging.DEBUG, logger="dandi")
    caplog.set_level(5, logger="backups2datalad")
    caplog.set_level(logging.DEBUG, logger="test_backups2datalad")


@pytest.fixture(autouse=True)
def tmp_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    # Override of upstream's `tmp_home` -- adds the `git config --global`
    # bootstrap our datalad-using tests need.
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
    """
    Backwards-compatible adapter exposing the surface the test files import as
    `from conftest import Archive`.  Wraps upstream's :class:`DandiAPI`.
    """

    api: DandiAPI
    s3endpoint: str = LOCAL_S3_ENDPOINT
    s3bucket: str = LOCAL_S3_BUCKET

    @property
    def instance_id(self) -> str:
        # dandi.tests.fixtures isn't typed (`ignore_missing_imports`), so the
        # attribute access leaks `Any`; cast to satisfy `warn_return_any`.
        instance_id: str = self.api.instance_id
        return instance_id

    @property
    def api_url(self) -> str:
        api_url: str = self.api.api_url
        return api_url

    @property
    def api_token(self) -> str:
        api_token: str = self.api.api_key
        return api_token


def _setup_minio_bucket() -> None:
    """
    Create the test bucket (if it doesn't yet exist), enable versioning, and
    grant anonymous read.

    Upstream's docker-compose dropped the ``createbuckets`` MinIO sidecar
    that used to do this (it called ``mc mb --with-versioning`` +
    ``mc anonymous set public`` on session start).  dandi-archive itself
    creates the bucket lazily on first PUT and does not enable versioning,
    which is fine for upstream tests (they only hit the Django API) but
    breaks backups2datalad: ``blob.get_file_bucket_url`` HEAD-requests the
    asset's bucket URL directly and reads ``x-amz-version-id`` from the
    response (`src/backups2datalad/blob.py:48`), which MinIO only returns
    when versioning is enabled.  Anonymous read is also needed because the
    backup downloads blobs via the public bucket URL without an auth
    header.
    """
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=LOCAL_S3_ENDPOINT,
        aws_access_key_id="minioAccessKey",
        aws_secret_access_key="minioSecretKey",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    try:
        s3.create_bucket(Bucket=LOCAL_S3_BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    s3.put_bucket_versioning(
        Bucket=LOCAL_S3_BUCKET,
        VersioningConfiguration={"Status": "Enabled"},
    )
    s3.put_bucket_policy(
        Bucket=LOCAL_S3_BUCKET,
        Policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": ["*"]},
                        "Action": [
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:GetObject",
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{LOCAL_S3_BUCKET}",
                            f"arn:aws:s3:::{LOCAL_S3_BUCKET}/*",
                        ],
                    },
                ],
            }
        ),
    )


@pytest.fixture(scope="session")
def docker_archive(local_dandi_api: DandiAPI) -> Iterator[Archive]:
    """
    Adapter over upstream's session-scoped ``local_dandi_api`` (which itself
    transitively pulls in ``docker_compose_setup`` from
    ``dandi.pytest_plugin``).

    As a side effect this also exports ``DANDI_API_KEY`` and
    ``DANDI_API_LOCAL_DOCKER_TESTS_API_KEY`` into the real process
    environment.  We do this here rather than in a session-autouse fixture
    because:

    * our CLI tests spawn ``backups2datalad`` subprocesses (via
      ``asyncclick.testing.CliRunner`` and direct ``subprocess.run``); those
      need the key in ``os.environ``, not just monkeypatched in pytest's
      namespace.  Upstream's ``DandiAPI.monkeypatch_set_api_key_env`` is not
      enough because subprocesses inherit ``os.environ``, not the
      monkeypatch state;
    * keeping it scoped to ``docker_archive`` (rather than session-autouse)
      means pure-mock tests don't accidentally trigger docker startup.
    """
    prev = os.environ.get("DANDI_API_KEY")
    prev_local = os.environ.get("DANDI_API_LOCAL_DOCKER_TESTS_API_KEY")
    os.environ["DANDI_API_KEY"] = local_dandi_api.api_key
    os.environ["DANDI_API_LOCAL_DOCKER_TESTS_API_KEY"] = local_dandi_api.api_key
    _setup_minio_bucket()
    try:
        yield Archive(api=local_dandi_api)
    finally:
        if prev is None:
            os.environ.pop("DANDI_API_KEY", None)
        else:
            os.environ["DANDI_API_KEY"] = prev
        if prev_local is None:
            os.environ.pop("DANDI_API_LOCAL_DOCKER_TESTS_API_KEY", None)
        else:
            os.environ["DANDI_API_LOCAL_DOCKER_TESTS_API_KEY"] = prev_local


@pytest.fixture
async def dandi_client(docker_archive: Archive) -> AsyncIterator[AsyncDandiClient]:
    async with AsyncDandiClient.for_dandi_instance(
        docker_archive.instance_id, token=docker_archive.api_token
    ) as client:
        yield client


@dataclass
class SampleDandiset(_UpstreamSampleDandiset):
    """
    backups2datalad-specific extension of upstream's
    :class:`dandi.tests.fixtures.SampleDandiset`.

    Adds:

    * an async wrapper around the sync ``upload()`` (run in a thread);
    * ``add_text`` / ``add_blob`` / ``add_zarr`` / ``rmasset`` helpers that
      track what was uploaded so the backup can be verified later;
    * ``check_backup`` / ``check_all_zarrs`` / ``check_zarr_backup`` --
      backups2datalad-specific assertions about the cloned dataset.

    We also narrow ``client`` and ``dandiset`` to backups2datalad's async
    wrappers (upstream exposes the sync :class:`dandi.dandiapi.DandiAPIClient`
    and :class:`dandi.dandiapi.RemoteDandiset` of the same names; existing
    tests call async methods on both, so we replace them at construction
    time).
    """

    # Narrow type vs. upstream's `dandiset: dandi.dandiapi.RemoteDandiset`.
    # The async instance is fetched in `_make_sample` and passed in.
    dandiset: RemoteDandiset  # type: ignore[assignment]
    async_client: AsyncDandiClient | None = None  # type: ignore[assignment]

    #: Mapping from asset relative paths to their contents
    text_assets: dict[str, str] = field(default_factory=dict)
    #: Mapping from asset relative paths to their contents
    blob_assets: dict[str, bytes] = field(default_factory=dict)
    #: Mapping from asset relative paths to mappings from Zarr entry paths to
    #: their contents
    zarr_assets: dict[str, dict[str, bytes]] = field(default_factory=dict)

    @property
    def client(self) -> AsyncDandiClient:  # type: ignore[override]
        # Existing tests use `sample.client` as the async client.  Upstream's
        # `SampleDandiset.client` returns a sync `DandiAPIClient`; we shadow
        # it with the async one we attached in the fixture.
        assert self.async_client is not None
        return self.async_client

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

    async def upload(  # type: ignore[override]
        self, paths: list[str | Path] | None = None, **kwargs: Any
    ) -> None:
        # Async wrapper around dandi-cli's sync `upload()`.  We don't delegate
        # to `super().upload()` because that uses `pytest.MonkeyPatch().context`
        # which is sync; instead we set the same env var directly (it's already
        # exported session-wide by `_export_dandi_api_key`) and run in a
        # thread.
        await anyio.to_thread.run_sync(
            partial(
                upload,
                paths=paths or [self.dspath],
                dandi_instance=self.api.instance_id,
                devel_debug=True,
                allow_any_path=True,
                validation="skip",
                **kwargs,
            )
        )
        # dandi-cli's `upload()` returns as soon as the asset is registered
        # with the API, but the SHA-256 digest is computed asynchronously by
        # the dandi-archive celery worker.  `update-from-backup` skips the
        # download for any asset whose SHA-256 isn't ready yet, which
        # produced flaky "missing file" failures in `test_backup_command`
        # (see CI run 26239575722 / pre-existing `test_binary` flake on
        # main).  Wait until the server has computed digests for every blob
        # asset we just uploaded before returning, so callers can run
        # `update-from-backup` deterministically.
        await self._wait_for_blob_digests()

    async def _wait_for_blob_digests(
        self, timeout: float = 60.0, poll_interval: float = 1.0
    ) -> None:
        expected = set(self.text_assets) | set(self.blob_assets)
        if not expected:
            return
        deadline = anyio.current_time() + timeout
        not_ready: list[str] = list(expected)
        while True:
            not_ready = []
            async for asset in self.dandiset.aget_assets():
                if asset.path not in expected:
                    continue
                try:
                    sha = asset.get_digest_value(DigestType.sha2_256)
                except NotFoundError:
                    sha = None
                if sha is None:
                    not_ready.append(asset.path)
            if not not_ready:
                return
            if anyio.current_time() >= deadline:
                raise TimeoutError(
                    f"dandi-archive did not compute SHA-256 for"
                    f" {len(not_ready)} asset(s) within {timeout}s:"
                    f" {sorted(not_ready)}"
                )
            await anyio.sleep(poll_interval)

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


async def _make_sample(
    factory: SampleDandisetFactory,
    async_client: AsyncDandiClient,
    name: str,
    embargo: bool = False,
) -> SampleDandiset:
    base = factory.mkdandiset(name, embargo=embargo)
    # Fetch the async wrapper for the dandiset upstream just created, so that
    # tests can call `sample.dandiset.aget_asset_by_path(...)`,
    # `.aget_zarr_assets()`, `.unembargo()`, `.apublish()`,
    # `.await_until_valid(...)` etc.  Without this swap, `sample.dandiset`
    # would be the sync `dandi.dandiapi.RemoteDandiset` from upstream's
    # factory, which has only the sync analogues of those methods.
    async_dandiset = await async_client.get_dandiset(base.dandiset_id, "draft")
    return SampleDandiset(
        api=base.api,
        dspath=base.dspath,
        dandiset=async_dandiset,  # type: ignore[arg-type]
        dandiset_id=base.dandiset_id,
        upload_kwargs=base.upload_kwargs,
        async_client=async_client,
    )


@pytest.fixture
async def new_dandiset(
    dandi_client: AsyncDandiClient,
    sample_dandiset_factory: SampleDandisetFactory,
    request: pytest.FixtureRequest,
) -> AsyncIterator[SampleDandiset]:
    ds = await _make_sample(
        sample_dandiset_factory,
        dandi_client,
        f"Dandiset for testing backups2datalad ({request.node.name})",
    )
    yield ds
    try:
        await dandi_client.delete(f"/dandisets/{ds.dandiset_id}/")
    except Exception:
        # Already deleted or never visible; the test has completed -- ignore.
        pass


@pytest.fixture
async def embargoed_dandiset(
    dandi_client: AsyncDandiClient,
    sample_dandiset_factory: SampleDandisetFactory,
    request: pytest.FixtureRequest,
) -> AsyncIterator[SampleDandiset]:
    ds = await _make_sample(
        sample_dandiset_factory,
        dandi_client,
        f"Embargoed Dandiset for testing backups2datalad ({request.node.name})",
        embargo=True,
    )
    yield ds
    try:
        await dandi_client.delete(f"/dandisets/{ds.dandiset_id}/")
    except Exception:
        pass


@dataclass
class PopulateManifest:
    keys2blobs: dict[str, bytes]

    def check(self, root: Path) -> None:
        files = {p.name: p.read_bytes() for p in find_filepaths(root)}
        assert files == self.keys2blobs


@pytest.fixture
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
    # Cleanup is handled by the `new_dandiset` fixture.
