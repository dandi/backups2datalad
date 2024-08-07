from __future__ import annotations

import logging
from pathlib import Path
from shutil import rmtree
from time import sleep

from conftest import Archive, SampleDandiset
from datalad.api import Dataset
import numpy as np
import pytest
from test_util import GitRepo

from backups2datalad.adandi import RemoteZarrAsset
from backups2datalad.adataset import AsyncDataset, DatasetStats
from backups2datalad.aioutil import arequest
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager
from backups2datalad.zarr import sync_zarr

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

    assert await AsyncDataset(ds.pathobj).get_stats(config=di.config) == DatasetStats(
        files=6, size=1535
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
