from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import random
from traceback import format_exception

from asyncclick.testing import CliRunner, Result
from conftest import Archive, SampleDandiset
from datalad.api import Dataset
from datalad.tests.utils_pytest import assert_repo_status
import numpy as np
import pytest
from test_util import GitRepo

from backups2datalad.__main__ import main
from backups2datalad.adataset import AssetsState, AsyncDataset
from backups2datalad.aioutil import areadcmd
from backups2datalad.config import BackupConfig, Remote, ResourceConfig
from backups2datalad.logging import log as plog
from backups2datalad.manager import Manager
from backups2datalad.zarr import sync_zarr

pytestmark = pytest.mark.anyio


def show_result(r: Result) -> str:
    if r.exception is not None:
        assert isinstance(r.exc_info, tuple)
        return "".join(format_exception(*r.exc_info))
    else:
        return r.output


async def test_backup_command(
    docker_archive: Archive, text_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text(
        f"dandi_instance: {docker_archive.instance_id}\n"
        f"s3bucket: {docker_archive.s3bucket}\n"
        f"s3endpoint: {docker_archive.s3endpoint}\n"
        f"content_url_regex: '{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/'\n"
        "dandisets:\n"
        "  path: ds\n"
    )

    r = await CliRunner().invoke(
        main,
        [
            "--backup-root",
            str(tmp_path),
            "-c",
            str(cfgfile),
            "update-from-backup",
            text_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert_repo_status(tmp_path / "ds")
    ds = Dataset(tmp_path / "ds" / text_dandiset.dandiset_id)
    await text_dandiset.check_backup(ds)
    repo = GitRepo(ds.pathobj)
    state = AssetsState.model_validate_json(
        repo.get_blob("HEAD", ".dandi/assets-state.json")
    )
    d = await text_dandiset.client.get_dandiset(text_dandiset.dandiset_id, "draft")
    # The server seems to sometimes randomly update the Dandiset after the
    # backup command fetches its information, leading to the state timestamp
    # not being equal to the draft modified timestamp.
    assert state.timestamp <= d.version.modified
    last_commit = repo.get_commitish_hash("HEAD")

    r = await CliRunner().invoke(
        main,
        [
            "--backup-root",
            str(tmp_path),
            "-c",
            str(cfgfile),
            "update-from-backup",
            "--mode=verify",
            "--gc-assets",
            text_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert_repo_status(tmp_path / "ds")

    # see above comment about "random" server behavior
    if state.timestamp < d.version.modified:
        assert repo.get_commitish_hash("HEAD^") == last_commit
        assert repo.get_commit_subject("HEAD").endswith("Only some metadata updates")
    else:
        assert repo.get_commitish_hash("HEAD") == last_commit


async def test_populate(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_text("file.txt", "This is test text.\n")
    new_dandiset.add_text("fruit/apple.txt", "Apple\n")
    new_dandiset.add_text("fruit/banana.txt", "Banana\n")
    new_dandiset.add_blob("nulls.dat", b"\0\0\0\0\0")
    new_dandiset.add_blob(
        "hi.txt.gz",
        bytes.fromhex(
            "1f 8b 08 08 0b c1 a0 62 00 03 68 69 2e 74 78 74"
            "00 f3 c8 e4 02 00 9a 3c 22 d5 03 00 00 00"
        ),
    )
    new_dandiset.add_blob(
        "img/png/pixel.png",
        bytes.fromhex(
            "89 50 4e 47 0d 0a 1a 0a 00 00 00 0d 49 48 44 52"
            "00 00 00 01 00 00 00 01 01 00 00 00 00 37 6e f9"
            "24 00 00 00 0a 49 44 41 54 08 99 63 68 00 00 00"
            "82 00 81 cb 13 b2 61 00 00 00 00 49 45 4e 44 ae"
            "42 60 82"
        ),
    )
    new_dandiset.add_zarr("z/sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.add_zarr("z/eye.zarr", np.eye(5))
    await new_dandiset.upload()

    backup_root = tmp_path / "backup"
    remote_root = tmp_path / "remote"
    (remote_root / "ds").mkdir(parents=True, exist_ok=True)
    (remote_root / "zarr").mkdir(parents=True, exist_ok=True)

    cfgfile = tmp_path / "config.yaml"
    cfg = BackupConfig(
        dandi_instance=docker_archive.instance_id,
        s3bucket=docker_archive.s3bucket,
        s3endpoint=docker_archive.s3endpoint,
        content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
        backup_root=backup_root,
        dandisets=ResourceConfig(
            path="ds",
            remote=Remote(
                name="remote-dandisets",
                type="directory",
                options={
                    "directory": str(remote_root / "ds"),
                    "encryption": "none",
                },
            ),
        ),
        zarrs=ResourceConfig(
            path="zarr",
            remote=Remote(
                name="remote-zarrs",
                type="directory",
                options={
                    "directory": str(remote_root / "zarr"),
                    "encryption": "none",
                },
            ),
        ),
    )
    cfg.dump_yaml(cfgfile)

    r = await CliRunner().invoke(
        main,
        ["-c", str(cfgfile), "update-from-backup", new_dandiset.dandiset_id],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert_repo_status(backup_root / "ds")
    ds = Dataset(backup_root / "ds" / new_dandiset.dandiset_id)
    manifest, zarr_manifest = await new_dandiset.check_backup(ds, backup_root / "zarr")

    r = await CliRunner().invoke(
        main, ["-c", str(cfgfile), "populate"], standalone_mode=False
    )
    assert r.exit_code == 0, show_result(r)
    manifest.check(remote_root / "ds")

    r = await CliRunner().invoke(
        main, ["-c", str(cfgfile), "populate-zarrs"], standalone_mode=False
    )
    assert r.exit_code == 0, show_result(r)
    zarr_manifest.check(remote_root / "zarr")


async def test_backup_zarrs(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.add_zarr("z/eye.zarr", np.eye(5))
    await new_dandiset.upload()
    assets = {
        asset.path: asset async for asset in new_dandiset.dandiset.aget_zarr_assets()
    }

    backup_root = tmp_path / "backup"

    cfgfile = tmp_path / "config.yaml"
    cfg = BackupConfig(
        dandi_instance=docker_archive.instance_id,
        s3bucket=docker_archive.s3bucket,
        s3endpoint=docker_archive.s3endpoint,
        content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
        backup_root=backup_root,
        dandisets=ResourceConfig(path="ds"),
        zarrs=ResourceConfig(path="zarr"),
    )
    cfg.dump_yaml(cfgfile)

    # The superdataset needs to be created before creating the Dandiset dataset
    await AsyncDataset(backup_root / "ds").ensure_installed("superdataset")
    ds = AsyncDataset(backup_root / "ds" / new_dandiset.dandiset_id)
    await ds.ensure_installed(f"Dandiset {new_dandiset.dandiset_id}")
    (backup_root / "zarr").mkdir(parents=True, exist_ok=True)

    r = await CliRunner().invoke(
        main,
        ["-c", str(cfgfile), "backup-zarrs", new_dandiset.dandiset_id],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert list((backup_root / "partial-zarrs").iterdir()) == []
    assert sorted(p.name for p in (backup_root / "zarr").iterdir()) == sorted(
        asset.zarr for asset in assets.values()
    )
    await new_dandiset.check_all_zarrs(ds.ds, backup_root / "zarr")


async def test_backup_committed_zarr(
    docker_archive: Archive, new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    new_dandiset.add_zarr("sample.zarr", np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.add_zarr("z/eye.zarr", np.eye(5))
    await new_dandiset.upload()
    assets = {
        asset.path: asset async for asset in new_dandiset.dandiset.aget_zarr_assets()
    }

    backup_root = tmp_path / "backup"

    cfgfile = tmp_path / "config.yaml"
    cfg = BackupConfig(
        dandi_instance=docker_archive.instance_id,
        s3bucket=docker_archive.s3bucket,
        s3endpoint=docker_archive.s3endpoint,
        content_url_regex=f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/",
        backup_root=backup_root,
        dandisets=ResourceConfig(path="ds"),
        zarrs=ResourceConfig(path="zarr"),
    )
    cfg.dump_yaml(cfgfile)

    # The superdataset needs to be created before creating the Dandiset dataset
    await AsyncDataset(backup_root / "ds").ensure_installed("superdataset")
    ds = AsyncDataset(backup_root / "ds" / new_dandiset.dandiset_id)
    await ds.ensure_installed(f"Dandiset {new_dandiset.dandiset_id}")
    (backup_root / "zarr").mkdir(parents=True, exist_ok=True)

    a0 = assets["sample.zarr"]
    (backup_root / "partial-zarrs" / a0.zarr).mkdir(parents=True, exist_ok=True)
    await sync_zarr(
        a0,
        a0.get_digest_value(),
        backup_root / "partial-zarrs" / a0.zarr,
        Manager(config=cfg, gh=None, log=plog, token=new_dandiset.client.token),
    )

    r = await CliRunner().invoke(
        main,
        ["-c", str(cfgfile), "backup-zarrs", new_dandiset.dandiset_id],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert list((backup_root / "partial-zarrs").iterdir()) == []
    assert sorted(p.name for p in (backup_root / "zarr").iterdir()) == sorted(
        asset.zarr for asset in assets.values()
    )
    await new_dandiset.check_all_zarrs(ds.ds, backup_root / "zarr")


async def test_backup_embargoed(
    docker_archive: Archive, embargoed_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    embargoed_dandiset.add_text(
        "file.txt",
        "This is a brand new file that has never been uploaded before.\n"
        f"Date: {datetime.now()}\n"
        f"Random: {random.randrange(4294967296)}\n",
    )

    embargoed_dandiset.add_blob("nulls.dat", b"\0\0\0\0\0")
    embargoed_dandiset.add_blob(
        "hi.txt.gz",
        bytes.fromhex(
            "1f 8b 08 08 0b c1 a0 62 00 03 68 69 2e 74 78 74"
            "00 f3 c8 e4 02 00 9a 3c 22 d5 03 00 00 00"
        ),
    )
    embargoed_dandiset.add_blob(
        "img/png/pixel.png",
        bytes.fromhex(
            "89 50 4e 47 0d 0a 1a 0a 00 00 00 0d 49 48 44 52"
            "00 00 00 01 00 00 00 01 01 00 00 00 00 37 6e f9"
            "24 00 00 00 0a 49 44 41 54 08 99 63 68 00 00 00"
            "82 00 81 cb 13 b2 61 00 00 00 00 49 45 4e 44 ae"
            "42 60 82"
        ),
    )
    await embargoed_dandiset.upload()

    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text(
        f"dandi_instance: {docker_archive.instance_id}\n"
        f"s3bucket: {docker_archive.s3bucket}\n"
        f"s3endpoint: {docker_archive.s3endpoint}\n"
        f"content_url_regex: '{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/'\n"
        "dandisets:\n"
        "  path: ds\n"
    )

    r = await CliRunner().invoke(
        main,
        [
            "--backup-root",
            str(tmp_path),
            "-c",
            str(cfgfile),
            "update-from-backup",
            embargoed_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert_repo_status(tmp_path / "ds")
    ds = Dataset(tmp_path / "ds" / embargoed_dandiset.dandiset_id)
    await embargoed_dandiset.check_backup(ds)

    embargo_status = await areadcmd(
        "git",
        "config",
        "--file",
        ".datalad/config",
        "--get",
        "dandi.dandiset.embargo-status",
        cwd=ds.path,
    )
    assert embargo_status == "EMBARGOED"

    for path in embargoed_dandiset.blob_assets.keys():
        whereis = json.loads(
            await areadcmd("git-annex", "whereis", "--json", "--", path, cwd=ds.path)
        )
        (web_urls,) = [
            w["urls"] for w in whereis["whereis"] if w["description"] == "[datalad]"
        ]
        assert len(web_urls) == 1
        assert web_urls[0].startswith(docker_archive.api_url)

    for path, contents in embargoed_dandiset.text_assets.items():
        p = ds.pathobj / path
        assert p.is_file()
        assert p.read_text() == contents

    await embargoed_dandiset.dandiset.unembargo()

    r = await CliRunner().invoke(
        main,
        [
            "--backup-root",
            str(tmp_path),
            "-c",
            str(cfgfile),
            "update-from-backup",
            embargoed_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    assert_repo_status(tmp_path / "ds")
    ds = Dataset(tmp_path / "ds" / embargoed_dandiset.dandiset_id)
    await embargoed_dandiset.check_backup(ds)

    embargo_status = await areadcmd(
        "git",
        "config",
        "--file",
        ".datalad/config",
        "--get",
        "dandi.dandiset.embargo-status",
        cwd=ds.path,
    )
    assert embargo_status == "OPEN"

    for path in embargoed_dandiset.blob_assets.keys():
        whereis = json.loads(
            await areadcmd("git-annex", "whereis", "--json", "--", path, cwd=ds.path)
        )
        (web_urls,) = [
            w["urls"] for w in whereis["whereis"] if w["description"] == "web"
        ]
        assert len(web_urls) == 2
        assert any(u.startswith(docker_archive.api_url) for u in web_urls)
        assert any(
            u.startswith(f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/")
            for u in web_urls
        )
