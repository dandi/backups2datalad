from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from urllib.parse import quote, quote_plus

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore import UNSIGNED
from pydantic import BaseModel
from zarr_checksum.tree import ZarrChecksumTree

from .adandi import RemoteZarrAsset
from .adataset import AsyncDataset
from .annex import AsyncAnnex
from .config import BackupConfig, ZarrMode
from .consts import MAX_ZARR_SYNCS
from .logging import PrefixedLogger
from .manager import Manager
from .util import UnexpectedChangeError, is_meta_file, key2hash, maxdatetime, quantify

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import ObjectTypeDef


OLD_CHECKSUM_FILE = Path(".zarr-checksum")
CHECKSUM_FILE = Path(".dandi", "zarr-checksum")

SYNC_FILE = Path(".dandi", "s3sync.json")


class SyncData(BaseModel):
    bucket: str
    prefix: str
    last_modified: datetime | None


@dataclass
class ZarrLink:
    zarr_dspath: Path
    timestamp: datetime | None
    asset_paths: list[str]
    commit_hash: str | None = None


@dataclass
class ZarrEntry:
    path: str
    size: int
    md5_digest: str
    last_modified: datetime
    bucket_url: str

    def __str__(self) -> str:
        return self.path

    @property
    def parts(self) -> tuple[str, ...]:
        return tuple(self.path.split("/"))

    @property
    def name(self) -> str:
        return self.parts[-1]

    @property
    def parents(self) -> Iterator[str]:
        parts = self.parts
        while parts:
            parts = parts[:-1]
            yield "/".join(parts)


@dataclass
class ZarrReport:
    added: int = 0
    updated: int = 0
    deleted: int = 0
    checksum: bool = False

    def __bool__(self) -> bool:
        return bool(self.added or self.updated or self.deleted or self.checksum)

    def get_summary(self) -> str:
        msgparts = []
        if self.added:
            msgparts.append(f"{quantify(self.added, 'file')} added")
        if self.updated:
            msgparts.append(f"{quantify(self.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if self.checksum:
            msgparts.append("checksum updated")
        if not msgparts:
            msgparts.append("No changes")
        return ", ".join(msgparts)


@dataclass
class ZarrSyncer:
    asset: RemoteZarrAsset
    ds: AsyncDataset
    annex: AsyncAnnex
    config: BackupConfig
    backup_remote: str | None
    checksum: str | None
    log: PrefixedLogger
    last_timestamp: datetime | None = None
    error_on_change: bool = False
    report: ZarrReport = field(default_factory=ZarrReport)
    _local_checksum: str | None = None

    @property
    def api_url(self) -> str:
        return self.asset.aclient.api_url

    @property
    def zarr_id(self) -> str:
        return self.asset.zarr

    @property
    def repo(self) -> Path:
        return self.ds.pathobj

    @property
    def s3bucket(self) -> str:
        return self.config.s3bucket

    @property
    def s3prefix(self) -> str:
        return f"zarr/{self.zarr_id}/"

    @property
    def mode(self) -> ZarrMode:
        return self.config.zarr_mode

    async def run(self) -> None:
        last_sync = self.read_sync_file()
        async with aclosing(self.annex.list_files()) as fileiter:
            to_delete = {f async for f in fileiter if not is_meta_file(f)}
        async with get_session().create_client(
            "s3",
            config=AioConfig(signature_version=UNSIGNED),
            endpoint_url=self.config.s3endpoint,
        ) as client:
            if not await self.needs_sync(client, last_sync, to_delete):
                self.log.info("backup up to date")
                return
            self.log.info("sync needed")
            i = 0
            while True:
                orig_checksum = await self.get_local_checksum()
                zcc = ZarrChecksumTree()
                to_update = []
                async with aclosing(self.aiter_file_entries(client)) as ait:
                    async for entry in ait:
                        if is_meta_file(str(entry)):
                            raise RuntimeError(
                                f"Zarr {self.zarr_id} contains file at meta path"
                                f" {str(entry)!r}"
                            )
                        self.log.debug("%s: Syncing", entry)
                        zcc.add_leaf(Path(entry.path), entry.size, entry.md5_digest)
                        to_delete.discard(str(entry))
                        if self.mode is ZarrMode.TIMESTAMP:
                            if (
                                last_sync is not None
                                and entry.last_modified < last_sync
                            ):
                                self.log.debug(
                                    "%s: file not modified since last backup", entry
                                )
                                continue
                            self.check_change(f"entry {entry!r} was modified/added")
                        dest = self.repo / str(entry)
                        conflicted = False
                        if dest.is_dir():
                            # File path is replacing a directory, which needs
                            # to be deleted
                            self.check_change(
                                "path type conflict between server & backup for"
                                f" {str(entry)!r}"
                            )
                            self.log.debug(
                                "%s: deleting conflicting directory at same path",
                                entry,
                            )
                            to_delete |= await self.under_tree(dest)
                            conflicted = True
                        else:
                            for ep in entry.parents:
                                pp = self.repo / ep
                                if pp.is_file() or pp.is_symlink():
                                    # Annexed file at parent path of `entry`
                                    # needs to be replaced with a directory
                                    self.check_change(
                                        f"backup path {str(ep)!r} conflicts with"
                                        f" server path {str(entry)!r}"
                                    )
                                    self.log.debug(
                                        "%s: deleting conflicting file path %s",
                                        entry,
                                        ep,
                                    )
                                    to_delete.add(str(ep))
                                    conflicted = True
                                    break
                                elif pp.is_dir():
                                    break
                        if conflicted or not (dest.exists() or dest.is_symlink()):
                            self.check_change(f"entry {str(entry)!r} added")
                            self.log.debug("%s: Not in dataset; will add", entry)
                            to_update.append(entry)
                            self.report.added += 1
                        else:
                            self.log.debug("%s: About to fetch hash from annex", entry)
                            if entry.md5_digest == self.get_annex_hash(dest):
                                self.log.debug(
                                    "%s: File in dataset, and hash shows no"
                                    " modification; will not update",
                                    entry,
                                )
                            else:
                                self.check_change(f"entry {str(entry)!r} modified")
                                self.log.debug(
                                    "%s: Asset in dataset, and hash shows"
                                    " modification; will update",
                                    entry,
                                )
                                to_update.append(entry)
                                to_delete.add(str(entry))
                                self.report.updated += 1
                await self.prune_deleted(to_delete)
                for entry in to_update:
                    key = await self.annex.mkkey(
                        entry.name, entry.size, entry.md5_digest
                    )
                    remotes = await self.annex.get_key_remotes(key)
                    await self.annex.from_key(key, str(entry))
                    await self.register_url(str(entry), key, entry.bucket_url)
                    prefix = quote_plus(str(entry))
                    await self.register_url(
                        str(entry),
                        key,
                        (
                            f"{self.api_url}/zarr/{self.zarr_id}/files"
                            f"?prefix={prefix}&download=true"
                        ),
                    )
                    if (
                        remotes is not None
                        and self.backup_remote is not None
                        and self.backup_remote not in remotes
                    ):
                        self.log.info(
                            "%s: Not in backup remote %s",
                            entry,
                            self.backup_remote,
                        )
                final_checksum = str(zcc.process())
                modern_asset = await self.asset.refetch()
                changed_during_sync = self.asset.modified != modern_asset.modified
                if changed_during_sync:
                    self.log.info(
                        "`modified` timestamp on server changed during backup"
                    )
                    if orig_checksum != final_checksum:
                        self.log.info("Local content changed during sync")
                remote_checksum = modern_asset.get_digest_value()
                if remote_checksum is None:
                    self.log.info("Checksum still not available from server")
                elif final_checksum != remote_checksum:
                    if changed_during_sync:
                        self.log.warning(
                            "Zarr was modified during backup and there is a"
                            " checksum mismatch: local=%s, remote=%s",
                            final_checksum,
                            remote_checksum,
                        )
                    else:
                        i += 1
                        if i < MAX_ZARR_SYNCS:
                            self.log.warning(
                                "Local checksum %r differs from remote checksum"
                                " %r after backup, and no change on server was"
                                " detected; running sync again",
                                final_checksum,
                                remote_checksum,
                            )
                            continue
                        else:
                            raise RuntimeError(
                                f"Zarr {self.zarr_id}: local checksum"
                                f" {final_checksum!r} differs from remote"
                                f" checksum {remote_checksum!r} after backup,"
                                " and no change on server was detected"
                            )
                break
        if self.get_stored_checksum() != final_checksum:
            self.check_change("checksum modified")
            self.log.info("Updating checksum file")
            (self.repo / CHECKSUM_FILE).parent.mkdir(exist_ok=True)
            (self.repo / CHECKSUM_FILE).write_text(f"{final_checksum}\n")
            self.report.checksum = True
            await self.ds.add(str(CHECKSUM_FILE))
        # Remove a possibly still-present previous location for the checksum
        # file:
        if (self.repo / OLD_CHECKSUM_FILE).exists():
            if self.error_on_change:
                raise UnexpectedChangeError(
                    f"Dandiset {self.asset.dandiset_id}: Zarr {self.zarr_id}:"
                    " old checksum file present, but we are in verify mode"
                )
            await self.ds.remove(str(OLD_CHECKSUM_FILE))
        self.write_sync_file()
        await self.ds.add(str(SYNC_FILE))

    def read_sync_file(self) -> datetime | None:
        sync_file_path = self.repo / SYNC_FILE
        if sync_file_path.exists() or sync_file_path.is_symlink():
            with sync_file_path.open() as fp:
                data = SyncData.model_validate(json.load(fp))
        else:
            return None
        if data.bucket != self.s3bucket:
            raise RuntimeError(
                f"Bucket {self.s3bucket!r} for Zarr {self.zarr_id} does not"
                f" match bucket in {SYNC_FILE} ({data.bucket!r})"
            )
        if data.prefix != self.s3prefix:
            raise RuntimeError(
                f"Key prefix {self.s3prefix!r} for Zarr {self.zarr_id} does not"
                f" match prefix in {SYNC_FILE} ({data.prefix!r})"
            )
        return data.last_modified

    def write_sync_file(self) -> None:
        data = SyncData(
            bucket=self.s3bucket,
            prefix=self.s3prefix,
            last_modified=self.last_timestamp,
        )
        (self.repo / SYNC_FILE).parent.mkdir(exist_ok=True)
        (self.repo / SYNC_FILE).write_text(data.model_dump_json(indent=4) + "\n")

    async def needs_sync(
        self, client: S3Client, last_sync: datetime | None, local_paths: set[str]
    ) -> bool:
        if self.mode is ZarrMode.FORCE:
            return True
        elif self.mode is ZarrMode.TIMESTAMP:
            if last_sync is None:
                return True
            local_paths = local_paths.copy()
            # We fetch a list of all objects from the server here (using
            # `list_objects_v2`) in order to decide whether to sync, and then
            # the actual syncing fetches all objects again using
            # `list_object_versions`.  The latter endpoint is the only one that
            # includes version IDs, yet it's also considerably slower than
            # `list_objects_v2`, so we try to optimize for the presumed-common
            # case of Zarrs rarely being modified.
            leadlen = len(self.s3prefix)
            async with aclosing(self.aiter_objects(client)) as ao:
                async for obj in ao:
                    path = obj["Key"][leadlen:]
                    try:
                        local_paths.remove(path)
                    except KeyError:
                        self.check_change(f"entry {path!r} added")
                        self.log.info("%s on server but not in backup", path)
                        return True
                    if obj["LastModified"] > last_sync:
                        self.check_change(f"entry {path!r} modified")
                        self.log.info(
                            "%s was modified on server at %s, after last sync at %s",
                            path,
                            obj["LastModified"],
                            last_sync,
                        )
                        return True
            if local_paths:
                self.check_change(f"{quantify(len(local_paths), 'file')} deleted")
                self.log.info(
                    "%s in local backup but no longer on server",
                    quantify(len(local_paths), "file"),
                )
                return True
            return False
        else:
            assert self.mode in (ZarrMode.CHECKSUM, ZarrMode.ASSET_CHECKSUM)
            stored_checksum = self.get_stored_checksum()
            if stored_checksum is None:
                self.log.info("No checksum stored for Zarr")
                return True
            elif stored_checksum != self.checksum:
                self.check_change("Checksum on server differs from stored checksum")
                self.log.info("Checksum on server differs from stored checksum")
                return True
            elif stored_checksum != await self.get_local_checksum():
                self.check_change(
                    "Checksum computed for local entries is not as expected"
                )
                self.log.info("Checksum computed for local entries is not as expected")
                return True
            else:
                return False

    async def under_tree(self, dirpath: Path) -> set[str]:
        reldir = dirpath.relative_to(self.repo)
        async with aclosing(self.annex.list_files(reldir)) as fileiter:
            return {f async for f in fileiter}

    async def prune_deleted(self, to_delete: set[str]) -> None:
        if to_delete:
            self.check_change(f"{quantify(len(to_delete), 'file')} deleted from Zarr")
        self.log.info("deleting extra files")
        for p in to_delete:
            self.log.debug("deleting %s", p)
        self.report.deleted += len(to_delete)
        await self.ds.remove_batch(to_delete)
        for p in to_delete:
            d = self.repo / PurePosixPath(p).parent
            while d != self.repo and (not d.exists() or not any(d.iterdir())):
                with suppress(FileNotFoundError):
                    d.rmdir()
                d = d.parent
        self.log.info("finished deleting extra files")

    async def aiter_objects(
        self, client: S3Client
    ) -> AsyncGenerator[ObjectTypeDef, None]:
        async for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=self.s3bucket, Prefix=self.s3prefix
        ):
            for obj in page.get("Contents", []):
                yield obj

    async def aiter_file_entries(
        self, client: S3Client
    ) -> AsyncGenerator[ZarrEntry, None]:
        leadlen = len(self.s3prefix)
        async for page in client.get_paginator("list_object_versions").paginate(
            Bucket=self.s3bucket, Prefix=self.s3prefix
        ):
            for v in page.get("Versions", []):
                if v["IsLatest"]:
                    self.last_timestamp = maxdatetime(
                        self.last_timestamp, v["LastModified"]
                    )
                    yield ZarrEntry(
                        path=v["Key"][leadlen:],
                        size=v["Size"],
                        md5_digest=v["ETag"].strip('"'),
                        last_modified=v["LastModified"],
                        bucket_url=f"{self.config.bucket_url}/{quote(v['Key'])}?versionId={v['VersionId']}",
                    )
            for dm in page.get("DeleteMarkers", []):
                if dm["IsLatest"]:
                    self.last_timestamp = maxdatetime(
                        self.last_timestamp, dm["LastModified"]
                    )

    def get_annex_hash(self, filepath: Path) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return key2hash(os.path.basename(realpath))
        else:
            raise RuntimeError(f"{filepath} unexpectedly not under git-annex")

    async def register_url(self, path: str, key: str, url: str) -> None:
        self.log.info("%s: Registering URL %s", path, url)
        await self.annex.register_url(key, url)

    async def get_local_checksum(self) -> str:
        if self._local_checksum is None:
            self._local_checksum = await self.ds.compute_zarr_checksum()
        return self._local_checksum

    def get_stored_checksum(self) -> str | None:
        try:
            return (self.repo / CHECKSUM_FILE).read_text().strip()
        except FileNotFoundError:
            return None

    def check_change(self, event: str) -> None:
        if self.error_on_change:
            raise UnexpectedChangeError(
                f"Dandiset {self.asset.dandiset_id}: Zarr {self.zarr_id}:"
                f" {event}, but Dandiset draft timestamp was not updated on"
                " server"
            )


async def sync_zarr(
    asset: RemoteZarrAsset,
    checksum: str | None,
    dsdir: Path,
    manager: Manager,
    link: ZarrLink | None = None,
    error_on_change: bool = False,
) -> None:
    async with manager.config.zarr_limit:
        assert manager.config.zarrs is not None
        ds = AsyncDataset(dsdir)
        if error_on_change and not ds.pathobj.exists():
            raise UnexpectedChangeError(
                f"Dandiset {asset.dandiset_id}: Zarr {asset.zarr} added to"
                f" Dandiset at {asset.path!r} but draft timestamp was not"
                " updated on server"
            )
        await ds.ensure_installed(
            desc=f"Zarr {asset.zarr}",
            commit_date=asset.created,
            backup_remote=manager.config.zarrs.remote,
            backend="MD5E",
            cfg_proc=None,
        )
        if not (ds.pathobj / ".dandi" / ".gitattributes").exists():
            manager.log.debug("Excluding .dandi/ from git-annex")
            (ds.pathobj / ".dandi").mkdir(parents=True, exist_ok=True)
            (ds.pathobj / ".dandi" / ".gitattributes").write_text(
                "* annex.largefiles=nothing\n"
            )
            await ds.add(".dandi/.gitattributes")
            await ds.commit(
                message="Exclude .dandi/ from git-annex",
                paths=[".dandi/.gitattributes"],
                commit_date=asset.created,
                check_dirty=False,
            )
        if (zgh := manager.config.zarrs.github_org) is not None:
            manager.log.debug("Creating GitHub sibling")
            await ds.create_github_sibling(
                owner=zgh, name=asset.zarr, backup_remote=manager.config.zarrs.remote
            )
            manager.log.debug("Created GitHub sibling")
        if await ds.is_dirty():
            raise RuntimeError(
                f"Zarr {asset.zarr} in Dandiset {asset.dandiset_id} is dirty;"
                " clean or save before running"
            )
        async with AsyncAnnex(dsdir, digest_type="MD5") as annex:
            if (r := manager.config.zarrs.remote) is not None:
                backup_remote = r.name
            else:
                backup_remote = None
            zsync = ZarrSyncer(
                asset=asset,
                ds=ds,
                annex=annex,
                config=manager.config,
                backup_remote=backup_remote,
                checksum=checksum,
                log=manager.log,
                error_on_change=error_on_change,
            )
            await zsync.run()
        report = zsync.report
        if report or await zsync.ds.is_dirty():
            if report:
                summary = report.get_summary()
            else:
                summary = "No changes to zarr content, some other changes"
            manager.log.info("%s; committing", summary)
            if zsync.last_timestamp is None:
                commit_ts = asset.created
            else:
                commit_ts = zsync.last_timestamp
            await ds.commit(
                message=f"[backups2datalad] {summary}", commit_date=commit_ts
            )
            manager.log.debug("Commit made")
            manager.log.debug("Running `git gc`")
            await ds.gc()
            manager.log.debug("Finished running `git gc`")
            if manager.config.zarr_gh_org is not None:
                manager.log.debug("Pushing to GitHub")
                await ds.push(to="github", jobs=manager.config.jobs, data="nothing")
                manager.log.debug("Finished pushing to GitHub")
            if link is not None:
                link.timestamp = commit_ts
        else:
            manager.log.info("no changes; not committing")
        if link is not None:
            if manager.gh is not None:
                stats = await ds.get_stats(config=manager.config)
                await manager.set_zarr_description(asset.zarr, stats)
            link.commit_hash = await ds.get_commit_hash()
