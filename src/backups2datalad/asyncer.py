from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
import hashlib
import json
from operator import attrgetter
import os
import os.path
from pathlib import Path, PurePosixPath
import subprocess
from types import TracebackType

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from dandi.consts import EmbargoStatus
from dandi.dandiapi import AssetType
from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
import datalad
from datalad.api import clone
import httpx

from .adandi import RemoteAsset, RemoteBlobAsset, RemoteDandiset, RemoteZarrAsset
from .adataset import AssetsState, AsyncDataset
from .aioutil import TextProcess, aruncmd, open_git_annex
from .annex import AsyncAnnex
from .blob import BlobBackup
from .config import BackupConfig, ZarrMode
from .consts import GIT_OPTIONS, USER_AGENT
from .logging import PrefixedLogger, log
from .manager import Manager
from .util import (
    AssetTracker,
    UnexpectedChangeError,
    format_errors,
    key2hash,
    maxdatetime,
    quantify,
)
from .zarr import ZarrLink, sync_zarr


@dataclass
class ToDownload:
    blob: BlobBackup
    url: str


@dataclass
class Report:
    commits: int = 0
    added: int = 0
    updated: int = 0
    registered: int = 0
    downloaded: int = 0
    failed: int = 0
    hash_mismatches: int = 0
    old_unhashed: int = 0

    def update(self, other: Report) -> None:
        self.commits += other.commits
        self.added += other.added
        self.updated += other.updated
        self.registered += other.registered
        self.downloaded += other.downloaded
        self.failed += other.failed
        self.hash_mismatches += other.hash_mismatches
        self.old_unhashed += other.old_unhashed

    def get_commit_message(self) -> str:
        msgparts = []
        if self.added:
            msgparts.append(f"{quantify(self.added, 'file')} added")
        if self.updated:
            msgparts.append(f"{quantify(self.updated, 'file')} updated")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"

    def check(self) -> None:
        errors: list[str] = []
        if self.failed:
            errors.append(f"{quantify(self.failed, 'asset')} failed to download")
        if self.hash_mismatches:
            errors.append(
                f"{quantify(self.hash_mismatches, 'asset')} had the wrong hash"
                " after downloading"
            )
        if self.old_unhashed:
            errors.append(
                f"{quantify(self.old_unhashed, 'asset')} on server had no"
                " SHA256 hash despite advanced age"
            )
        if errors:
            raise RuntimeError(
                f"Errors occurred while downloading: {'; '.join(errors)}"
            )


@dataclass
class Downloader:
    dandiset_id: str
    embargoed: bool
    ds: AsyncDataset
    manager: Manager
    tracker: AssetTracker
    s3client: httpx.AsyncClient
    annex: AsyncAnnex
    nursery: anyio.abc.TaskGroup
    error_on_change: bool = False
    addurl: TextProcess | None = None
    addurl_lock: anyio.Lock = field(init=False, default_factory=anyio.Lock)
    last_timestamp: datetime | None = None
    report: Report = field(init=False, default_factory=Report)
    in_progress: dict[str, ToDownload] = field(init=False, default_factory=dict)
    download_sender: MemoryObjectSendStream[ToDownload] = field(init=False)
    download_receiver: MemoryObjectReceiveStream[ToDownload] = field(init=False)
    zarrs: dict[str, ZarrLink] = field(init=False, default_factory=dict)
    need_add: list[str] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        (
            self.download_sender,
            self.download_receiver,
        ) = anyio.create_memory_object_stream(0)

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    @property
    def repo(self) -> Path:
        return self.ds.pathobj

    async def __aenter__(self) -> Downloader:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            if self.addurl is not None:
                await self.addurl.aclose()
        else:
            with anyio.CancelScope(shield=True):
                if self.addurl is not None:
                    await self.addurl.force_aclose()

    async def asset_loop(self, aia: AsyncIterator[RemoteAsset | None]) -> None:
        now = datetime.now(timezone.utc)
        downloading = True
        async with self.download_sender:
            async for asset in aia:
                if asset is None:
                    break
                self.tracker.remote_assets.add(asset.path)
                if downloading:
                    if asset.asset_type == AssetType.ZARR:
                        if self.embargoed:
                            raise RuntimeError(
                                f"Dandiset {self.dandiset_id} is embargoed and"
                                f" contains a Zarr at {asset.path}; do not know"
                                " how to handle"
                            )
                        try:
                            zarr_digest = asset.get_digest_value()
                        except NotFoundError:
                            self.log.info(
                                "%s: Zarr checksum has not been computed yet;"
                                " not downloading any more assets",
                                asset.path,
                            )
                            downloading = False
                        else:
                            assert isinstance(asset, RemoteZarrAsset)
                            self.nursery.start_soon(
                                self.process_zarr,
                                asset,
                                zarr_digest,
                                name=f"process_zarr:{asset.path}",
                            )
                    else:
                        assert isinstance(asset, RemoteBlobAsset)
                        try:
                            sha256_digest = asset.get_digest_value(DigestType.sha2_256)
                            assert sha256_digest is not None
                        except NotFoundError:
                            self.log.info(
                                "%s: SHA256 has not been computed yet;"
                                " not downloading any more assets",
                                asset.path,
                            )
                            downloading = False
                        else:
                            blob = BlobBackup(
                                asset=asset,
                                sha256_digest=sha256_digest,
                                manager=self.manager.with_sublogger(
                                    f"Asset {asset.path}"
                                ),
                            )
                            self.nursery.start_soon(
                                self.process_blob,
                                blob,
                                self.download_sender.clone(),
                                name=f"process_blob:{asset.path}",
                            )
                # Not `else`, as we want to "fall through" if `downloading`
                # is negated above.
                if not downloading:
                    self.log.info("%s: Will download in a future run", asset.path)
                    self.tracker.mark_future(asset)
                    if (
                        asset.asset_type == AssetType.BLOB
                        and now - asset.created > timedelta(days=1)
                    ):
                        try:
                            asset.get_digest_value(DigestType.sha2_256)
                        except NotFoundError:
                            self.log.error(
                                "%s: Asset created more than a day ago"
                                " but SHA256 digest has not yet been computed",
                                asset.path,
                            )
                            self.report.old_unhashed += 1

    async def process_blob(
        self,
        blob: BlobBackup,
        sender: MemoryObjectSendStream[ToDownload],
    ) -> None:
        async with sender:
            self.last_timestamp = maxdatetime(self.last_timestamp, blob.asset.created)
            dest = self.repo / blob.path
            md_diff = self.tracker.register_asset(blob.asset, force=self.config.force)
            if md_diff is None:
                blob.log.debug("metadata unchanged; not taking any further action")
                self.tracker.finish_asset(blob.path)
                return
            if not self.config.match_asset(blob.path):
                blob.log.debug("Skipping asset")
                self.tracker.finish_asset(blob.path)
                return
            if self.error_on_change:
                raise UnexpectedChangeError(
                    f"Dandiset {self.dandiset_id}: Metadata for asset"
                    f" {blob.path} was changed/added but draft timestamp was"
                    f" not updated on server:\n\nMetadata diff:\n\n{md_diff}\n"
                )
            blob.log.info("Syncing")
            dest.parent.mkdir(parents=True, exist_ok=True)
            to_update = False
            if not (dest.exists() or dest.is_symlink()):
                blob.log.info("Not in dataset; will add")
                to_update = True
                self.report.added += 1
            else:
                blob.log.debug("About to fetch hash from annex")
                if blob.sha256_digest == await self.get_annex_hash(dest):
                    blob.log.info(
                        "Asset in dataset, and hash shows no modification;"
                        " will not update",
                    )
                    self.tracker.finish_asset(blob.path)
                else:
                    blob.log.info(
                        "Asset in dataset, and hash shows modification; will update",
                    )
                    to_update = True
                    self.report.updated += 1
            if to_update:
                await self.ds.remove(blob.path)
                if blob.is_binary():
                    blob.log.info("File is binary; registering key with git-annex")
                    key = await self.annex.mkkey(
                        PurePosixPath(blob.path).name,
                        blob.asset.size,
                        blob.sha256_digest,
                    )
                    await self.annex.from_key(key, blob.path)
                    if not self.embargoed:
                        bucket_url = await blob.get_file_bucket_url(self.s3client)
                        await blob.register_url(self.annex, key, bucket_url)
                    await blob.register_url(
                        self.annex, key, blob.asset.base_download_url
                    )
                    remotes = await self.annex.get_key_remotes(key)
                    if (
                        remotes is not None
                        and self.config.dandisets.remote is not None
                        and self.config.dandisets.remote.name not in remotes
                    ):
                        blob.log.info(
                            "Not in backup remote %r",
                            self.config.dandisets.remote.name,
                        )
                    self.tracker.finish_asset(blob.path)
                    self.report.registered += 1
                elif blob.asset.size > (10 << 20):
                    raise RuntimeError(
                        f"{blob.path} identified as text but is"
                        f" {blob.asset.size} bytes!"
                    )
                else:
                    await self.ensure_addurl()
                    url = blob.asset.base_download_url
                    blob.log.info(
                        "File is text; sending off for download from %s",
                        url,
                    )
                    await sender.send(ToDownload(blob=blob, url=url))

    async def process_zarr(
        self, asset: RemoteZarrAsset, zarr_digest: str | None
    ) -> None:
        if self.manager.config.zarr_mode is ZarrMode.ASSET_CHECKSUM:
            if not self.tracker.register_asset_by_timestamp(
                asset, force=self.config.force
            ):
                self.log.info(
                    "Zarr %s: asset timestamp up to date; not syncing", asset.zarr
                )
                self.tracker.finish_asset(asset.path)
                return
        else:
            self.tracker.register_asset(asset, force=self.config.force)
        self.tracker.finish_asset(asset.path)
        # In case the Zarr is empty:
        self.last_timestamp = maxdatetime(self.last_timestamp, asset.created)
        if asset.zarr in self.zarrs:
            self.zarrs[asset.zarr].asset_paths.append(asset.path)
        elif self.config.zarr_root is None:
            raise RuntimeError(
                f"Zarr encountered in Dandiset {self.dandiset_id} but"
                " Zarr backups not configured in config file"
            )
        else:
            zarr_dspath = self.config.zarr_root / asset.zarr
            zl = ZarrLink(
                zarr_dspath=zarr_dspath,
                timestamp=None,
                asset_paths=[asset.path],
            )
            self.nursery.start_soon(
                partial(sync_zarr, link=zl, error_on_change=self.error_on_change),
                asset,
                zarr_digest,
                zarr_dspath,
                self.manager.with_sublogger(f"Zarr {asset.zarr}"),
            )
            self.zarrs[asset.zarr] = zl

    async def get_annex_hash(self, filepath: Path) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return key2hash(os.path.basename(realpath))
        else:
            self.log.debug(
                "%s: Not under annex; calculating sha256 digest ourselves", filepath
            )
            return await self.asha256(filepath)

    async def ensure_addurl(self) -> None:
        async with self.addurl_lock:
            if self.addurl is None:
                env = os.environ.copy()
                if self.embargoed:
                    env["DATALAD_dandi_token"] = self.manager.token
                    opts = ["--raw-except=datalad"]
                else:
                    opts = ["--raw"]
                self.addurl = await open_git_annex(
                    "addurl",
                    "-c",
                    "annex.alwayscompact=false",
                    "--batch",
                    "--with-files",
                    "--jobs",
                    str(self.manager.config.jobs),
                    "--json",
                    "--json-error-messages",
                    "--json-progress",
                    *opts,
                    path=self.ds.pathobj,
                    env=env,
                )
                self.nursery.start_soon(self.feed_addurl)
                self.nursery.start_soon(self.read_addurl)

    async def feed_addurl(self) -> None:
        assert self.addurl is not None
        assert self.addurl.p.stdin is not None
        async with self.addurl.p.stdin:
            async with self.download_receiver:
                async for td in self.download_receiver:
                    if not self.in_progress:
                        # Lock dataset while there are any downloads in
                        # progress in order to prevent conflicts with `git rm`.
                        await self.ds.lock.acquire()
                    self.in_progress[td.blob.path] = td
                    td.blob.log.info("Downloading from %s", td.url)
                    await self.addurl.send(f"{td.url} {td.blob.path}\n")
                self.log.debug("Done feeding URLs to addurl")

    def pop_in_progress(self, path: str) -> ToDownload:
        val = self.in_progress.pop(path)
        if not self.in_progress:
            self.ds.lock.release()
        return val

    async def read_addurl(self) -> None:
        assert self.addurl is not None
        async for line in self.addurl:
            data = json.loads(line)
            if "byte-progress" in data:
                # Progress message
                self.log.info(
                    "%s: Downloaded %d / %s bytes (%s)",
                    data["action"]["file"],
                    data["byte-progress"],
                    data.get("total-size", "???"),
                    data.get("percent-progress", "??.??%"),
                )
            elif not data["success"]:
                msg = format_errors(data["error-messages"])
                self.log.error("%s: download failed:%s", data["file"], msg)
                self.pop_in_progress(data["file"])
                if "exited 123" in msg:
                    self.log.info(
                        "Will try `git add`ing %s manually later", data["file"]
                    )
                    self.need_add.append(data["file"])
                else:
                    self.report.failed += 1
            else:
                path = data["file"]
                key = data.get("key")
                self.log.info("%s: Finished downloading (key = %s)", path, key)
                self.report.downloaded += 1
                dl = self.pop_in_progress(path)
                self.tracker.finish_asset(dl.blob.path)
                self.nursery.start_soon(
                    self.check_unannexed_hash,
                    dl.blob,
                    name=f"check_unannexed_hash:{dl.blob.path}",
                )
        self.log.debug("Done reading from addurl")

    async def check_unannexed_hash(self, blob: BlobBackup) -> None:
        annex_hash = await self.asha256(self.repo / blob.path)
        if blob.sha256_digest != annex_hash:
            blob.log.error(
                "Hash mismatch!  Dandiarchive reports %s, local file has %s",
                blob.sha256_digest,
                annex_hash,
            )
            self.report.hash_mismatches += 1

    async def asha256(self, path: Path) -> str:
        self.log.debug("Starting to compute sha256 digest of %s", path)
        tp = anyio.Path(path)
        digester = hashlib.sha256()
        async with await tp.open("rb") as fp:
            while True:
                blob = await fp.read(65535)
                if blob == b"":
                    break
                digester.update(blob)
        self.log.debug("Finished computing sha256 digest of %s", path)
        return digester.hexdigest()


async def async_assets(
    dandiset: RemoteDandiset,
    ds: AsyncDataset,
    manager: Manager,
    tracker: AssetTracker,
    error_on_change: bool = False,
) -> Report:
    if datalad.support.external_versions.external_versions["cmd:annex"] < "10.20220724":
        raise RuntimeError(
            "git-annex does not support annex.alwayscompact=false;"
            " v10.20220724 required"
        )
    done_flag = anyio.Event()
    total_report = Report()
    async with aclosing(aiterassets(dandiset, done_flag)) as aia:
        while not done_flag.is_set():
            try:
                async with (
                    AsyncAnnex(ds.pathobj) as annex,
                    httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as s3client,
                    anyio.create_task_group() as nursery,
                ):
                    dm = Downloader(
                        dandiset_id=dandiset.identifier,
                        embargoed=dandiset.embargo_status is EmbargoStatus.EMBARGOED,
                        ds=ds,
                        manager=manager,
                        tracker=tracker,
                        s3client=s3client,
                        annex=annex,
                        nursery=nursery,
                        error_on_change=error_on_change,
                    )
                    async with dm:
                        nursery.start_soon(dm.asset_loop, aia)
            finally:
                tracker.dump()

            await ds.add(".dandi/assets.json")

            for fpath in dm.need_add:
                manager.log.info("Manually running `git add %s`", fpath)
                try:
                    await ds.call_git("add", fpath)
                except subprocess.CalledProcessError:
                    manager.log.error("Manual `git add %s` failed", fpath)
                    dm.report.failed += 1

            timestamp = dm.last_timestamp
            for zarr_id, zarrlink in dm.zarrs.items():
                # We've left the task group, so all of the Zarr tasks have
                # finished and set the timestamps in link
                ts = zarrlink.timestamp
                if ts is not None:
                    timestamp = maxdatetime(timestamp, ts)
                for asset_path in zarrlink.asset_paths:
                    if not (ds.pathobj / asset_path).exists():
                        if error_on_change:
                            raise UnexpectedChangeError(
                                f"Dandiset {dandiset.identifier}: Zarr asset"
                                f" added at {asset_path} but draft timestamp"
                                " was not updated on server"
                            )
                        manager.log.info("Zarr asset added at %s; cloning", asset_path)
                        dm.report.downloaded += 1
                        dm.report.added += 1
                        assert manager.config.zarr_root is not None
                        zarr_path = manager.config.zarr_root / zarr_id
                        if manager.config.zarr_gh_org is not None:
                            src = (
                                "https://github.com/"
                                f"{manager.config.zarr_gh_org}/{zarr_id}"
                            )
                        else:
                            src = str(zarr_path)
                        await anyio.to_thread.run_sync(
                            partial(clone, source=src, path=ds.pathobj / asset_path)
                        )
                        if manager.config.zarr_gh_org is not None:
                            await aruncmd(
                                "git",
                                *GIT_OPTIONS,
                                "remote",
                                "rename",
                                "origin",
                                "github",
                                cwd=ds.pathobj / asset_path,
                            )
                        await ds.add_submodule(
                            path=asset_path,
                            url=src,
                            datalad_id=await AsyncDataset(zarr_path).get_datalad_id(),
                        )
                        manager.log.debug("Finished cloning Zarr to %s", asset_path)
                    elif ts is not None:
                        manager.log.info(
                            "Zarr asset modified at %s; updating", asset_path
                        )
                        dm.report.downloaded += 1
                        dm.report.updated += 1
                        assert zarrlink.commit_hash is not None
                        await ds.update_submodule(
                            asset_path, commit_hash=zarrlink.commit_hash
                        )
                        manager.log.debug("Finished updating Zarr at %s", asset_path)

            if dandiset.version_id == "draft":
                if dm.report.registered or dm.report.downloaded:
                    manager.log.info(
                        "%s registered, %s downloaded for this version segment;"
                        " committing",
                        quantify(dm.report.registered, "asset"),
                        quantify(dm.report.downloaded, "asset"),
                    )
                    assert timestamp is not None
                    if done_flag.is_set():
                        ts = dandiset.version.modified
                    else:
                        ts = timestamp
                    await ds.set_assets_state(AssetsState(timestamp=ts))
                    manager.log.debug("Checking whether repository is dirty ...")
                    if await ds.is_dirty():
                        manager.log.info("Committing changes")
                        await ds.commit(
                            message=dm.report.get_commit_message(),
                            commit_date=timestamp,
                        )
                        manager.log.debug("Commit made")
                        manager.log.debug("Running `git gc`")
                        await ds.gc()
                        manager.log.debug("Finished running `git gc`")
                        total_report.commits += 1
                    else:
                        manager.log.debug("Repository is clean")
                else:
                    if done_flag.is_set():
                        await ds.set_assets_state(
                            AssetsState(timestamp=dandiset.version.modified)
                        )
                    manager.log.info(
                        "No assets downloaded for this version segment; not committing"
                    )
            else:
                await ds.set_assets_state(
                    AssetsState(timestamp=dandiset.version.created)
                )
            total_report.update(dm.report)
    return total_report


async def aiterassets(
    dandiset: RemoteDandiset, done_flag: anyio.Event
) -> AsyncGenerator[RemoteAsset | None, None]:
    last_ts: datetime | None = None
    if dandiset.version_id == "draft":
        vs = [v async for v in dandiset.aget_versions(include_draft=False)]
        vs.sort(key=attrgetter("created"))
        versions = deque(vs)
    else:
        versions = deque()
    async for asset in dandiset.aget_assets():
        assert last_ts is None or last_ts <= asset.created, (
            f"Asset {asset.path} created at {asset.created} but"
            f" returned after an asset created at {last_ts}!"
        )
        if (
            versions
            and (last_ts is None or last_ts < versions[0].created)
            and asset.created >= versions[0].created
        ):
            log.info(
                "Dandiset %s: All assets up to creation of version %s found;"
                " will commit soon",
                dandiset.identifier,
                versions[0].identifier,
            )
            versions.popleft()
            yield None
        last_ts = asset.created
        yield asset
    log.info("Dandiset %s: Finished getting assets from API", dandiset.identifier)
    done_flag.set()
