from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os.path
from pathlib import Path, PurePosixPath
import sys
import textwrap
from typing import AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
import httpx
import trio

from . import log
from .annex import AsyncAnnex
from .util import (
    AssetTracker,
    Config,
    Report,
    TextProcess,
    aiter,
    key2hash,
    open_git_annex,
)

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class ToDownload:
    path: str
    url: str
    extra_urls: List[str]
    sha256_digest: str


@dataclass
class Downloader(trio.abc.AsyncResource):
    addurl: TextProcess
    repo: Path
    config: Config
    tracker: AssetTracker
    s3client: httpx.AsyncClient
    nursery: Optional[trio.Nursery] = None
    report: Report = field(init=False, default_factory=Report)
    in_progress: Dict[str, ToDownload] = field(init=False, default_factory=dict)
    annex: AsyncAnnex = field(init=False)
    download_sender: trio.MemorySendChannel[ToDownload] = field(init=False)
    download_receiver: trio.MemoryReceiveChannel[ToDownload] = field(init=False)
    post_sender: trio.MemorySendChannel[Tuple[ToDownload, Optional[str]]] = field(
        init=False
    )
    post_receiver: trio.MemoryReceiveChannel[Tuple[ToDownload, Optional[str]]] = field(
        init=False
    )

    def __post_init__(self) -> None:
        self.annex = AsyncAnnex(self.repo)
        self.download_sender, self.download_receiver = trio.open_memory_channel(0)
        self.post_sender, self.post_receiver = trio.open_memory_channel(0)

    async def aclose(self) -> None:
        await self.annex.aclose()
        await self.download_sender.aclose()

    async def asset_loop(self, dandiset: RemoteDandiset) -> None:
        now = datetime.now(timezone.utc)
        downloading = True
        async with self.download_sender:
            async with aclosing(aiterassets(dandiset)) as aia:  # type: ignore[type-var]
                async for asset in aia:
                    if downloading:
                        try:
                            sha256_digest = asset.get_digest(DigestType.sha2_256)
                        except NotFoundError:
                            log.info(
                                "%s: SHA256 has not been computed yet;"
                                " not downloading any more assets",
                                asset.path,
                            )
                            downloading = False
                        else:
                            assert self.nursery is not None
                            self.nursery.start_soon(
                                self.process_asset,
                                asset,
                                sha256_digest,
                                self.download_sender.clone(),
                                name=f"process_asset:{asset.path}",
                            )
                    # Not `else`, as we want to "fall through" if `downloading`
                    # is negated above.
                    if not downloading:
                        log.info("%s: Will download in a future run", asset.path)
                        self.tracker.mark_future(asset)
                        if now - asset.created > timedelta(days=1):
                            log.error(
                                "%s: Asset created more than a day ago"
                                " but SHA256 digest has not yet been computed",
                                asset.path,
                            )
                            self.report.old_unhashed += 1

    async def process_asset(
        self,
        asset: RemoteAsset,
        sha256_digest: str,
        sender: trio.MemorySendChannel[ToDownload],
    ) -> None:
        async with sender:
            dest = self.repo / asset.path
            if self.tracker.register_asset(asset, force=self.config.force):
                log.debug(
                    "%s: metadata unchanged; not taking any further action",
                    asset.path,
                )
                return
            if not self.config.match_asset(asset.path):
                log.debug("%s: Skipping asset", asset.path)
                return
            log.info("%s: Syncing", asset.path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            to_update = False
            if not (dest.exists() or dest.is_symlink()):
                log.info("%s: Not in dataset; will add", asset.path)
                to_update = True
                self.report.added += 1
            else:
                log.debug("%s: About to fetch hash from annex", asset.path)
                if sha256_digest == await self.get_annex_hash(dest):
                    log.info(
                        "%s: Asset in dataset, and hash shows no modification;"
                        " will not update",
                        asset.path,
                    )
                else:
                    log.info(
                        "%s: Asset in dataset, and hash shows modification; will update",
                        asset.path,
                    )
                    to_update = True
                    self.report.updated += 1
            if to_update:
                bucket_url = await self.get_file_bucket_url(asset)
                dest.unlink(missing_ok=True)
                key = await self.annex.mkkey(
                    PurePosixPath(asset.path).name, asset.size, sha256_digest
                )
                remotes = await self.annex.get_key_remotes(key)
                if remotes is not None:
                    log.info(
                        "%s: Key is known to git-annex; registering new path", asset.path
                    )
                    await self.annex.from_key(key, asset.path)
                    await self.register_url(asset.path, key, bucket_url)
                    await self.register_url(asset.path, key, asset.base_download_url)
                    if (
                        self.config.backup_remote is not None
                        and self.config.backup_remote not in remotes
                    ):
                        log.warn(
                            "%s: Not in backup remote %s",
                            asset.path,
                            self.config.backup_remote,
                        )
                else:
                    log.info("%s: Sending off for download from %s", asset.path, bucket_url)
                    await sender.send(
                        ToDownload(
                            path=asset.path,
                            url=bucket_url,
                            extra_urls=[asset.base_download_url],
                            sha256_digest=sha256_digest,
                        )
                    )

    async def get_file_bucket_url(self, asset: RemoteAsset) -> str:
        log.debug("%s: Fetching bucket URL", asset.path)
        aws_url = asset.get_content_url(self.config.content_url_regex)
        urlbits = urlparse(aws_url)
        key = urlbits.path.lstrip("/")
        log.debug("%s: About to query S3", asset.path)
        # aiobotocore doesn't work with trio, so we have to make the request
        # directly.  Fortunately, it's very simple.
        r = await self.s3client.head(
            f"https://{self.config.s3bucket}.s3.amazonaws.com/{key}"
        )
        r.raise_for_status()
        version_id = r.headers["x-amz-version-id"]
        log.debug("%s: Got bucket URL", asset.path)
        return urlunparse(urlbits._replace(query=f"versionId={version_id}"))

    async def get_annex_hash(self, filepath: Path) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return key2hash(os.path.basename(realpath))
        else:
            log.debug(
                "%s: Not under annex; calculating sha256 digest ourselves", filepath
            )
            return await asha256(filepath)

    async def feed_addurl(self) -> None:
        assert self.addurl.p.stdin is not None
        async with self.addurl.p.stdin:
            async with self.download_receiver:
                async for td in self.download_receiver:
                    self.in_progress[td.path] = td
                    log.info("%s: Downloading from %s", td.path, td.url)
                    await self.addurl.send(f"{td.url} {td.path}\n")
                log.debug("Done feeding URLs to addurl")

    async def read_addurl(self) -> None:
        async with self.post_sender:
            async with aclosing(
                aiter(self.addurl)
            ) as lineiter:  # type: ignore[type-var]
                async for line in lineiter:
                    data = json.loads(line)
                    if "success" not in data:
                        # Progress message
                        log.info(
                            "%s: Downloaded %d / %s bytes (%s)",
                            data["action"]["file"],
                            data["byte-progress"],
                            data.get("total-size", "???"),
                            data.get("percent-progress", "??.??%"),
                        )
                    elif not data["success"]:
                        log.error(
                            "%s: download failed; error messages:\n\n%s",
                            data["file"],
                            textwrap.indent("".join(data["error-messages"]), " " * 4),
                        )
                        self.in_progress.pop(data["file"])
                        self.report.failed += 1
                    else:
                        path = data["file"]
                        key = data.get("key")
                        log.info("%s: Finished downloading (key = %s)", path, key)
                        self.report.downloaded += 1
                        dl = self.in_progress.pop(path)
                        await self.post_sender.send((dl, key))
            log.debug("Done reading from addurl")

    async def postprocess(self) -> None:
        async with self.post_receiver:
            async for dl, key in self.post_receiver:
                if key is not None:
                    for u in dl.extra_urls or []:
                        await self.register_url(dl.path, key, u)
                    annex_hash = key2hash(key)
                    if dl.sha256_digest != annex_hash:
                        log.error(
                            "%s: Hash mismatch!  Dandiarchive reports %s,"
                            " local file has %s",
                            dl.path,
                            dl.sha256_digest,
                            annex_hash,
                        )
                        self.report.hash_mismatches += 1
                else:
                    log.info("%s: Not managed by git annex; not adding URLs", dl.path)
                    assert self.nursery is not None
                    self.nursery.start_soon(
                        self.check_unannexed_hash,
                        dl.path,
                        dl.sha256_digest,
                        name=f"check_unannexed_hash:{dl.path}",
                    )
            log.debug("Done with download post-processing")

    async def register_url(self, path: str, key: str, url: str) -> None:
        log.info("%s: Registering URL %s", path, url)
        await self.annex.register_url(key, url)

    async def check_unannexed_hash(self, asset_path: str, sha256_digest: str) -> None:
        annex_hash = await asha256(self.repo / asset_path)
        if sha256_digest != annex_hash:
            log.error(
                "%s: Hash mismatch!  Dandiarchive reports %s, local file has %s",
                asset_path,
                sha256_digest,
                annex_hash,
            )
            self.report.hash_mismatches += 1


async def async_assets(
    dandiset: RemoteDandiset, repo: Path, config: Config, tracker: AssetTracker
) -> Report:
    async with await open_git_annex(
        "addurl",
        "--batch",
        "--with-files",
        "--jobs",
        str(config.jobs),
        "--json",
        "--json-error-messages",
        "--json-progress",
        "--raw",
        path=repo,
    ) as p:
        async with httpx.AsyncClient() as s3client:
            async with Downloader(p, repo, config, tracker, s3client) as dm:
                async with trio.open_nursery() as nursery:
                    dm.nursery = nursery
                    nursery.start_soon(dm.asset_loop, dandiset)
                    nursery.start_soon(dm.feed_addurl)
                    nursery.start_soon(dm.read_addurl)
                    nursery.start_soon(dm.postprocess)
    return dm.report


async def aiterassets(dandiset: RemoteDandiset) -> AsyncIterator[RemoteAsset]:
    async with httpx.AsyncClient() as client:
        url: Optional[
            str
        ] = f"{dandiset.client.api_url}{dandiset.version_api_path}assets/?order=created"
        while url is not None:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            for item in data["results"]:
                r = await client.get(
                    f"{dandiset.client.api_url}/assets/{item['asset_id']}/"
                )
                r.raise_for_status()
                metadata = r.json()
                yield RemoteAsset.from_data(dandiset, item, metadata)
            url = data.get("next")
    log.info("Finished getting assets from API")


async def asha256(path: Path) -> str:
    log.debug("Starting to compute sha256 digest of %s", path)
    tp = trio.Path(path)
    digester = hashlib.sha256()
    async with await tp.open("rb") as fp:
        while True:
            blob = await fp.read1()
            if blob == b"":
                break
            digester.update(blob)
    log.debug("Finished computing sha256 digest of %s", path)
    return digester.hexdigest()