from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

import httpx
from identify.identify import tags_from_filename

from .adandi import RemoteBlobAsset
from .aioutil import arequest
from .annex import AsyncAnnex
from .config import BackupConfig
from .logging import PrefixedLogger
from .manager import Manager


@dataclass
class BlobBackup:
    asset: RemoteBlobAsset
    sha256_digest: str
    manager: Manager

    @property
    def path(self) -> str:
        return self.asset.path

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    def is_binary(self) -> bool:
        return "text" not in tags_from_filename(self.path)

    async def get_file_bucket_url(self, s3client: httpx.AsyncClient) -> str:
        self.log.debug("Fetching bucket URL")
        aws_url = self.asset.get_content_url(self.config.content_url_regex)
        urlbits = urlparse(aws_url)
        key = urlbits.path.lstrip("/")
        self.log.debug("About to query S3")
        r = await arequest(
            s3client,
            "HEAD",
            f"https://{self.config.s3bucket}.s3.amazonaws.com/{key}",
        )
        r.raise_for_status()
        version_id = r.headers["x-amz-version-id"]
        self.log.debug("Got bucket URL")
        return urlunparse(urlbits._replace(query=f"versionId={version_id}"))

    async def register_url(self, annex: AsyncAnnex, key: str, url: str) -> None:
        self.log.info("Registering URL %s", url)
        await annex.register_url(key, url)
