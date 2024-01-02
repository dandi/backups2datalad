from __future__ import annotations

from contextlib import aclosing

from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
import httpx

from .adandi import RemoteBlobAsset, RemoteDandiset
from .adataset import AsyncDataset
from .annex import AsyncAnnex
from .blob import BlobBackup
from .consts import USER_AGENT
from .manager import Manager
from .util import key2hash


async def register_s3urls(
    manager: Manager, dandiset: RemoteDandiset, ds: AsyncDataset
) -> None:
    paths2keys = {
        anxfile.file: anxfile.key async for anxfile in ds.aiter_annexed_files()
    }
    async with AsyncAnnex(ds.pathobj) as annex, httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT}
    ) as s3client, aclosing(dandiset.aget_assets()) as ait:
        async for asset in ait:
            if isinstance(asset, RemoteBlobAsset):
                try:
                    sha256_digest = asset.get_digest_value(DigestType.sha2_256)
                    assert sha256_digest is not None
                except NotFoundError:
                    raise NotImplementedError
                else:
                    blob = BlobBackup(
                        asset=asset,
                        sha256_digest=sha256_digest,
                        manager=manager.with_sublogger(f"Asset {asset.path}"),
                    )
                    try:
                        key = paths2keys[blob.path]
                    except KeyError:
                        continue
                    if key2hash(key) == blob.sha256_digest:
                        bucket_url = await blob.get_file_bucket_url(s3client)
                        await blob.register_url(annex, key, bucket_url)
            # else: asset is a Zarr and thus could not have been added while
            # embargoed and thus is not missing S3 URLs
