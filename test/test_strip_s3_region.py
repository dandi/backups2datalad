"""Tests for undoing the Region in S3 URLs (dandi-archive#2962)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from dandi.dandiapi import Version
import pytest

from backups2datalad.adandi import (
    AsyncDandiClient,
    RemoteBlobAsset,
    RemoteDandiset,
    RemoteZarrAsset,
    strip_s3_region,
)

BLOB_KEY = "blobs/dd9/f84/dd9f8493-87ff-4191-9738-70ac2824ea81"
ZARR_KEY = "zarr/0f6e7cc7-d9b7-4e5d-8f8a-3b3a2b4f2c11/"
DOWNLOAD_URL = (
    "https://api.dandiarchive.org/api/assets/"
    "a5571daa-2b18-45d2-a758-e204456e7e5b/download/"
)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "url,expected",
    [
        (
            f"https://dandiarchive.s3.us-east-2.amazonaws.com/{BLOB_KEY}",
            f"https://dandiarchive.s3.amazonaws.com/{BLOB_KEY}",
        ),
        (
            f"https://dandiarchive.s3.us-east-2.amazonaws.com/{ZARR_KEY}",
            f"https://dandiarchive.s3.amazonaws.com/{ZARR_KEY}",
        ),
        (
            f"https://dandiarchive.s3.amazonaws.com/{BLOB_KEY}",
            f"https://dandiarchive.s3.amazonaws.com/{BLOB_KEY}",
        ),
        (DOWNLOAD_URL, DOWNLOAD_URL),
        (
            f"http://localhost:9000/dandi-dandisets/{BLOB_KEY}",
            f"http://localhost:9000/dandi-dandisets/{BLOB_KEY}",
        ),
    ],
)
def test_strip_s3_region(url: str, expected: str) -> None:
    assert strip_s3_region(url) == expected


def make_dandiset() -> RemoteDandiset:
    return RemoteDandiset(
        aclient=cast(AsyncDandiClient, SimpleNamespace()),
        identifier="000026",
        version=Version.model_validate(
            {
                "version": "draft",
                "name": "Test Dandiset",
                "asset_count": 1,
                "size": 1132,
                "status": "Valid",
                "created": "2020-08-17T19:48:58.540000Z",
                "modified": "2026-03-25T19:59:19.751604Z",
            }
        ),
    )


def asset_data(**kwargs: Any) -> dict[str, Any]:
    return {
        "asset_id": "a5571daa-2b18-45d2-a758-e204456e7e5b",
        "path": "derivatives/EPIC/dataset_description.json",
        "size": 1132,
        "created": "2021-07-03T13:11:11.495525Z",
        "modified": "2021-07-03T13:11:11.495525Z",
        "blob": None,
        "zarr": None,
        **kwargs,
    }


@pytest.mark.ai_generated
def test_from_data_strips_s3_region_blob() -> None:
    asset = RemoteBlobAsset.from_data(
        make_dandiset(),
        asset_data(
            blob="dd9f8493-87ff-4191-9738-70ac2824ea81",
            metadata={
                "contentSize": 1132,
                "contentUrl": [
                    DOWNLOAD_URL,
                    f"https://dandiarchive.s3.us-east-2.amazonaws.com/{BLOB_KEY}",
                ],
            },
        ),
    )
    assert isinstance(asset, RemoteBlobAsset)
    assert asset.metadata["contentUrl"] == [
        DOWNLOAD_URL,
        f"https://dandiarchive.s3.amazonaws.com/{BLOB_KEY}",
    ]
    # What `.dandi/assets.json` records:
    assert asset.json_dict()["metadata"]["contentUrl"][1] == (
        f"https://dandiarchive.s3.amazonaws.com/{BLOB_KEY}"
    )


@pytest.mark.ai_generated
def test_from_data_strips_s3_region_zarr() -> None:
    asset = RemoteZarrAsset.from_data(
        make_dandiset(),
        asset_data(
            zarr="0f6e7cc7-d9b7-4e5d-8f8a-3b3a2b4f2c11",
            metadata={
                "contentUrl": [
                    DOWNLOAD_URL,
                    f"https://dandiarchive.s3.us-east-2.amazonaws.com/{ZARR_KEY}",
                ],
            },
        ),
    )
    assert isinstance(asset, RemoteZarrAsset)
    assert asset.metadata["contentUrl"] == [
        DOWNLOAD_URL,
        f"https://dandiarchive.s3.amazonaws.com/{ZARR_KEY}",
    ]
