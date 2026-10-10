"""
Tests for the error with which ``update-from-backup`` ends when backing up
some Dandisets failed, which names them so that the log need not be searched.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator
import logging
from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import cast

from dandi.dandiapi import Version
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset
from backups2datalad.aioutil import pool_amap
from backups2datalad.util import MAX_FAILED_NAMED, describe_failed_dandisets

pytestmark = pytest.mark.anyio


def make_dandiset(identifier: str) -> RemoteDandiset:
    return RemoteDandiset(
        aclient=cast(AsyncDandiClient, SimpleNamespace()),
        identifier=identifier,
        version=Version.model_validate(
            {
                "version": "draft",
                "name": f"Dandiset {identifier}",
                "asset_count": 0,
                "size": 0,
                "status": "Valid",
                "created": "2020-08-17T19:48:58.540000Z",
                "modified": "2026-03-25T19:59:19.751604Z",
            }
        ),
    )


@pytest.mark.ai_generated
def test_describe_failed_dandisets_names_all() -> None:
    """Up to `MAX_FAILED_NAMED` failures are all named, sorted, with no hint"""
    assert (
        describe_failed_dandisets(["001412", "000026", "000571"], Path("x.log"))
        == "Backups for 3 Dandisets failed: 000026, 000571, 001412"
    )
    assert (
        describe_failed_dandisets(["000026"]) == "Backups for 1 Dandiset failed: 000026"
    )
    ids = [f"{i:06d}" for i in range(MAX_FAILED_NAMED)]
    msg = describe_failed_dandisets(ids, Path("x.log"))
    assert msg == (
        f"Backups for {MAX_FAILED_NAMED} Dandisets failed: " + ", ".join(ids)
    )


@pytest.mark.ai_generated
def test_describe_failed_dandisets_too_many() -> None:
    """Past `MAX_FAILED_NAMED`, the first are named and the log command given"""
    ids = [f"{i:06d}" for i in range(MAX_FAILED_NAMED + 2, 0, -1)]
    logfile = Path("/mnt/backup/dandisets/.git/dandi/backups2datalad/x y.log")
    named = ", ".join(sorted(ids)[:MAX_FAILED_NAMED])
    assert describe_failed_dandisets(ids, logfile) == (
        f"Backups for {MAX_FAILED_NAMED + 2} Dandisets failed: {named}, ..."
        " (2 more)\nList them all with: sed -nE"
        r" '/Job failed/s,.*Dandiset ([0-9]{6})/.*,\1,gp'"
        " '/mnt/backup/dandisets/.git/dandi/backups2datalad/x y.log'"
    )
    # Without a log file (no `debug_logfile()`), the log is all we can point to
    assert describe_failed_dandisets(ids) == (
        f"Backups for {MAX_FAILED_NAMED + 2} Dandisets failed: {named}, ..."
        " (2 more); see the 'Job failed' lines in the log for the rest"
    )


@pytest.fixture
def logfile(tmp_path: Path) -> Iterator[Path]:
    """Log to a file the way `DandiDatasetter.debug_logfile()` does"""
    path = tmp_path / "2026.10.09.23.30.21Z.log"
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    logger = logging.getLogger("backups2datalad")
    logger.addHandler(handler)
    try:
        yield path
    finally:
        logger.removeHandler(handler)
        handler.close()


@pytest.mark.ai_generated
async def test_failed_dandisets_hint_lists_all(logfile: Path) -> None:
    """The command given lists every Dandiset `pool_amap()` logged as failed"""
    ids = [f"{i:06d}" for i in range(1, MAX_FAILED_NAMED + 6)]

    async def dandisets() -> AsyncGenerator[RemoteDandiset, None]:
        for did in ids:
            yield make_dandiset(did)

    async def fail(d: RemoteDandiset) -> None:
        raise RuntimeError(f"{d} (a Dandiset 999999/draft) is broken")

    report = await pool_amap(fail, dandisets(), workers=3)
    msg = describe_failed_dandisets((d.identifier for d in report.failed), logfile)
    cmd = msg.splitlines()[-1].removeprefix("List them all with: ")
    assert cmd != msg.splitlines()[-1]
    r = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    assert sorted(r.stdout.split()) == ids
