from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import subprocess

from dandi.consts import EmbargoStatus
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.mirrorstate import (
    KEY_ACCESS,
    KEY_LAST_MODIFIED,
    KEY_STATS,
    KEY_TAGGED_RELEASES,
    EmbargoedPolicy,
    MirrorResult,
    Reason,
    RemoteInfo,
    mirror_state_keys,
    parse_gitmodules_config,
    plan_updates,
    stats_from_key,
)

pytestmark = pytest.mark.anyio

T0 = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
NOW = T0 + timedelta(days=1)


def remote(
    did: str,
    modified: datetime = T0,
    size: int = 100,
    status: EmbargoStatus = EmbargoStatus.OPEN,
    published: str | None = None,
) -> RemoteInfo:
    return RemoteInfo(
        identifier=did,
        modified=modified,
        embargo_status=status,
        size=size,
        latest_published=published,
    )


def recorded(
    last_modified: datetime | None = T0,
    tags: str | None = None,
    access: str = "public",
) -> dict[str, str]:
    entry = {"path": "x", "url": "u", KEY_ACCESS: access}
    if last_modified is not None:
        entry[KEY_LAST_MODIFIED] = last_modified.isoformat()
    if tags is not None:
        entry[KEY_TAGGED_RELEASES] = tags
    return entry


@pytest.mark.ai_generated
def test_parse_gitmodules_config_names_with_dots() -> None:
    listing = (
        "submodule.000003.path\n000003\0"
        "submodule.000003.dandiset-stats\nabc,1,2\0"
        "submodule.sub-1/a.ome.zarr.url\nhttps://example.com/z\0"
        "core.bare\nfalse\0"
    )
    assert parse_gitmodules_config(listing) == {
        "000003": {"path": "000003", "dandiset-stats": "abc,1,2"},
        "sub-1/a.ome.zarr": {"url": "https://example.com/z"},
    }


@pytest.mark.ai_generated
def test_mirror_state_keys_hide_embargoed_stats() -> None:
    public = mirror_state_keys(T0, ["0.2", "0.1"], ("c0ffee", 3, 42), embargoed=False)
    assert public == {
        KEY_LAST_MODIFIED: T0.isoformat(),
        KEY_TAGGED_RELEASES: "0.1,0.2",
        KEY_STATS: "c0ffee,3,42",
    }
    embargoed = mirror_state_keys(T0, [], ("c0ffee", 3, 42), embargoed=True)
    assert embargoed == {
        KEY_LAST_MODIFIED: T0.isoformat(),
        KEY_TAGGED_RELEASES: None,
        KEY_STATS: None,
    }


@pytest.mark.ai_generated
def test_stats_from_key() -> None:
    assert stats_from_key("abc,3,42", "abc") == (3, 42)
    assert stats_from_key("abc,3,42", "def") is None
    assert stats_from_key("garbage", "abc") is None
    assert stats_from_key(None, "abc") is None


@pytest.mark.ai_generated
def test_mirror_result_roundtrip() -> None:
    r = MirrorResult(
        identifier="000003",
        commit="abc",
        embargoed=True,
        keys={KEY_STATS: None},
        subjects=["[backups2datalad] 1 file added"],
    )
    assert MirrorResult.from_json(r.to_json()) == r
    assert r.access == "private"


@pytest.mark.ai_generated
def test_plan_reasons() -> None:
    gm = {
        "000001": recorded(),
        "000002": recorded(),
        "000003": recorded(last_modified=None),
        "000004": recorded(tags="0.1"),
        "000005": recorded(access="private"),
        "000006": recorded(tags="0.1"),
    }
    plan = plan_updates(
        [
            remote("000001"),  # up to date
            remote("000002", modified=T0 + timedelta(hours=1)),
            remote("000003"),
            remote("000004", published="0.2"),
            remote("000005"),  # unembargoed, recorded as private
            remote("000006", published="0.1"),  # up to date
            remote("000007"),
        ],
        gm,
        now=NOW,
    )
    assert {d.identifier: d.reason for d in plan.dandisets} == {
        "000002": Reason.MODIFIED,
        "000003": Reason.NO_STATE,
        "000004": Reason.UNTAGGED,
        "000005": Reason.EMBARGO,
        "000007": Reason.NEW,
    }
    forced = plan_updates([remote("000001")], gm, now=NOW, force=True)
    assert [d.reason for d in forced.dandisets] == [Reason.FORCED]


@pytest.mark.ai_generated
def test_plan_skips() -> None:
    gm = {"000001": recorded(), "000009": recorded(), "000010": recorded()}
    newer = T0 + timedelta(hours=1)
    plan = plan_updates(
        [
            remote("000001", modified=NOW - timedelta(seconds=5)),
            remote("000002", status=EmbargoStatus.UNEMBARGOING),
            remote("000003", status=EmbargoStatus.EMBARGOED),
            remote("000004", modified=newer),
            remote("001412", modified=newer),
        ],
        {**gm, "000008": recorded(access="private")},
        exclude=re.compile("001412"),
        quiescent_period=30,
        now=NOW,
    )
    assert [d.identifier for d in plan.dandisets] == ["000004"]
    assert plan.quiescent == 1
    assert plan.unembargoing == 1
    assert plan.embargoed_skipped == 1
    # Deleted from the archive; 000008's mirror is private already
    assert plan.deleted == ["000009", "000010"]
    # Embargoed IDs never appear in the JSON of a "skip" plan
    assert "000003" not in str(plan.as_json())


@pytest.mark.ai_generated
def test_plan_embargoed_only() -> None:
    plan = plan_updates(
        [remote("000003", status=EmbargoStatus.EMBARGOED), remote("000004")],
        {"000009": recorded()},
        now=NOW,
        embargoed=EmbargoedPolicy.ONLY,
    )
    assert [(d.identifier, d.embargoed) for d in plan.dandisets] == [("000003", True)]
    # Deletions are the public run's business
    assert plan.deleted == []


@pytest.mark.ai_generated
def test_plan_cap_orders_by_size_but_keeps_the_stalest() -> None:
    remotes = []
    gm = {}
    for i in range(10):
        did = f"00000{i}"
        # 000000 is both the biggest and stale the longest
        gm[did] = recorded(last_modified=T0 + timedelta(minutes=i))
        remotes.append(remote(did, modified=NOW - timedelta(hours=1), size=1000 - i))
    plan = plan_updates(remotes, gm, now=NOW, max_dandisets=4)
    assert [d.identifier for d in plan.dandisets] == [
        "000009",
        "000008",
        "000007",
        "000000",
    ]
    assert plan.deferred == 6


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=path, check=True, capture_output=True, text=True
    ).stdout


@pytest.mark.ai_generated
async def test_update_gitmodules_keys(tmp_path: Path) -> None:
    git(tmp_path, "init", "-q")
    (tmp_path / ".gitmodules").write_text(
        '[submodule "000003"]\n\tpath = 000003\n\tdandiset-stats = old,1,1\n'
    )
    ds = AsyncDataset(tmp_path)
    current = await ds.read_gitmodules()
    assert await ds.update_gitmodules_keys(
        "000003",
        {KEY_STATS: None, KEY_LAST_MODIFIED: T0.isoformat(), "path": "000003"},
        current["000003"],
    )
    assert (await ds.read_gitmodules())["000003"] == {
        "path": "000003",
        KEY_LAST_MODIFIED: T0.isoformat(),
    }
    # Nothing to change
    assert not await ds.update_gitmodules_keys(
        "000003", {KEY_STATS: None}, (await ds.read_gitmodules())["000003"]
    )


def make_mirror(path: Path) -> str:
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "draft")
    git(path, "commit", "-q", "--allow-empty", "-m", f"[backups2datalad] {path.name}")
    return git(path, "rev-parse", "HEAD").strip()


@pytest.mark.ai_generated
async def test_record_registers_updates_and_retries(tmp_path: Path) -> None:
    from typing import cast

    from backups2datalad.adandi import AsyncDandiClient
    from backups2datalad.config import BackupConfig
    from backups2datalad.datasetter import DandiDatasetter

    # A superdataset recording 000001, published to a bare "forge" repo
    origin = tmp_path / "origin"
    old = make_mirror(origin / "000001")
    git(origin, "init", "-q", "-b", "draft")
    (origin / ".gitmodules").write_text(
        '[submodule "000001"]\n\tpath = 000001\n\turl = ./000001\n'
        "\tdatalad-id = id-1\n"
    )
    git(origin, "add", ".gitmodules")
    git(origin, "update-index", "--add", "--cacheinfo", f"160000,{old},000001")
    git(origin, "commit", "-q", "-m", "Initial")
    forge = tmp_path / "forge.git"
    git(tmp_path, "clone", "-q", "--bare", str(origin), str(forge))
    ci = tmp_path / "ci" / "dandisets"
    git(tmp_path, "clone", "-q", str(forge), str(ci))
    # Someone else pushes meanwhile
    other = tmp_path / "other"
    git(tmp_path, "clone", "-q", str(forge), str(other))
    (other / "README.md").write_text("hi\n")
    git(other, "add", "README.md")
    git(other, "commit", "-q", "-m", "Concurrent")
    git(other, "push", "-q", "origin", "HEAD")

    new1 = make_mirror(tmp_path / "m1")
    new2 = make_mirror(tmp_path / "m2")
    keys = mirror_state_keys(T0, ["0.1"], (new1, 1, 2), embargoed=False)
    results = [
        MirrorResult(
            identifier="000001",
            commit=new1,
            embargoed=False,
            keys=keys,
            changed=True,
            subjects=["[backups2datalad] 1 file added"],
        ),
        MirrorResult(
            identifier="000002",
            commit=new2,
            embargoed=True,
            keys=mirror_state_keys(T0, [], (new2, 1, 2), embargoed=True),
            changed=True,
            url="git@github.com:dandisets/000002.git",
            datalad_id="id-2",
            subjects=["[backups2datalad] initial"],
        ),
    ]
    di = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, None),
        config=BackupConfig(backup_root=ci.parent),
    )
    await di.record(results, push=True)
    assert git(forge, "rev-parse", "HEAD:000001").strip() == new1
    assert git(forge, "rev-parse", "HEAD:000002").strip() == new2
    assert git(forge, "show", "HEAD:README.md") == "hi\n"
    message = git(forge, "log", "-1", "--format=%B")
    assert message.startswith("1 added (000002), 1 updated (000001)")
    assert " - [backups2datalad] 1 file added" in message
    gm = parse_gitmodules_config(
        git(forge, "config", "--blob", "HEAD:.gitmodules", "-z", "--list")
    )
    assert gm["000001"][KEY_STATS] == f"{new1},1,2"
    assert gm["000001"][KEY_TAGGED_RELEASES] == "0.1"
    assert gm["000002"]["url"] == "git@github.com:dandisets/000002.git"
    assert gm["000002"]["datalad-id"] == "id-2"
    assert KEY_STATS not in gm["000002"]
    # Recording the same again changes nothing
    before = git(forge, "rev-parse", "HEAD")
    await di.record(results, push=True)
    assert git(forge, "rev-parse", "HEAD") == before
