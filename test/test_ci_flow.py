"""
The CI flow (see "CI Mirroring" in CLAUDE.md): `plan`, then
`update-from-backup --clone-missing --results-dir`, then `record`, run against
a checkout of the superdataset that has none of the mirrors installed.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any

from conftest import Archive, SampleDandiset
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.config import BackupConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.mirrorstate import (
    KEY_LAST_MODIFIED,
    KEY_STATS,
    EmbargoedPolicy,
    MirrorResult,
    Reason,
    stats_from_key,
)

pytestmark = pytest.mark.anyio


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=path, check=True, capture_output=True, text=True
    ).stdout.strip()


def gitlink(repo: Path, path: str, rev: str = "HEAD") -> str:
    return git(repo, "rev-parse", f"{rev}:{path}")


@pytest.mark.ai_generated
async def test_ci_flow(
    docker_archive: Archive, text_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    did = text_dandiset.dandiset_id

    def config(root: Path, **kwargs: Any) -> BackupConfig:
        return BackupConfig(
            backup_root=root,
            dandi_instance=docker_archive.instance_id,
            s3bucket=docker_archive.s3bucket,
            s3endpoint=docker_archive.s3endpoint,
            content_url_regex=(
                f"{docker_archive.s3endpoint}/{docker_archive.s3bucket}/.*blobs/"
            ),
            **kwargs,
        )

    # A full backup, as on drogon, records the mirror's state in .gitmodules
    full = tmp_path / "full"
    await DandiDatasetter(
        dandi_client=text_dandiset.client, config=config(full)
    ).update_from_backup([did])
    full_superds = full / "dandisets"
    entry = (await AsyncDataset(full_superds).read_gitmodules())[did]
    assert entry[KEY_LAST_MODIFIED]
    assert stats_from_key(entry[KEY_STATS], gitlink(full_superds, did)) is not None

    # Publish both to stand-ins for GitHub
    forge = tmp_path / "forge"
    forge.mkdir()
    mirror_url = str(forge / f"{did}.git")
    git(forge, "clone", "-q", "--bare", str(full_superds / did), mirror_url)
    git(full_superds, "config", "-f", ".gitmodules", f"submodule.{did}.url", mirror_url)
    git(full_superds, "commit", "-q", "-m", "Point at the forge", ".gitmodules")
    superds_url = str(forge / "dandisets.git")
    git(forge, "clone", "-q", "--bare", str(full_superds), superds_url)

    # CI: a checkout without the mirrors
    ci = tmp_path / "ci"
    ci.mkdir()
    git(ci, "clone", "-q", superds_url, "dandisets")
    ci_superds = ci / "dandisets"
    assert not (ci_superds / did / ".git").exists()

    async def plan_ids(root: Path) -> list[tuple[str, Reason]]:
        di = DandiDatasetter(dandi_client=text_dandiset.client, config=config(root))
        plan = await di.plan([did], None, EmbargoedPolicy.SKIP, None)
        return [(d.identifier, d.reason) for d in plan.dandisets]

    assert await plan_ids(ci) == []

    (text_dandiset.dspath / "new.txt").write_text("This is a new file.\n")
    await text_dandiset.upload()
    assert await plan_ids(ci) == [(did, Reason.MODIFIED)]

    results = ci / "results"
    await DandiDatasetter(
        dandi_client=text_dandiset.client,
        config=config(ci, clone_missing=True, results_dir=results),
    ).update_from_backup([did])
    clone = ci_superds / did
    assert (clone / "new.txt").read_text() == "This is a new file.\n"
    # The superdataset was left alone ...
    assert git(ci_superds, "status", "--porcelain", "--ignore-submodules=all") == ""
    # ... and the clone did not record itself in the git-annex branch
    assert git(clone, "config", "annex.private") == "true"
    uuid = git(clone, "config", "annex.uuid")
    assert uuid not in git(clone, "show", "git-annex:uuid.log")
    result = MirrorResult.from_json((results / f"{did}.json").read_text())
    assert result.changed
    assert result.commit == git(clone, "rev-parse", "HEAD")
    assert result.subjects
    assert result.url is None  # registered already

    # Someone else records something meanwhile
    other = tmp_path / "other"
    git(tmp_path, "clone", "-q", superds_url, str(other))
    (other / "README.md").write_text("Concurrent change\n")
    git(other, "add", "README.md")
    git(other, "commit", "-q", "-m", "Concurrent change")
    git(other, "push", "-q", "origin", "HEAD")

    di = DandiDatasetter(dandi_client=text_dandiset.client, config=config(ci))
    await di.record([result], push=True)
    forge_superds = Path(superds_url)
    assert gitlink(forge_superds, did) == result.commit
    assert git(forge_superds, "show", "HEAD:README.md") == "Concurrent change"
    message = git(forge_superds, "log", "-1", "--format=%B")
    assert message.startswith(f"1 updated ({did})")
    recorded = (await AsyncDataset(ci_superds).read_gitmodules())[did]
    assert recorded[KEY_LAST_MODIFIED] == result.keys[KEY_LAST_MODIFIED]
    assert stats_from_key(recorded[KEY_STATS], result.commit) is not None

    # A fresh checkout sees nothing left to do
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    git(fresh, "clone", "-q", superds_url, "dandisets")
    assert await plan_ids(fresh) == []
