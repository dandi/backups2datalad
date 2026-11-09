"""
What the dry run of the CI mirroring (.github/workflows/mirror-dryrun.yml)
relies on, short of the archive: the CLI options that keep a run off GitHub
and the archive's keys, cloning at the recorded commit, and `record` from the
command line.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any, cast

from asyncclick.testing import CliRunner
import pytest

from backups2datalad.__main__ import main
from backups2datalad.adandi import AsyncDandiClient
from backups2datalad.adataset import AsyncDataset, DatasetStats
from backups2datalad.config import BackupConfig
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.mirrorstate import (
    KEY_STATS,
    MirrorResult,
    Plan,
    PlannedDandiset,
    Reason,
)

pytestmark = pytest.mark.anyio


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=path, check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(path: Path, message: str) -> str:
    git(path, "commit", "-q", "--allow-empty", "-m", message)
    return git(path, "rev-parse", "HEAD")


def make_superds(root: Path, did: str, url: str, gitlink: str, **keys: str) -> Path:
    superds = root / "dandisets"
    superds.mkdir(parents=True)
    git(superds, "init", "-q", "-b", "draft")
    lines = [f'[submodule "{did}"]', f"\tpath = {did}", f"\turl = {url}"]
    lines += [f"\t{k} = {v}" for k, v in keys.items()]
    (superds / ".gitmodules").write_text("\n".join(lines) + "\n")
    git(superds, "add", ".gitmodules")
    git(superds, "update-index", "--add", "--cacheinfo", f"160000,{gitlink},{did}")
    git(superds, "commit", "-q", "-m", "Initial")
    return superds


@pytest.fixture
def mirror(tmp_path: Path) -> tuple[Path, str, str]:
    # A mirror on the "forge" whose latest commit the superdataset has not
    # recorded yet
    path = tmp_path / "forge" / "000001"
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "draft")
    recorded = commit(path, "[backups2datalad] recorded")
    latest = commit(path, "[backups2datalad] pushed later")
    return path, recorded, latest


@pytest.mark.ai_generated
@pytest.mark.parametrize("at_gitlink", [False, True])
async def test_clone_dandiset_mirror(
    tmp_path: Path, mirror: tuple[Path, str, str], at_gitlink: bool
) -> None:
    path, recorded, latest = mirror
    root = tmp_path / "ci"
    make_superds(root, "000001", str(path), recorded, **{KEY_STATS: f"{recorded},3,42"})
    di = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, None),
        config=BackupConfig(
            backup_root=root, clone_missing=True, clone_at_gitlink=at_gitlink
        ),
    )
    dsdir = root / "dandisets" / "000001"
    assert await di.clone_dandiset_mirror("000001", dsdir)
    ds = AsyncDataset(dsdir)
    assert await ds.get_commit_hash() == (recorded if at_gitlink else latest)
    assert await ds.get_upstream_commit() == latest
    assert git(dsdir, "config", "annex.private") == "true"
    # Stats are seeded only when recorded for the cloned commit
    assert await ds.get_stored_stats() == (
        DatasetStats(files=3, size=42) if at_gitlink else None
    )
    # Not recorded at all: nothing to clone
    assert not await di.clone_dandiset_mirror("000002", root / "dandisets" / "000002")


@pytest.mark.ai_generated
async def test_clone_at_missing_gitlink(
    tmp_path: Path, mirror: tuple[Path, str, str], caplog: pytest.LogCaptureFixture
) -> None:
    """A recorded commit the mirror lacks: start from the mirror's latest"""
    path, _, latest = mirror
    root = tmp_path / "ci"
    missing = "0" * 39 + "1"
    make_superds(root, "000001", str(path), missing)
    di = DandiDatasetter(
        dandi_client=cast(AsyncDandiClient, None),
        config=BackupConfig(
            backup_root=root, clone_missing=True, clone_at_gitlink=True
        ),
    )
    dsdir = root / "dandisets" / "000001"
    assert await di.clone_dandiset_mirror("000001", dsdir)
    assert await AsyncDataset(dsdir).get_commit_hash() == latest
    assert f"recorded commit {missing} is not in its mirror" in caplog.text


@pytest.mark.ai_generated
async def test_plan_cli_dry_run_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--no-github drops the GitHub organizations; --anonymous needs no key"""
    root = tmp_path / "ci"
    (root / "dandisets").mkdir(parents=True)
    git(root / "dandisets", "init", "-q", "-b", "draft")
    commit(root / "dandisets", "Initial")
    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text(
        "dandisets:\n  path: dandisets\n  github_org: dandisets\n"
        "zarrs:\n  path: dandizarrs\n  github_org: dandizarrs\n"
    )
    seen: dict[str, Any] = {}

    async def fake_plan(self: DandiDatasetter, *_args: Any, **kwargs: Any) -> Plan:
        seen["gh_org"] = self.config.gh_org
        seen["zarr_gh_org"] = self.config.zarr_gh_org
        seen["token"] = self.dandi_client.token
        seen["max"] = kwargs["max_dandisets"]
        return Plan(
            dandisets=[
                PlannedDandiset(
                    identifier="000001",
                    reason=Reason.MODIFIED,
                    size=1,
                    embargoed=False,
                    last_modified=None,
                    source="history",
                )
            ]
        )

    monkeypatch.setattr(DandiDatasetter, "plan", fake_plan)
    monkeypatch.delenv("DANDI_API_KEY", raising=False)
    args = ["-B", str(root), "-c", str(cfgfile), "plan", "--max", "3"]
    r = await CliRunner().invoke(main, args, standalone_mode=False)
    assert r.exit_code != 0  # No key, and not --anonymous
    assert seen == {}
    r = await CliRunner().invoke(
        main, ["--anonymous", "--no-github", *args], standalone_mode=False
    )
    assert r.exit_code == 0, r.output
    assert seen == {"gh_org": None, "zarr_gh_org": None, "token": "", "max": 3}
    plan = json.loads(r.stdout)
    assert plan["dandisets"][0]["id"] == "000001"
    assert plan["dandisets"][0]["state_from"] == "history"


@pytest.mark.ai_generated
async def test_record_cli_no_push(
    tmp_path: Path, mirror: tuple[Path, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path, recorded, latest = mirror
    root = tmp_path / "ci"
    superds = make_superds(root, "000001", str(path), recorded)
    result = MirrorResult(
        identifier="000001",
        commit=latest,
        embargoed=False,
        keys={},
        changed=True,
        subjects=["[backups2datalad] pushed later"],
    )
    rfile = tmp_path / "000001.json"
    rfile.write_text(result.to_json())
    monkeypatch.delenv("DANDI_API_KEY", raising=False)
    r = await CliRunner().invoke(
        main,
        ["--anonymous", "-B", str(root), "record", "--no-push", str(rfile)],
        standalone_mode=False,
    )
    assert r.exit_code == 0, r.output
    assert git(superds, "rev-parse", "HEAD:000001") == latest
    assert git(superds, "log", "-1", "--format=%s").startswith("1 updated (000001)")
