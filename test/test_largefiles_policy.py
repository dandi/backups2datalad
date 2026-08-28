"""
Tests for applying the ``annex.largefiles`` policy to datasets.

These tests require `git-annex`.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

from asyncclick.testing import CliRunner
import pytest

from backups2datalad.__main__ import main
from backups2datalad.adataset import AsyncDataset
from backups2datalad.gitattributes import (
    LARGEFILES_EXPRESSION,
    TEXT_SIZE_LIMIT_BYTES,
    set_policy,
)
from backups2datalad.util import load_metadata_json
from test_gitattributes import TEXT2GIT

pytestmark = pytest.mark.anyio


def check_attr(dspath: Path, path: str) -> str:
    r = subprocess.run(
        ["git", "check-attr", "annex.largefiles", "--", path],
        cwd=dspath,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return r.stdout.rstrip("\n").split(": ")[-1]


@pytest.mark.ai_generated
async def test_ensure_installed_applies_policy(tmp_path: Path) -> None:
    ds = AsyncDataset(tmp_path / "ds")
    assert await ds.ensure_installed("test dataset")
    attributes = (ds.pathobj / ".gitattributes").read_text()
    assert attributes == set_policy(attributes)
    # Assets and Dandiset metadata alike are subject to the size limit ...
    assert check_attr(ds.pathobj, "sub-01/sub-01_ephys.tsv") == LARGEFILES_EXPRESSION
    assert check_attr(ds.pathobj, ".dandi/assets.json") == LARGEFILES_EXPRESSION
    # ... but Git's own files are not:
    assert check_attr(ds.pathobj, ".gitmodules") == "nothing"
    assert not await ds.is_dirty()


@pytest.mark.ai_generated
async def test_ensure_gitattributes_migrates(tmp_path: Path) -> None:
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("test dataset")
    # Put the dataset back in the state that `datalad create -c text2git` would
    # have left it in:
    (tmp_path / ".gitattributes").write_text(TEXT2GIT)
    await ds.save(
        "Restore legacy .gitattributes",
        commit_date=datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc),
    )
    commit_date = await ds.get_last_commit_date()
    assert commit_date == datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc)

    assert await ds.ensure_gitattributes("test dataset", commit_date=commit_date)
    assert (tmp_path / ".gitattributes").read_text() == set_policy(TEXT2GIT)
    assert not await ds.is_dirty()
    # The migration must not introduce a jump in commit timestamps:
    assert await ds.get_last_commit_date() == commit_date
    assert check_attr(tmp_path, "sub-01/sub-01_ephys.tsv") == LARGEFILES_EXPRESSION
    assert check_attr(tmp_path, ".gitmodules") == "nothing"

    # Applying the policy again is a no-op:
    assert not await ds.ensure_gitattributes("test dataset", commit_date=commit_date)


@pytest.mark.ai_generated
async def test_get_largefiles_impact(tmp_path: Path) -> None:
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("test dataset")
    (tmp_path / "small.txt").write_text("This is test text.\n" * 100)
    (tmp_path / "big.txt").write_text("This is test text.\n" * 600_000)
    assert (tmp_path / "big.txt").stat().st_size > TEXT_SIZE_LIMIT_BYTES
    await ds.save("Add files")
    # The policy was in effect when the files were added, so each is where it
    # belongs:
    impact = await ds.get_largefiles_impact()
    assert impact.to_annex == []
    assert impact.maybe_to_git == []
    assert not impact

    # With a lower limit, the text file in Git would be annexed:
    impact = await ds.get_largefiles_impact(limit=1024)
    to_annex = [f.path for f in impact.to_annex]
    assert "small.txt" in to_annex
    assert "big.txt" not in to_annex
    # Git's own files are exempt from the limit and so are never reported:
    assert not any(Path(p).name.startswith(".git") for p in to_annex)
    assert impact.to_annex_size == sum(f.size or 0 for f in impact.to_annex)
    largest = impact.largest_in_git
    assert largest is not None and largest.path == "small.txt"

    # With a higher limit, the annexed text file would go into Git:
    impact = await ds.get_largefiles_impact(limit=TEXT_SIZE_LIMIT_BYTES * 2)
    assert [f.path for f in impact.maybe_to_git] == ["big.txt"]
    assert impact.maybe_to_git[0].size == (tmp_path / "big.txt").stat().st_size
    assert impact.to_annex == []


@pytest.mark.ai_generated
def test_load_metadata_json(tmp_path: Path) -> None:
    filepath = tmp_path / "assets.json"
    assert load_metadata_json(filepath) == []
    filepath.write_text(json.dumps([{"path": "foo.txt"}]))
    assert load_metadata_json(filepath) == [{"path": "foo.txt"}]


@pytest.mark.ai_generated
async def test_policy_commit_is_not_a_backup_commit(tmp_path: Path) -> None:
    """
    The `[backups2datalad]` prefix marks the commits that back up a Dandiset's
    assets, and the test suite pairs those commits with the contents of
    `.dandi/assets.json`.  The commit that configures the policy has no
    `assets.json`, so it must not carry the prefix.
    """
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("test dataset")
    subjects = subprocess.run(
        ["git", "log", "--format=%s"],
        cwd=tmp_path,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.splitlines()
    assert "Configure annex.largefiles policy" in subjects
    assert not [s for s in subjects if "[backups2datalad]" in s]


@pytest.mark.ai_generated
async def test_load_metadata_json_annexed(tmp_path: Path) -> None:
    """
    A large enough `.dandi/assets.json` is annexed, and its content then has to
    be fetched before it can be read.
    """
    ds = AsyncDataset(tmp_path / "ds")
    assert await ds.ensure_installed("test dataset")
    filepath = ds.pathobj / ".dandi" / "assets.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    metadata = [{"path": "sub-01/sub-01_ecephys.nwb"}]
    filepath.write_text(json.dumps(metadata))
    # `--force-large` annexes the file regardless of its size, saving the test
    # from having to write out something over the limit:
    await ds.call_annex("add", "--force-large", ".dandi/assets.json")
    await ds.commit("Add an annexed assets.json", check_dirty=False)
    assert filepath.is_symlink()

    # Drop the content, having first copied it somewhere it can be fetched
    # back from:
    (tmp_path / "store").mkdir()
    await ds.call_annex(
        "initremote",
        "store",
        "type=directory",
        f"directory={tmp_path / 'store'}",
        "encryption=none",
    )
    await ds.call_annex("copy", "--to=store", ".dandi/assets.json")
    await ds.call_annex("drop", ".dandi/assets.json")
    assert not filepath.exists()

    assert load_metadata_json(filepath) == metadata


@pytest.mark.ai_generated
async def test_check_largefiles_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DANDI_API_KEY", "dummy")
    root = tmp_path / "dandisets"
    await AsyncDataset(root).ensure_installed("superdataset")
    ds = AsyncDataset(root / "000001")
    assert await ds.ensure_installed("Dandiset 000001")
    (ds.pathobj / "big.txt").write_text("This is test text.\n" * 600_000)
    # Add the file the way the old policy would have: straight into Git
    await ds.call_git("-c", "annex.largefiles=nothing", "add", "big.txt")
    await ds.commit("Add a large text file to Git", check_dirty=False)

    # A Dandiset that complies with the policy, and so is not reported at all:
    assert await AsyncDataset(root / "000002").ensure_installed("Dandiset 000002")

    # A Dandiset with an annexed text file that the policy would put in Git:
    ds3 = AsyncDataset(root / "000003")
    assert await ds3.ensure_installed("Dandiset 000003")
    (ds3.pathobj / "small.txt").write_text("This is test text.\n")
    await ds3.call_annex("add", "--force-large", "small.txt")
    await ds3.commit("Annex a small text file", check_dirty=False)

    r = await CliRunner().invoke(
        main, ["-B", str(tmp_path), "-l", "WARNING", "check-largefiles"]
    )
    assert r.exit_code == 0, r.output
    assert "000001" in r.output
    assert "big.txt" in r.output
    # Nothing is stored on the wrong side in 000002, so it gets no row:
    assert "000002" not in r.output
    # 000003 has nothing in Git to report as the largest:
    assert "000003" in r.output

    # A single Dandiset can be named on the command line:
    r = await CliRunner().invoke(
        main, ["-B", str(tmp_path), "-l", "WARNING", "check-largefiles", "000001"]
    )
    assert r.exit_code == 0, r.output
    assert "1 Dandiset checked" in r.output

    # ... and Dandisets can be excluded:
    r = await CliRunner().invoke(
        main,
        ["-B", str(tmp_path), "-l", "WARNING", "check-largefiles", "-e", "00000[13]"],
    )
    assert r.exit_code == 0, r.output
    assert "1 Dandiset checked" in r.output
    assert "big.txt" not in r.output

    r = await CliRunner().invoke(
        main, ["-B", str(tmp_path), "-l", "WARNING", "check-largefiles", "--json"]
    )
    assert r.exit_code == 0, r.output
    lines = r.output.splitlines()
    report = json.loads("\n".join(lines[lines.index("[") :]))
    assert [
        (
            d["dandiset"],
            [f["path"] for f in d["to_annex"]],
            [f["path"] for f in d["maybe_to_git"]],
        )
        for d in report
    ] == [
        ("000001", ["big.txt"], []),
        ("000002", [], []),
        ("000003", [], ["small.txt"]),
    ]

    r = await CliRunner().invoke(
        main, ["-B", str(tmp_path), "check-largefiles", "--limit", "bogus"]
    )
    assert r.exit_code != 0
    assert "Invalid size specification" in r.output
