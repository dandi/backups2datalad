"""
Tests for applying the ``annex.largefiles`` policy to datasets.

These tests require `git-annex`.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

import pytest

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
