"""
Asset paths reach Git and git-annex unquoted, so check that the characters
dandi-archive admits in them do not change what a command acts on.

dandi-archive's `ASSET_CHARS_REGEX` is ``[A-z0-9(),&\\s#+~_=-]``: besides
spaces and parentheses (e.g., 001449's "space-Unified mouse brain atlas (Kim
lab)"), the ``A-z`` range admits ``[``, ``\\``, ``]``, ``^`` and the backtick,
and ``\\s`` admits tabs.  See also #103.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex

pytestmark = pytest.mark.anyio

UNUSUAL_PATHS = [
    "sub-A1_space-Unified mouse brain atlas (Kim lab)_desc-atlas_cells.tsv",
    "two  spaces /and trailing .txt",
    "foo[1].txt",
    "back\\slash.txt",
    "-leading-dash.txt",
    "tab\there.txt",
    "amp&hash#plus+tilde~eq=comma,.txt",
    "caret^back`tick.txt",
]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout


def tracked_files(repo: Path) -> set[str]:
    return set(git(repo, "ls-files", "-z").split("\0")) - {""}


async def make_dataset(path: Path) -> AsyncDataset:
    ds = AsyncDataset(path)
    assert await ds.ensure_installed("Test dataset", cfg_proc=None)
    return ds


@pytest.mark.ai_generated
async def test_annex_batch_unusual_paths(tmp_path: Path) -> None:
    # `fromkey` and `examinekey` take "KEY FILE" lines, split at the first
    # space only, so the file name may contain any further whitespace.
    ds = await make_dataset(tmp_path)
    keys: dict[str, str] = {}
    async with AsyncAnnex(ds.pathobj) as annex:
        for path in UNUSUAL_PATHS:
            content = path.encode("utf-8")
            digest = hashlib.sha256(content).hexdigest()
            key = await annex.mkkey(Path(path).name, len(content), digest)
            assert key.startswith(f"SHA256E-s{len(content)}--{digest}")
            assert key.endswith(Path(path).suffix)
            (ds.pathobj / path).parent.mkdir(parents=True, exist_ok=True)
            await annex.from_key(key, path)
            await annex.register_url(key, f"https://example.com/{digest}")
            keys[path] = key
    found = {
        (d := json.loads(line))["file"]: d["key"]
        for line in git(
            ds.pathobj, "annex", "find", "--include=*", "--json"
        ).splitlines()
    }
    assert found == keys


@pytest.mark.ai_generated
async def test_remove_is_literal(tmp_path: Path) -> None:
    # As a pathspec, "foo[1].txt" also matches "foo1.txt"
    ds = await make_dataset(tmp_path)
    for path in [*UNUSUAL_PATHS, "foo1.txt", "back/slash.txt"]:
        p = ds.pathobj / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"{path}\n")
    await ds.commit_if_changed("Add files")
    before = tracked_files(ds.pathobj)
    await ds.remove("foo[1].txt")
    await ds.remove("back\\slash.txt")
    await ds.remove("-leading-dash.txt")
    assert tracked_files(ds.pathobj) == before - {
        "foo[1].txt",
        "back\\slash.txt",
        "-leading-dash.txt",
    }
    assert (ds.pathobj / "foo1.txt").exists()
    assert (ds.pathobj / "back" / "slash.txt").exists()


@pytest.mark.ai_generated
async def test_remove_batch_is_literal(tmp_path: Path) -> None:
    ds = await make_dataset(tmp_path)
    for path in [*UNUSUAL_PATHS, "foo1.txt"]:
        p = ds.pathobj / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"{path}\n")
    await ds.commit_if_changed("Add files")
    before = tracked_files(ds.pathobj)
    await ds.remove_batch(UNUSUAL_PATHS)
    assert tracked_files(ds.pathobj) == before - set(UNUSUAL_PATHS)
    assert (ds.pathobj / "foo1.txt").exists()


@pytest.mark.ai_generated
async def test_add_unusual_paths(tmp_path: Path) -> None:
    ds = await make_dataset(tmp_path)
    (ds.pathobj / "foo1.txt").write_text("not to be added\n")
    for path in UNUSUAL_PATHS:
        p = ds.pathobj / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"{path}\n")
        await ds.add(path)
    staged = set(
        git(ds.pathobj, "diff", "--cached", "--name-only", "-z").split("\0")
    ) - {""}
    assert staged == set(UNUSUAL_PATHS)
