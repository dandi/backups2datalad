"""
Asset paths reach Git and git-annex unquoted, so check that obscure names (see
`obscure.py`) do not change what a command acts on.  See also #103.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

from obscure import OBSCURE_NAMES, glob_sibling
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.annex import AsyncAnnex

pytestmark = pytest.mark.anyio

#: Each obscure name as a directory and as a file in it, as DataLad does
OBSCURE_PATHS = [f"{n}/{n}" for n in OBSCURE_NAMES]


#: The paths that would also match others as globs, and those others
GLOB_PATHS = [p for p in OBSCURE_PATHS if "[1]" in p]
GLOB_SIBLINGS = [glob_sibling(p) for p in GLOB_PATHS]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout


def tracked_files(repo: Path) -> set[str]:
    return set(git(repo, "ls-files", "-z").split("\0")) - {""}


def write_files(repo: Path, paths: list[str]) -> None:
    for path in paths:
        p = repo / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"{path}\n")


async def make_dataset(path: Path) -> AsyncDataset:
    ds = AsyncDataset(path)
    assert await ds.ensure_installed("Test dataset", cfg_proc=None)
    return ds


@pytest.mark.ai_generated
def test_obscure_paths() -> None:
    # Guard against the names losing what makes them obscure
    assert GLOB_PATHS
    assert any(p.startswith("-") for p in OBSCURE_PATHS)
    assert any(p.startswith(" ") and p.endswith(" ") for p in OBSCURE_PATHS)
    for c in " \t'\"\\|;&<>{}%":
        assert any(c in p for p in OBSCURE_PATHS), repr(c)


@pytest.mark.ai_generated
async def test_annex_batch_obscure_paths(tmp_path: Path) -> None:
    # `fromkey` and `examinekey` take "KEY FILE" lines, split at the first
    # space only, so the file name may contain any further whitespace.
    ds = await make_dataset(tmp_path)
    keys: dict[str, str] = {}
    async with AsyncAnnex(ds.pathobj) as annex:
        for path in OBSCURE_PATHS:
            content = path.encode("utf-8")
            digest = hashlib.sha256(content).hexdigest()
            key = await annex.mkkey(Path(path).name, len(content), digest)
            assert key.startswith(f"SHA256E-s{len(content)}--{digest}")
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
    ds = await make_dataset(tmp_path)
    write_files(ds.pathobj, OBSCURE_PATHS + GLOB_SIBLINGS)
    await ds.commit_if_changed("Add files")
    before = tracked_files(ds.pathobj)
    for path in OBSCURE_PATHS:
        await ds.remove(path)
    assert tracked_files(ds.pathobj) == before - set(OBSCURE_PATHS)
    for path in GLOB_SIBLINGS:
        assert (ds.pathobj / path).exists()


@pytest.mark.ai_generated
async def test_remove_batch_is_literal(tmp_path: Path) -> None:
    ds = await make_dataset(tmp_path)
    write_files(ds.pathobj, OBSCURE_PATHS + GLOB_SIBLINGS)
    await ds.commit_if_changed("Add files")
    before = tracked_files(ds.pathobj)
    await ds.remove_batch(OBSCURE_PATHS)
    assert tracked_files(ds.pathobj) == before - set(OBSCURE_PATHS)
    for path in GLOB_SIBLINGS:
        assert (ds.pathobj / path).exists()


@pytest.mark.ai_generated
async def test_add_obscure_paths(tmp_path: Path) -> None:
    ds = await make_dataset(tmp_path)
    write_files(ds.pathobj, GLOB_SIBLINGS)
    write_files(ds.pathobj, OBSCURE_PATHS)
    for path in OBSCURE_PATHS:
        await ds.add(path)
    staged = set(
        git(ds.pathobj, "diff", "--cached", "--name-only", "-z").split("\0")
    ) - {""}
    assert staged == set(OBSCURE_PATHS)
