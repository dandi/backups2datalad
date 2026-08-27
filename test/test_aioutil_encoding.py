"""
Tests that subprocess output containing bytes that aren't valid UTF-8 is
tolerated rather than aborting the run.

A filesystem path is a byte string on POSIX and need not be valid UTF-8, and
git emits paths verbatim, so every one of these commands can produce such
bytes.  git-annex's ``--json`` output is the exception -- it replaces them
with U+FFFD -- which is why `aiter_annexed_files()` uses ``--format``; see
`test_get_file_stats_non_utf8`.
"""

from __future__ import annotations

from contextlib import aclosing
import os
from pathlib import Path
import subprocess

import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.aioutil import (
    areadcmd,
    open_git_annex,
    stream_lines_command,
    stream_null_command,
)

pytestmark = pytest.mark.anyio

#: Not valid UTF-8, so `surrogateescape` renders the bad byte as the lone
#: surrogate U+DCFF, which is what `os.fsdecode()` produces for it.
BAD_NAME = b"bad-\xff-name.txt"


def run(path: Path, *args: str) -> None:
    subprocess.run(args, cwd=path, check=True, capture_output=True)


def init_repo(path: Path, annex: bool = False) -> None:
    path.mkdir(parents=True, exist_ok=True)
    run(path, "git", "init", "-q", "-b", "draft")
    run(path, "git", "config", "user.name", "Test")
    run(path, "git", "config", "user.email", "test@example.com")
    if annex:
        run(path, "git", "annex", "init", "-q", "test")


def make_repo_with_bad_name(path: Path, annex: bool = False) -> str:
    init_repo(path, annex=annex)
    (path / os.fsdecode(BAD_NAME)).write_bytes(b"content")
    if annex:
        # `git annex add`, not `git add`: `git annex find` only sees annexed
        # files, and that is the command whose path spelling is under test.
        run(path, "git", "annex", "add", "-q", ".")
    else:
        run(path, "git", "add", "-A")
    run(path, "git", "commit", "-qm", "bad name")
    return os.fsdecode(BAD_NAME)


@pytest.mark.ai_generated
async def test_stream_null_command_non_utf8(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    expected = make_repo_with_bad_name(repo)
    entries = []
    async with aclosing(
        stream_null_command(
            "git", "ls-tree", "-r", "--name-only", "-z", "HEAD", cwd=repo
        )
    ) as p:
        async for entry in p:
            entries.append(entry)
    assert entries == [expected]
    # Round-trips back to the original bytes.
    assert os.fsencode(entries[0]) == BAD_NAME


@pytest.mark.ai_generated
async def test_stream_lines_command_non_utf8(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    expected = make_repo_with_bad_name(repo)
    lines = []
    # `core.quotePath=false` stops git escaping the byte, so the raw bytes
    # reach the decoder; without it git emits an ASCII "\377" escape and this
    # would pass whatever the decoding does.
    async with aclosing(
        stream_lines_command("git", "-c", "core.quotePath=false", "ls-files", cwd=repo)
    ) as p:
        async for line in p:
            lines.append(line.rstrip("\n"))
    assert lines == [expected]
    assert os.fsencode(lines[0]) == BAD_NAME


@pytest.mark.ai_generated
async def test_areadcmd_non_utf8(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo_with_bad_name(repo)
    out = await areadcmd(
        "git", "-c", "core.quotePath=false", "ls-files", cwd=repo, strip=True
    )
    assert os.fsencode(out) == BAD_NAME


@pytest.mark.ai_generated
async def test_text_process_send_non_utf8(tmp_path: Path) -> None:
    # Covers the *send* side: `TextProcess.send()` encodes, and a filename
    # that isn't valid UTF-8 must not raise on the way out.  (git-annex keeps
    # only the extension, so the byte doesn't come back; the receive side goes
    # through the same `_text_stream()` as the tests above.)
    repo = tmp_path / "repo"
    init_repo(repo, annex=True)
    name = os.fsdecode(BAD_NAME)
    p = await open_git_annex(
        "examinekey", "--batch", "--migrate-to-backend=MD5E", path=repo
    )
    try:
        await p.send(f"MD5-s7--0123456789abcdef0123456789abcdef {name}\n")
        key = (await p.receive()).strip()
    finally:
        await p.aclose()
    assert key == "MD5E-s7--0123456789abcdef0123456789abcdef.txt"


@pytest.mark.ai_generated
async def test_get_file_stats_non_utf8(tmp_path: Path) -> None:
    # The motivating case.  `git ls-tree -lrz` emits the path bytes verbatim,
    # but `git annex find --json` replaces the invalid ones with U+FFFD, so
    # matching the two by path needs `--format`, which is verbatim too.
    repo = tmp_path / "repo"
    expected = make_repo_with_bad_name(repo, annex=True)
    stats = await AsyncDataset(repo).get_file_stats()
    by_path = {s.path: s for s in stats}
    assert expected in by_path
    assert by_path[expected].size == len(b"content")


@pytest.mark.ai_generated
async def test_open_git_annex_receives_non_utf8(tmp_path: Path) -> None:
    # Covers the *receive* side of `open_git_annex()`: git-annex keeps the
    # filename's extension in the key it derives, so a bad byte placed in the
    # extension comes back out over the pipe.
    repo = tmp_path / "repo"
    make_repo_with_bad_name(repo, annex=True)
    name = os.fsdecode(b"file.t\xffxt")
    p = await open_git_annex(
        "examinekey", "--batch", "--migrate-to-backend=MD5E", path=repo
    )
    try:
        await p.send(f"MD5-s7--0123456789abcdef0123456789abcdef {name}\n")
        key = (await p.receive()).strip()
    finally:
        await p.aclose()
    assert key == "MD5E-s7--0123456789abcdef0123456789abcdef" + name[len("file") :]
    assert os.fsencode(key).endswith(b".t\xffxt")


@pytest.mark.ai_generated
async def test_remove_batch_non_utf8(tmp_path: Path) -> None:
    # `remove_batch()` writes the paths to a temp file for
    # `--pathspec-file-nul`, and is fed straight from `AsyncAnnex.list_files()`
    # -- i.e. from `git ls-tree -z`, which emits the bytes verbatim.
    repo = tmp_path / "repo"
    name = make_repo_with_bad_name(repo, annex=True)
    ds = AsyncDataset(repo)
    await ds.remove_batch([name])
    assert not (repo / name).exists()


@pytest.mark.ai_generated
async def test_read_file_from_commit_non_utf8(tmp_path: Path) -> None:
    # The point of decoding *and* encoding leniently here: `read_file_from_commit`
    # must not raise `UnicodeEncodeError`, which is a sibling of
    # `UnicodeDecodeError` rather than a subclass and so would escape the
    # `except` clause in `commit_has_assets()`.
    repo = tmp_path / "repo"
    init_repo(repo)
    # No trailing newline: `read_git(strip=True)` would eat one, so the bytes
    # only round-trip exactly without it.
    (repo / "plain.json").write_bytes(b'[{"path": "a\xff b"}]')
    run(repo, "git", "add", "-A")
    run(repo, "git", "commit", "-qm", "bad content")
    ds = AsyncDataset(repo)
    commit = (await ds.get_commit_hash()).strip()
    content = await ds.read_file_from_commit(commit, "plain.json")
    # Whatever comes back, the failure mode that matters is the exception type
    # a caller has to catch, so assert on that rather than on the bytes.
    with pytest.raises(UnicodeDecodeError):
        content.decode("utf-8")


@pytest.mark.ai_generated
async def test_get_file_stats_key_containing_space(tmp_path: Path) -> None:
    # git-annex escapes spaces out of the keys it generates, but `fromkey
    # --force` accepts one, and then splitting a `find --format` record on
    # spaces assigns the tail of the key to the filename.  The forged key
    # needs a *size*, or `int()` fails before the mis-parse can show.
    repo = tmp_path / "repo"
    init_repo(repo, annex=True)
    key = (
        "SHA256E-s7--"
        "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73"
        " with space.txt"
    )
    run(repo, "git", "annex", "fromkey", "--force", key, "spaced.dat")
    run(repo, "git", "commit", "-qm", "forged key")
    stats = await AsyncDataset(repo).get_file_stats()
    by_path = {s.path: s for s in stats}
    assert "spaced.dat" in by_path
    assert by_path["spaced.dat"].size == 7


@pytest.mark.ai_generated
async def test_update_submodule_non_utf8_path(tmp_path: Path) -> None:
    # `git update-index --index-info` makes a gitlink at any path, so this
    # needs no actual submodule.
    repo = tmp_path / "repo"
    init_repo(repo)
    (repo / "seed.txt").write_text("seed\n")
    run(repo, "git", "add", "-A")
    run(repo, "git", "commit", "-qm", "seed")
    ds = AsyncDataset(repo)
    commit_hash = (await ds.get_commit_hash()).strip()
    await ds.update_submodule(os.fsdecode(b"zarr-\xff-dir"), commit_hash)
    index = subprocess.run(
        ["git", "ls-files", "-s", "-z"], cwd=repo, capture_output=True, check=True
    ).stdout
    assert b"zarr-\xff-dir" in index
