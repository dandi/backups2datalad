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

from backups2datalad.aioutil import (
    areadcmd,
    open_git_annex,
    stream_lines_command,
    stream_null_command,
)

pytestmark = pytest.mark.anyio

#: Not valid UTF-8, so `surrogateescape` renders it as the lone surrogate
#: U+DCFF.  This is what `os.fsdecode()` produces for the same byte.
BAD_BYTE = b"\xff"
BAD_NAME = b"bad-\xff-name.txt"


def make_repo_with_bad_name(path: Path, annex: bool = False) -> str:
    path.mkdir(parents=True, exist_ok=True)

    def run(*args: str) -> None:
        subprocess.run(args, cwd=path, check=True, capture_output=True)

    run("git", "init", "-q", "-b", "draft")
    run("git", "config", "user.name", "Test")
    run("git", "config", "user.email", "test@example.com")
    if annex:
        run("git", "annex", "init", "-q", "test")
    (path / os.fsdecode(BAD_NAME)).write_bytes(b"content")
    if annex:
        # `git annex add`, not `git add`: `git annex find` only sees annexed
        # files, and that is the command whose path spelling is under test.
        run("git", "annex", "add", "-q", ".")
    else:
        run("git", "add", "-A")
    run("git", "commit", "-qm", "bad name")
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
    # through the same `text_stream()` as the tests above.)
    repo = tmp_path / "repo"
    repo.mkdir()
    for args in (
        ["git", "init", "-q", "-b", "draft"],
        ["git", "config", "user.name", "Test"],
        ["git", "config", "user.email", "test@example.com"],
        ["git", "annex", "init", "-q", "test"],
    ):
        subprocess.run(args, cwd=repo, check=True, capture_output=True)
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
    from backups2datalad.adataset import AsyncDataset

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
