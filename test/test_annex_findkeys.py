"""Tests for `AsyncAnnex.get_keys_missing_from()`."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from backups2datalad.annex import AsyncAnnex

pytestmark = pytest.mark.anyio


def make_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    def run(*args: str) -> None:
        subprocess.run(args, cwd=path, check=True, capture_output=True)

    run("git", "init", "-q", "-b", "draft")
    run("git", "config", "user.name", "Test")
    run("git", "config", "user.email", "test@example.com")
    run("git", "config", "annex.backend", "MD5E")
    run("git", "annex", "init", "-q", "test")


def examinekey(path: Path, size: int, digest: str, name: str) -> str:
    r = subprocess.run(
        ["git", "annex", "examinekey", "--batch", "--migrate-to-backend=MD5E"],
        input=f"MD5-s{size}--{digest} {name}\n",
        cwd=path,
        capture_output=True,
        text=True,
        check=True,
    )
    return r.stdout.strip()


@pytest.mark.ai_generated
async def test_get_keys_missing_from(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    backup = tmp_path / "backup"
    backup.mkdir()
    subprocess.run(
        [
            "git",
            "annex",
            "initremote",
            "backup",
            "type=directory",
            f"directory={backup}",
            "encryption=none",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )

    # A key with a registered URL, so present in the web remote but not in the
    # backup remote; a key that is only pointed at by a file, so present
    # nowhere; and a key whose content is present locally and in the backup.
    web_key = examinekey(repo, 10, "0" * 31 + "1", "web.dat")
    absent_key = examinekey(repo, 10, "0" * 31 + "2", "absent.dat")
    subprocess.run(
        ["git", "annex", "fromkey", "--force", "--batch", "--json"],
        input=f"{web_key} web.dat\n{absent_key} absent.dat\n",
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "annex", "registerurl", web_key, "https://example.com/web.dat"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    (repo / "local.dat").write_text("hello\n")
    subprocess.run(
        ["git", "annex", "add", "local.dat"], cwd=repo, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "annex", "copy", "--to=backup", "local.dat"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    local_key = subprocess.run(
        ["git", "annex", "lookupkey", "local.dat"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    async with AsyncAnnex(repo, digest_type="MD5") as annex:
        missing = await annex.get_keys_missing_from("backup")

    # Known somewhere but not in the backup remote:
    assert web_key in missing
    # In the backup remote:
    assert local_key not in missing
    # Known to no repository at all, so nothing to report:
    assert absent_key not in missing


@pytest.mark.ai_generated
async def test_get_keys_missing_from_empty_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    backup = tmp_path / "backup"
    backup.mkdir()
    subprocess.run(
        [
            "git",
            "annex",
            "initremote",
            "backup",
            "type=directory",
            f"directory={backup}",
            "encryption=none",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    async with AsyncAnnex(repo, digest_type="MD5") as annex:
        assert await annex.get_keys_missing_from("backup") == set()


@pytest.mark.ai_generated
async def test_get_keys_missing_from_no_remote(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    async with AsyncAnnex(repo, digest_type="MD5") as annex:
        assert await annex.get_keys_missing_from(None) == set()


@pytest.mark.ai_generated
async def test_get_keys_missing_from_is_cached(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    backup = tmp_path / "backup"
    backup.mkdir()
    subprocess.run(
        [
            "git",
            "annex",
            "initremote",
            "backup",
            "type=directory",
            f"directory={backup}",
            "encryption=none",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    async with AsyncAnnex(repo, digest_type="MD5") as annex:
        assert await annex.get_keys_missing_from("backup") == set()
        (repo / "new.dat").write_bytes(b"new content")
        subprocess.run(
            ["git", "annex", "add", "new.dat"],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        # A second call returns the set sampled by the first one rather than
        # re-running `findkeys`.
        assert await annex.get_keys_missing_from("backup") == set()
    async with AsyncAnnex(repo, digest_type="MD5") as annex2:
        assert await annex2.get_keys_missing_from("backup") != set()
