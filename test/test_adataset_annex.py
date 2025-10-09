"""Tests for AsyncDataset.read_file_from_commit() with annexed files."""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess

import pytest

from backups2datalad.adataset import AsyncDataset

pytestmark = pytest.mark.anyio


@pytest.mark.ai_generated
async def test_read_annexed_file_from_commit(tmp_path: Path) -> None:
    """Test reading an annexed file from a git commit."""
    # Clone the test repository
    repo_url = "https://github.com/datalad/testrepo--minimalds"
    repo_path = tmp_path / "testrepo--minimalds"

    subprocess.run(
        ["git", "clone", repo_url, str(repo_path)],
        check=True,
        capture_output=True,
    )

    # Initialize git-annex
    subprocess.run(
        ["git", "annex", "init"],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )

    ds = AsyncDataset(repo_path)

    # Read the annexed file inannex/animated.gif
    # First, get the HEAD commit
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_path,
        check=True,
        capture_output=True,
        text=True,
    )
    head_commit = result.stdout.strip()

    # Get the symlink target to extract the annex key
    result = subprocess.run(
        ["git", "show", f"{head_commit}:inannex/animated.gif"],
        cwd=repo_path,
        check=True,
        capture_output=True,
        text=True,
    )
    symlink_target = result.stdout.strip()

    # Extract the annex key from the symlink
    # Format: ../../.git/annex/objects/XX/YY/KEY/KEY
    annex_key = Path(symlink_target).name

    # Extract checksum from the annex key
    # Format: MD5E-s144625--4c458c62b7ac8ec8e19c8ff14b2e34ad.gif
    # Strip before -- and strip extension to get checksum
    checksum_part = annex_key.split("--", 1)[1]  # Get part after --
    expected_checksum = Path(checksum_part).stem  # Remove extension

    # Read the file content
    content = await ds.read_file_from_commit(head_commit, "inannex/animated.gif")

    # Verify it's binary content (GIF magic bytes)
    assert content[:6] == b"GIF89a" or content[:6] == b"GIF87a"

    # Calculate MD5 to verify it matches the key
    md5 = hashlib.md5(content).hexdigest()
    assert (
        md5 == expected_checksum
    ), f"MD5 mismatch: got {md5}, expected {expected_checksum} from key {annex_key}"


@pytest.mark.ai_generated
async def test_read_non_annexed_file_from_commit(tmp_path: Path) -> None:
    """Test reading a regular (non-annexed) file from a git commit."""
    repo_url = "https://github.com/datalad/testrepo--minimalds"
    repo_path = tmp_path / "testrepo--minimalds"

    subprocess.run(
        ["git", "clone", repo_url, str(repo_path)],
        check=True,
        capture_output=True,
    )

    ds = AsyncDataset(repo_path)

    # Get HEAD commit
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_path,
        check=True,
        capture_output=True,
        text=True,
    )
    head_commit = result.stdout.strip()

    # Read a regular file (not annexed)
    content = await ds.read_file_from_commit(head_commit, "README.md")

    # Should contain some text
    assert b"minimal" in content.lower() or b"test" in content.lower()
