"""Tests for the ``annex.largefiles`` policy in `backups2datalad.gitattributes`."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from backups2datalad.gitattributes import (
    LARGEFILES_EXPRESSION,
    TEXT_SIZE_LIMIT,
    TEXT_SIZE_LIMIT_BYTES,
    apply_policy,
    is_exempt,
    looks_textual,
    parse_size,
    policy_lines,
    set_policy,
)

#: `.gitattributes` as written by `datalad create -c text2git`
TEXT2GIT = (
    "* annex.backend=SHA256E\n"
    "**/.git* annex.largefiles=nothing\n"
    "* annex.largefiles=((mimeencoding=binary)and(largerthan=0))\n"
)

#: `.gitattributes` as hand-patched in some Dandisets before this was automated
HAND_PATCHED = (
    "* annex.backend=SHA256E\n"
    "**/.git* annex.largefiles=nothing\n"
    "* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or(largerthan=10m))\n"
)

#: `.gitattributes` complying with the current policy
CURRENT = "* annex.backend=SHA256E\n" + "".join(f"{ln}\n" for ln in policy_lines())


@pytest.mark.ai_generated
@pytest.mark.parametrize("attributes", [TEXT2GIT, HAND_PATCHED, CURRENT, ""])
def test_set_policy(attributes: str) -> None:
    assert set_policy(attributes).endswith(
        "".join(f"{ln}\n" for ln in policy_lines())
    )
    # The exemption for Git's own files has to come after the general rule in
    # order to take effect:
    lines = set_policy(attributes).splitlines()
    assert lines.index(f"* annex.largefiles={LARGEFILES_EXPRESSION}") < lines.index(
        "**/.git* annex.largefiles=nothing"
    )


@pytest.mark.ai_generated
def test_set_policy_is_idempotent() -> None:
    once = set_policy(TEXT2GIT)
    assert set_policy(once) == once
    assert set_policy(CURRENT) == CURRENT


@pytest.mark.ai_generated
def test_set_policy_preserves_other_lines() -> None:
    attributes = (
        "* annex.backend=SHA256E\n"
        "* annex.largefiles=((mimeencoding=binary)and(largerthan=0))\n"
        "*.tsv text\n"
        "sub-01/** annex.largefiles=anything\n"
    )
    new_attributes = set_policy(attributes)
    assert "* annex.backend=SHA256E" in new_attributes.splitlines()
    assert "*.tsv text" in new_attributes.splitlines()
    # Only the patterns we manage are rewritten:
    assert "sub-01/** annex.largefiles=anything" in new_attributes.splitlines()
    assert "((mimeencoding=binary)and(largerthan=0))\n" not in new_attributes


@pytest.mark.ai_generated
def test_apply_policy(tmp_path: Path) -> None:
    gitattributes = tmp_path / ".gitattributes"
    # A dataset without a `.gitattributes` file at all:
    assert apply_policy(tmp_path) is True
    assert gitattributes.read_text() == "".join(f"{ln}\n" for ln in policy_lines())
    # Already up-to-date:
    assert apply_policy(tmp_path) is False
    gitattributes.write_text(TEXT2GIT)
    assert apply_policy(tmp_path) is True
    assert apply_policy(tmp_path) is False


@pytest.mark.ai_generated
def test_policy_takes_effect(tmp_path: Path) -> None:
    """
    Check with Git itself that the policy assigns the attributes we expect.
    """
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    apply_policy(tmp_path)
    r = subprocess.run(
        [
            "git",
            "check-attr",
            "annex.largefiles",
            "--",
            "sub-01/sub-01_ephys.tsv",
            ".gitattributes",
            ".gitmodules",
            ".dandi/assets.json",
        ],
        cwd=tmp_path,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    attrs = dict(
        (path, value)
        for path, _, value in (line.split(": ") for line in r.stdout.splitlines())
    )
    assert attrs["sub-01/sub-01_ephys.tsv"] == LARGEFILES_EXPRESSION
    # Metadata is subject to the same rule as everything else ...
    assert attrs[".dandi/assets.json"] == LARGEFILES_EXPRESSION
    # ... but Git's own files can never be annexed:
    assert attrs[".gitattributes"] == "nothing"
    assert attrs[".gitmodules"] == "nothing"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "size,expected",
    [
        ("10MiB", 10485760),
        ("10mib", 10485760),
        ("10MB", 10000000),
        ("1kB", 1000),
        ("1KiB", 1024),
        ("512 bytes", 512),
        ("1.5MiB", 1572864),
    ],
)
def test_parse_size(size: str, expected: int) -> None:
    assert parse_size(size) == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize("size", ["10", "10m", "10 lightyears", "", "MiB"])
def test_parse_size_invalid(size: str) -> None:
    # git-annex does not accept single-letter unit abbreviations, so neither do
    # we; this is what makes the `largerthan=10m` in hand-edited files suspect.
    with pytest.raises(ValueError):
        parse_size(size)


@pytest.mark.ai_generated
def test_size_limit_constants() -> None:
    assert parse_size(TEXT_SIZE_LIMIT) == TEXT_SIZE_LIMIT_BYTES


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "path,expected",
    [
        (".gitattributes", True),
        (".gitmodules", True),
        ("sub-01/.gitattributes", True),
        ("sub-01/data.git", False),
        (".dandi/assets.json", False),
        ("dandiset.yaml", False),
    ],
)
def test_is_exempt(path: str, expected: bool) -> None:
    assert is_exempt(path) is expected


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "path,expected",
    [
        ("sub-01/sub-01_ephys.tsv", True),
        (".dandi/assets.json", True),
        ("dandiset.yaml", True),
        ("sub-01/sub-01_ecephys.nwb", False),
    ],
)
def test_looks_textual(path: str, expected: bool) -> None:
    assert looks_textual(path) is expected
