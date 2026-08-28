"""Tests for the ``annex.largefiles`` policy in `backups2datalad.gitattributes`."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from backups2datalad.gitattributes import (
    LARGEFILES_EXPRESSION,
    SIZE_UNITS,
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
        ("10m", 10000000),
        ("10M", 10000000),
        ("10MB", 10000000),
        ("10MiB", 10485760),
        ("10mib", 10485760),
        ("1kB", 1000),
        ("1KiB", 1024),
        ("512 bytes", 512),
        ("512", 512),
        ("1.5MiB", 1572864),
    ],
)
def test_parse_size(size: str, expected: int) -> None:
    assert parse_size(size) == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize("size", ["10 lightyears", "", "MiB", "-1m", "1.2.3m"])
def test_parse_size_invalid(size: str) -> None:
    with pytest.raises(ValueError):
        parse_size(size)


@pytest.fixture(scope="module")
def annex_repo(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A git-annex repository for running `git annex matchexpression` in"""
    dspath = tmp_path_factory.mktemp("matchexpression")
    subprocess.run(["git", "init", "-q"], cwd=dspath, check=True)
    subprocess.run(
        ["git", "annex", "init", "test"],
        cwd=dspath,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return dspath


def matchexpression(
    dspath: Path, expression: str, size: int, mimeencoding: str = "us-ascii"
) -> bool:
    """Ask git-annex whether ``expression`` matches a file of the given size"""
    r = subprocess.run(
        [
            "git",
            "annex",
            "matchexpression",
            "--largefiles",
            expression,
            "--file=file.txt",
            f"--size={size}",
            f"--mimeencoding={mimeencoding}",
        ],
        cwd=dspath,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if r.stderr.strip():
        # git-annex reports a malformed expression (e.g., an unrecognized size
        # unit) on stderr and exits nonzero, which is not the same as "no match"
        raise ValueError(f"git-annex rejected {expression!r}: {r.stderr.strip()}")
    return r.returncode == 0


@pytest.mark.ai_generated
@pytest.mark.parametrize("unit,multiplier", sorted(SIZE_UNITS.items()))
def test_parse_size_matches_git_annex(
    annex_repo: Path, unit: str, multiplier: int
) -> None:
    """
    Check `parse_size()` against git-annex itself: `TEXT_SIZE_LIMIT` and the
    `--limit` option of `check-largefiles` are only meaningful if we agree with
    git-annex on what a size specification means.
    """
    spec = f"1{unit}"
    assert parse_size(spec) == multiplier
    expression = f"largerthan={spec}"
    assert not matchexpression(annex_repo, expression, multiplier)
    assert matchexpression(annex_repo, expression, multiplier + 1)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "size,mimeencoding,annexed",
    [
        # Text files go into Git, up to the size limit:
        (0, "us-ascii", False),
        (TEXT_SIZE_LIMIT_BYTES, "us-ascii", False),
        (TEXT_SIZE_LIMIT_BYTES + 1, "us-ascii", True),
        (TEXT_SIZE_LIMIT_BYTES, "utf-8", False),
        # Binary files are annexed no matter how small ...
        (1, "binary", True),
        (TEXT_SIZE_LIMIT_BYTES + 1, "binary", True),
        # ... but an empty file is not a large file:
        (0, "binary", False),
    ],
)
def test_policy_expression_matches(
    annex_repo: Path, size: int, mimeencoding: str, annexed: bool
) -> None:
    """
    Check with git-annex itself that the policy annexes what we think it does.
    """
    assert (
        matchexpression(annex_repo, LARGEFILES_EXPRESSION, size, mimeencoding)
        is annexed
    )


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
