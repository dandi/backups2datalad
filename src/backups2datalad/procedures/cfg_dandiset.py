#!/usr/bin/env python3
"""
DataLad configuration procedure establishing the ``.gitattributes`` policy for
Dandiset mirrors.

This replaces DataLad's ``cfg_text2git`` procedure, which puts *all* text files
into Git; some Dandisets contain text files that are far too large for that
(they bloat the Git repository and are rejected by GitHub), so we cap the size
of what goes into Git and let git-annex take everything above the cap.  That
goes for every file in the mirror, the metadata we maintain ourselves
(``dandiset.yaml``, ``.dandi/``) included: if it is big or binary, it belongs
in git-annex.

The policy is written to ``.gitattributes`` as a block delimited by marker
comments::

    ### BEGIN dandiset default policy (backups2datalad)
    ...
    ### END dandiset default policy (backups2datalad)

Only the contents of that block are managed: anything a user puts outside of it
is preserved, and lines placed *after* the block override the policy (later
lines win in ``.gitattributes``).  When there is no block yet, one is appended
at the end of the file (and any pre-existing catch-all ``annex.largefiles``
rule, such as the one left behind by ``cfg_text2git``, is dropped).  Applying
the procedure to a dataset that already has an up-to-date block is a no-op, so
it is safe (and cheap) to run it on every backup run.

Usage::

    # As part of dataset creation (this is what backups2datalad does):
    datalad -c datalad.locations.extra-procedures=<dir> create -c dandiset <path>

    # On an already-existing dataset:
    datalad run-procedure -d <path> cfg_dandiset [SIZE-LIMIT]
    python -m backups2datalad.procedures.cfg_dandiset <path> [SIZE-LIMIT]

The size limit can be given on the command line or via the
``BACKUPS2DATALAD_TEXT_SIZE_LIMIT`` environment variable (the latter is how
backups2datalad passes it down to the procedure); it defaults to `10MiB`, the
same limit that backups2datalad applies when deciding whether to hand an asset
to git-annex.

The commit is made with whatever git identity and dates the environment
provides: keeping a mirror's timeline from jumping into the present is
backups2datalad's business, and it sets ``GIT_AUTHOR_*`` accordingly around
running this (see `AsyncDataset.ensure_installed()`).

This module is deliberately restricted to the standard library: DataLad
executes it as a plain script (outside of any package context), and
backups2datalad imports it to apply the very same policy to already-existing
datasets.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

#: Default upper bound on the size of a file that is committed to Git
DEFAULT_SIZE_LIMIT = "10MiB"

#: Environment variable overriding `DEFAULT_SIZE_LIMIT`
SIZE_LIMIT_ENVVAR = "BACKUPS2DATALAD_TEXT_SIZE_LIMIT"

BLOCK_START = "### BEGIN dandiset default policy (backups2datalad)"
BLOCK_END = "### END dandiset default policy (backups2datalad)"

# Deliberately without a "[backups2datalad]" marker: that marker identifies the
# commits that record a backup state (`mkrelease()` greps for it when looking
# for the commit to tag a published version at), and a configuration commit is
# not one of those.
COMMIT_MESSAGE = "Set .gitattributes to the dandiset default policy (backups2datalad)"

GITATTRIBUTES = ".gitattributes"

#: Size units understood by git-annex' ``largerthan=``
SIZE_UNITS = {
    "": 1,
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
}


def parse_size(spec: str) -> int:
    """Parse a git-annex style size specification (e.g. ``10MiB``) to bytes"""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*", spec)
    if m is None or m[2].lower() not in SIZE_UNITS:
        raise ValueError(f"Invalid size specification: {spec!r}")
    return int(float(m[1]) * SIZE_UNITS[m[2].lower()])


def get_size_limit(size_limit: str | None = None) -> str:
    """
    Resolve the size limit to use: an explicitly-given one, else
    `SIZE_LIMIT_ENVVAR`, else `DEFAULT_SIZE_LIMIT`.  The value is validated
    (and thus may raise `ValueError`) so that we never write a
    ``.gitattributes`` that git-annex cannot parse.
    """
    if size_limit is None:
        size_limit = os.environ.get(SIZE_LIMIT_ENVVAR) or DEFAULT_SIZE_LIMIT
    parse_size(size_limit)
    return size_limit


def size_limit_bytes(size_limit: str | None = None) -> int:
    """`get_size_limit()`, in bytes"""
    return parse_size(get_size_limit(size_limit))


def policy_lines(size_limit: str | None = None) -> list[str]:
    """The lines of the policy block, markers included"""
    limit = get_size_limit(size_limit)
    return [
        BLOCK_START,
        # Everything binary or above the size limit goes to git-annex; the rest
        # (i.e. text files up to the limit) goes into Git.
        "* annex.largefiles="
        f"((mimeencoding=binary)and(largerthan=0))or(largerthan={limit})",
        BLOCK_END,
    ]


def strip_catchall_largefiles(line: str) -> str | None:
    """
    Remove any ``annex.largefiles`` setting for the ``*`` pattern from
    ``line``, as our block would silently override it anyway.  This is what
    gets rid of the line left behind by DataLad's ``cfg_text2git``.

    Returns the (possibly modified) line, or `None` if nothing is left of it.
    """
    fields = line.split()
    if len(fields) < 2 or fields[0] != "*":
        return line
    attrs = [
        f for f in fields[1:] if not re.fullmatch(r"-?annex\.largefiles(=.*)?", f)
    ]
    if len(attrs) == len(fields) - 1:
        return line
    elif attrs:
        return " ".join(["*", *attrs])
    else:
        return None


def new_gitattributes(current: str, size_limit: str | None = None) -> str:
    """
    Return the contents that ``.gitattributes`` should have, given its
    ``current`` contents
    """
    lines = current.splitlines()
    block = policy_lines(size_limit)
    if BLOCK_START in lines:
        start = lines.index(BLOCK_START)
        if BLOCK_END in lines[start:]:
            end = lines.index(BLOCK_END, start)
        else:
            # Someone mangled the block; assume it extends to the end of file
            end = len(lines) - 1
        newlines = lines[:start] + block + lines[end + 1 :]
    else:
        kept = []
        for ln in lines:
            if (stripped := strip_catchall_largefiles(ln)) is not None:
                kept.append(stripped)
        newlines = kept + block
    return "".join(f"{ln}\n" for ln in newlines)


def apply_policy(dspath: str | Path, size_limit: str | None = None) -> bool:
    """
    Ensure that the ``.gitattributes`` of the dataset at ``dspath`` carries an
    up-to-date policy block.  Returns `True` if the file was changed.
    """
    path = Path(dspath) / GITATTRIBUTES
    try:
        # Reading in text mode translates CRLF to LF, so a file written with
        # DOS line endings still compares equal to the LF-only policy we build
        # here; writing with newline="\n" keeps it that way.
        current = path.read_text()
    except FileNotFoundError:
        current = ""
    new = new_gitattributes(current, size_limit)
    if new == current:
        return False
    path.write_text(new, newline="\n")
    return True


def commit_policy(dspath: Path) -> None:
    """
    Commit an updated ``.gitattributes``.

    The commit takes its identity and dates from the environment; the caller
    sets ``GIT_AUTHOR_*`` when it cares (as backups2datalad does, so that
    configuring a mirror does not move its timeline into the present).
    """
    subprocess.run(["git", "-C", str(dspath), "add", GITATTRIBUTES], check=True)
    subprocess.run(
        ["git", "-C", str(dspath), "commit", "-m", COMMIT_MESSAGE, "--", GITATTRIBUTES],
        check=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cfg_dandiset",
        description=(
            "Set the .gitattributes policy for a Dandiset mirror: text files up"
            " to a size limit go into Git, everything else into git-annex."
        ),
    )
    parser.add_argument(
        "-n",
        "--no-commit",
        action="store_true",
        help="Update .gitattributes but do not commit the change",
    )
    parser.add_argument(
        "-s",
        "--size-limit",
        default=None,
        help=(
            "Largest file to keep in Git, as a git-annex size specification"
            f" [env: {SIZE_LIMIT_ENVVAR}; default: {DEFAULT_SIZE_LIMIT}]"
        ),
    )
    parser.add_argument("dataset", type=Path, help="Path to the dataset")
    parser.add_argument(
        "size_limit_arg",
        nargs="?",
        default=None,
        metavar="SIZE-LIMIT",
        help="Same as --size-limit (for `datalad run-procedure`)",
    )
    args = parser.parse_args(argv)
    size_limit = (
        args.size_limit if args.size_limit is not None else args.size_limit_arg
    )
    if apply_policy(args.dataset, size_limit) and not args.no_commit:
        commit_policy(args.dataset)
    return 0


if __name__ == "__main__":
    sys.exit(main())
