"""
Obscure path names for tests, see #103.

Asset paths reach Git and git-annex as they come from the archive, so tests use
names that break wherever a path is split, globbed, quoted, stripped or taken
for an option along the way.

The base is DataLad's `OBSCURE_FILENAME`: the most obscure name the filesystem
under test supports (``' |;&%b5{}\\'"<> .datc '`` on Linux).  It is not usable
as an asset path as is, because dandi-archive admits only some of those
characters there -- and admits others that DataLad's name lacks.  Hence:

- `OBSCURE_NAMES`, for tests that do not go through dandi-archive;
- `OBSCURE_ASSET_NAME`, as obscure as dandi-archive allows, for asset paths
  (built with `obscure_asset_path()`, which checks them against its regex).
"""

from __future__ import annotations

import re

from datalad.tests.utils_pytest import OBSCURE_FILENAME, UNICODE_FILENAME

#: dandi-archive's `ASSET_CHARS_REGEX` and `ASSET_PATH_REGEX`
#: (`dandiapi/api/models/asset.py`).  The ``A-z`` range admits ``[\]^_```
#: too, and ``\s`` admits tabs.
ASSET_CHARS_REGEX = r"[A-z0-9(),&\s#+~_=-]"
ASSET_PATH_REGEX = rf"^({ASSET_CHARS_REGEX}?\/?\.?{ASSET_CHARS_REGEX})+$"

#: What dandi-archive admits beyond `OBSCURE_FILENAME`: a glob character class
#: and escape, a trigger of Git's C-quoting of paths (backslash), and 001449's
#: parentheses.  A tab, which it admits too, is left out: as of DataLad 1.6.5,
#: `datalad status` (and thereby `assert_repo_status()`) misreports a file
#: with a tab in its name as deleted.
DANDI_OBSCURE_PART = "[1]\\^`(#+~=,)"

# As of DataLad 1.6.5, `UNICODE_FILENAME` is appended to the parts only where
# the filesystem encoding is *not* UTF-8, so it is usually missing.
_unicode = "" if UNICODE_FILENAME in OBSCURE_FILENAME else UNICODE_FILENAME

#: File names (no "/"), each obscure in its own way, since one name cannot both
#: start with a space and with a dash
OBSCURE_NAMES = [
    OBSCURE_FILENAME,
    f"{OBSCURE_FILENAME}{_unicode}{DANDI_OBSCURE_PART}\t.txt",
    f"-{OBSCURE_FILENAME.strip()}{DANDI_OBSCURE_PART}\t.txt",
]

#: `OBSCURE_FILENAME` restricted to what dandi-archive admits, with a leading
#: dash and `DANDI_OBSCURE_PART` added; no extension, so that it can be used
#: for directories as well as (with an extension) files.  Git ignores a
#: submodule whose path starts with "-", so prefix a Zarr's path with a
#: directory.
OBSCURE_ASSET_NAME = (
    "-"
    + "".join(c for c in OBSCURE_FILENAME if re.fullmatch(ASSET_CHARS_REGEX, c))
    + DANDI_OBSCURE_PART
)


def obscure_asset_path(filename: str) -> str:
    """
    ``filename`` (e.g., ``"data.txt"``) prefixed with `OBSCURE_ASSET_NAME`, in
    a directory named `OBSCURE_ASSET_NAME`
    """
    path = f"{OBSCURE_ASSET_NAME}/{OBSCURE_ASSET_NAME} {filename}"
    if not re.fullmatch(ASSET_PATH_REGEX, path):
        raise ValueError(f"dandi-archive would not accept {path!r}")
    return path


def glob_sibling(path: str) -> str:
    """
    Another path that ``path`` matches when Git takes it for a pattern, as it
    does a pathspec unless told otherwise: ``[1]`` is a character class
    matching ``1``, and a backslash escapes the next character
    """
    sibling = path.replace("[1]", "1").replace("\\", "")
    if sibling == path:
        raise ValueError(f"{path!r} is no glob")
    return sibling
