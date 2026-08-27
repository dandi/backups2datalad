"""
Management of the ``annex.largefiles`` policy for Dandiset mirrors.

DataLad's stock ``text2git`` procedure instructs git-annex to put *every*
non-binary file into Git, no matter its size, which resulted in Dandisets with
hundreds of megabytes of text assets committed directly to Git.  We use our own
policy instead (applied via the ``cfg_dandi_text2git`` procedure in
`backups2datalad.procedures`), which is the same rule plus an upper size limit.

This module is the single source of truth for that policy; it deliberately does
not import `datalad` so that it can be used from anywhere.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath
import re

from identify.identify import tags_from_filename

#: Size at which even a text file is put into git-annex rather than Git.
#:
#: The value is a git-annex size specification, as used by ``largerthan``.
#: git-annex matches unit names case-insensitively but does not recognize
#: single-letter abbreviations, so the unit is spelled out; hand-written
#: ``.gitattributes`` files in the backup that say ``largerthan=10m`` are
#: rewritten to this by the ``cfg_dandi_text2git`` procedure.
TEXT_SIZE_LIMIT = "10MiB"

#: Size units understood by git-annex, as a mapping from lowercased unit name
#: to the number of bytes therein
SIZE_UNITS = {
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def parse_size(size: str) -> int:
    """
    Convert a git-annex size specification (e.g., ``"10MiB"``) to a number of
    bytes.  Raises `ValueError` for a specification git-annex would not accept.
    """
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*", size)
    if m is None or (mult := SIZE_UNITS.get(m[2].lower())) is None:
        raise ValueError(f"Invalid size specification: {size!r}")
    return round(float(m[1]) * mult)


#: `TEXT_SIZE_LIMIT` in bytes, for evaluating the policy in Python
TEXT_SIZE_LIMIT_BYTES = parse_size(TEXT_SIZE_LIMIT)

#: The ``annex.largefiles`` expression applied to everything: files are put into
#: git-annex if they are binary or if they are larger than `TEXT_SIZE_LIMIT`;
#: everything else (i.e., text files of a reasonable size) goes into Git.
LARGEFILES_EXPRESSION = (
    f"(((mimeencoding=binary)and(largerthan=0))or(largerthan={TEXT_SIZE_LIMIT}))"
)

#: The ``annex.largefiles`` settings that we manage, in the order in which they
#: are written to ``.gitattributes``.  Later entries override earlier ones, so
#: the exemption for Git's own files has to come last; note that DataLad's
#: ``text2git`` gets this backwards, leaving the ``**/.git*`` line that
#: ``datalad create`` writes without effect.
POLICY: list[tuple[str, str]] = [
    ("*", LARGEFILES_EXPRESSION),
    # `.gitattributes`, `.gitmodules`, etc., must never become annex symlinks
    ("**/.git*", "nothing"),
]

#: Patterns whose ``annex.largefiles`` settings this module considers its own
#: and thus rewrites.  Patterns that we no longer set but that older versions of
#: our policy (or DataLad's ``text2git``) may have left behind must be listed
#: here as well.
MANAGED_PATTERNS = frozenset(pattern for pattern, _ in POLICY)


def is_exempt(path: str) -> bool:
    """
    Whether ``path`` (relative to the dataset root, with forward slashes) is
    exempt from the size limit by the ``**/.git*`` rule in `POLICY`
    """
    return PurePosixPath(path).name.startswith(".git")


def looks_textual(path: str) -> bool:
    """
    Approximate git-annex's ``mimeencoding`` check using only the filename.

    Used for reporting on files whose contents may not be available locally;
    git-annex itself always goes by the contents.
    """
    return "text" in tags_from_filename(path)


def make_gitattributes_line(pattern: str, value: str) -> str:
    return f"{pattern} annex.largefiles={value}"


def policy_lines() -> list[str]:
    return [make_gitattributes_line(pattern, value) for pattern, value in POLICY]


def set_policy(attributes: str) -> str:
    """
    Return the contents of a ``.gitattributes`` file with our
    ``annex.largefiles`` policy applied.

    Any line that sets ``annex.largefiles`` for a pattern in `MANAGED_PATTERNS`
    is removed, regardless of where in the file it occurs, and the canonical
    policy is then appended.  All other lines (e.g., ``* annex.backend=SHA256E``
    or per-Dandiset customizations) are left alone.
    """
    lines = attributes.splitlines()
    kept = [ln for ln in lines if not _is_managed_line(ln)]
    while kept and not kept[-1].strip():
        kept.pop()
    return "".join(f"{ln}\n" for ln in kept + policy_lines())


def apply_policy(dspath: str | Path) -> bool:
    """
    Apply our ``annex.largefiles`` policy to the ``.gitattributes`` file of the
    dataset at ``dspath``.  Returns `True` if the file was changed and `False`
    if it already complied with the policy.
    """
    path = Path(dspath) / ".gitattributes"
    try:
        attributes = path.read_text()
    except FileNotFoundError:
        attributes = ""
    new_attributes = set_policy(attributes)
    if new_attributes == attributes:
        return False
    path.write_text(new_attributes)
    return True


def _is_managed_line(line: str) -> bool:
    # `.gitattributes` lines are whitespace-separated fields; the first field is
    # the pattern, the rest are attributes.  We only claim ownership of a line
    # if it sets `annex.largefiles` for one of our patterns and sets nothing
    # else, so that a line combining our attribute with someone else's is left
    # for a human to sort out.
    fields = line.split()
    if len(fields) != 2:
        return False
    pattern, attr = fields
    return pattern in MANAGED_PATTERNS and re.fullmatch(
        r"-?annex\.largefiles(=.*)?", attr
    ) is not None
