from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from dandi.utils import find_files

from backups2datalad.procedures.cfg_dandiset import BLOCK_END, BLOCK_START
from backups2datalad.util import is_meta_file


@dataclass
class GitRepo:
    path: Path

    def runcmd(self, *args: str | Path, **kwargs: Any) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=self.path, **kwargs)

    def readcmd(self, *args: str | Path) -> str:
        r = self.runcmd(*args, stdout=subprocess.PIPE, text=True, check=True)
        assert isinstance(r.stdout, str)
        return r.stdout.strip()

    def get_tag_date(self, tag: str) -> str:
        return unzulu(
            self.readcmd(
                "for-each-ref", "--format=%(creatordate:iso-strict)", f"refs/tags/{tag}"
            )
        )

    def get_tag_creator(self, tag: str) -> str:
        # `tag` must be an annotated tag.
        return self.readcmd(
            "for-each-ref", "--format=%(taggername) %(taggeremail)", f"refs/tags/{tag}"
        )

    def get_commit_date(self, commitish: str) -> str:
        return unzulu(
            self.readcmd("show", "-s", "--format=%aI", f"{commitish}^{{commit}}")
        )

    def get_commit_author(self, commitish: str) -> str:
        return self.readcmd(
            "show", "-s", "--format=%an <%ae>", f"{commitish}^{{commit}}"
        )

    def get_commit_subject(self, commitish: str) -> str:
        return self.readcmd("show", "-s", "--format=%s", f"{commitish}^{{commit}}")

    def get_commit_message(self, commitish: str) -> str:
        return self.readcmd("show", "-s", "--format=%B", f"{commitish}^{{commit}}")

    def get_commitish_hash(self, commitish: str) -> str:
        return self.readcmd("rev-parse", f"{commitish}^{{commit}}")

    def is_ancestor(self, commit1: str, commit2: str) -> bool:
        return (
            self.runcmd("merge-base", "--is-ancestor", commit1, commit2).returncode == 0
        )

    def parent_is_ancestor(self, commit1: str, commit2: str) -> bool:
        return (
            self.runcmd(
                "merge-base", "--is-ancestor", f"{commit1}^", commit2
            ).returncode
            == 0
        )

    def get_blob(self, treeish: str, path: str) -> str:
        return self.readcmd("show", f"{treeish}:{path}")

    def get_tags(self) -> list[str]:
        return self.readcmd("tag", "-l", "--sort=creatordate").splitlines()

    def get_diff_tree(self, commitish: str) -> dict[str, str]:
        stat = self.readcmd(
            "diff-tree", "--no-commit-id", "--name-status", "-r", commitish
        )
        status: dict[str, str] = {}
        for line in stat.splitlines():
            sym, _, path = line.partition("\t")
            status[path] = sym
        return status

    def get_backup_commits(self) -> list[str]:
        return self.readcmd(
            "rev-list", "--tags", r"--grep=\[backups2datalad\]", "HEAD"
        ).splitlines()

    def get_asset_files(self, commitish: str) -> set[str]:
        return {
            fname
            for fname in self.readcmd(
                "ls-tree", "-r", "--name-only", commitish
            ).splitlines()
            if not is_meta_file(fname, dandiset=True)
        }

    def get_assets_json(self, commitish: str) -> list[dict]:
        assets = json.loads(self.get_blob(commitish, ".dandi/assets.json"))
        assert isinstance(assets, list)
        return assets

    def get_merge_commits(self, branch: str) -> list[str]:
        out = self.readcmd("log", "--merges", "--format=%H", branch)
        return out.splitlines() if out else []

    def get_commit_count(self) -> int:
        return int(self.readcmd("rev-list", "--count", "HEAD"))


def gitattributes_policy(dspath: Path) -> list[str]:
    """
    Return the lines (markers included) of the `cfg_dandiset` policy block in
    the ``.gitattributes`` of the dataset at ``dspath``
    """
    lines = (dspath / ".gitattributes").read_text().splitlines()
    assert BLOCK_START in lines, f"No policy block in {dspath}/.gitattributes"
    start = lines.index(BLOCK_START)
    assert BLOCK_END in lines[start:], f"Unterminated policy block in {dspath}"
    return lines[start : lines.index(BLOCK_END, start) + 1]


def find_filepaths(dirpath: Path) -> Iterator[Path]:
    return map(
        Path,
        find_files(
            r".*",
            [dirpath],
            exclude_dotfiles=False,
            exclude_dotdirs=False,
            exclude_vcs=False,
        ),
    )


def unzulu(ts: str) -> str:
    # Git v2.45.0 changed its iso-strict date format to display the UTC offset
    # as "Z" rather than "+00:00", which breaks comparisons against stringified
    # Python datetimes.
    return re.sub(r"Z$", "+00:00", ts)


def zarr_format_of(names: Iterable[str]) -> str:
    """Return the Zarr serialisation format ("2" or "3") implied by a Zarr
    store's file inventory.

    V3 stores carry ``zarr.json`` array/group metadata files; V2 stores carry
    ``.zarray`` / ``.zgroup``.  ``zarr.save`` defaults to whichever format the
    installed ``zarr-python`` version writes — that's V2 in zarr-python 2.x
    and V3 in 3.x, and on-disk digests / sizes differ between the two
    layouts (see dandi/dandi-cli#1858).
    """
    seen = set(names)
    if any(n.endswith("zarr.json") for n in seen):
        return "3"
    if any(n.endswith(".zarray") or n.endswith(".zgroup") for n in seen):
        return "2"
    raise ValueError(f"Cannot determine Zarr format from files: {sorted(seen)!r}")
