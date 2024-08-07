from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from difflib import unified_diff
from io import StringIO
import json
import os
from pathlib import Path
import random
import sys
import textwrap
from types import TracebackType
from typing import TYPE_CHECKING, Any

from dandi.consts import dandiset_metadata_file
from dandi.dandiset import Dandiset
from datalad.api import Dataset
from datalad.support.json_py import dump
from ruamel.yaml import YAML

from .config import BackupConfig
from .consts import MINIMUM_GIT_ANNEX_VERSION
from .logging import PrefixedLogger

if TYPE_CHECKING:
    from .adandi import RemoteAsset, RemoteDandiset
    from .adataset import AsyncDataset


@dataclass
class AssetTracker:
    #: The path to the .dandi/assets.json file that this instance manages
    filepath: Path
    #: Paths of files found when the syncing started, minus the paths for any
    #: assets downloaded during syncing
    local_assets: set[str]
    #: Metadata for assets currently being downloaded, as a mapping from asset
    #: paths to metadata
    in_progress: dict[str, dict] = field(init=False, default_factory=dict)
    #: Asset metadata from previous sync, plus metadata for any assets
    #: completely downloaded during the sync, as a mapping from asset paths to
    #: metadata
    asset_metadata: dict[str, dict]
    #: Paths of assets that are not being downloaded this run due to a lack of
    #: SHA256 digests
    future_assets: set[str] = field(init=False, default_factory=set)
    #: Paths of assets fetched from the remote server
    remote_assets: set[str] = field(init=False, default_factory=set)

    @classmethod
    def from_dataset(cls, dspath: Path) -> AssetTracker:
        filepath = dspath / ".dandi" / "assets.json"
        local_assets = set(dataset_files(dspath))
        asset_metadata: dict[str, dict] = {}
        try:
            with filepath.open() as fp:
                for md in json.load(fp):
                    if isinstance(md, str):
                        raise RuntimeError(f"Old assets.json format found in {dspath}")
                    else:
                        asset_metadata[md["path"].lstrip("/")] = md
        except FileNotFoundError:
            pass
        return cls(
            filepath=filepath, local_assets=local_assets, asset_metadata=asset_metadata
        )

    def register_asset(self, asset: RemoteAsset, force: str | None) -> str | None:
        # If the asset's metadata has changed (or if we should act like it's
        # changed) since the last sync, a diff is returned
        self.local_assets.discard(asset.path)
        adict = asset2dict(asset)
        self.in_progress[asset.path] = adict
        old_metadata = self.asset_metadata.get(asset.path)
        if old_metadata != adict:
            return (
                diff_metadata(old_metadata, adict)
                or "<metadata unequal but no diff generated>"
            )
        elif force == "assets-update":
            return "<Forced update via --force assets-update>"
        else:
            return None

    def register_asset_by_timestamp(
        self, asset: RemoteAsset, force: str | None
    ) -> bool:
        # Returns True if the asset's `modified` timestamp has changed (or if
        # we should act like it's changed) since the last sync
        self.local_assets.discard(asset.path)
        adict = asset2dict(asset)
        self.in_progress[asset.path] = adict
        return (
            adict["modified"] != self.asset_metadata.get(asset.path, {}).get("modified")
            or force == "assets-update"
        )

    def finish_asset(self, asset_path: str) -> None:
        self.asset_metadata[asset_path] = self.in_progress.pop(asset_path)

    def mark_future(self, asset: RemoteAsset) -> None:
        self.future_assets.add(asset.path)

    def get_deleted(self, config: BackupConfig) -> Iterator[str]:
        """
        Yields paths of deleted assets and removes their metadata from
        `asset_metadata`
        """
        for apath in self.local_assets:
            if config.match_asset(apath):
                self.asset_metadata.pop(apath, None)
                yield apath

    def prune_metadata(self) -> list[str]:
        pruned = []
        for path in list(self.asset_metadata):
            if path not in self.remote_assets:
                self.asset_metadata.pop(path)
                pruned.append(path)
        return pruned

    def dump(self) -> None:
        self.filepath.parent.mkdir(exist_ok=True, parents=True)
        dump([md for _, md in sorted(self.asset_metadata.items())], self.filepath)

    @property
    def future_qty(self) -> int:
        return len(self.future_assets)


def custom_commit_env(dt: datetime | None) -> dict[str, str]:
    env = os.environ.copy()
    if dt is not None:
        env["GIT_AUTHOR_NAME"] = "DANDI User"
        env["GIT_AUTHOR_EMAIL"] = "info@dandiarchive.org"
        env["GIT_AUTHOR_DATE"] = str(dt)
    return env


def dataset_files(dspath: Path) -> Iterator[str]:
    files = deque(
        p for p in dspath.iterdir() if not is_meta_file(p.name, dandiset=True)
    )
    while files:
        p = files.popleft()
        if p.is_file() or p.is_symlink():
            yield str(p.relative_to(dspath))
        elif p.is_dir():
            if (p / ".git").exists():
                # installed subdataset (or not even added/known yet)
                yield str(p.relative_to(dspath))
            else:
                files.extend(p.iterdir())
    # there could be uninstalled, such as .zarr/ subdatasets, report them as well
    for p in Dataset(dspath).subdatasets(
        result_xfm="relpaths", state="absent", result_renderer=None
    ):
        yield str(p)


def is_interactive() -> bool:
    """Return True if all in/outs are tty"""
    return sys.stdin.isatty() and sys.stdout.isatty() and sys.stderr.isatty()


def pdb_excepthook(
    exc_type: type[BaseException], exc_value: BaseException, tb: TracebackType | None
) -> None:
    import traceback

    traceback.print_exception(exc_type, exc_value, tb)
    print()
    if is_interactive():
        import pdb

        pdb.post_mortem(tb)


def asset2dict(asset: RemoteAsset) -> dict[str, Any]:
    return asset.json_dict()


def assets_eq(remote_assets: list[RemoteAsset], local_assets: list[dict]) -> bool:
    return {a.identifier: asset2dict(a) for a in remote_assets} == {
        a["asset_id"]: a for a in local_assets
    }


async def update_dandiset_metadata(
    dandiset: RemoteDandiset, ds: AsyncDataset, log: PrefixedLogger
) -> None:
    log.info("Updating metadata file")
    (ds.pathobj / dandiset_metadata_file).unlink(missing_ok=True)
    metadata = await dandiset.aget_raw_metadata()
    Dandiset(ds.pathobj, allow_empty=True).update_metadata(metadata)
    await ds.add(dandiset_metadata_file)


def quantify(qty: int, singular: str, plural: str | None = None) -> str:
    if qty == 1:
        return f"{qty} {singular}"
    elif plural is None:
        return f"{qty} {singular}s"
    else:
        return f"{qty} {plural}"


def key2hash(key: str) -> str:
    return key.split("-")[-1].partition(".")[0]


def format_errors(messages: list[str]) -> str:
    if not messages:
        return " <no error message>"
    elif len(messages) == 1:
        return " " + messages[0]
    else:
        return "\n\n" + textwrap.indent("".join(messages), " " * 4) + "\n"


def exp_wait(
    base: float = 1.25,
    multiplier: float = 1,
    attempts: int | None = None,
    jitter: float = 0.1,
) -> Iterator[float]:
    """
    Returns a generator of values usable as `sleep()` times when retrying
    something with exponential backoff.

    :param float base:
    :param float multiplier: value to multiply values by after exponentiation
    :param Optional[int] attempts: how many values to yield; set to `None` to
        yield forever
    :param Optional[float] jitter: add +1 of that jitter ratio for the time
        randomly so that wait track is unique.
    :rtype: Iterator[float]
    """
    n = 0
    while attempts is None or n < attempts:
        yield (base**n * multiplier) * (1 + (random.random() - 0.5) * jitter)
        n += 1


def maxdatetime(state: datetime | None, candidate: datetime) -> datetime:
    if state is None or state < candidate:
        return candidate
    else:
        return state


def is_meta_file(path: str, dandiset: bool = False) -> bool:
    root = path.split("/")[0]
    if dandiset and root == dandiset_metadata_file:
        return True
    return root in (".dandi", ".datalad", ".git", ".gitattributes", ".gitmodules")


def diff_metadata(old: Any, new: Any) -> str:
    old_yaml = yaml_dump(old)
    new_yaml = yaml_dump(new)
    return "".join(
        unified_diff(
            old_yaml.splitlines(True),
            new_yaml.splitlines(True),
            fromfile="old-metadata",
            tofile="new-metadata",
        )
    )


def yaml_dump(data: Any) -> str:
    yaml = YAML(typ="safe")
    yaml.default_flow_style = False
    out = StringIO()
    yaml.dump(data, out)
    return out.getvalue()


class UnexpectedChangeError(Exception):
    pass


def check_git_annex_version() -> None:
    # Call this function at the start of subcommand functions rather than in
    # `main()` so that it doesn't run if a user does `backups2datalad
    # subcommand --help`
    from datalad.support.external_versions import external_versions

    gaversion = external_versions["cmd:annex"]
    if gaversion is None:
        raise RuntimeError("git-annex not installed")
    elif gaversion < MINIMUM_GIT_ANNEX_VERSION:
        raise RuntimeError(
            f"git-annex {MINIMUM_GIT_ANNEX_VERSION} or later required, but"
            f" version {gaversion} found"
        )
