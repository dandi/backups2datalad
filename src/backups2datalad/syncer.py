from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re

from dandi.consts import EmbargoStatus
from ghrepo import GHRepo

from .adandi import RemoteDandiset
from .adataset import AsyncDataset
from .asyncer import Report, async_assets
from .config import BackupConfig
from .logging import PrefixedLogger
from .manager import Manager
from .register_s3 import register_s3urls
from .util import AssetTracker, UnexpectedChangeError, quantify


def ssh_to_https_url(url: str) -> str:
    """Convert SSH GitHub URL to HTTPS URL.

    Example: git@github.com:org/repo -> https://github.com/org/repo
    """
    match = re.match(r"git@github\.com:(.+?)(?:\.git)?$", url)
    if match:
        # Remove .git suffix if present
        path = match.group(1).removesuffix(".git")
        return f"https://github.com/{path}"
    return url


def extract_repo_name(url: str) -> str:
    """Extract repository name from GitHub URL (SSH or HTTPS).

    Examples:
        git@github.com:org/repo -> repo
        https://github.com/org/repo -> repo
    """
    # Handle SSH URLs: git@github.com:org/repo
    match = re.match(r"git@github\.com:.+/(.+?)(?:\.git)?$", url)
    if match:
        return match.group(1).removesuffix(".git")
    # Handle HTTPS URLs: https://github.com/org/repo
    match = re.match(r"https://github\.com/.+/(.+?)(?:\.git)?$", url)
    if match:
        return match.group(1).removesuffix(".git")
    # Fallback to Path.name for backward compatibility
    return Path(url).name.removesuffix(".git")


@dataclass
class Syncer:
    manager: Manager
    dandiset: RemoteDandiset
    ds: AsyncDataset
    tracker: AssetTracker
    error_on_change: bool
    deleted: int = 0
    # value of garbage_assets will be assigned but to pacify mypy - assign factory
    garbage_assets: list[str] = field(default_factory=list)
    report: Report = field(init=False, default_factory=Report)

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    async def update_embargo_status(self) -> None:
        old_status = await self.ds.get_embargo_status()
        new_status = self.dandiset.embargo_status
        if old_status != new_status:
            if self.error_on_change:
                raise UnexpectedChangeError(
                    f"Dandiset {self.dandiset.identifier}: Embargo status"
                    f" changed from {old_status.value} to {new_status.value}"
                    " but timestamp was not updated on server"
                )
            self.log.info(
                "Updating embargo status from %s to %s",
                old_status.value,
                new_status.value,
            )
            await self.ds.set_embargo_status(self.dandiset.embargo_status)
            commit_date = await self.ds.get_last_commit_date()
            await self.ds.save(
                "[backups2datalad] Update embargo status", commit_date=commit_date
            )
            self.report.commits += 1
            if (
                old_status is EmbargoStatus.EMBARGOED
                and new_status is EmbargoStatus.OPEN
            ):
                self.log.info("Registering S3 URLs ...")
                await register_s3urls(self.manager, self.dandiset, self.ds)
                self.log.info("Disabling datalad special remote ...")
                await self.ds.disable_dandi_provider()
                if self.config.gh_org is not None and await self.ds.has_github_remote():
                    self.log.info("Making GitHub repository public ...")
                    dandiset_id = self.dandiset.identifier
                    await self.manager.edit_github_repo(
                        GHRepo(self.config.gh_org, dandiset_id),
                        private=False,
                    )

                    # Update GitHub access status for all Zarr repositories
                    if self.config.zarr_gh_org is not None:
                        await self.update_zarr_repos_privacy()

    async def sync_assets(self) -> None:
        self.log.info("Syncing assets...")
        report = await async_assets(
            self.dandiset, self.ds, self.manager, self.tracker, self.error_on_change
        )
        self.log.info("Asset sync complete!")
        self.log.info("%s added", quantify(report.added, "asset"))
        self.log.info("%s updated", quantify(report.updated, "asset"))
        self.log.info("%s registered", quantify(report.registered, "asset"))
        self.log.info(
            "%s successfully downloaded", quantify(report.downloaded, "asset")
        )
        report.check()
        self.report.update(report)

    async def prune_deleted(self) -> None:
        for asset_path in self.tracker.get_deleted(self.config):
            if self.error_on_change:
                raise UnexpectedChangeError(
                    f"Dandiset {self.dandiset.identifier}: Asset {asset_path!r}"
                    " deleted from Dandiset but timestamp was not updated on"
                    " server"
                )
            self.log.info(
                "%s: Asset is in dataset but not in Dandiarchive; deleting", asset_path
            )
            await self.ds.remove(asset_path)
            self.deleted += 1

    async def dump_asset_metadata(self) -> None:
        self.garbage_assets = self.tracker.prune_metadata()
        if self.garbage_assets and not self.config.gc_assets:
            # to ease troubleshooting, let's list some which were GCed
            listing = ", ".join(self.garbage_assets[:3])
            if len(self.garbage_assets) > 3:
                listing += f" and {len(self.garbage_assets) - 3} more."
            raise UnexpectedChangeError(
                f"Dandiset {self.dandiset.identifier}:"
                f" {quantify(len(self.garbage_assets), 'asset')}"
                f" garbage-collected from assets.json: {listing}"
            )
        self.tracker.dump()
        await self.ds.add(".dandi/assets.json")

    def get_commit_message(self) -> str:
        msgparts = []
        if self.dandiset.version_id != "draft":
            if self.report.added:
                msgparts.append(f"{quantify(self.report.added, 'file')} added")
            if self.report.updated:
                msgparts.append(f"{quantify(self.report.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if self.garbage_assets:
            msgparts.append(
                f"{quantify(len(self.garbage_assets), 'asset')} garbage-collected"
                " from .dandi/assets.json"
            )
        if futures := self.tracker.future_qty:
            msgparts.append(f"{quantify(futures, 'asset')} not yet downloaded")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"

    async def update_zarr_repos_privacy(self) -> None:
        """
        Update all Zarr GitHub repositories to public when the parent Dandiset
        is unembargoed. Also updates the github-access-status in .gitmodules
        for all Zarr submodules.

        Raises an exception if any Zarr repository fails to update, ensuring
        problems are noticed and addressed rather than silently ignored.
        """
        # Only proceed if we have GitHub org configured for both
        # Dandisets and Zarrs
        if not (self.config.gh_org and self.config.zarr_gh_org):
            return

        self.log.info("Updating privacy for Zarr repositories...")

        # Get all submodules from the dataset
        submodules = await self.ds.get_subdatasets()

        # Track Zarr submodules to update
        zarr_submodules = []
        for submodule in submodules:
            path = submodule["path"]
            basename = Path(path).name

            # Check if this is a Zarr submodule (typical zarr files end
            # with .zarr or .ngff)
            if basename.endswith((".zarr", ".ngff")):
                zarr_submodules.append(submodule)

        if not zarr_submodules:
            self.log.info("No Zarr repositories found to update")
            return

        # Update all Zarr repositories - fail fast if any update fails
        updated_submodules = {}
        for submodule in zarr_submodules:
            submodule_path = submodule["gitmodule_path"]
            old_url = submodule["gitmodule_url"]
            zarr_id = extract_repo_name(old_url)

            self.log.info("Making Zarr repository %s public", zarr_id)
            # Let exceptions propagate - we want to know if this fails
            await self.manager.edit_github_repo(
                GHRepo(self.config.zarr_gh_org, zarr_id),
                private=False,
            )

            # Convert SSH URL to HTTPS for public repos
            new_url = ssh_to_https_url(old_url)

            # Track for updating .gitmodules
            updated_submodules[submodule_path] = {
                "status": "public",
                "old_url": old_url,
                "new_url": new_url,
                "full_path": submodule["path"],
            }

        # Update github-access-status and URLs in .gitmodules for all Zarr submodules
        self.log.info(
            "Updating github-access-status in .gitmodules for %d Zarr " "submodules",
            len(updated_submodules),
        )

        for path, info in updated_submodules.items():
            await self.ds.set_repo_config(
                f"submodule.{path}.github-access-status",
                info["status"],
                file=".gitmodules",
            )
            # Update URL from SSH to HTTPS if it changed
            if info["old_url"] != info["new_url"]:
                await self.ds.set_repo_config(
                    f"submodule.{path}.url", info["new_url"], file=".gitmodules"
                )

        # Update local git config URLs in subdatasets
        for path, info in updated_submodules.items():
            if info["old_url"] != info["new_url"]:
                self.log.debug(
                    "Updating local git config URL for %s from %s to %s",
                    path,
                    info["old_url"],
                    info["new_url"],
                )
                await self.ds.call_git(
                    "config",
                    "--file",
                    f"{info['full_path']}/.git/config",
                    "remote.github.url",
                    info["new_url"],
                )

        # Commit the changes to .gitmodules
        await self.ds.commit_if_changed(
            "[backups2datalad] Update github-access-status for Zarr " "submodules",
            paths=[".gitmodules"],
            check_dirty=False,
        )
