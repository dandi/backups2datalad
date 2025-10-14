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


def https_to_ssh_url(url: str) -> str:
    """Convert HTTPS GitHub URL to SSH URL.

    Example: https://github.com/org/repo -> git@github.com:org/repo
    """
    match = re.match(r"https://github\.com/(.+?)(?:\.git)?$", url)
    if match:
        # Remove .git suffix if present
        path = match.group(1).removesuffix(".git")
        return f"git@github.com:{path}"
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

    async def update_zarr_repos_privacy(self, *, update_github: bool = True) -> bool:
        """
        Update Zarr submodule URLs and optionally GitHub repository privacy
        based on parent Dandiset's embargo status.

        If parent Dandiset is embargoed, Zarr submodules should use SSH URLs.
        If parent Dandiset is public, Zarr submodules should use HTTPS URLs.

        Args:
            update_github: If True, also update GitHub repository privacy via API.
                          If False, only fix URLs in .gitmodules and local configs.

        Returns:
            True if any changes were made, False otherwise.

        Raises:
            Exception if update_github=True and any GitHub API call fails.
        """
        # Only proceed if we have GitHub org configured for Zarrs
        if not self.config.zarr_gh_org:
            return False

        embargo_status = await self.ds.get_embargo_status()
        is_embargoed = embargo_status is EmbargoStatus.EMBARGOED

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
            return False

        # Process all Zarr repositories
        updated_submodules = {}
        for submodule in zarr_submodules:
            submodule_path = submodule["gitmodule_path"]
            old_url = submodule["gitmodule_url"]
            zarr_id = extract_repo_name(old_url)

            is_ssh = old_url.startswith("git@github.com:")
            is_https = old_url.startswith("https://github.com/")

            # Determine if URL needs fixing
            needs_fix = False
            if is_embargoed and is_https:
                # Should be SSH
                new_url = https_to_ssh_url(old_url)
                needs_fix = True
            elif not is_embargoed and is_ssh:
                # Should be HTTPS
                new_url = ssh_to_https_url(old_url)
                needs_fix = True
            else:
                # URL is correct
                new_url = old_url

            if not needs_fix and not update_github:
                # Nothing to do for this submodule
                continue

            # If update_github is True, update GitHub repository privacy
            if update_github and self.config.gh_org:
                if is_embargoed:
                    self.log.info("Making Zarr repository %s private", zarr_id)
                    await self.manager.edit_github_repo(
                        GHRepo(self.config.zarr_gh_org, zarr_id),
                        private=True,
                    )
                else:
                    self.log.info("Making Zarr repository %s public", zarr_id)
                    await self.manager.edit_github_repo(
                        GHRepo(self.config.zarr_gh_org, zarr_id),
                        private=False,
                    )

            # Track for updating .gitmodules
            updated_submodules[submodule_path] = {
                "status": "private" if is_embargoed else "public",
                "old_url": old_url,
                "new_url": new_url,
                "full_path": submodule["path"],
                "url_changed": needs_fix,
            }

        if not updated_submodules:
            return False

        # Update github-access-status and URLs in .gitmodules
        if update_github:
            self.log.info(
                "Updating github-access-status in .gitmodules for %d Zarr submodules",
                len(updated_submodules),
            )
        else:
            self.log.info(
                "Fixing URLs in .gitmodules for %d Zarr submodules",
                len(updated_submodules),
            )

        for path, info in updated_submodules.items():
            # Always update github-access-status
            await self.ds.set_repo_config(
                f"submodule.{path}.github-access-status",
                info["status"],
                file=".gitmodules",
            )
            # Update URL if it changed
            if info["url_changed"]:
                await self.ds.set_repo_config(
                    f"submodule.{path}.url", info["new_url"], file=".gitmodules"
                )

        # Update local git config URLs in subdatasets
        for path, info in updated_submodules.items():
            if info["url_changed"]:
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
        if update_github:
            commit_msg = (
                "[backups2datalad] Update github-access-status for Zarr submodules"
            )
        else:
            url_fix_count = sum(
                1 for info in updated_submodules.values() if info["url_changed"]
            )
            commit_msg = (
                f"[backups2datalad] Fix {url_fix_count} Zarr submodule URL(s) "
                "for embargo status"
            )

        await self.ds.commit_if_changed(
            commit_msg,
            paths=[".gitmodules"],
            check_dirty=False,
        )

        return True
