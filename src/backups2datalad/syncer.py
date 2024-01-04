from __future__ import annotations

from dataclasses import dataclass, field

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
                if self.config.gh_org is not None and await self.ds.has_github_remote():
                    self.log.info("Making GitHub repository public ...")
                    dandiset_id = self.dandiset.identifier
                    await self.manager.edit_github_repo(
                        GHRepo(self.config.gh_org, dandiset_id),
                        private=False,
                    )

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
