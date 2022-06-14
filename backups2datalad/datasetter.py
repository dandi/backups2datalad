from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
import json
import logging
from operator import attrgetter
from pathlib import Path
import re
import shlex
import subprocess
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import DandiAPIClient, RemoteDandiset
import datalad
from datalad.api import Dataset
from ghrepo import GHRepo
from github import Github
from github.GithubException import UnknownObjectException
from github.Repository import Repository
from humanize import naturalsize
from morecontext import envset
from packaging.version import Version
from urllib3.util.retry import Retry

from .config import Config
from .consts import DEFAULT_BRANCH
from .syncer import Syncer
from .util import (
    assets_eq,
    create_github_sibling,
    custom_commit_date,
    dandi_logging,
    init_dataset,
    is_meta_file,
    log,
    quantify,
    readcmd,
    update_dandiset_metadata,
)


@dataclass
class DandiDatasetter:
    dandi_client: DandiAPIClient
    config: Config

    def ensure_superdataset(self) -> Dataset:
        superds = Dataset(self.config.dandiset_root)
        if not superds.is_installed():
            log.info("Creating Datalad superdataset")
            with envset(
                "GIT_CONFIG_PARAMETERS", f"'init.defaultBranch={DEFAULT_BRANCH}'"
            ):
                superds.create(cfg_proc="text2git")
        return superds

    def update_from_backup(
        self,
        dandiset_ids: Sequence[str] = (),
        exclude: Optional[re.Pattern[str]] = None,
    ) -> None:
        datalad.cfg.set("datalad.repo.backend", "SHA256E", scope="override")
        superds = self.ensure_superdataset()
        to_save: List[str] = []
        ds_stats: List[DandisetStats] = []
        for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            dsdir = self.config.dandiset_root / d.identifier
            ds = self.init_dataset(
                dsdir, dandiset_id=d.identifier, create_time=d.version.created
            )
            changed = self.sync_dataset(d, ds)
            to_save.append(d.identifier)
            self.ensure_github_remote(ds, d.identifier)
            self.tag_releases(d, ds, push=self.config.gh_org is not None)
            if self.config.gh_org is not None:
                if changed:
                    log.info("Pushing to sibling")
                    ds.push(to="github", jobs=self.config.jobs, data="nothing")
                ds_stats.append(self.set_dandiset_gh_metadata(d, ds))
        log.debug("Committing superdataset")
        superds.save(message="CRON update", path=to_save)
        log.debug("Superdataset committed")
        if self.config.gh_org is not None and not dandiset_ids and exclude is None:
            self.set_superds_description(superds, ds_stats)

    def init_dataset(
        self, dsdir: Path, dandiset_id: str, create_time: datetime
    ) -> Dataset:
        ds = Dataset(dsdir)
        if not ds.is_installed():
            init_dataset(
                ds,
                desc=f"Dandiset {dandiset_id}",
                commit_date=create_time,
                backup_remote=self.config.dandisets.remote,
            )
        return ds

    def ensure_github_remote(self, ds: Dataset, dandiset_id: str) -> None:
        if self.config.gh_org is not None:
            if create_github_sibling(
                ds,
                owner=self.config.gh_org,
                name=dandiset_id,
                backup_remote=self.config.dandisets.remote,
            ):
                while True:
                    try:
                        r = self.get_github_repo(f"{self.config.gh_org}/{dandiset_id}")
                    except UnknownObjectException:
                        log.warning(
                            "GitHub sibling for %s not created yet; sleeping"
                            " and retrying",
                            dandiset_id,
                        )
                        sleep(5)
                    else:
                        break
                r.edit(homepage=f"https://identifiers.org/DANDI:{dandiset_id}")

    def sync_dataset(self, dandiset: RemoteDandiset, ds: Dataset) -> bool:
        # Returns true if any changes were committed to the repository
        log.info("Syncing Dandiset %s", dandiset.identifier)
        if ds.repo.dirty:
            raise RuntimeError(f"Dirty {dandiset}; clean or save before running")
        with Syncer(config=self.config, dandiset=dandiset, ds=ds) as syncer:
            with dandi_logging(ds.pathobj) as logfile:
                update_dandiset_metadata(dandiset, ds)
                syncer.sync_assets()
                syncer.prune_deleted()
                syncer.dump_asset_metadata()
            assert syncer.report is not None
            log.debug("Checking whether repository is dirty ...")
            if any(r["state"] != "clean" for r in ds.status(result_renderer=None)):
                log.info("Committing changes")
                with custom_commit_date(dandiset.version.modified):
                    ds.save(message=syncer.get_commit_message())
                log.debug("Commit made")
                syncer.report.commits += 1
            else:
                log.debug("Repository is clean")
                if syncer.report.commits == 0:
                    log.info("No changes made to repository; deleting logfile")
                    logfile.unlink()
            log.debug("Running `git gc`")
            try:
                subprocess.run(["git", "gc"], cwd=ds.path, check=True)
            except subprocess.CalledProcessError as e:
                if e.returncode == 128:
                    log.warning("`git gc` in %s exited with code 128", ds.path)
                else:
                    raise
            log.debug("Finished running `git gc`")
            return syncer.report.commits > 0

    def get_remote_url(self, ds: Dataset) -> str:
        upstream = ds.repo.config.get(f"branch.{DEFAULT_BRANCH}.remote")
        if upstream is None:
            raise ValueError(
                f"Upstream branch not set for {DEFAULT_BRANCH} in {ds.path}"
            )
        url = ds.repo.config.get(f"remote.{upstream}.url")
        if url is None:
            raise ValueError(f"{upstream!r} remote URL not set for {ds.path}")
        assert isinstance(url, str)
        return url

    def get_github_repo_for_dataset(self, ds: Dataset) -> Repository:
        url = self.get_remote_url(ds)
        r = GHRepo.parse_url(url)
        return self.get_github_repo(str(r))

    def update_github_metadata(
        self,
        dandiset_ids: Sequence[str],
        exclude: Optional[re.Pattern[str]],
    ) -> None:
        ds_stats: List[DandisetStats] = []
        for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            ds = Dataset(self.config.dandiset_root / d.identifier)
            ds_stats.append(self.set_dandiset_gh_metadata(d, ds))
        if not dandiset_ids and exclude is None:
            superds = Dataset(self.config.dandiset_root)
            self.set_superds_description(superds, ds_stats)

    def get_dandisets(
        self, dandiset_ids: Sequence[str], exclude: Optional[re.Pattern[str]]
    ) -> Iterator[RemoteDandiset]:
        if dandiset_ids:
            diter = (
                self.dandi_client.get_dandiset(did, "draft", lazy=False)
                for did in dandiset_ids
            )
        else:
            diter = (d.for_version("draft") for d in self.dandi_client.get_dandisets())
        for d in diter:
            if exclude is not None and exclude.search(d.identifier):
                log.debug("Skipping dandiset %s", d.identifier)
            else:
                yield d

    def get_dandiset_stats(
        self, ds: Dataset
    ) -> Tuple[DandisetStats, Dict[str, DandisetStats]]:
        files = 0
        size = 0
        substats: Dict[str, DandisetStats] = {}
        for filestat in ds.status(annex="basic", result_renderer=None):
            path = Path(filestat["path"]).relative_to(ds.pathobj)
            if not is_meta_file(path.parts[0], dandiset=True):
                if filestat["type"] == "dataset":
                    zarr_ds = Dataset(filestat["path"])
                    zarr_id = Path(self.get_remote_url(zarr_ds)).name
                    try:
                        zarr_stat = substats[zarr_id]
                    except KeyError:
                        zarr_stat, subsubstats = self.get_dandiset_stats(zarr_ds)
                        assert not subsubstats
                        substats[zarr_id] = zarr_stat
                    files += zarr_stat.files
                    size += zarr_stat.size
                else:
                    files += 1
                    size += filestat["bytesize"]
        return (DandisetStats(files=files, size=size), substats)

    def set_dandiset_gh_metadata(self, d: RemoteDandiset, ds: Dataset) -> DandisetStats:
        assert self.config.gh_org is not None
        assert self.config.zarr_gh_org is not None
        repo = self.get_github_repo(f"{self.config.gh_org}/{d.identifier}")
        log.info("Setting metadata for %s ...", repo.full_name)
        stats, zarrstats = self.get_dandiset_stats(ds)
        repo.edit(
            homepage=f"https://identifiers.org/DANDI:{d.identifier}",
            description=self.describe_dandiset(d, stats),
        )
        for zarr_id, zarr_stat in zarrstats.items():
            zarr_repo = self.get_github_repo(f"{self.config.zarr_gh_org}/{zarr_id}")
            log.info("Setting metadata for %s ...", zarr_repo.full_name)
            zarr_repo.edit(description=self.describe_zarr(zarr_stat))
        return stats

    def describe_dandiset(self, dandiset: RemoteDandiset, stats: DandisetStats) -> str:
        metadata = dandiset.get_raw_metadata()
        desc = dandiset.version.name
        contact = ", ".join(
            c["name"]
            for c in metadata.get("contributor", [])
            if "dandi:ContactPerson" in c.get("roleName", []) and "name" in c
        )
        if contact:
            desc = f"{contact}, {desc}"
        versions = sum(1 for v in dandiset.get_versions() if v.identifier != "draft")
        if versions:
            desc = f"{quantify(versions, 'release')}, {desc}"
        size = naturalsize(stats.size)
        return f"{quantify(stats.files, 'file')}, {size}, {desc}"

    def describe_zarr(self, stats: DandisetStats) -> str:
        size = naturalsize(stats.size)
        return f"{quantify(stats.files, 'file')}, {size}"

    def set_superds_description(
        self, superds: Dataset, ds_stats: List[DandisetStats]
    ) -> None:
        log.info("Setting repository description for superdataset")
        repo = self.get_github_repo_for_dataset(superds)
        total_size = naturalsize(sum(s.size for s in ds_stats))
        desc = (
            f"{quantify(len(ds_stats), 'Dandiset')}, {total_size} total."
            "  DataLad super-dataset of all Dandisets from https://github.com/dandisets"
        )
        repo.edit(description=desc)

    def tag_releases(self, dandiset: RemoteDandiset, ds: Dataset, push: bool) -> None:
        if not self.config.enable_tags:
            return
        log.info("Tagging releases for Dandiset %s", dandiset.identifier)
        versions = [v for v in dandiset.get_versions() if v.identifier != "draft"]
        for v in versions:
            if readcmd("git", "tag", "-l", v.identifier, cwd=ds.path):
                log.debug("Version %s already tagged", v.identifier)
            else:
                log.info("Tagging version %s", v.identifier)
                self.mkrelease(dandiset.for_version(v), ds, push=push)
        if versions:
            latest = max(map(attrgetter("identifier"), versions), key=Version)
            description = readcmd(
                "git", "describe", "--tags", "--long", "--always", cwd=ds.path
            )
            if "-" not in description:
                # No tags on default branch
                merge = True
            else:
                m = re.fullmatch(
                    r"(?P<tag>.+)-(?P<distance>[0-9]+)-g(?P<rev>[0-9a-f]+)?",
                    description,
                )
                assert m, f"Could not parse `git describe` output: {description!r}"
                merge = Version(latest) > Version(m["tag"])
            if merge:
                log.debug("Running: git merge -s ours %s", shlex.quote(latest))
                subprocess.run(
                    [
                        "git",
                        "merge",
                        "-s",
                        "ours",
                        "-m",
                        f"Merge '{latest}' into drafts branch (no differences"
                        " in content merged)",
                        latest,
                    ],
                    cwd=ds.path,
                    check=True,
                )
            if push:
                ds.push(to="github", jobs=self.config.jobs, data="nothing")

    def mkrelease(
        self,
        dandiset: RemoteDandiset,
        ds: Dataset,
        push: bool,
        commitish: Optional[str] = None,
    ) -> None:
        # `dandiset` must have its version set to the published version
        repo: Path = ds.pathobj
        remote_assets = list(dandiset.get_assets())

        def git(*args: str, **kwargs: Any) -> None:
            log.debug("Running: git %s", " ".join(shlex.quote(str(a)) for a in args))
            subprocess.run(["git", *args], cwd=repo, check=True, **kwargs)

        def commit_has_assets(commit_hash: str) -> bool:
            repo_assets = json.loads(
                readcmd("git", "show", f"{commit_hash}:.dandi/assets.json", cwd=repo)
            )
            return (not remote_assets and not repo_assets) or (
                repo_assets
                and isinstance(repo_assets[0], dict)
                and "asset_id" in repo_assets[0]
                and assets_eq(remote_assets, repo_assets)
            )

        candidates: List[str]
        if commitish is None:
            candidates = []
            # --before orders by commit date, not author date, so we need to
            # filter commits ourselves.
            commits = readcmd(
                "git", "log", r"--grep=\[backups2datalad\]", "--format=%H %aI", cwd=repo
            ).splitlines()
            for cmt in commits:
                chash, _, cdate = cmt.partition(" ")
                ts = datetime.fromisoformat(cdate)
                if ts <= dandiset.version.created:
                    candidates.append(chash)
                    break
            assert candidates, "we should have had at least a single commit"
            if (
                # --reverse is applied after -n 1, so we cannot use it to get
                # just one commit in chronological order after the first
                # candidate, so we will get all and take last
                cmt := readcmd(
                    "git",
                    "rev-list",
                    r"--grep=\[backups2datalad\]",
                    f"{candidates[0]}..HEAD",
                    cwd=repo,
                )
            ) != "":
                candidates.append(cmt.split()[-1])
        else:
            candidates = [commitish]
        matching = list(filter(commit_has_assets, candidates))
        assert len(matching) < 2, (
            f"Commits both before and after {dandiset.version.created} have"
            " matching asset metadata"
        )
        if matching:
            log.info(
                "Found commit %s with matching asset metadata;"
                " updating Dandiset metadata",
                matching[0],
            )
            git("checkout", "-b", f"release-{dandiset.version_id}", matching[0])
            update_dandiset_metadata(dandiset, ds)
            log.debug("Committing changes")
            with custom_commit_date(dandiset.version.created):
                ds.save(message=f"[backups2datalad] {dandiset_metadata_file} updated")
            log.debug("Commit made")
        else:
            log.info(
                "Assets in candidate commits do not match assets in version %s;"
                " syncing",
                dandiset.version_id,
            )
            git("checkout", "-b", f"release-{dandiset.version_id}", candidates[0])
            self.sync_dataset(dandiset, ds)
        with envset("GIT_COMMITTER_NAME", "DANDI User"):
            with envset("GIT_COMMITTER_EMAIL", "info@dandiarchive.org"):
                with envset("GIT_COMMITTER_DATE", str(dandiset.version.created)):
                    git(
                        "tag",
                        "-m",
                        f"Version {dandiset.version_id} of Dandiset"
                        f" {dandiset.identifier}",
                        dandiset.version_id,
                    )
        git("checkout", DEFAULT_BRANCH)
        git("branch", "-D", f"release-{dandiset.version_id}")
        if push:
            git("push", "github", dandiset.version_id)

    @cached_property
    def gh(self) -> Github:
        token = readcmd("git", "config", "hub.oauthtoken")
        return Github(
            token,
            retry=Retry(
                total=12,
                backoff_factor=1.25,
                status_forcelist=[500, 502, 503, 504],
            ),
        )

    def get_github_repo(self, repo_fullname: str) -> Repository:
        return self.gh.get_repo(repo_fullname)

    def debug_logfile(self) -> None:
        """
        Log all log messages at DEBUG or higher to a file without disrupting or
        altering the logging to the screen
        """
        root = logging.getLogger()
        screen_level = root.getEffectiveLevel()
        root.setLevel(logging.NOTSET)
        for h in root.handlers:
            h.setLevel(screen_level)
        # Superdataset must exist before creating anything in the directory:
        self.ensure_superdataset()
        logdir = self.config.dandiset_root / ".git" / "dandi" / "backups2datalad"
        logdir.mkdir(exist_ok=True, parents=True)
        filename = "{:%Y.%m.%d.%H.%M.%SZ}.log".format(datetime.utcnow())
        logfile = logdir / filename
        handler = logging.FileHandler(logfile, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        fmter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        handler.setFormatter(fmter)
        root.addHandler(handler)
        log.info("Saving logs to %s", logfile)


@dataclass
class DandisetStats:
    files: int
    size: int
