from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import aclosing
from functools import partial, wraps
import json
import logging
import os
from pathlib import Path
import re
import shlex
import sys
from typing import Concatenate, ParamSpec

import asyncclick as click
from dandi.consts import DANDISET_ID_REGEX, EmbargoStatus
from datalad.api import Dataset

from .adandi import AsyncDandiClient
from .adataset import AsyncDataset
from .aioutil import pool_amap, stream_lines_command
from .config import BackupConfig, Mode, ZarrMode
from .consts import GIT_OPTIONS
from .datasetter import DandiDatasetter
from .logging import log
from .register_s3 import register_s3urls
from .util import check_git_annex_version, format_errors, pdb_excepthook, quantify


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-B",
    "--backup-root",
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False, exists=True, path_type=Path),
)
@click.option(
    "-J",
    "--jobs",
    type=int,
    help="How many parallel jobs to use when downloading and pushing",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]),
    default="INFO",
    help="Set logging level",
    show_default=True,
)
@click.option("--pdb", is_flag=True, help="Drop into debugger if an error occurs")
@click.option(
    "--quiet-debug",
    is_flag=True,
    help="Log backups2datalad at DEBUG and all other loggers at INFO",
)
@click.pass_context
async def main(
    ctx: click.Context,
    jobs: int | None,
    log_level: str,
    pdb: bool,
    quiet_debug: bool,
    backup_root: Path,
    config: Path | None,
) -> None:
    """
    Mirror Dandisets as git-annex repositories.

    Visit <https://github.com/dandi/backups2datalad> for more information.
    """
    if config is None:
        cfg = BackupConfig()
    else:
        cfg = BackupConfig.load_yaml(config)
    if backup_root is not None:
        cfg.backup_root = backup_root
    if jobs is not None:
        cfg.jobs = jobs
    api_token = os.environ.get("DANDI_API_KEY", "").strip()
    if api_token == "":
        raise click.UsageError("DANDI_API_KEY environment variable not set")
    ctx.obj = DandiDatasetter(
        dandi_client=AsyncDandiClient.for_dandi_instance(
            cfg.dandi_instance, token=api_token
        ),
        config=cfg,
    )
    if pdb:
        sys.excepthook = pdb_excepthook
    if quiet_debug:
        log.setLevel(logging.DEBUG)
        log_level = "INFO"
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=getattr(logging, log_level),
    )
    await ctx.obj.debug_logfile(quiet_debug)
    log.info("COMMAND: %s", shlex.join(sys.argv))


P = ParamSpec("P")


def print_logfile(
    f: Callable[Concatenate[DandiDatasetter, P], Awaitable[None]]
) -> Callable[Concatenate[DandiDatasetter, P], Awaitable[None]]:
    @wraps(f)
    async def wrapped(
        datasetter: DandiDatasetter, *args: P.args, **kwargs: P.kwargs
    ) -> None:
        ok = True
        try:
            await f(datasetter, *args, **kwargs)
        except Exception:
            log.exception("An error occurred:")
            ok = False
        if datasetter.logfile is not None:
            print("Logs saved to", datasetter.logfile, file=sys.stderr)
        sys.exit(0 if ok else 1)

    return wrapped


@main.command()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-f",
    "--force",
    type=click.Choice(["assets-update"]),
    help="Force all assets to be updated, even those whose metadata hasn't changed",
)
@click.option(
    "--gc-assets",
    is_flag=True,
    default=None,
    help=(
        "If assets.json contains any assets neither on the server nor in the"
        " backup, delete them instead of erroring"
    ),
)
@click.option(
    "--mode",
    type=click.Choice(list(Mode)),
    default=None,
    help=(
        "How to decide whether to back up a Dandiset.  'timestamp' — only if"
        " timestamp of last backup is older than modified timestamp on server;"
        " 'force' — always; 'verify' — always, but error if there are any"
        " changes without a change to the timestamp.  [default: timestamp,"
        " unless different value set via config file]"
    ),
)
@click.option(
    "--tags/--no-tags",
    default=None,
    help="Enable/disable creation of tags for releases  [default: enabled]",
)
@click.option("-w", "--workers", type=int, help="Number of workers to run concurrently")
@click.option(
    "--zarr-mode",
    type=click.Choice(list(ZarrMode)),
    default=None,
    help=(
        "How to decide whether to back up a Zarr.  'timestamp' — only if"
        " timestamp of last backup is older than some Zarr entry in S3;"
        " 'checksum' — only if Zarr checksum is out of date or doesn't match"
        " expected value; 'asset-checksum' — only if Zarr asset's 'modified'"
        " timestamp is later than that in assets.json and checksum is out of"
        " date or doesn't match expected value; 'force' — always.  [default:"
        " timestamp, unless different value set via config file]"
    ),
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
@print_logfile
async def update_from_backup(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: re.Pattern[str] | None,
    tags: bool | None,
    asset_filter: re.Pattern[str] | None,
    force: str | None,
    workers: int | None,
    gc_assets: bool | None,
    mode: Mode | None,
    zarr_mode: ZarrMode | None,
) -> None:
    """
    Create & update local mirrors of Dandisets and the Zarrs within them.

    By default, this command operates on all Dandisets in the configured DANDI
    instance, but it can be restricted to only operate on specific Dandisets by
    giving the IDs of the desired Dandisets as command-line arguments.
    """
    check_git_annex_version()
    async with datasetter:
        if asset_filter is not None:
            datasetter.config.asset_filter = asset_filter
        if force is not None:
            datasetter.config.force = force
        if tags is not None:
            datasetter.config.enable_tags = tags
        if workers is not None:
            datasetter.config.workers = workers
        if mode is not None:
            datasetter.config.mode = mode
        if zarr_mode is not None:
            datasetter.config.zarr_mode = zarr_mode
        if gc_assets is not None:
            datasetter.config.gc_assets = gc_assets
        await datasetter.update_from_backup(dandisets, exclude=exclude)


@main.command()
@click.option(
    "-P",
    "--partial-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help=(
        "Directory in which to store in-progress Zarr backups.  [default:"
        " `partial-zarrs/` in the backup root]"
    ),
)
@click.option("-w", "--workers", type=int, help="Number of workers to run concurrently")
@click.argument("dandiset")
@click.pass_obj
@print_logfile
async def backup_zarrs(
    datasetter: DandiDatasetter,
    dandiset: str,
    workers: int | None,
    partial_dir: Path | None,
) -> None:
    """
    Create (but do not update) local mirrors of Zarrs for a single Dandiset
    """
    check_git_annex_version()
    async with datasetter:
        if datasetter.config.zarrs is None:
            raise click.UsageError("Zarr backups not configured in config file")
        if workers is not None:
            datasetter.config.workers = workers
        if partial_dir is None:
            partial_dir = datasetter.config.backup_root / "partial-zarrs"
        await datasetter.backup_zarrs(dandiset, partial_dir)


@main.command()
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
@print_logfile
async def update_github_metadata(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: re.Pattern[str] | None,
) -> None:
    """
    Update the homepages and descriptions for the GitHub repositories for the
    given Dandiset mirrors (or all Dandiset mirrors if no arguments are given).
    If all Dandisets are updated, the description for the superdataset is set
    afterwards as well.

    This is a maintenance command that should rarely be necessary to run.
    """
    check_git_annex_version()
    async with datasetter:
        await datasetter.update_github_metadata(dandisets, exclude=exclude)


@main.command()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-f",
    "--force",
    type=click.Choice(["assets-update"]),
    help="Force all assets to be updated, even those whose metadata hasn't changed",
)
@click.option(
    "--commitish", metavar="COMMITISH", help="The commitish to apply the tag to"
)
@click.option(
    "--push/--no-push",
    default=True,
    help="Whether to push the tag to GitHub and create a GitHub release",
    show_default=True,
)
@click.argument("dandiset")
@click.argument("version")
@click.pass_obj
@print_logfile
async def release(
    datasetter: DandiDatasetter,
    dandiset: str,
    version: str,
    commitish: str | None,
    push: bool,
    asset_filter: re.Pattern[str] | None,
    force: str | None,
) -> None:
    """
    Create a tag in the mirror of the given Dandiset for the given published
    version.

    If the mirror is configured to be pushed to GitHub, a GitHub release will
    be created for the tag as well.
    """
    check_git_annex_version()
    async with datasetter:
        if asset_filter is not None:
            datasetter.config.asset_filter = asset_filter
        if force is not None:
            datasetter.config.force = force
        dandiset_obj = await datasetter.dandi_client.get_dandiset(dandiset, version)
        dataset = AsyncDataset(datasetter.config.dandiset_root / dandiset)
        await datasetter.mkrelease(
            dandiset_obj,
            dataset,
            commitish=commitish,
            push=push,
            log=datasetter.manager.log.sublogger(f"Dandiset {dandiset}/{version}"),
        )
        if push:
            await dataset.push(to="github", jobs=datasetter.config.jobs)


@main.command("populate")
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "--force-fast",
    is_flag=True,
    help="Always populate; do not skip population if Dandisets look backed up",
)
@click.option("-w", "--workers", type=int, help="Number of workers to run concurrently")
@click.argument("dandisets", nargs=-1)
@click.pass_obj
@print_logfile
async def populate_cmd(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: re.Pattern[str] | None,
    workers: int | None,
    force_fast: bool,
) -> None:
    """
    Copy assets from local Dandiset mirrors to the git-annex special remote.

    By default, this command operates on all Dandiset mirrors in the local
    Dandisets directory, but it can be restricted to only operate on specific
    mirrors by giving the IDs of the desired Dandisets as command-line
    arguments.
    """
    check_git_annex_version()
    async with datasetter:
        if (r := datasetter.config.dandisets.remote) is not None:
            backup_remote = r.name
        else:
            raise click.UsageError("dandisets.remote not set in config file")
        if workers is not None:
            datasetter.config.workers = workers
        if dandisets:
            diriter = (datasetter.config.dandiset_root / d for d in dandisets)
        else:
            diriter = datasetter.config.dandiset_root.iterdir()
        dirs: list[Path] = []
        for p in diriter:
            if p.is_dir() and re.fullmatch(DANDISET_ID_REGEX, p.name):
                if exclude is not None and exclude.search(p.name):
                    log.debug("Skipping dandiset %s", p.name)
                else:
                    dirs.append(p)
            else:
                log.debug("Skipping non-Dandiset node %s", p.name)
        report = await pool_amap(
            partial(
                populate,
                backup_remote=backup_remote,
                pathtype="Dandiset",
                jobs=datasetter.config.jobs,
                has_github=datasetter.config.gh_org is not None,
                force=force_fast,
            ),
            afilter_installed(dirs),
            workers=datasetter.config.workers,
        )
        if report.failed:
            sys.exit(f"{quantify(len(report.failed), 'populate job')} failed")


@main.command()
@click.option(
    "--force-fast",
    is_flag=True,
    help="Always populate; do not skip population if Zarrs look backed up",
)
@click.option("-w", "--workers", type=int, help="Number of workers to run concurrently")
@click.argument("zarrs", nargs=-1)
@click.pass_obj
@print_logfile
async def populate_zarrs(
    datasetter: DandiDatasetter,
    zarrs: Sequence[str],
    workers: int | None,
    force_fast: bool,
) -> None:
    """
    Copy assets from local Zarr mirrors to the git-annex special remote.

    By default, this command operates on all Zarr mirrors in the local Zarrs
    directory, but it can be restricted to only operate on specific mirrors by
    giving the asset IDs of the desired Zarrs as command-line arguments.
    """
    check_git_annex_version()
    async with datasetter:
        zcfg = datasetter.config.zarrs
        if zcfg is None:
            raise click.UsageError("Zarr backups not configured in config file")
        if (r := zcfg.remote) is not None:
            backup_remote = r.name
        else:
            raise click.UsageError("zarrs.remote not set in config file")
        if workers is not None:
            datasetter.config.workers = workers
        zarr_root = datasetter.config.zarr_root
        assert zarr_root is not None
        if zarrs:
            diriter = (zarr_root / z for z in zarrs)
        else:
            diriter = zarr_root.iterdir()
        dirs: list[Path] = []
        for p in diriter:
            if p.is_dir() and p.name not in (".git", ".datalad"):
                dirs.append(p)
            else:
                log.debug("Skipping non-Zarr node %s", p.name)
        report = await pool_amap(
            partial(
                populate,
                backup_remote=backup_remote,
                pathtype="Zarr",
                jobs=datasetter.config.jobs,
                has_github=datasetter.config.gh_org is not None,
                force=force_fast,
            ),
            afilter_installed(dirs),
            workers=datasetter.config.workers,
        )
        if report.failed:
            sys.exit(f"{quantify(len(report.failed), 'populate-zarr job')} failed")


@main.command()
@click.argument(
    "dirpath", type=click.Path(file_okay=False, exists=True, path_type=Path)
)
async def zarr_checksum(dirpath: Path) -> None:
    """
    Compute the Zarr checksum for the git-annex dataset at `dirpath` using the
    hashes stored in the annexed files' keys
    """
    check_git_annex_version()
    ds = AsyncDataset(dirpath)
    print(await ds.compute_zarr_checksum())


@main.command("register-s3urls")
@click.argument("dandiset_id")
@click.pass_obj
@print_logfile
async def register_s3urls_cmd(datasetter: DandiDatasetter, dandiset_id: str) -> None:
    """
    Ensure that all blob assets in the backup of the given Dandiset have their
    S3 URLs registered with git-annex.

    This command should only be necessary if something went wrong when
    processing the unembargoing of a Dandiset.
    """
    check_git_annex_version()
    async with datasetter:
        p = datasetter.config.dandiset_root / dandiset_id
        ds = AsyncDataset(p)
        if not ds.ds.is_installed():
            raise click.UsageError(f"Dataset at {p} not installed")
        d = await datasetter.dandi_client.get_dandiset(dandiset_id, "draft")
        await register_s3urls(manager=datasetter.manager, dandiset=d, ds=ds)


async def populate(
    dirpath: Path,
    backup_remote: str,
    pathtype: str,
    jobs: int,
    has_github: bool,
    force: bool = False,
) -> None:
    desc = f"{pathtype} {dirpath.name}"
    ds = AsyncDataset(dirpath)
    if await ds.get_embargo_status() is EmbargoStatus.EMBARGOED:
        log.info("%s: embargoed Dandiset; not populating")
        return
    if not force and await ds.populate_up_to_date():
        log.info("%s: no need to populate", desc)
        return
    log.info("Copying files for %s to backup remote", desc)
    for opts in [(), ("--from", "web")]:
        i = 0
        while True:
            try:
                # everything but content of .dandi/ should be moved to backup
                await call_annex_json(
                    "copy",
                    "-c",
                    "annex.retry=3",
                    "--jobs",
                    str(jobs),
                    "--fast",
                    *opts,
                    "--to",
                    backup_remote,
                    "--not",
                    "--in",
                    backup_remote,
                    "--exclude",
                    ".dandi/*",
                    path=dirpath,
                )
            except RuntimeError as e:
                i += 1
                if i < 5:
                    log.error("%s; retrying", e)
                    continue
                else:
                    raise
            else:
                break
    if has_github:
        await ds.call_git("push", "github", "git-annex")
    await ds.update_populate_status()


async def call_annex_json(cmd: str, *args: str, path: Path) -> None:
    success = 0
    failed = 0
    cmdstr = shlex.join([cmd, *args])
    async with aclosing(
        stream_lines_command(
            "git",
            *GIT_OPTIONS,
            "annex",
            cmd,
            *args,
            "--json",
            "--json-error-messages",
            cwd=path,
        )
    ) as p:
        async for line in p:
            data = json.loads(line)
            if data["success"]:
                success += 1
            else:
                log.error(
                    "`git-annex %s` failed for %s:%s",
                    cmdstr,
                    data["file"],
                    format_errors(data["error-messages"]),
                )
                failed += 1
    log.info(
        "git-annex %s: %s succeeded, %s failed",
        cmdstr,
        quantify(success, "file"),
        quantify(failed, "file"),
    )
    if failed:
        raise RuntimeError(
            f"`git-annex {cmdstr}` failed for {quantify(failed, 'file')}"
        )


async def afilter_installed(datasets: list[Path]) -> AsyncGenerator[Path, None]:
    for p in datasets:
        ds = Dataset(p)
        if not ds.is_installed():
            log.info("Dataset %s is not installed; skipping", p.name)
        else:
            yield p


if __name__ == "__main__":
    main(_anyio_backend="asyncio")
