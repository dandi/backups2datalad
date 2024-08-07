from __future__ import annotations

from collections import Counter
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import aclosing
from dataclasses import InitVar, dataclass, field, replace
from datetime import datetime
from enum import Enum
from functools import partial
import json
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
from typing import Any, ClassVar

import anyio
from anyio.to_thread import run_sync
from dandi.consts import EmbargoStatus
from datalad.api import Dataset
from datalad.runner.exception import CommandError
from ghrepo import GHRepo
from pydantic import BaseModel
from zarr_checksum.tree import ZarrChecksumTree

from .aioutil import areadcmd, aruncmd, stream_lines_command, stream_null_command
from .config import BackupConfig, Remote
from .consts import DEFAULT_BRANCH, GIT_OPTIONS
from .logging import log
from .util import custom_commit_env, exp_wait, is_meta_file, key2hash

EMBARGO_STATUS_KEY = "dandi.dandiset.embargo-status"

DATALAD_CREDS_REMOTE_UUID = "cf13d535-b47c-5df6-8590-0793cb08a90a"


@dataclass
class AsyncDataset:
    ds: Dataset = field(init=False)
    dirpath: InitVar[str | Path]
    lock: anyio.Semaphore = field(
        init=False, default_factory=lambda: anyio.Semaphore(1)
    )

    def __post_init__(self, dirpath: str | Path) -> None:
        self.ds = Dataset(dirpath)

    @property
    def path(self) -> str:
        assert isinstance(self.ds.path, str)
        return self.ds.path

    @property
    def pathobj(self) -> Path:
        assert isinstance(self.ds.pathobj, Path)
        return self.ds.pathobj

    async def ensure_installed(
        self,
        desc: str,
        commit_date: datetime | None = None,
        backup_remote: Remote | None = None,
        backend: str = "SHA256E",
        cfg_proc: str | None = "text2git",
        embargo_status: EmbargoStatus = EmbargoStatus.OPEN,
    ) -> bool:
        # Returns True if the dataset was freshly created
        if self.ds.is_installed():
            return False
        log.info("Creating dataset for %s", desc)
        argv = ["datalad", "-c", f"datalad.repo.backend={backend}", "create"]
        if cfg_proc is not None:
            argv.append("-c")
            argv.append(cfg_proc)
        argv.append(self.path)
        await aruncmd(
            *argv,
            env={
                **custom_commit_env(commit_date),
                "GIT_CONFIG_PARAMETERS": f"'init.defaultBranch={DEFAULT_BRANCH}'",
            },
        )
        if embargo_status is not EmbargoStatus.OPEN:
            await self.set_embargo_status(embargo_status)
            await self.save(
                "[backups2datalad] Set embargo status", commit_date=commit_date
            )
        await self.call_annex(
            "initremote",
            "--sameas=web",
            "dandiapi",
            "type=web",
            "urlinclude=*//api.dandiarchive.org/*",
            "cost=300",
        )
        if backup_remote is not None:
            await self.call_annex(
                "initremote",
                backup_remote.name,
                f"type={backup_remote.type}",
                *[f"{k}={v}" for k, v in backup_remote.options.items()],
            )
            await self.call_annex("untrust", backup_remote.name)
            await self.call_annex(
                "wanted",
                backup_remote.name,
                "(not metadata=distribution-restrictions=*)",
            )
        log.debug("Dataset for %s created", desc)
        return True

    async def ensure_dandi_provider(self, api_url: str) -> None:
        prov_cfg = Path(".datalad", "providers", "dandi.cfg")
        provider_file = self.pathobj / prov_cfg
        if not provider_file.exists():
            url_re = re.escape(api_url)
            url_re = re.sub(r"^https?:", "https?:", url_re)
            if not url_re.endswith("/"):
                url_re += "/"
            url_re += ".*"
            provider_file.parent.mkdir(parents=True, exist_ok=True)
            provider_file.write_text(
                "[provider:dandi]\n"
                f"url_re = {url_re}\n"
                "authentication_type = http_token\n"
                "credential = dandi\n"
                "\n"
                "[credential:dandi]\n"
                "type = token\n"
            )
            await self.call_git("add", str(prov_cfg))
            await self.commit(
                message="[backups2datalad] Add dandi provider config",
                paths=[prov_cfg],
                check_dirty=False,
            )
        if "datalad" in (await self.read_git("remote")).splitlines():
            annex_uuid = await self.get_repo_config("remote.datalad.annex-uuid")
            if annex_uuid != DATALAD_CREDS_REMOTE_UUID:
                raise RuntimeError(
                    f"Dataset {self.path}: expected remote.datalad.annex-uuid"
                    f" to be {DATALAD_CREDS_REMOTE_UUID!r} but got"
                    f" {annex_uuid!r}"
                )
        else:
            info = json.loads(await self.read_annex("info", "--json"))
            if any(
                sr["uuid"] == DATALAD_CREDS_REMOTE_UUID
                for sr in info["semitrusted repositories"]
            ):
                await self.call_annex("enableremote", "datalad")
            else:
                await self.call_annex(
                    "initremote",
                    "datalad",
                    "type=external",
                    "externaltype=datalad",
                    "encryption=none",
                    "autoenable=true",
                    f"uuid={DATALAD_CREDS_REMOTE_UUID}",
                )

    async def disable_dandi_provider(self) -> None:
        # `anyio.run_process()` doesn't support files or streams as stdin to
        # subprocesses, so we have to do this the synchronous way.

        def reregister_keys() -> None:
            with subprocess.Popen(
                [
                    "git",
                    *GIT_OPTIONS,
                    "annex",
                    "find",
                    "--include=*",
                    "--format=${key}\\n",
                ],
                cwd=self.pathobj,
                stdout=subprocess.PIPE,
            ) as p:
                try:
                    subprocess.run(
                        [
                            "git",
                            *GIT_OPTIONS,
                            "annex",
                            "reregisterurl",
                            "--batch",
                            "--move-from=datalad",
                        ],
                        cwd=self.pathobj,
                        stdin=p.stdout,
                        check=True,
                    )
                finally:
                    p.terminate()

        await run_sync(reregister_keys)
        await self.call_git("remote", "remove", "datalad")

    async def is_dirty(self) -> bool:
        return (
            await self.read_git(
                "status",
                "--porcelain",
                # Forcibly use default values for these options in case they
                # were overridden by user's gitconfig:
                "--untracked-files=normal",
                "--ignore-submodules=none",
            )
            != ""
        )

    async def has_changes(
        self, paths: Sequence[str | Path] = (), cached: bool = False
    ) -> bool:
        args: list[str | Path] = ["diff", "--quiet"]
        if cached:
            args.append("--cached")
        if paths:
            args.append("--")
            args.extend(paths)
        try:
            await self.call_git(*args, quiet_rcs=[1])
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return True
            else:
                raise
        else:
            return False

    async def get_repo_config(self, key: str, file: str | None = None) -> str | None:
        args = ["--file", file] if file is not None else []
        try:
            return await self.read_git("config", *args, "--get", key, quiet_rcs=[1])
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return None
            else:
                raise

    async def set_repo_config(
        self, key: str, value: str, file: str | None = None
    ) -> None:
        args = ["--file", file] if file is not None else ["--local"]
        await self.call_git(
            "config",
            *args,
            "--replace-all",
            key,
            value,
        )

    async def get_datalad_id(self) -> str:
        r = await self.get_repo_config("datalad.dataset.id", file=".datalad/config")
        assert r is not None
        return r

    async def get_embargo_status(self) -> EmbargoStatus:
        value = await self.get_repo_config(EMBARGO_STATUS_KEY, file=".datalad/config")
        if value is None:
            return EmbargoStatus.OPEN
        else:
            return EmbargoStatus(value)

    async def set_embargo_status(self, status: EmbargoStatus) -> None:
        await self.set_repo_config(
            EMBARGO_STATUS_KEY, status.value, file=".datalad/config"
        )

    async def call_git(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd(
            "git",
            *GIT_OPTIONS,
            *args,
            cwd=self.path,
            **kwargs,
        )

    async def read_git(self, *args: str | Path, **kwargs: Any) -> str:
        return await areadcmd(
            "git",
            *GIT_OPTIONS,
            *args,
            cwd=self.path,
            **kwargs,
        )

    async def call_annex(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd("git", *GIT_OPTIONS, "annex", *args, cwd=self.path, **kwargs)

    async def read_annex(self, *args: str | Path, **kwargs: Any) -> str:
        return await areadcmd(
            "git", *GIT_OPTIONS, "annex", *args, cwd=self.path, **kwargs
        )

    async def save(
        self,
        message: str,
        path: Sequence[str | Path] = (),
        commit_date: datetime | None = None,
    ) -> None:
        # TODO: Improve
        await aruncmd(
            "datalad",
            "save",
            "-d",
            ".",
            "-m",
            message,
            *path,
            cwd=self.path,
            env=custom_commit_env(commit_date),
        )

    async def commit(
        self,
        message: str,
        commit_date: datetime | None = None,
        paths: Sequence[str | Path] = (),
        check_dirty: bool = True,
    ) -> None:
        """Use git commit directly, verify that all is committed

        Raises RuntimeError if dataset remains dirty.

        Primarily to be used to overcome inability of datalad save to save
        updated states of subdatasets without them being installed in the tree.
        Ref: https://github.com/datalad/datalad/issues/7074
        """
        await self.call_git(
            "commit",
            "-m",
            message,
            "--",
            *map(str, paths),
            env=custom_commit_env(commit_date),
        )
        if check_dirty and await self.is_dirty():
            raise RuntimeError(
                f"{self.path} is still dirty after committing."
                "  Please check if all changes were staged."
            )

    async def commit_if_changed(
        self,
        message: str,
        commit_date: datetime | None = None,
        paths: Sequence[str | Path] = (),
        check_dirty: bool = True,
    ) -> None:
        await self.call_git("add", "-A", *paths)
        if await self.has_changes(paths=paths, cached=True):
            await self.commit(
                message,
                commit_date=commit_date,
                paths=paths,
                check_dirty=check_dirty,
            )

    async def push(self, to: str, jobs: int, data: str | None = None) -> None:
        waits = exp_wait(attempts=6, base=2.1)
        while True:
            try:
                # TODO: Improve
                await anyio.to_thread.run_sync(
                    partial(self.ds.push, to=to, jobs=jobs, data=data)
                )
            except CommandError as e:
                if "unexpected disconnect" in str(e):
                    try:
                        delay = next(waits)
                    except StopIteration:
                        raise e
                    log.warning(
                        "Push of dataset at %s failed with unexpected"
                        " disconnect; retrying",
                        self.path,
                    )
                    await anyio.sleep(delay)
                    continue
                else:
                    raise
            else:
                break

    async def gc(self) -> None:
        try:
            await self.call_git("gc")
        except subprocess.CalledProcessError as e:
            if e.returncode == 128:
                log.warning("`git gc` in %s exited with code 128", self.path)
            else:
                raise

    async def add(self, path: str) -> None:
        # `path` must be relative to the root of the dataset
        await self.call_annex("add", path)

    async def remove(self, path: str) -> None:
        # `path` must be relative to the root of the dataset
        async with self.lock:
            # to avoid problems with locking etc. Same is done in DataLad's
            # invocation of rm
            self.ds.repo.precommit()
            delays = iter([1, 2, 6, 15, 36])
            while True:
                try:
                    await self.call_git(
                        "rm",
                        "-f",
                        "--ignore-unmatch",
                        "--",
                        path,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                    )
                    return
                except subprocess.CalledProcessError as e:
                    lockfile = self.pathobj / ".git" / "index.lock"
                    output = e.stdout.decode("utf-8")
                    if lockfile.exists() and str(lockfile) in output:
                        r = await aruncmd(
                            "fuser",
                            "-v",
                            lockfile,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            check=False,
                        )
                        log.error(
                            "%s: Unable to remove %s due to lockfile; `fuser -v`"
                            " output on lockfile (return code %d):\n%s",
                            self.pathobj,
                            path,
                            r.returncode,
                            textwrap.indent(r.stdout.decode("utf-8"), "> "),
                        )
                    else:
                        log.error(
                            "%s: `git rm` on %s failed with output:\n%s",
                            self.pathobj,
                            path,
                            textwrap.indent(output, "> "),
                        )
                    try:
                        delay = next(delays)
                    except StopIteration:
                        raise e
                    else:
                        log.info(
                            "Retrying deletion of %s under %s in %d seconds",
                            path,
                            self.pathobj,
                            delay,
                        )
                        await anyio.sleep(delay)

    async def remove_batch(self, paths: Iterable[str]) -> None:
        pathlist = list(paths)
        if not pathlist:
            return
        # `paths` must be relative to the root of the dataset
        async with self.lock:
            # to avoid problems with locking etc. Same is done in DataLad's
            # invocation of rm
            self.ds.repo.precommit()
            with tempfile.NamedTemporaryFile(mode="w") as fp:
                for p in pathlist:
                    print(p, end="\0", file=fp)
                fp.flush()
                fp.seek(0)
                await self.call_git(
                    "rm",
                    "-f",
                    "--ignore-unmatch",
                    f"--pathspec-from-file={fp.name}",
                    "--pathspec-file-nul",
                )

    async def update(self, how: str, sibling: str | None = None) -> None:
        await anyio.to_thread.run_sync(
            partial(self.ds.update, how=how, sibling=sibling)
        )

    async def get_file_stats(self) -> list[FileStat]:
        filedict: dict[str, FileStat] = {}
        async with aclosing(
            stream_null_command("git", "ls-tree", "-lrz", "HEAD", cwd=self.pathobj)
        ) as p:
            async for entry in p:
                try:
                    fst = FileStat.from_entry(entry)
                except Exception:
                    log.exception(
                        "Error parsing ls-tree line %r for %s:", entry, self.path
                    )
                    raise
                filedict[fst.path] = fst
        async with aclosing(self.aiter_annexed_files()) as afiles:
            async for f in afiles:
                filedict[f.file] = replace(filedict[f.file], size=f.bytesize)
        return list(filedict.values())

    async def aiter_annexed_files(self) -> AsyncGenerator[AnnexedFile, None]:
        async with aclosing(
            stream_lines_command(
                "git",
                *GIT_OPTIONS,
                "annex",
                "find",
                "--include=*",
                "--json",
                cwd=self.pathobj,
            )
        ) as p:
            async for line in p:
                try:
                    data = AnnexedFile.model_validate_json(line)
                except Exception:
                    log.exception(
                        "Error parsing `git-annex find` output for %s: bad"
                        " output line %r",
                        self.path,
                        line,
                    )
                    raise
                else:
                    yield data

    async def compute_zarr_checksum(self) -> str:
        log.debug(
            "Computing Zarr checksum for locally-annexed files in %s", self.pathobj
        )
        zcc = ZarrChecksumTree()
        # rely on the fact that every data component of zarr folder is in annex
        # and we keep .dandi/ folder content directly in git
        async with aclosing(self.aiter_annexed_files()) as afiles:
            async for f in afiles:
                if f.backend not in ("MD5", "MD5E"):
                    raise RuntimeError(
                        f"{f.file} in {self.pathobj} has {f.backend} backend"
                        " instead of MD5 or MD5E required for Zarr checksum"
                    )
                zcc.add_leaf(Path(f.file), f.bytesize, key2hash(f.key))
        checksum = str(zcc.process())
        log.debug("Computed Zarr checksum %s for %s", checksum, self.pathobj)
        return checksum

    async def has_github_remote(self) -> bool:
        return "github" in (await self.read_git("remote")).splitlines()

    async def create_github_sibling(
        self,
        owner: str,
        name: str,
        backup_remote: Remote | None,
        *,
        existing: str = "reconfigure",
    ) -> bool:
        # Returns True iff sibling was created
        if not await self.has_github_remote():
            log.info("Creating GitHub sibling for %s", name)
            private = await self.get_embargo_status() is EmbargoStatus.EMBARGOED
            await anyio.to_thread.run_sync(
                partial(
                    self.ds.create_sibling_github,
                    reponame=name,
                    existing=existing,
                    name="github",
                    access_protocol="https",
                    github_organization=owner,
                    publish_depends=(
                        backup_remote.name if backup_remote is not None else None
                    ),
                    private=private,
                )
            )
            for key, value in [
                ("remote.github.pushurl", f"git@github.com:{owner}/{name}.git"),
                (f"branch.{DEFAULT_BRANCH}.remote", "github"),
                (f"branch.{DEFAULT_BRANCH}.merge", f"refs/heads/{DEFAULT_BRANCH}"),
            ]:
                await self.set_repo_config(key, value)
            return True
        else:
            log.debug("GitHub remote already exists for %s", name)
            return False

    async def get_remote_url(self) -> str:
        upstream = await self.get_repo_config(f"branch.{DEFAULT_BRANCH}.remote")
        if upstream is None:
            raise ValueError(
                f"Upstream branch not set for {DEFAULT_BRANCH} in {self.path}"
            )
        url = await self.get_repo_config(f"remote.{upstream}.url")
        if url is None:
            raise ValueError(f"{upstream!r} remote URL not set for {self.path}")
        return url

    async def get_ghrepo(self) -> GHRepo:
        url = await self.get_remote_url()
        return GHRepo.parse_url(url)

    async def get_stats(
        self,
        config: BackupConfig,  # for path to zarrs
    ) -> DatasetStats:
        stored_stats = await self.get_stored_stats()
        if stored_stats is not None:
            # stats were stored and state of the dataset did not change since then
            return stored_stats
        log.info("%s: Counting up files ...", self.path)
        files = 0
        size = 0
        # get them all and remap per path
        subdatasets = {s["path"]: s for s in await self.get_subdatasets()}
        for filestat in await self.get_file_stats():
            path = Path(filestat.path)
            if not is_meta_file(path.parts[0], dandiset=True):
                if filestat.type is ObjectType.COMMIT:
                    # this zarr should not be present locally as a submodule
                    # so we should get its id from its information in submodules.
                    sub_info = subdatasets[str(self.pathobj / path)]
                    _, zarr_stat = await self.get_zarr_sub_stats(sub_info, config)
                    files += zarr_stat.files
                    size += zarr_stat.size
                else:
                    files += 1
                    assert filestat.size is not None
                    size += filestat.size
        log.info("%s: Done counting up files", self.path)
        stats = DatasetStats(files=files, size=size)
        await self.store_stats(stats)
        return stats

    @staticmethod
    async def get_zarr_sub_stats(
        sub_info: dict, config: BackupConfig
    ) -> tuple[str, DatasetStats]:
        zarr_id = Path(sub_info["gitmodule_url"]).name
        assert config.zarr_root is not None
        zarr_ds = AsyncDataset(config.zarr_root / zarr_id)
        # here we assume that HEAD among dandisets is the same as of
        # submodule, which might not necessarily be the case.
        # TODO: get for the specific commit
        zarr_stat = await zarr_ds.get_stats(config=config)
        return zarr_id, zarr_stat

    async def get_stored_stats(self) -> DatasetStats | None:
        if (stored_stats := self.ds.config.get("dandi.stats", None)) is not None:
            try:
                stored_commit, files_str, size_str = stored_stats.split(",")
                files = int(files_str)
                size = int(size_str)
            except Exception:
                return None
            if stored_commit == await self.get_commit_hash():
                return DatasetStats(files=files, size=size)
        return None

    async def store_stats(self, stats: DatasetStats) -> None:
        commit = await self.get_commit_hash()
        value = f"{commit},{stats.files},{stats.size}"
        self.ds.config.set("dandi.stats", value, scope="local")

    async def get_commit_hash(self) -> str:
        return await self.read_git("show", "-s", "--format=%H")

    async def get_last_commit_date(self) -> datetime:
        ts = await self.read_git("show", "-s", "--format=%aI")
        return datetime.fromisoformat(ts)

    def assert_no_duplicates_in_gitmodules(self) -> None:
        filepath = self.pathobj / ".gitmodules"
        if not filepath.exists():
            return
        qtys: Counter[str] = Counter()
        with filepath.open() as fp:
            for line in fp:
                if m := re.fullmatch(r'\[submodule "(.+)"\]\s*', line):
                    qtys[m[1]] += 1
        dupped = [name for (name, count) in qtys.most_common() if count > 1]
        assert not dupped, f"Duplicates found in {filepath}: {dupped}"

    def get_assets_state(self) -> AssetsState | None:
        try:
            with (self.pathobj / AssetsState.PATH).open() as fp:
                return AssetsState.model_validate(json.load(fp))
        except FileNotFoundError:
            return None

    async def set_assets_state(self, state: AssetsState) -> None:
        path = self.pathobj / AssetsState.PATH
        path.parent.mkdir(exist_ok=True)
        path.write_text(state.model_dump_json(indent=4) + "\n")
        await self.add(str(AssetsState.PATH))

    async def get_subdatasets(self, **kwargs: Any) -> list:
        return await anyio.to_thread.run_sync(
            partial(self.ds.subdatasets, result_renderer=None, **kwargs)
        )

    async def uninstall_subdatasets(self) -> None:
        # dropping all dandisets is not trivial :-/
        # https://github.com/datalad/datalad/issues/7013
        #  --reckless kill is not working
        # https://github.com/datalad/datalad/issues/6933#issuecomment-1239402621
        #   '*' pathspec is not supported
        # so could resort to this ad-hoc way but we might want just to pair
        subdatasets = await self.get_subdatasets(result_xfm="relpaths", state="present")
        if subdatasets:
            log.debug("Will uninstall %d subdatasets", len(subdatasets))
            res = await anyio.to_thread.run_sync(
                partial(
                    self.ds.drop,
                    what="datasets",
                    recursive=True,
                    path=subdatasets,
                    reckless="kill",
                )
            )
            assert all(r["status"] == "ok" for r in res)
        else:
            # yet another case where [] is treated as None?
            log.debug("No subdatasets to uninstall")

    async def add_submodule(self, path: str, url: str, datalad_id: str) -> None:
        await self.call_git("submodule", "add", "--", url, path)
        await self.set_repo_config(
            f"submodule.{path}.datalad-id",
            datalad_id,
            file=".gitmodules",
        )
        await self.add(".gitmodules")

    async def update_submodule(self, path: str, commit_hash: str) -> None:
        await self.call_git(
            "update-index",
            "-z",
            # apparently must be the last argument!
            "--index-info",
            input=f"160000 commit {commit_hash}\t{path}\0".encode(),
        )

    async def populate_up_to_date(self) -> bool:
        return (await self.get_repo_config("dandi.populated")) == (
            await self.get_commit_hash()
        )

    async def update_populate_status(self) -> None:
        head = await self.get_commit_hash()
        await self.set_repo_config("dandi.populated", head)


class ObjectType(Enum):
    COMMIT = "commit"
    BLOB = "blob"
    TREE = "tree"


@dataclass
class FileStat:
    path: str
    type: ObjectType
    size: int | None

    @classmethod
    def from_entry(cls, entry: str) -> FileStat:
        stats, _, path = entry.partition("\t")
        _, typename, _, sizestr = stats.split()
        return cls(
            path=path,
            type=ObjectType(typename),
            size=None if sizestr == "-" else int(sizestr),
        )


@dataclass
class DatasetStats:
    files: int
    size: int


class AssetsState(BaseModel):
    PATH: ClassVar[Path] = Path(".dandi", "assets-state.json")
    timestamp: datetime


class AnnexedFile(BaseModel):
    backend: str
    bytesize: int
    # error-messages: list[str]
    file: str
    # hashdirlower: str
    # hashdirmixed: str
    # humansize: str
    key: str
    # keyname: str
    # mtime: Literal["unknown"] | ???
