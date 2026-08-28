from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from datalad.tests.utils_pytest import assert_repo_status
import pytest
from test_util import GitRepo, gitattributes_policy

from backups2datalad.adataset import AsyncDataset
from backups2datalad.procedures.cfg_dandiset import (
    BLOCK_END,
    BLOCK_START,
    COMMIT_MESSAGE,
    DEFAULT_SIZE_LIMIT,
    SIZE_LIMIT_ENVVAR,
    apply_policy,
    new_gitattributes,
    parse_size,
    policy_lines,
    size_limit_bytes,
)

TEXT2GIT_LINE = "* annex.largefiles=((mimeencoding=binary)and(largerthan=0))"

# What `datalad create` (without any `-c`) leaves in .gitattributes:
DATALAD_LINES = "* annex.backend=SHA256E\n**/.git* annex.largefiles=nothing\n"


@pytest.mark.ai_generated
def test_parse_size() -> None:
    assert parse_size("0") == 0
    assert parse_size("500") == 500
    assert parse_size("1kb") == 1000
    assert parse_size("10MiB") == 10 << 20
    assert parse_size(DEFAULT_SIZE_LIMIT) == 10 << 20
    with pytest.raises(ValueError):
        parse_size("10 bushels")


@pytest.mark.ai_generated
def test_size_limit_envvar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SIZE_LIMIT_ENVVAR, raising=False)
    assert size_limit_bytes() == 10 << 20
    monkeypatch.setenv(SIZE_LIMIT_ENVVAR, "1kb")
    assert size_limit_bytes() == 1000
    assert "or(largerthan=1kb)" in "\n".join(policy_lines())
    # An explicitly-given limit wins over the environment:
    assert size_limit_bytes("2MB") == 2000000


@pytest.mark.ai_generated
def test_new_gitattributes_from_scratch() -> None:
    assert new_gitattributes(DATALAD_LINES) == DATALAD_LINES + "".join(
        f"{ln}\n" for ln in policy_lines()
    )


@pytest.mark.ai_generated
def test_new_gitattributes_is_idempotent() -> None:
    once = new_gitattributes(DATALAD_LINES)
    assert new_gitattributes(once) == once


@pytest.mark.ai_generated
def test_new_gitattributes_drops_text2git_line() -> None:
    # `cfg_text2git` appends its catch-all rule after what `create` wrote:
    text2git = DATALAD_LINES + TEXT2GIT_LINE + "\n"
    new = new_gitattributes(text2git)
    assert TEXT2GIT_LINE not in new.splitlines()
    assert new == new_gitattributes(DATALAD_LINES)


@pytest.mark.ai_generated
def test_new_gitattributes_keeps_other_attributes() -> None:
    # A catch-all line that sets more than just `annex.largefiles` only loses
    # the `annex.largefiles` bit:
    new = new_gitattributes("* annex.backend=MD5E annex.largefiles=anything\n")
    assert new.splitlines()[0] == "* annex.backend=MD5E"


@pytest.mark.ai_generated
def test_new_gitattributes_preserves_customizations() -> None:
    custom = (
        "# some comment\n"
        + DATALAD_LINES
        + "".join(f"{ln}\n" for ln in policy_lines())
        # Rules after the block override it, which is the supported way of
        # customizing a dataset:
        + "*.csv annex.largefiles=nothing\n"
    )
    assert new_gitattributes(custom) == custom


@pytest.mark.ai_generated
def test_new_gitattributes_updates_block_in_place(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(SIZE_LIMIT_ENVVAR, raising=False)
    before = new_gitattributes(DATALAD_LINES) + "*.csv annex.largefiles=nothing\n"
    monkeypatch.setenv(SIZE_LIMIT_ENVVAR, "42kb")
    after = new_gitattributes(before)
    assert after.splitlines()[-1] == "*.csv annex.largefiles=nothing"
    assert "or(largerthan=42kb)" in after
    assert "or(largerthan=10MiB)" not in after
    assert after.count(BLOCK_START) == 1


@pytest.mark.ai_generated
def test_apply_policy(tmp_path: Path) -> None:
    assert apply_policy(tmp_path) is True
    assert (tmp_path / ".gitattributes").read_text() == "".join(
        f"{ln}\n" for ln in policy_lines()
    )
    assert apply_policy(tmp_path) is False


@pytest.mark.anyio
@pytest.mark.ai_generated
async def test_create_sets_policy(tmp_path: Path) -> None:
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed(
        "Test dataset",
        commit_date=datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc),
    )
    assert_repo_status(ds.path)
    assert gitattributes_policy(tmp_path) == policy_lines()
    repo = GitRepo(tmp_path)
    # The procedure's commit must not stray from the requested commit date:
    assert repo.get_commit_date("HEAD") == "2021-06-01T12:34:56+00:00"
    assert repo.get_commit_subject("HEAD") == COMMIT_MESSAGE
    # A second call is a no-op:
    commits = repo.get_commit_count()
    assert not await ds.ensure_installed("Test dataset")
    assert repo.get_commit_count() == commits


@pytest.mark.anyio
@pytest.mark.ai_generated
async def test_create_honors_size_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(SIZE_LIMIT_ENVVAR, "1kb")
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("Test dataset")
    assert "or(largerthan=1kb)" in (tmp_path / ".gitattributes").read_text()


@pytest.mark.anyio
@pytest.mark.ai_generated
async def test_migrate_text2git_dataset(tmp_path: Path) -> None:
    # Set up a dataset the way they were created before `cfg_dandiset`:
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed(
        "Test dataset",
        commit_date=datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc),
        cfg_proc="text2git",
    )
    attrs = (tmp_path / ".gitattributes").read_text()
    assert TEXT2GIT_LINE in attrs.splitlines()
    assert BLOCK_START not in attrs
    repo = GitRepo(tmp_path)
    old_head = repo.get_commitish_hash("HEAD")
    old_date = repo.get_commit_date("HEAD")

    # Every backup run reapplies the policy, which migrates the dataset:
    assert not await ds.ensure_installed("Test dataset")
    assert_repo_status(ds.path)
    assert gitattributes_policy(tmp_path) == policy_lines()
    assert TEXT2GIT_LINE not in (tmp_path / ".gitattributes").read_text().splitlines()
    assert repo.get_commit_subject("HEAD") == COMMIT_MESSAGE
    assert repo.parent_is_ancestor("HEAD", old_head)
    # The reconfiguration must not move the mirror's timeline into the present:
    assert repo.get_commit_date("HEAD") == old_date

    # ... and it does not do it again:
    commits = repo.get_commit_count()
    assert not await ds.ensure_installed("Test dataset")
    assert repo.get_commit_count() == commits


@pytest.mark.anyio
@pytest.mark.ai_generated
async def test_migrate_keeps_custom_rules(tmp_path: Path) -> None:
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("Test dataset", cfg_proc="text2git")
    with (tmp_path / ".gitattributes").open("a") as fp:
        print("*.csv annex.largefiles=nothing", file=fp)
    await ds.save("Add custom .gitattributes rule")
    assert not await ds.ensure_installed("Test dataset")
    lines = (tmp_path / ".gitattributes").read_text().splitlines()
    # Pre-existing rules are kept, though the block that gets appended on
    # migration takes precedence over them:
    assert "*.csv annex.largefiles=nothing" in lines
    assert lines[-1] == BLOCK_END

    # Rules placed after the block override it and survive later runs:
    with (tmp_path / ".gitattributes").open("a") as fp:
        print("*.tsv annex.largefiles=nothing", file=fp)
    await ds.save("Add another custom .gitattributes rule")
    assert not await ds.ensure_installed("Test dataset")
    lines = (tmp_path / ".gitattributes").read_text().splitlines()
    assert lines[-1] == "*.tsv annex.largefiles=nothing"
    assert lines[-2] == BLOCK_END


@pytest.mark.anyio
@pytest.mark.ai_generated
async def test_zarr_datasets_are_not_touched(tmp_path: Path) -> None:
    # Zarr datasets are created with `cfg_proc=None` and must stay that way.
    ds = AsyncDataset(tmp_path)
    assert await ds.ensure_installed("Test Zarr", backend="MD5E", cfg_proc=None)
    assert BLOCK_START not in (tmp_path / ".gitattributes").read_text()
    assert not await ds.ensure_installed("Test Zarr", backend="MD5E", cfg_proc=None)
    assert BLOCK_START not in (tmp_path / ".gitattributes").read_text()
