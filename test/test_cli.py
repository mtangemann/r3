"""Unit tests for the R3 command line interface."""

from pathlib import Path
from typing import List

import pytest
import yaml
from click.testing import CliRunner
from pytest_mock.plugin import MockerFixture

from r3.cli import cli
from r3.job import Job, JobDependency
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def repository(tmp_path: Path) -> Repository:
    return Repository.init(tmp_path / "repository")


def get_dummy_job(name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    return Job(path)


def commit_dependent_job(repository: Repository, job: Job, destination: str) -> Job:
    """Commits a job depending on the given job and returns it."""
    assert job.id is not None

    dependent_job = get_dummy_job("base")
    dependency = JobDependency(destination, job.id)
    dependent_job._dependencies = [dependency]
    dependent_job._config["dependencies"] = [dependency.to_config()]

    return repository.commit(dependent_job)


def _commit_with(repository: Repository, name: str, path: str, tags):
    """Commits `test/data/jobs/<name>` with the given path/tags metadata.

    NB: `storage.add` sets timestamp = now(), so rely on commit *order* for time
    and read timestamps back from the returned committed job.
    """
    job = get_dummy_job(name)
    job.metadata["path"] = path
    job.metadata["tags"] = list(tags)
    return repository.commit(job)


def test_remove_removes_job(repository: Repository) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    result = CliRunner().invoke(
        cli, ["remove", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code == 0
    assert job not in repository


def test_remove_fails_if_other_jobs_depend_on_job(repository: Repository) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None
    dependent_job = commit_dependent_job(repository, job, "destination")
    assert dependent_job.id is not None

    result = CliRunner().invoke(
        cli, ["remove", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert any(dependent_job.id in line for line in lines)
    # The dependent must be reported with an explanation, not on its own. The exact
    # wording is asserted in ``test_repository.py``.
    assert len(lines) > 1
    assert job in repository


def test_remove_fails_if_job_does_not_exist(repository: Repository) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"

    result = CliRunner().invoke(
        cli, ["remove", job_id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output


def test_edit_updates_metadata_and_index(
    repository: Repository, mocker: MockerFixture
) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    def edit_metadata(filename: str) -> None:
        with open(filename, "r") as metadata_file:
            metadata = yaml.safe_load(metadata_file)
        metadata["tags"].append("edited")
        with open(filename, "w") as metadata_file:
            yaml.dump(metadata, metadata_file)

    mocker.patch("r3.cli.click.edit", side_effect=edit_metadata)

    result = CliRunner().invoke(
        cli, ["edit", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code == 0

    # Read the repository afresh, so that the index is queried rather than any state
    # cached by the repository instance used above.
    jobs = Repository(repository.path).find({"tags": {"$all": ["edited"]}})
    assert [found_job.id for found_job in jobs] == [job.id]


def test_edit_fails_if_job_does_not_exist(repository: Repository) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"

    result = CliRunner().invoke(
        cli, ["edit", job_id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    # The error must be reported, not raised as an unhandled exception.
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output


def test_checkout_checks_out_job(repository: Repository, tmp_path: Path) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None
    target_path = tmp_path / "checkout"

    result = CliRunner().invoke(
        cli,
        ["checkout", job.id, str(target_path), "--repository", str(repository.path)],
    )

    assert result.exit_code == 0
    assert (target_path / "run.py").is_file()


def test_checkout_fails_if_job_does_not_exist(
    repository: Repository, tmp_path: Path
) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"
    target_path = tmp_path / "checkout"

    result = CliRunner().invoke(
        cli,
        ["checkout", job_id, str(target_path), "--repository", str(repository.path)],
    )

    assert result.exit_code != 0
    # The error must be reported, not raised as an unhandled exception.
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output
    assert not target_path.exists()


def commands_taking_a_repository(tmp_path: Path) -> List[List[str]]:
    """Returns a minimal invocation of every command that takes a repository.

    The path arguments must exist, so that click's own validation passes and the command
    body is actually reached.
    """
    job_path = tmp_path / "job"
    job_path.mkdir(exist_ok=True)
    job_id = "00000000-0000-0000-0000-000000000000"

    return [
        ["find"],
        ["rebuild-index"],
        ["remove", job_id],
        ["edit", job_id],
        ["checkout", job_id, str(tmp_path / "target")],
        ["commit", str(job_path)],
    ]


def test_commands_report_a_missing_repository(tmp_path: Path) -> None:
    # Looped rather than parametrized, since the commands need `tmp_path`.
    for command in commands_taking_a_repository(tmp_path):
        result = CliRunner().invoke(cli, command, env={"R3_REPOSITORY": None})

        assert result.exit_code != 0, command
        # Reported, not raised as an unhandled exception.
        assert (
            result.exception is None or isinstance(result.exception, SystemExit)
        ), command
        # Whichever way the user meant to pass it, name it.
        assert "--repository" in result.output, command
        assert "R3_REPOSITORY" in result.output, command


def test_commands_report_an_empty_repository_env_var() -> None:
    result = CliRunner().invoke(cli, ["find"], env={"R3_REPOSITORY": ""})

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "R3_REPOSITORY" in result.output


def test_commands_report_a_path_that_is_not_a_repository(tmp_path: Path) -> None:
    not_a_repository = tmp_path / "not-a-repository"
    not_a_repository.mkdir()

    result = CliRunner().invoke(cli, ["find", "--repository", str(not_a_repository)])

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert str(not_a_repository) in result.output


def test_commands_report_an_outdated_repository_version(repository: Repository) -> None:
    with open(repository.path / "r3.yaml", "w") as config_file:
        yaml.dump({"version": "1.0.0-beta.1"}, config_file)

    result = CliRunner().invoke(cli, ["find", "--repository", str(repository.path)])

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "1.0.0-beta.1" in result.output


def test_help_mentions_the_repository_env_var() -> None:
    result = CliRunner().invoke(cli, ["find", "--help"])

    assert result.exit_code == 0
    assert "R3_REPOSITORY" in result.output


def test_find_long_shows_path_before_tags(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", ["a", "b"])
    result = CliRunner().invoke(
        cli, ["find", "-l", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    line = result.output.strip().splitlines()[0]
    # Columns: id | datetime | path | tags
    assert " | proj/exp/pilot | " in line
    assert line.index("proj/exp/pilot") < line.index("#a")


def test_find_long_blank_path_column_when_absent(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["only-tag"]
    repository.commit(job)
    result = CliRunner().invoke(
        cli, ["find", "-l", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    # path column present but empty, still before tags
    assert " |  | #only-tag" in result.output


def test_find_path_filter_is_literal_glob(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", [])
    _commit_with(repository, "base", "proj/other/run", [])
    result = CliRunner().invoke(
        cli, ["find", "-p", "proj/exp/*", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    ids = set(result.output.split())
    pilot = repository.find({"path": "proj/exp/pilot"})[0]
    other = repository.find({"path": "proj/other/run"})[0]
    assert pilot.id in ids
    assert other.id not in ids


def test_find_path_filter_no_automatic_wildcard(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp", [])
    _commit_with(repository, "base", "proj/experiment", [])
    result = CliRunner().invoke(
        cli, ["find", "-p", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    exp = repository.find({"path": "proj/exp"})[0]
    experiment = repository.find({"path": "proj/experiment"})[0]
    ids = set(result.output.split())
    assert exp.id in ids
    assert experiment.id not in ids


def test_find_path_and_tag_are_anded(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/a", ["keep"])
    _commit_with(repository, "base", "proj/exp/b", ["drop"])
    result = CliRunner().invoke(
        cli,
        ["find", "-p", "proj/exp/*", "-t", "keep",
         "--repository", str(repository.path)],
    )
    assert result.exit_code == 0
    keep = repository.find({"path": "proj/exp/a"})[0]
    drop = repository.find({"path": "proj/exp/b"})[0]
    ids = set(result.output.split())
    assert keep.id in ids and drop.id not in ids


def test_find_no_tags_drops_tags_column(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", ["a"])
    result = CliRunner().invoke(
        cli, ["find", "-l", "--no-tags", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    assert "#a" not in result.output
    assert "proj/exp/pilot" in result.output


def test_ls_lists_one_level(repository: Repository) -> None:
    pilot = _commit_with(repository, "base", "proj/exp/pilot", [])
    _commit_with(repository, "base", "proj/exp/grid/run1", [])
    _commit_with(repository, "base", "proj/exp/grid/run2", [])
    result = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    lines = result.output.strip().splitlines()
    assert len(lines) == 2  # grid/run1 + grid/run2 collapse into a single grid/ entry
    # alphabetical, interleaved: "grid" (dir) before "pilot" (leaf)
    assert lines[0].strip() == "grid/"
    assert lines[1].startswith("pilot")
    assert pilot.timestamp.strftime(r"%Y-%m-%d %H:%M:%S") in lines[1]


def test_ls_self_entry_for_job_at_prefix(repository: Repository) -> None:
    self_job = _commit_with(repository, "base", "proj/exp", [])
    _commit_with(repository, "base", "proj/exp/pilot", [])
    result = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    lines = result.output.strip().splitlines()
    assert lines[0].startswith(".")
    assert self_job.timestamp.strftime(r"%Y-%m-%d %H:%M:%S") in lines[0]


def test_ls_collision_shows_job_and_dir(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/analysis", [])
    _commit_with(repository, "base", "proj/exp/analysis/report", [])
    result = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    lines = result.output.strip().splitlines()
    # analysis is both a job leaf and a directory: both lines, leaf then dir
    assert lines[0].startswith("analysis") and not lines[0].strip().endswith("/")
    assert lines[1].strip() == "analysis/"


def test_ls_revision_count_and_latest(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", ["v1"])
    newer = _commit_with(repository, "base", "proj/exp/pilot", ["v2"])
    result = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    assert "(2 revisions)" in result.output
    # latest revision (committed last -> later now()) supplies the timestamp
    assert newer.timestamp.strftime(r"%Y-%m-%d %H:%M:%S") in result.output


def test_ls_root_lists_top_level(repository: Repository) -> None:
    _commit_with(repository, "base", "projA/exp/pilot", [])
    _commit_with(repository, "base", "single", [])
    result = CliRunner().invoke(
        cli, ["ls", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    out = result.output
    assert "projA/" in out
    assert "single" in out


def test_ls_trailing_slash_is_insignificant(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", [])
    with_slash = CliRunner().invoke(
        cli, ["ls", "proj/exp/", "--repository", str(repository.path)]
    )
    without_slash = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert with_slash.output == without_slash.output


def test_ls_empty_when_no_match(repository: Repository) -> None:
    _commit_with(repository, "base", "proj/exp/pilot", [])
    result = CliRunner().invoke(
        cli, ["ls", "nope/here", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    assert result.output.strip() == ""


def test_ls_long_shows_id_and_tags(repository: Repository) -> None:
    job = _commit_with(repository, "base", "proj/exp/pilot", ["t1"])
    result = CliRunner().invoke(
        cli, ["ls", "-l", "proj/exp", "--repository", str(repository.path)]
    )
    assert result.exit_code == 0
    assert job.id in result.output
    assert "#t1" in result.output
    no_tags = CliRunner().invoke(
        cli, ["ls", "-l", "--no-tags", "proj/exp", "--repository", str(repository.path)]
    )
    assert "#t1" not in no_tags.output


def test_ls_time_sort_differs_from_alphabetical(repository: Repository) -> None:
    # Commit "aaa" first (older), "zzz" second (newer):
    # alphabetical -> aaa, zzz ; time (newest first) -> zzz, aaa.
    _commit_with(repository, "base", "proj/exp/aaa", [])
    _commit_with(repository, "base", "proj/exp/zzz", [])
    alpha = CliRunner().invoke(
        cli, ["ls", "proj/exp", "--repository", str(repository.path)]
    )
    assert [ln.split()[0] for ln in alpha.output.strip().splitlines()] == ["aaa", "zzz"]
    timed = CliRunner().invoke(
        cli, ["ls", "-t", "proj/exp", "--repository", str(repository.path)]
    )
    assert [ln.split()[0] for ln in timed.output.strip().splitlines()] == ["zzz", "aaa"]
