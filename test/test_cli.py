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
