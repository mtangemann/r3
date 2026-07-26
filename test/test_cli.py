"""Unit tests for the R3 command line interface."""

from pathlib import Path

import pytest
from click.testing import CliRunner

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


def is_in_repository(repository: Repository, job_id: str) -> bool:
    """Checks whether a job with the given ID exists in the repository.

    ``job in repository`` only checks whether the job path points into the repository,
    which stays true after the job has been removed.
    """
    try:
        repository.get_job_by_id(job_id)
    except KeyError:
        return False
    return True


def test_remove_removes_job(repository: Repository) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    result = CliRunner().invoke(
        cli, ["remove", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code == 0
    assert not is_in_repository(repository, job.id)


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
    assert is_in_repository(repository, job.id)


def test_remove_fails_if_job_does_not_exist(repository: Repository) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"

    result = CliRunner().invoke(
        cli, ["remove", job_id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output
