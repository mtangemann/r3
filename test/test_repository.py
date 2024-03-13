"""Unit tests for ``r3.Repository``."""

import filecmp
import os
import stat
import tempfile
from pathlib import Path

import pytest
import yaml
from executor import execute
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock.plugin import MockerFixture

from r3.job import (
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def repository(fs: FakeFilesystem) -> Repository:
    return Repository.init("/repository")


def get_dummy_job(fs: FakeFilesystem, name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    if not fs.exists(path):
        fs.add_real_directory(path, read_only=True)
    return Job(path)


def test_init_fails_if_path_exists(fs: FakeFilesystem) -> None:
    path = "/repository"
    fs.create_dir(path)

    with pytest.raises(FileExistsError):
        Repository.init(path)


def test_init_calls_storage_init(fs: FakeFilesystem, mocker: MockerFixture) -> None:
    storage_init = mocker.patch("r3.storage.Storage.init")

    path = "/test/repository"
    Repository.init(path)

    storage_init.assert_called_once_with(Path(path))


def test_init_creates_config_file_with_version(fs: FakeFilesystem) -> None:
    path = Path("/test/repository")
    Repository.init(path)

    assert (path / "r3.yaml").exists()

    with open(path / "r3.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    assert "version" in config


def test_repository_jobs_calls_storage_jobs(
    fs: FakeFilesystem, mocker: MockerFixture
) -> None:
    storage_jobs = mocker.patch("r3.storage.Storage.jobs")

    path = "/repository"
    repository = Repository.init(path)
    list(repository.jobs())

    storage_jobs.assert_called_once()


def test_repository_contains_job_calls_storage_contains(
    fs: FakeFilesystem, mocker: MockerFixture
) -> None:
    storage_contains = mocker.patch("r3.storage.Storage.__contains__")

    path = "/repository"
    repository = Repository.init(path)
    job = get_dummy_job(fs, "base")
    job in repository  # noqa: B015

    storage_contains.assert_called_once_with(job)


def test_repository_contains_job_dependency(fs: FakeFilesystem) -> None:
    repository = Repository.init("/repository")

    dependency = JobDependency("123abc", "destination")
    assert dependency not in repository

    job = get_dummy_job(fs, "base")
    job = repository.commit(job)
    assert job.id is not None

    dependency = JobDependency(job.id, "destination")
    assert dependency in repository

    dependency = JobDependency(job.id, "destination.py", "run.py")
    assert dependency in repository

    dependency = JobDependency(job.id, "destination.py", "does_not_exist.py")
    assert dependency not in repository


def test_repository_contains_git_dependency() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        repository = Repository.init(f"{tempdir}/repository")

        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination",
        )
        assert dependency not in repository

        r3_path = Path(__file__).parent.parent
        assert (r3_path / ".git").is_dir()
        repository_path = Path(f"{tempdir}/repository/git/github.com/mtangemann/r3")
        execute(f"git clone {r3_path} {repository_path}")

        assert dependency in repository

        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination.py",
            source="test/test_repository.py",
        )
        assert dependency in repository

        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination.py",
            source="does_not_exist.py",
        )
        assert dependency not in repository


def test_repository_contains_query_dependency(fs: FakeFilesystem) -> None:
    repository = Repository.init("/repository")

    dependency = QueryDependency("#test", "destination")
    assert dependency not in repository

    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    assert dependency in repository

    dependency = QueryDependency("#test #does-not-exist", "destination")
    assert dependency not in repository

    dependency = QueryDependency("#test", "destination.py", "run.py")
    assert dependency in repository

    dependency = QueryDependency("#test", "destination.py", "does_not_exist.py")
    assert dependency not in repository


def test_repository_contains_query_all_dependency(fs: FakeFilesystem) -> None:
    repository = Repository.init("/repository")

    dependency = QueryAllDependency("#test", "destination")
    assert dependency not in repository

    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    assert dependency in repository


def test_commit_creates_job_folder(fs: FakeFilesystem, repository: Repository) -> None:
    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 0

    job = get_dummy_job(fs, "base")
    repository.commit(job)

    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 1
    assert job_paths[0].is_dir()


def test_commit_returns_the_updated_job(
    fs: FakeFilesystem, repository: Repository
) -> None:
    """Unit test for ``r3.Repository.commit``.

    ``r3.Repository.commit`` should return the ``r3.Job`` instance within the
    repository.
    """
    job = get_dummy_job(fs, "base")
    assert job.id is None
    assert not str(job.path).startswith(str(repository.path))

    job = repository.commit(job)
    assert job.id is not None
    assert str(job.path).startswith(str(repository.path))


def test_commit_copies_files_write_protected(
    fs: FakeFilesystem, repository: Repository
) -> None:
    """Unit test for ``r3.Repository.commit``.

    When adding a job to a repository, all files should be copied to the repository. The
    files in the repository should be write protected.
    """
    original_job = get_dummy_job(fs, "base")
    assert original_job.path is not None

    added_job = repository.commit(original_job)

    assert added_job.path is not None
    assert (added_job.path / "run.py").is_file()
    assert filecmp.cmp(
        added_job.path / "run.py", original_job.path / "run.py", shallow=False
    )

    mode = stat.S_IMODE(os.lstat(added_job.path / "run.py").st_mode)
    assert mode & stat.S_IWOTH == 0
    assert mode & stat.S_IWGRP == 0
    assert mode & stat.S_IWUSR == 0


def test_commit_copies_nested_files(
    fs: FakeFilesystem, repository: Repository
) -> None:
    """Unit test for ``r3.Repository.add``."""
    original_job = get_dummy_job(fs, "nested")
    assert original_job.path is not None

    added_job = repository.commit(original_job)

    assert added_job.path is not None
    assert (added_job.path / "code" / "run.py").is_file()
    assert filecmp.cmp(
        added_job.path / "code" / "run.py",
        original_job.path / "code" / "run.py",
        shallow=False,
    )


def test_repository_remove_fails_if_other_jobs_depend_on_job(
    fs: FakeFilesystem, repository: Repository
) -> None:
    base_job = get_dummy_job(fs, "base")

    job = repository.commit(base_job)
    assert job.id is not None

    dependency = JobDependency(job.id, "destination")
    base_job._dependencies = [dependency]
    base_job._config["dependencies"] = [dependency.to_config()]
    dependent_job = repository.commit(base_job)

    with pytest.raises(ValueError):
        repository.remove(job)

    repository.remove(dependent_job)
    repository.remove(job)


def test_resolve_query_dependency(fs: FakeFilesystem, repository: Repository) -> None:
    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    dependency = QueryDependency("#test", "destination")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == job.id

    with pytest.raises(ValueError):
        repository.resolve(QueryDependency("#does-not-exist", "destination"))


def test_resolve_query_all_dependency(
    fs: FakeFilesystem,
    repository: Repository,
) -> None:
    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    commited_job_1 = repository.commit(job)

    dependency = QueryAllDependency("#test", "destination")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, list)
    assert len(resolved_dependency) == 1
    assert resolved_dependency[0].job == commited_job_1.id

    job.metadata["tags"] = ["test", "another"]
    committed_job_2 = repository.commit(job)

    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, list)
    assert len(resolved_dependency) == 2
    assert set([dependency.job for dependency in resolved_dependency]) == {
        commited_job_1.id,
        committed_job_2.id,
    }


def test_resolve_job(fs: FakeFilesystem, repository: Repository) -> None:
    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    committed_job = repository.commit(job)

    dependency = QueryDependency("#test", "destination")
    job._dependencies = [dependency]
    job._config["dependencies"] = [dependency.to_config()]

    resolved_job = repository.resolve(job)
    assert isinstance(resolved_job, Job)
    assert all(dependency.is_resolved() for dependency in resolved_job.dependencies)
    assert isinstance(resolved_job.dependencies[0], JobDependency)
    assert resolved_job.dependencies[0].job == committed_job.id
