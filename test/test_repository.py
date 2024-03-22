"""Unit tests for ``r3.Repository``."""

import filecmp
import os
import stat
import tempfile
from pathlib import Path
from typing import Union

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


class ExampleGitRepository:
    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        execute(f"git init {self.path}")
        with open(self.path / "test.txt", "w") as file:
            file.write("original content")
        execute("git add test.txt", directory=self.path)
        execute("git commit -m 'Initial commit'", directory=self.path)

    def head_commit(self) -> str:
        return execute("git rev-parse HEAD", directory=self.path, capture=True).strip()

    def update(self) -> None:
        execute("git switch main", directory=self.path)
        with open(self.path / "test.txt", "w") as file:
            file.write("updated content")
        execute("git add test.txt", directory=self.path)
        execute("git commit -m 'Update'", directory=self.path)

    def update_branch(self) -> None:
        execute("git checkout -b branch", directory=self.path)
        with open(self.path / "test.txt", "w") as file:
            file.write("branch content")
        execute("git add test.txt", directory=self.path)
        execute("git commit -m 'Branch commit'", directory=self.path)

    def force_update(self) -> None:
        with open(self.path / "test.txt", "w") as file:
            file.write("forced content")
        execute("git add test.txt", directory=self.path)
        execute("git commit --amend -m 'Force update'", directory=self.path)
    
    def add_tag(self, tag: str) -> None:
        execute(f"git tag {tag} -m 'Test tag'", directory=self.path)


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


def test_repository_contains_git_dependency_clones_repository(
    mocker: MockerFixture,
) -> None:
    # If the repository specified by a GitDependency does not exist locally yet, the
    # __contains__ method should clone the repository before checking whether the
    # commit exists.
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        repository = Repository.init(f"{tempdir}/r3")
        dependency = GitDependency(
            repository=origin_url,
            commit=origin.head_commit(),
            destination="destination",
        )

        git_clone_called = False

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            nonlocal git_clone_called
            if command.startswith("git clone"):
                git_clone_called = True
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        assert dependency in repository
        assert git_clone_called

        git_clone_called = False
        assert dependency in repository
        assert not git_clone_called


def test_repository_contains_git_dependency_fetches_all_branches(
    mocker: MockerFixture,
) -> None:
    # If the commit specified by a GitDependency does not exists locally yet, the
    # __contains__ method should fetch all branches before checking whether the commit
    # exists.
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        repository = Repository.init(f"{tempdir}/r3")
        dependency = GitDependency(
            repository=origin_url,
            commit=origin.head_commit(),
            destination="destination",
        )

        git_fetch_called = False

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            nonlocal git_fetch_called
            if command.startswith("git fetch"):
                git_fetch_called = True
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        assert dependency in repository
        assert not git_fetch_called

        origin.update()
        dependency.commit = origin.head_commit()

        assert dependency in repository
        assert git_fetch_called
        git_fetch_called = False

        origin.update_branch()
        dependency.commit = origin.head_commit()
        assert dependency in repository
        assert git_fetch_called
        git_fetch_called = False

        dependency.commit = "does-not-exist"
        assert dependency not in repository
        assert git_fetch_called


def test_repository_contains_git_dependency_fails_if_commit_does_not_exist(
    mocker: MockerFixture,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        repository = Repository.init(f"{tempdir}/r3")
        dependency = GitDependency(
            repository=origin_url,
            commit="does-not-exist",
            destination="destination",
        )

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        assert dependency not in repository


def test_repository_contains_git_dependency_checks_whether_source_exists(
    mocker: MockerFixture,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        repository = Repository.init(f"{tempdir}/r3")
        dependency = GitDependency(
            repository=origin_url,
            commit=origin.head_commit(),
            source="test.txt",
            destination="destination.txt",
        )

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        assert dependency in repository

        dependency.source = Path("does-not-exist.txt")
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


def test_commit_adds_git_tags_to_prevent_garbage_collection(
    mocker: MockerFixture,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        origin.update()

        repository = Repository.init(f"{tempdir}/r3")

        dependency = GitDependency(
            repository=origin_url,
            commit=origin.head_commit(),
            destination="destination",
        )

        job_path = Path(tempdir) / "job"
        job_path.mkdir()
        with open(job_path / "r3.yaml", "w") as file:
            yaml.dump({"dependencies": [dependency.to_config()]}, file)
        with open(job_path / "run.py", "w") as file:
            file.write("print('Hello, world!')")
        job = Job(job_path)

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        job = repository.commit(job)

        clone_path = repository.path / dependency.repository_path
        tags = execute("git tag", directory=clone_path, capture=True)
        assert f"r3/{job.id}" in tags.splitlines()
        ref = execute(f"git rev-parse r3/{job.id}", directory=clone_path, capture=True)
        assert ref.strip() == dependency.commit

        origin.force_update()

        updated_dependency = GitDependency(
            repository=origin_url,
            commit=origin.head_commit(),
            destination="destination",
        )
        assert updated_dependency in repository

        execute("git gc --prune=now", directory=clone_path)

        assert updated_dependency in repository
        assert dependency in repository


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


def test_find_dependents_requires_job_id(
    fs: FakeFilesystem, repository: Repository,
) -> None:
    job = get_dummy_job(fs, "base")
    job = repository.commit(job)

    repository.find_dependents(job)

    job.id = None
    with pytest.raises(ValueError):
        repository.find_dependents(job)


def test_find_dependents(fs: FakeFilesystem, repository: Repository) -> None:
    job1 = get_dummy_job(fs, "base")
    job1 = repository.commit(job1)
    assert job1.id is not None

    job2 = get_dummy_job(fs, "base")
    dependency = JobDependency(job1.id, "destination1")
    job2._dependencies = [dependency]
    job2._config["dependencies"] = [dependency.to_config()]
    job2 = repository.commit(job2)
    assert job2.id is not None

    job3 = get_dummy_job(fs, "base")
    dependency = JobDependency(job1.id, "destination2")
    job3._dependencies = [dependency]
    job3._config["dependencies"] = [dependency.to_config()]
    job3 = repository.commit(job3)
    assert job3.id is not None

    job4 = get_dummy_job(fs, "base")
    dependency = JobDependency(job2.id, "destination3")
    job4._dependencies = [dependency]
    job4._config["dependencies"] = [dependency.to_config()]
    dependency = JobDependency(job3.id, "destination4")
    job4._dependencies.append(dependency)
    job4._config["dependencies"].append(dependency.to_config())
    job4 = repository.commit(job4)

    dependents = repository.find_dependents(job4)
    assert len(dependents) == 0

    dependents = repository.find_dependents(job3)
    assert len(dependents) == 1
    assert {dependent.id for dependent in dependents} == {job4.id}

    dependents = repository.find_dependents(job2)
    assert len(dependents) == 1
    assert {dependent.id for dependent in dependents} == {job4.id}

    dependents = repository.find_dependents(job1)
    assert len(dependents) == 2
    assert {dependent.id for dependent in dependents} == {job2.id, job3.id}

    dependents = repository.find_dependents(job1, recursive=True)
    assert len(dependents) == 3
    assert {dependent.id for dependent in dependents} == {job2.id, job3.id, job4.id}


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


def test_resolve_git_dependency_from_url(mocker: MockerFixture) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")

        repository = Repository.init(f"{tempdir}/r3")

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            destination="destination",
        )

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        resolved_dependency = repository.resolve(dependency)
        assert isinstance(resolved_dependency, GitDependency)
        assert resolved_dependency.is_resolved()
        assert resolved_dependency.commit == origin.head_commit()

        origin.update()

        resolved_dependency = repository.resolve(dependency)
        assert isinstance(resolved_dependency, GitDependency)
        assert resolved_dependency.is_resolved()
        assert resolved_dependency.commit == origin.head_commit()


def test_resolve_git_dependency_from_branch(mocker: MockerFixture) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        origin.update_branch()
        branch_commit = origin.head_commit()
        origin.update()
        main_commit = origin.head_commit()

        repository = Repository.init(f"{tempdir}/r3")

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            branch="main",
            destination="destination",
        )
        resolved_dependency = repository.resolve(dependency)
        assert isinstance(resolved_dependency, GitDependency)
        assert resolved_dependency.is_resolved()
        assert resolved_dependency.commit == main_commit

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            branch="branch",
            destination="destination",
        )
        resolved_dependency = repository.resolve(dependency)
        assert isinstance(resolved_dependency, GitDependency)
        assert resolved_dependency.is_resolved()
        assert resolved_dependency.commit == branch_commit

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            branch="does-not-exist",
            destination="destination",
        )
        with pytest.raises(ValueError):
            repository.resolve(dependency)


def test_resolve_git_dependency_from_tag(mocker: MockerFixture) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        origin_url = "git@github.com:mtangemann/origin.git"
        origin = ExampleGitRepository(f"{tempdir}/origin")
        origin.add_tag("test")
        tag_commit = origin.head_commit()
        origin.update()

        repository = Repository.init(f"{tempdir}/r3")

        def patched_execute(command, **kwargs):
            command = command.replace(origin_url, str(origin.path))
            return execute(command, **kwargs)

        mocker.patch("r3.repository.execute", new=patched_execute)

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            tag="test",
            destination="destination",
        )
        resolved_dependency = repository.resolve(dependency)
        assert isinstance(resolved_dependency, GitDependency)
        assert resolved_dependency.is_resolved()
        assert resolved_dependency.commit == tag_commit

        dependency = GitDependency(
            repository=origin_url,
            commit=None,
            tag="does-not-exist",
            destination="destination",
        )
        with pytest.raises(ValueError):
            repository.resolve(dependency)


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
