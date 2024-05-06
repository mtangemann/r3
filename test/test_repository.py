"""Unit tests for ``r3.Repository``."""

import filecmp
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Union

import pytest
import yaml
from executor import execute
from pytest_mock.plugin import MockerFixture

from r3.job import (
    FindAllDependency,
    FindLatestDependency,
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
        execute(f"git init --initial-branch=main {self.path}")
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
def repository(tmp_path: Path) -> Repository:
    return Repository.init(tmp_path / "repository")


def get_dummy_job(name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    return Job(path)


def test_init_fails_if_path_exists(tmp_path: Path) -> None:
    path = tmp_path / "repository"
    path.mkdir()

    with pytest.raises(FileExistsError):
        Repository.init(path)


def test_init_calls_storage_init(tmp_path: Path, mocker: MockerFixture) -> None:
    storage_init = mocker.patch("r3.storage.Storage.init")
    mocker.patch("r3.index.Index.rebuild")

    path = str(tmp_path / "repository")
    Repository.init(path)

    storage_init.assert_called_once_with(Path(path))


def test_init_creates_config_file_with_version(tmp_path: Path) -> None:
    path = tmp_path / "repository"
    Repository.init(path)

    assert (path / "r3.yaml").exists()

    with open(path / "r3.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    assert "version" in config


def test_repository_jobs_calls_find(
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    path = tmp_path / "repository"
    repository = Repository.init(path)

    repository_find = mocker.patch("r3.repository.Repository.find")
    list(repository.jobs())

    repository_find.assert_called_once_with({}, latest=False)


def test_repository_contains_job_calls_storage_contains(
        tmp_path: Path, mocker: MockerFixture
) -> None:
    storage_contains = mocker.patch("r3.storage.Storage.__contains__")

    path = tmp_path / "repository"
    repository = Repository.init(path)
    job = get_dummy_job("base")
    job in repository  # noqa: B015

    storage_contains.assert_called_once_with(job)


def test_repository_contains_job_dependency(tmp_path: Path) -> None:
    repository = Repository.init(tmp_path  / "repository")

    dependency = JobDependency("destination", "123abc")
    assert dependency not in repository

    job = get_dummy_job("base")
    job = repository.commit(job)
    assert job.id is not None

    dependency = JobDependency("destination", job.id)
    assert dependency in repository

    dependency = JobDependency("destination.py", job.id, "run.py")
    assert dependency in repository

    dependency = JobDependency("destination.py", job.id, "does_not_exist.py")
    assert dependency not in repository


def test_repository_contains_git_dependency_clones_repository(
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    # If the repository specified by a GitDependency does not exist locally yet, the
    # __contains__ method should clone the repository before checking whether the
    # commit exists.
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    repository = Repository.init(tmp_path / "r3")
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
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    # If the commit specified by a GitDependency does not exists locally yet, the
    # __contains__ method should fetch all branches before checking whether the commit
    # exists.
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    repository = Repository.init(tmp_path / "r3")
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
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    repository = Repository.init(tmp_path / "r3")
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
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    repository = Repository.init(tmp_path / "r3")
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


def test_repository_contains_query_dependency(tmp_path: Path) -> None:
    repository = Repository.init(tmp_path / "repository")

    dependency = QueryDependency("destination", "#test")
    assert dependency not in repository

    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    assert dependency in repository

    dependency = QueryDependency("destination", "#test #does-not-exist")
    assert dependency not in repository

    dependency = QueryDependency("destination.py", "#test", "run.py")
    assert dependency in repository

    dependency = QueryDependency("destination.py", "#test", "does_not_exist.py")
    assert dependency not in repository


def test_repository_contains_query_all_dependency(tmp_path: Path) -> None:
    repository = Repository.init(tmp_path / "repository")

    dependency = QueryAllDependency("destination", "#test")
    assert dependency not in repository

    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    assert dependency in repository


def test_commit_creates_job_folder(repository: Repository) -> None:
    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 0

    job = get_dummy_job("base")
    repository.commit(job)

    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 1
    assert job_paths[0].is_dir()


def test_commit_returns_the_updated_job(repository: Repository) -> None:
    """Unit test for ``r3.Repository.commit``.

    ``r3.Repository.commit`` should return the ``r3.Job`` instance within the
    repository.
    """
    job = get_dummy_job("base")
    assert job.id is None
    assert not str(job.path).startswith(str(repository.path))

    job = repository.commit(job)
    assert job.id is not None
    assert str(job.path).startswith(str(repository.path))


def test_commit_sets_timestamp(repository: Repository) -> None:
    before = datetime.now()

    job = get_dummy_job("base")
    job = repository.commit(job)

    assert job.timestamp is not None
    assert isinstance(job.timestamp, datetime)
    assert job.timestamp >= before
    assert job.timestamp <= datetime.now()


def test_commit_copies_files_write_protected(repository: Repository) -> None:
    """Unit test for ``r3.Repository.commit``.

    When adding a job to a repository, all files should be copied to the repository. The
    files in the repository should be write protected.
    """
    original_job = get_dummy_job("base")
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


def test_commit_copies_nested_files(repository: Repository) -> None:
    """Unit test for ``r3.Repository.add``."""
    original_job = get_dummy_job("nested")
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
    tmp_path: Path, mocker: MockerFixture,
) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    origin.update()

    repository = Repository.init(tmp_path / "r3")

    dependency = GitDependency(
        repository=origin_url,
        commit=origin.head_commit(),
        destination="destination",
    )

    job_path = tmp_path / "job"
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
    repository: Repository
) -> None:
    base_job = get_dummy_job("base")

    job = repository.commit(base_job)
    assert job.id is not None

    dependency = JobDependency("destination", job.id)
    base_job._dependencies = [dependency]
    base_job._config["dependencies"] = [dependency.to_config()]
    dependent_job = repository.commit(base_job)

    with pytest.raises(ValueError):
        repository.remove(job)

    repository.remove(dependent_job)
    repository.remove(job)


def test_find_dependents_requires_job_id(repository: Repository) -> None:
    job = get_dummy_job("base")
    job = repository.commit(job)

    repository.find_dependents(job)

    job.id = None
    with pytest.raises(ValueError):
        repository.find_dependents(job)


def test_find_dependents(repository: Repository) -> None:
    job1 = get_dummy_job("base")
    job1 = repository.commit(job1)
    assert job1.id is not None

    job2 = get_dummy_job("base")
    dependency = JobDependency("destination1", job1.id)
    job2._dependencies = [dependency]
    job2._config["dependencies"] = [dependency.to_config()]
    job2 = repository.commit(job2)
    assert job2.id is not None

    job3 = get_dummy_job("base")
    dependency = JobDependency("destination2", job1.id)
    job3._dependencies = [dependency]
    job3._config["dependencies"] = [dependency.to_config()]
    job3 = repository.commit(job3)
    assert job3.id is not None

    job4 = get_dummy_job("base")
    dependency = JobDependency("destination3", job2.id)
    job4._dependencies = [dependency]
    job4._config["dependencies"] = [dependency.to_config()]
    dependency = JobDependency("destination4", job3.id)
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


def test_resolve_query_dependency(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job = repository.commit(job)

    dependency = QueryDependency("destination", "#test")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == job.id

    with pytest.raises(ValueError):
        repository.resolve(QueryDependency("destination", "#does-not-exist"))


def test_resolve_find_latest_dependency(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job.metadata["image_size"] = 28
    committed_job_1 = repository.commit(job)

    dependency = FindLatestDependency("destination", {"tags": "test"})
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_1.id
    assert resolved_dependency.source == dependency.source

    job.metadata["tags"] = ["test", "test-again"]
    job.metadata["image_size"] = 32
    committed_job_2 = repository.commit(job)

    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_2.id
    assert resolved_dependency.source == dependency.source

    dependency = FindLatestDependency(
        "destination",
        {"image_size": {"$lt": 30}},
        source="output",
    )
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_1.id
    assert resolved_dependency.source == dependency.source


def test_resolve_find_latest_dependency_preserves_source(
    repository: Repository
) -> None:
    """Regression test."""
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    repository.commit(job)

    dependency = FindLatestDependency("destination", {"tags": "test"}, source="output")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.source == dependency.source


def test_resolve_find_all_dependency(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job.metadata["image_size"] = 28
    committed_job_1 = repository.commit(job)

    dependency = FindAllDependency("destination", {"tags": "test"})
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, list)
    assert len(resolved_dependency) == 1
    assert isinstance(resolved_dependency[0], JobDependency)
    assert resolved_dependency[0].job == committed_job_1.id

    job.metadata["tags"] = ["test", "test-again"]
    job.metadata["image_size"] = 32
    committed_job_2 = repository.commit(job)

    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, list)
    assert len(resolved_dependency) == 2
    assert all(
        isinstance(dependency, JobDependency) for dependency in resolved_dependency
    )
    assert set([dependency.job for dependency in resolved_dependency]) == {
        committed_job_1.id,
        committed_job_2.id,
    }

    dependency = FindAllDependency("destination", {"image_size": {"$lt": 30}})
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, list)
    assert len(resolved_dependency) == 1
    assert isinstance(resolved_dependency[0], JobDependency)
    assert resolved_dependency[0].job == committed_job_1.id


def test_resolve_query_all_dependency(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    commited_job_1 = repository.commit(job)

    dependency = QueryAllDependency("destination", "#test")
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


def test_resolve_git_dependency_from_url(tmp_path: Path, mocker: MockerFixture) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")

    repository = Repository.init(tmp_path / "r3")

    dependency = GitDependency("destination", origin_url)

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


def test_resolve_git_dependency_from_branch(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    origin.update_branch()
    branch_commit = origin.head_commit()
    origin.update()
    main_commit = origin.head_commit()

    repository = Repository.init(tmp_path / "r3")

    def patched_execute(command, **kwargs):
        command = command.replace(origin_url, str(origin.path))
        return execute(command, **kwargs)

    mocker.patch("r3.repository.execute", new=patched_execute)

    dependency = GitDependency("destination", origin_url, branch="main")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, GitDependency)
    assert resolved_dependency.is_resolved()
    assert resolved_dependency.commit == main_commit

    dependency = GitDependency("destination", origin_url, branch="branch")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, GitDependency)
    assert resolved_dependency.is_resolved()
    assert resolved_dependency.commit == branch_commit

    dependency = GitDependency("destination", origin_url, branch="does-not-exist")
    with pytest.raises(ValueError):
        repository.resolve(dependency)


def test_resolve_git_dependency_from_tag(tmp_path: Path, mocker: MockerFixture) -> None:
    origin_url = "git@github.com:mtangemann/origin.git"
    origin = ExampleGitRepository(tmp_path / "origin")
    origin.add_tag("test")
    tag_commit = origin.head_commit()
    origin.update()

    repository = Repository.init(tmp_path / "r3")

    def patched_execute(command, **kwargs):
        command = command.replace(origin_url, str(origin.path))
        return execute(command, **kwargs)

    mocker.patch("r3.repository.execute", new=patched_execute)

    dependency = GitDependency("destination", origin_url, tag="test")
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, GitDependency)
    assert resolved_dependency.is_resolved()
    assert resolved_dependency.commit == tag_commit

    dependency = GitDependency("destination", origin_url, tag="does-not-exist")
    with pytest.raises(ValueError):
        repository.resolve(dependency)


def test_resolve_job(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    committed_job = repository.commit(job)

    dependency = QueryDependency("destination", "#test")
    job._dependencies = [dependency]
    job._config["dependencies"] = [dependency.to_config()]

    resolved_job = repository.resolve(job)
    assert isinstance(resolved_job, Job)
    assert all(dependency.is_resolved() for dependency in resolved_job.dependencies)
    assert isinstance(resolved_job.dependencies[0], JobDependency)
    assert resolved_job.dependencies[0].job == committed_job.id

def test_repository_get_job_by_id(repository: Repository) -> None:
    job = get_dummy_job("base")
    job = repository.commit(job)
    assert job.id is not None
    
    retrieved_job = repository.get_job_by_id(job.id)
    retrieved_job_syntax_sugar = repository[job.id]

    assert retrieved_job.id == retrieved_job_syntax_sugar.id == job.id

    with pytest.raises(KeyError):
        repository.get_job_by_id("invalid-job-id")
    with pytest.raises(KeyError):
        repository["invalid-job-id"]
