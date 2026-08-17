"""Unit tests for ``r3.Repository``."""

import filecmp
import os
import shutil
import socket
import stat
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Generator, Sequence, Union

import boto3
import pytest
import pyzstd
import yaml
from executor import execute
from moto import mock_aws
from pytest_mock.plugin import MockerFixture

import r3.archive
import r3.index
import r3.manifest
import r3.repository
import r3.utils
from r3.job import (
    FilesUnavailableError,
    FindAllDependency,
    FindLatestDependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.remote import RemoteError, S3Remote
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


def assert_lists_dependents(message: str, dependent_ids: Sequence[str]) -> None:
    """Asserts that the message lists the given dependents below an explanation.

    This checks the structure of the message rather than its wording, so that it can
    be reworded freely: each dependent must be listed on a line of its own, in the
    given order, below a line explaining the refusal. The explanation used to be
    written as the *separator* between the dependents instead of as a prefix.
    """
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    listed = [line for line in lines if any(id in line for id in dependent_ids)]

    assert len(listed) == len(dependent_ids)
    for index, dependent_id in enumerate(dependent_ids):
        assert listed[index].endswith(dependent_id)

    assert lines[0] not in listed


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


def _archive_job(repository: Repository, job: Job) -> None:
    """Simulates a move: flip the job to a remote location and drop its local
    files, leaving the metadata-only index row a projection is built from."""
    assert job.id is not None
    repository._index.set_location(job.id, "archive")
    repository._storage.remove(job)


def test_repository_contains_local_job(repository: Repository) -> None:
    committed = repository.commit(get_dummy_job("base"))
    assert committed in repository


def test_repository_contains_remote_projection_from_find(
    repository: Repository,
) -> None:
    committed = repository.commit(get_dummy_job("base"))
    _archive_job(repository, committed)

    projection = repository.find({})[0]
    assert projection in repository


def test_repository_contains_remote_projection_from_get(
    repository: Repository,
) -> None:
    committed = repository.commit(get_dummy_job("base"))
    assert committed.id is not None
    _archive_job(repository, committed)

    projection = repository.get_job_by_id(committed.id)
    assert projection in repository


def test_repository_does_not_contain_same_id_job_from_other_repository(
    tmp_path: Path,
) -> None:
    repository = Repository.init(tmp_path / "repository")
    other_repository = Repository.init(tmp_path / "other")
    committed = repository.commit(get_dummy_job("base"))
    assert committed.id is not None

    # A Job with the same id but living under another repository's job path must
    # not be adopted merely because the id is known to this repository's index.
    foreign = Job(other_repository.path / "jobs" / committed.id, committed.id)
    assert foreign not in repository


def test_repository_does_not_contain_unknown_job(repository: Repository) -> None:
    unknown_id = str(uuid.uuid4())
    foreign = Job(repository.path / "jobs" / unknown_id, unknown_id)
    assert foreign not in repository


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


def test_commit_excludes_output_directory_by_default(
    repository: Repository, tmp_path: Path
) -> None:
    """Commit should exclude ``output/`` from the committed job and its hashes.

    ``output/`` holds re-runnable results. Per the repository format specification, its
    contents are not part of the job's identity and must not be frozen into the
    committed job. This must hold even when the job does not declare ``ignore:
    [/output]``, and it must cover nested output files.
    """
    job_path = tmp_path / "job"
    (job_path / "output" / "sub").mkdir(parents=True)
    with open(job_path / "run.py", "w") as file:
        file.write("print('hello')\n")
    with open(job_path / "r3.yaml", "w") as file:
        yaml.dump({"dependencies": []}, file)
    with open(job_path / "output" / "top.txt", "w") as file:
        file.write("top result\n")
    with open(job_path / "output" / "sub" / "nested.txt", "w") as file:
        file.write("nested result\n")

    committed_job = repository.commit(Job(job_path))

    # The committed job has an (empty) output directory; no output files are copied.
    assert (committed_job.path / "output").is_dir()
    assert list((committed_job.path / "output").iterdir()) == []

    # No output files are folded into the job hashes, but regular files still are.
    with open(committed_job.path / "r3.yaml") as config_file:
        config = yaml.safe_load(config_file)
    assert "run.py" in config["hashes"]
    assert not any(path.startswith("output") for path in config["hashes"])


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

    assert dependent_job.id is not None

    with pytest.raises(ValueError) as exception_info:
        repository.remove(job)

    assert_lists_dependents(str(exception_info.value), [dependent_job.id])

    repository.remove(dependent_job)
    repository.remove(job)


def test_repository_remove_error_message_lists_all_dependents(
    repository: Repository
) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    dependent_ids = []
    for index in range(2):
        dependent_job = get_dummy_job("base")
        dependency = JobDependency(f"destination{index}", job.id)
        dependent_job._dependencies = [dependency]
        dependent_job._config["dependencies"] = [dependency.to_config()]
        dependent_job = repository.commit(dependent_job)
        assert dependent_job.id is not None
        dependent_ids.append(dependent_job.id)

    with pytest.raises(ValueError) as exception_info:
        repository.remove(job)

    # Sorted, since ``Index.find_dependents`` returns an unordered set and the message
    # should not depend on the iteration order.
    assert_lists_dependents(str(exception_info.value), sorted(dependent_ids))


def test_repository_remove_fails_if_job_was_already_removed(
    repository: Repository
) -> None:
    job = repository.commit(get_dummy_job("base"))
    repository.remove(job)

    with pytest.raises(ValueError):
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
    assert resolved_dependency.recursive_checkout

    with pytest.raises(ValueError):
        repository.resolve(QueryDependency("destination", "#does-not-exist"))


def test_resolve_find_latest_dependency(repository: Repository) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job.metadata["image_size"] = 28
    committed_job_1 = repository.commit(job)

    dependency = FindLatestDependency(
        "destination",
        {"tags": "test"},
        recursive_checkout=False
    )

    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_1.id
    assert resolved_dependency.source == dependency.source
    assert not resolved_dependency.recursive_checkout

    job.metadata["tags"] = ["test", "test-again"]
    job.metadata["image_size"] = 32
    committed_job_2 = repository.commit(job)

    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_2.id
    assert resolved_dependency.source == dependency.source
    assert not resolved_dependency.recursive_checkout

    dependency = FindLatestDependency(
        "destination",
        {"image_size": {"$lt": 30}},
        source="output",
    )
    resolved_dependency = repository.resolve(dependency)
    assert isinstance(resolved_dependency, JobDependency)
    assert resolved_dependency.job == committed_job_1.id
    assert resolved_dependency.source == dependency.source
    assert resolved_dependency.recursive_checkout



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

    # A canonical-but-absent id still raises KeyError (unknown). Non-canonical ids
    # raise ValueError instead; that is covered by the id-validation tests.
    with pytest.raises(KeyError):
        repository.get_job_by_id("00000000-0000-4000-8000-00000000dead")
    with pytest.raises(KeyError):
        repository["00000000-0000-4000-8000-00000000dead"]


# --- Remote / move / fetch tests ---

BUCKET = "test-bucket"
PREFIX = "r3/jobs/"


@pytest.fixture
def repository_with_remote(tmp_path: Path) -> Generator[Repository, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        repo = Repository.init(tmp_path / "repository")
        config_path = repo.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        config["remotes"] = {
            "archive": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}
        }

        with open(config_path, "w") as f:
            yaml.dump(config, f)

        repo = Repository(repo.path)
        yield repo


def test_repository_loads_remotes_from_config(
    repository_with_remote: Repository,
) -> None:
    assert "archive" in repository_with_remote.remotes


def test_repository_without_remotes(repository: Repository) -> None:
    assert len(repository.remotes) == 0


def _write_remotes_config(repo_path: Path, remotes: dict) -> None:
    """Overwrites the repository's r3.yaml with the given remotes map."""
    config_path = repo_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["remotes"] = remotes
    with open(config_path, "w") as f:
        yaml.dump(config, f)


def test_repository_open_rejects_reserved_local_remote(tmp_path: Path) -> None:
    """A stored 'local' remote collides with the index's location sentinel and must
    be rejected at open, with a recovery hint, before the index/bucket is touched.

    No moto: validation happens before any remote/index construction, so opening must
    fail with the reserved-name ValueError rather than any AWS/network error.
    """
    repo = Repository.init(tmp_path / "repository")
    _write_remotes_config(
        repo.path, {"local": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}}
    )

    with pytest.raises(ValueError, match="reserved"):
        Repository(repo.path)


def test_repository_open_rejects_empty_remote_name(tmp_path: Path) -> None:
    """A stored empty-named remote is rejected at open."""
    repo = Repository.init(tmp_path / "repository")
    _write_remotes_config(
        repo.path, {"": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}}
    )

    with pytest.raises(ValueError):
        Repository(repo.path)


def test_repository_move_uploads_and_removes_local(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    job_path = repository_with_remote.path / "jobs" / job.id
    assert job_path.exists()

    repository_with_remote.move(job.id, "archive")

    # Local files should be gone
    assert not job_path.exists()

    # Index should still find the job

    location = repository_with_remote._index.get_location(job.id)
    assert location == "archive"


def test_repository_move_raises_for_unknown_remote(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    with pytest.raises(ValueError):
        repository_with_remote.move(job.id, "nonexistent")


def test_repository_move_raises_for_unknown_job(
    repository_with_remote: Repository,
) -> None:
    # A canonical-but-absent id: move validates the id, then raises KeyError because
    # it is unknown. (A non-canonical id would be rejected with ValueError earlier;
    # that path is covered by the id-validation tests.)
    with pytest.raises(KeyError):
        repository_with_remote.move(
            "00000000-0000-4000-8000-00000000dead", "archive"
        )


def test_repository_fetch_downloads_and_restores_local(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    job_path = repository_with_remote.path / "jobs" / job.id
    assert not job_path.exists()

    repository_with_remote.fetch(job.id)

    assert job_path.exists()
    location = repository_with_remote._index.get_location(job.id)
    assert location == "local"


def test_repository_fetch_raises_for_local_job(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    with pytest.raises(ValueError):
        repository_with_remote.fetch(job.id)


def test_repository_move_warns_about_dependents(
    repository_with_remote: Repository,
) -> None:
    base_job = get_dummy_job("base")
    base_job = repository_with_remote.commit(base_job)
    assert base_job.id is not None

    dependent_job = get_dummy_job("base")
    dependency = JobDependency("destination", base_job.id)
    dependent_job._dependencies = [dependency]
    dependent_job._config["dependencies"] = [dependency.to_config()]
    dependent_job = repository_with_remote.commit(dependent_job)

    dependents = repository_with_remote.move(base_job.id, "archive")
    assert len(dependents) > 0
    assert dependent_job.id in {j.id for j in dependents}


def test_repository_find_still_works_after_move(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job.metadata["tags"] = ["findme"]
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    results = repository_with_remote.find({"tags": "findme"})
    assert len(results) == 1
    assert results[0].id == job.id

    # find() after move() yields a metadata-only projection, not a files-bearing job:
    # touching its files must raise rather than silently return an empty/wrong set.
    with pytest.raises(FilesUnavailableError):
        _ = results[0].files


def test_move_commits_location_and_file_list_together(
    repository_with_remote: Repository,
) -> None:
    """After a normal move to a caching remote, the index shows the new location and
    a non-NULL cached file list. There is no observable state with the remote
    location but an old/NULL file list: the two are committed in one transaction."""
    repo = repository_with_remote
    assert repo.remotes["archive"].cache_file_list  # this remote caches file lists
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    repo.move(job.id, "archive")

    assert repo._index.get_location(job.id) == "archive"
    cached = repo._index.get_file_list(job.id)
    assert cached is not None
    assert Path("r3.yaml") in cached


def _archive_member_names(archive_path: Path) -> set:
    """Returns the set of member arcnames in a seekable ``tar.zst`` archive.

    Mirrors how :mod:`r3.archive` reads an archive back (seekable zstd + streaming
    tar), so the assertions see exactly the members r3 wrote.
    """
    with pyzstd.SeekableZstdFile(str(archive_path), "r") as compressed:
        with tarfile.open(fileobj=compressed, mode="r|") as tar:
            return {member.name for member in tar}


def test_move_archive_includes_root_files_and_excludes_output(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A committable file at the job root must survive move() as an archive member.

    This pins the archive contents end to end: a payload at the job root reaches the
    remote's ``data.tar.zst``, while a payload under ``output/`` — excluded from the
    committed job — never enters the archive.
    """
    src = tmp_path / "job"
    src.mkdir()
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text("tags: [archive-contents]\n")
    (src / "data.bin").write_bytes(b"payload-at-root")
    (src / "output").mkdir()
    (src / "output" / "dropped.bin").write_bytes(b"payload-in-output")

    job = repository_with_remote.commit(Job(src))
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    archive_path = tmp_path / "downloaded.tar.zst"
    repository_with_remote.remotes["archive"].download_archive(job.id, archive_path)

    members = _archive_member_names(archive_path)
    assert "data.bin" in members
    # output/ is excluded from the committed job, so it never enters the archive.
    assert "output/dropped.bin" not in members


def test_checkout_raises_for_archived_job(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch"):
        repository_with_remote.checkout(job, tmp_path / "checkout")


def test_checkout_raises_for_archived_dependency(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    dep_job = get_dummy_job("base")
    dep_job.metadata["tags"] = ["dep"]
    dep_job = repository_with_remote.commit(dep_job)
    assert dep_job.id is not None

    main_job = get_dummy_job("base")
    dependency = JobDependency("data", dep_job.id, "run.py")
    main_job._dependencies = [dependency]
    main_job._config["dependencies"] = [dependency.to_config()]
    main_job = repository_with_remote.commit(main_job)

    repository_with_remote.move(dep_job.id, "archive")

    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch"):
        repository_with_remote.checkout(main_job, tmp_path / "checkout")


def test_checkout_remote_projection_raises_clean_error(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A remote top-level job must be refused with the clean 'fetch first' ValueError,
    not the raw FilesUnavailableError that resolve() would raise if it touched the
    projection's dependencies first. The projection is obtained from the index (as a
    real user would after the local files are gone), not from a stale local handle.
    """
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    projection = repository_with_remote.get_job_by_id(job.id)

    dest = tmp_path / "checkout"
    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch") as exc_info:
        repository_with_remote.checkout(projection, dest)
    assert job.id in str(exc_info.value)
    assert not dest.exists()


def test_checkout_refuses_transitive_archived_dependency(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A grand-dependency reachable through a recursive edge must be checked. With
    J --recursive--> A --> B and B archived, checkout(J) is refused naming B, and no
    partial checkout is left behind (the destination is never created).
    """
    repo = repository_with_remote

    grand = get_dummy_job("base")
    grand.metadata["tags"] = ["grand"]
    grand = repo.commit(grand)
    assert grand.id is not None

    middle = get_dummy_job("base")
    edge_mg = JobDependency("grand", grand.id)  # recursive (source=".") by default
    middle._dependencies = [edge_mg]
    middle._config["dependencies"] = [edge_mg.to_config()]
    middle = repo.commit(middle)
    assert middle.id is not None

    top = get_dummy_job("base")
    edge_tm = JobDependency("middle", middle.id)  # recursive: descend into middle
    top._dependencies = [edge_tm]
    top._config["dependencies"] = [edge_tm.to_config()]
    top = repo.commit(top)
    assert top.id is not None

    repo.move(grand.id, "archive")

    dest = tmp_path / "checkout"
    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch") as exc_info:
        repo.checkout(top, dest)
    assert grand.id in str(exc_info.value)
    assert not dest.exists()


def test_checkout_allows_remote_grand_dependency_behind_nonrecursive_edge(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A remote grand-dependency behind a non-recursive edge must NOT block checkout:
    Storage only symlinks jobs/<middle> and never dereferences middle's own deps, so a
    remote B behind J --non-recursive--> A --> B is irrelevant. The checkout succeeds.
    """
    repo = repository_with_remote

    grand = get_dummy_job("base")
    grand.metadata["tags"] = ["grand"]
    grand = repo.commit(grand)
    assert grand.id is not None

    middle = get_dummy_job("base")
    edge_mg = JobDependency("grand", grand.id)  # recursive middle --> grand
    middle._dependencies = [edge_mg]
    middle._config["dependencies"] = [edge_mg.to_config()]
    middle = repo.commit(middle)
    assert middle.id is not None

    top = get_dummy_job("base")
    # Non-recursive edge: Storage symlinks jobs/middle and never touches middle's deps.
    edge_tm = JobDependency("middle", middle.id, recursive_checkout=False)
    top._dependencies = [edge_tm]
    top._config["dependencies"] = [edge_tm.to_config()]
    top = repo.commit(top)
    assert top.id is not None

    repo.move(grand.id, "archive")

    dest = tmp_path / "checkout"
    repo.checkout(top, dest)

    assert dest.exists()
    assert (dest / "middle").is_symlink()


def test_checkout_refuses_nonrecursive_edge_to_remote_dependency(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A non-recursive edge whose own target job is remote must be refused: Storage
    would otherwise create a dangling symlink into jobs/<archived>. checkout(J) is
    refused naming A, and nothing is written.
    """
    repo = repository_with_remote

    dep_job = get_dummy_job("base")
    dep_job.metadata["tags"] = ["nrdep"]
    dep_job = repo.commit(dep_job)
    assert dep_job.id is not None

    top = get_dummy_job("base")
    edge = JobDependency("data", dep_job.id, "run.py", recursive_checkout=False)
    top._dependencies = [edge]
    top._config["dependencies"] = [edge.to_config()]
    top = repo.commit(top)
    assert top.id is not None

    repo.move(dep_job.id, "archive")

    dest = tmp_path / "checkout"
    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch") as exc_info:
        repo.checkout(top, dest)
    assert dep_job.id in str(exc_info.value)
    assert not dest.exists()


def test_rebuild_index_preserves_remote_jobs(
    repository_with_remote: Repository,
) -> None:
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")
    repository_with_remote.rebuild_index()

    # Job must still be findable by query
    results = repository_with_remote.find({"tags": "test"})
    assert len(results) == 1
    assert results[0].id == job.id

    # Location must still be "archive", not reverted to "local"
    location = repository_with_remote._index.get_location(job.id)
    assert location == "archive"


def test_rebuild_index_does_not_duplicate_dependency_rows(
    repository_with_remote: Repository,
) -> None:
    """Rebuild must not duplicate rows for local-child → remote-parent edges.

    The remote-jobs preservation loop should only keep edges where the
    child is remote; edges with a local child are re-inserted from each
    local job's r3.yaml during the rebuild.
    """
    import sqlite3

    base = repository_with_remote.commit(get_dummy_job("base"))
    assert base.id is not None

    dep_src = repository_with_remote.path.parent / "dep-src"
    dep_src.mkdir()
    (dep_src / "r3.yaml").write_text(
        yaml.dump(
            {"dependencies": [JobDependency("out", base.id).to_config()]}
        )
    )
    (dep_src / "metadata.yaml").write_text("tags: [dep]\n")
    repository_with_remote.commit(Job(dep_src))

    repository_with_remote.move(base.id, "archive")

    def edge_count() -> int:
        conn = sqlite3.connect(
            str(repository_with_remote.path / "index.sqlite")
        )
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM job_dependencies"
            ).fetchone()[0]
        finally:
            conn.close()

    before = edge_count()
    repository_with_remote.rebuild_index()
    after_first = edge_count()
    repository_with_remote.rebuild_index()
    after_second = edge_count()

    assert before == after_first == after_second


def test_move_populates_file_list_when_remote_caches(
    repository_with_remote: Repository,
) -> None:
    """When the remote sets cache_file_list=True, move stores the file list."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    expected_files = sorted(job.files.keys())

    repository_with_remote.move(job.id, "archive")

    cached = repository_with_remote._index.get_file_list(job.id)
    assert cached is not None
    assert sorted(cached) == expected_files


def test_move_skips_file_list_when_remote_does_not_cache(
    repository_with_remote: Repository,
) -> None:
    """When the remote sets cache_file_list=False, move does not store a file list."""
    repository_with_remote.remotes["archive"].cache_file_list = False
    try:
        job = get_dummy_job("base")
        job = repository_with_remote.commit(job)
        assert job.id is not None

        repository_with_remote.move(job.id, "archive")
        assert repository_with_remote._index.get_file_list(job.id) is None
    finally:
        repository_with_remote.remotes["archive"].cache_file_list = True


def test_rebuild_index_preserves_remote_job_file_list(
    repository_with_remote: Repository,
) -> None:
    """The cached file list for remote jobs must survive rebuild_index."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    file_list_before = repository_with_remote._index.get_file_list(job.id)
    assert file_list_before is not None
    assert len(file_list_before) > 0

    repository_with_remote.rebuild_index()

    file_list_after = repository_with_remote._index.get_file_list(job.id)
    assert file_list_after == file_list_before


def test_get_job_by_id_returns_remote_projection(
    repository_with_remote: Repository,
) -> None:
    """A moved job is retrievable by ID as a metadata-only projection."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    found = repository_with_remote.get_job_by_id(job.id)
    assert found.id == job.id
    assert isinstance(found.metadata, dict)
    with pytest.raises(FilesUnavailableError):
        _ = found.files


def test_get_job_by_id_unknown_raises_keyerror(repository: Repository) -> None:
    # Canonical but absent -> KeyError (a non-canonical id raises ValueError; see the
    # id-validation tests).
    with pytest.raises(KeyError):
        repository.get_job_by_id("00000000-0000-4000-8000-00000000dead")


def test_contains_remote_job_dependency_with_path_in_file_list(
    repository_with_remote: Repository,
) -> None:
    """A JobDependency on a remote job is contained iff source is in cached files."""
    base_job = get_dummy_job("base")
    base_job = repository_with_remote.commit(base_job)
    assert base_job.id is not None
    repository_with_remote.move(base_job.id, "archive")

    dep_present = JobDependency(
        destination="dest", job=base_job.id, source=Path("run.py")
    )
    dep_absent = JobDependency(
        destination="dest", job=base_job.id, source=Path("does-not-exist.txt")
    )

    assert dep_present in repository_with_remote
    assert dep_absent not in repository_with_remote


def test_contains_remote_job_dependency_with_default_source(
    repository_with_remote: Repository,
) -> None:
    """source=Path('.') is contained when the file list is non-empty."""
    base_job = get_dummy_job("base")
    base_job = repository_with_remote.commit(base_job)
    assert base_job.id is not None
    repository_with_remote.move(base_job.id, "archive")

    dep = JobDependency(destination="dest", job=base_job.id)
    assert dep in repository_with_remote


def test_contains_dependency_on_unknown_job_returns_false(
    repository_with_remote: Repository,
) -> None:
    """Unknown job ID returns False (no local file, no index entry)."""
    dep = JobDependency(
        destination="dest", job="nonexistent-id", source=Path("anything.txt")
    )
    assert dep not in repository_with_remote


def _commit_job_with_subdir(repository: Repository, tmp_path: Path) -> Job:
    """Commits a job whose files include ``results/model.pt`` beneath a directory."""
    src = tmp_path / "job-with-subdir"
    src.mkdir()
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text("tags: [subdir]\n")
    (src / "results").mkdir()
    (src / "results" / "model.pt").write_text("weights")
    return repository.commit(Job(src))


def test_contains_remote_directory_dependency_matches_entry_beneath(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """A directory source on a remote job is contained when the cached file list has
    any entry beneath it, not only on an exact match."""
    job = _commit_job_with_subdir(repository_with_remote, tmp_path)
    assert job.id is not None
    repository_with_remote.move(job.id, "archive")

    # A directory with a file beneath it is present, even though no entry equals it.
    assert JobDependency("dest", job.id, "results") in repository_with_remote
    # The file itself is present on an exact match.
    assert JobDependency("dest", job.id, "results/model.pt") in repository_with_remote
    # A sibling directory that has no entry beneath it is absent.
    assert JobDependency("dest", job.id, "other") not in repository_with_remote


def test_contains_remote_output_dependency_present_for_empty_output(
    repository_with_remote: Repository,
) -> None:
    """A source='output' dependency stays satisfied after a move even when the job's
    output/ is empty and leaves no manifest entry (the output/ convention)."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None
    repository_with_remote.move(job.id, "archive")

    # Local files are gone; resolution goes through the cached file list, which has
    # no entry beneath output/ (the committed output/ is empty).
    assert not (repository_with_remote.path / "jobs" / job.id).exists()
    cached = repository_with_remote._index.get_file_list(job.id)
    assert cached is not None
    assert not any(Path("output") in entry.parents for entry in cached)

    assert JobDependency("dest", job.id, "output") in repository_with_remote
    # The default source is present for a non-empty file list.
    assert JobDependency("dest", job.id, ".") in repository_with_remote


def test_move_parent_with_remote_child_succeeds(
    repository_with_remote: Repository,
) -> None:
    """Moving a parent whose child was already moved to a remote succeeds: the remote
    child is projected by find_dependents rather than raising FileNotFoundError.
    """
    repo = repository_with_remote

    parent = get_dummy_job("base")
    parent.metadata["tags"] = ["parent"]
    parent = repo.commit(parent)
    assert parent.id is not None

    child = get_dummy_job("base")
    child.metadata["tags"] = ["child"]
    dependency = JobDependency("parent", parent.id)
    child._dependencies = [dependency]
    child._config["dependencies"] = [dependency.to_config()]
    child = repo.commit(child)
    assert child.id is not None

    repo.move(child.id, "archive")
    dependents = repo.move(parent.id, "archive")

    assert child.id in {dependent.id for dependent in dependents}
    assert repo._index.get_location(parent.id) == "archive"


def test_remove_parent_with_remote_child_reports_dependents(
    repository_with_remote: Repository,
) -> None:
    """Removing a parent whose child is remote reaches the proper dependents-exist
    refusal (ValueError listing the child) instead of raising FileNotFoundError from
    find_dependents."""
    repo = repository_with_remote

    parent = get_dummy_job("base")
    parent.metadata["tags"] = ["parent"]
    parent = repo.commit(parent)
    assert parent.id is not None

    child = get_dummy_job("base")
    child.metadata["tags"] = ["child"]
    dependency = JobDependency("parent", parent.id)
    child._dependencies = [dependency]
    child._config["dependencies"] = [dependency.to_config()]
    child = repo.commit(child)
    assert child.id is not None

    repo.move(child.id, "archive")

    with pytest.raises(ValueError, match="depend on it") as exc_info:
        repo.remove(parent)
    assert child.id in str(exc_info.value)


def test_repository_re_move_after_fetch_preserves_file_list(
    repository_with_remote: Repository,
) -> None:
    """move → fetch → move: file list is captured fresh each time."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None
    expected = sorted(job.files.keys())

    repository_with_remote.move(job.id, "archive")
    first_list = repository_with_remote._index.get_file_list(job.id)
    assert first_list is not None
    first = sorted(first_list)
    assert first == expected

    repository_with_remote.fetch(job.id)
    repository_with_remote.move(job.id, "archive")
    second_list = repository_with_remote._index.get_file_list(job.id)
    assert second_list is not None
    second = sorted(second_list)
    assert second == expected


def test_format_version_is_beta_9() -> None:
    from r3.repository import R3_FORMAT_VERSION
    assert R3_FORMAT_VERSION == "1.0.0-beta.9"


def test_migration_beta_9_adds_files_column(tmp_path: Path) -> None:
    """The migration script bumps version and adds the files column via ALTER TABLE."""
    import importlib.util
    import sqlite3

    from click.testing import CliRunner

    # Import the migration's click command. The migration file isn't a package,
    # so import via importlib for a clean test.
    spec = importlib.util.spec_from_file_location(
        "migration_beta_9", "migration/1_0_0_beta_9.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Set up a fake beta.8 repository: r3.yaml + a beta.8-shaped index.
    repo_path = tmp_path / "old-repo"
    repo_path.mkdir()
    (repo_path / "r3.yaml").write_text("version: 1.0.0-beta.8\n")

    conn = sqlite3.connect(str(repo_path / "index.sqlite"))
    conn.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            metadata JSON NOT NULL,
            location TEXT NOT NULL DEFAULT 'local'
        )
        """
    )
    conn.execute(
        "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)",
        ("test-id", "2026-01-01T00:00:00", '{"tags": ["test"]}'),
    )
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        module.migrate,
        ["--repository", str(repo_path)],
        input="y\ny\n",
    )
    assert result.exit_code == 0, result.output

    # Verify the version bump.
    with open(repo_path / "r3.yaml") as f:
        new_config = yaml.safe_load(f)
    assert new_config["version"] == "1.0.0-beta.9"

    # Verify the column was added and existing row preserved with NULL files.
    conn = sqlite3.connect(str(repo_path / "index.sqlite"))
    columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
    assert "files" in columns
    row = conn.execute(
        "SELECT id, files FROM jobs WHERE id = 'test-id'"
    ).fetchone()
    assert row[0] == "test-id"
    assert row[1] is None
    conn.close()


# --- move/fetch crash-safety ---


def test_move_verifies_by_streaming_without_second_temp_file(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """move verifies the uploaded archive by streaming its digest from the remote,
    never downloading it into a second temp file. That keeps at most one archive-sized
    temp (data.tar.zst) in scratch at a time, so a large-job move cannot fail purely
    because verification duplicated the archive under /tmp."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]

    calls = {"stream": 0}
    real_archive_sha256 = remote.archive_sha256

    def counting_stream(job_id: str) -> str:
        calls["stream"] += 1
        return real_archive_sha256(job_id)

    def forbidden_download(job_id: str, destination: Path) -> None:
        raise AssertionError(
            "move must verify by streaming the digest, not by downloading the "
            "archive into a second temp file"
        )

    monkeypatch.setattr(remote, "archive_sha256", counting_stream)
    monkeypatch.setattr(remote, "download_archive", forbidden_download)

    repo.move(job.id, "archive")

    assert calls["stream"] == 1  # the streamed digest is what verifies the archive
    assert repo._index.get_location(job.id) == "archive"
    assert remote.exists(job.id)


def test_move_aborts_and_keeps_local_on_verify_failure(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the uploaded archive fails content verification, move aborts before
    publishing the manifest and leaves the local job intact. Verification streams the
    archive's digest from the remote, so a forced digest mismatch stands in for a
    corrupted upload."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]

    def wrong_digest(job_id: str) -> str:
        return "0" * 64

    monkeypatch.setattr(remote, "archive_sha256", wrong_digest)

    with pytest.raises(RemoteError):
        repo.move(job.id, "archive")

    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "local"
    assert not remote.exists(job.id)  # manifest never published


def test_fetch_aborts_and_leaves_no_local_on_corrupt_archive(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt archive on fetch raises and leaves no jobs/<id>; the index still
    points at the remote."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = repo.remotes["archive"]

    def corrupt_download(job_id: str, destination: Path) -> None:
        destination.write_bytes(b"corrupted")

    monkeypatch.setattr(remote, "download_archive", corrupt_download)

    with pytest.raises(RemoteError):
        repo.fetch(job.id)

    assert not (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"


def test_fetch_is_idempotent_after_interruption(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupting fetch after the staging rename + remote delete but before the
    index flip is recoverable: a re-run finalizes via the local receipt."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    real_set_location = repo._index.set_location
    state = {"tripped": False}

    def flaky_set_location(job_id: str, location: str) -> None:
        if location == "local" and not state["tripped"]:
            state["tripped"] = True
            raise RuntimeError("simulated interruption before index flip")
        real_set_location(job_id, location)

    monkeypatch.setattr(repo._index, "set_location", flaky_set_location)

    with pytest.raises(RuntimeError):
        repo.fetch(job.id)

    # Local restored, remote deleted, but the index flip did not land.
    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"

    # Re-run finalizes from the receipt (remote manifest is already gone).
    repo.fetch(job.id)
    assert repo._index.get_location(job.id) == "local"
    assert (repo.path / "jobs" / job.id).exists()


def test_fetch_aborts_and_cleans_staging_on_extraction_failure(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An extraction failure mid-fetch (safe_extract raises after some members were
    written) leaves no jobs/<id>, the index still pointing at the remote, and no
    leftover staging directory; a re-run then fetches cleanly."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = repo.remotes["archive"]
    fetch_dir = repo.path / ".fetch"

    real_safe_extract = r3.archive.safe_extract
    state = {"failed": False}

    def flaky_safe_extract(
        archive_path: Path, staging_dir: Path, expected: dict
    ) -> None:
        if not state["failed"]:
            state["failed"] = True
            # Fail mid-way: create the staging dir and a partial member, then raise, so
            # the test proves the finally-clause cleans a non-empty staging directory.
            staging_dir.mkdir(parents=True, exist_ok=True)
            (staging_dir / "partial").write_text("x")
            raise r3.archive.ArchiveError("simulated extraction failure mid-way")
        real_safe_extract(archive_path, staging_dir, expected)

    monkeypatch.setattr(r3.archive, "safe_extract", flaky_safe_extract)

    with pytest.raises(r3.archive.ArchiveError):
        repo.fetch(job.id)

    assert not (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"
    assert list(fetch_dir.glob(f"{job.id}-*")) == []  # staging cleaned up

    # A retry starts clean (no leftover staging trips it) and finalizes the fetch.
    repo.fetch(job.id)
    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "local"
    assert not remote.exists(job.id)


def test_move_retry_succeeds_after_verify_failure(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a move aborts on upload verification (no manifest published, local kept),
    a re-run starts clean and completes: the payload is re-uploaded and the manifest
    published, with the local copy then deleted."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]
    job_dir = repo.path / "jobs" / job.id

    real_archive_sha256 = remote.archive_sha256
    state = {"failed": False}

    def flaky_archive_sha256(job_id: str) -> str:
        if not state["failed"]:
            state["failed"] = True
            return "0" * 64  # first verify's streamed digest mismatches
        return real_archive_sha256(job_id)

    monkeypatch.setattr(remote, "archive_sha256", flaky_archive_sha256)

    with pytest.raises(RemoteError):
        repo.move(job.id, "archive")
    assert job_dir.exists()
    assert repo._index.get_location(job.id) == "local"
    assert not remote.exists(job.id)  # no manifest published

    # A retry starts clean and completes the move.
    repo.move(job.id, "archive")
    assert not job_dir.exists()
    assert repo._index.get_location(job.id) == "archive"
    assert remote.exists(job.id)


def test_fetch_surfaces_remote_delete_errors_and_stays_retryable(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A per-object failure reported in delete_objects' Errors array during fetch's
    remote cleanup (delete_job) surfaces as RemoteError; the restored local job stays
    in place and the index is not yet flipped, so the fetch remains retryable."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = repo.remotes["archive"]
    assert isinstance(remote, S3Remote)

    def failing_delete_objects(**kwargs: object) -> dict:
        return {"Errors": [{"Key": "k", "Code": "AccessDenied", "Message": "no"}]}

    monkeypatch.setattr(remote._client, "delete_objects", failing_delete_objects)

    with pytest.raises(RemoteError):
        repo.fetch(job.id)

    # Local job restored + still indexed remote (delete_job failed before the flip).
    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"


def test_move_aborts_on_quiescence_violation(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the job directory changes between capture and the pre-delete re-check, move
    aborts (keeping local) and removes the stale published manifest."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    real_snapshot = r3.repository._dir_snapshot
    calls = {"n": 0}

    def changing_snapshot(job_dir: Path) -> dict:
        calls["n"] += 1
        snapshot = dict(real_snapshot(job_dir))
        if calls["n"] >= 2:  # the pre-delete re-check sees a change
            snapshot[Path("__mutated__")] = (1, 1)
        return snapshot

    monkeypatch.setattr(r3.repository, "_dir_snapshot", changing_snapshot)

    with pytest.raises(RuntimeError, match="changed during move"):
        repo.move(job.id, "archive")

    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "local"
    assert not repo.remotes["archive"].exists(job.id)  # stale manifest removed


def test_move_special_entry_after_publish_routes_through_cleanup(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A special entry appearing after the manifest is published but before the step-6
    quiescence re-check must route through the SAME cleanup as a size/mtime change: the
    re-check's ManifestError is caught, the published manifest is deleted, and the
    "changed during move" RuntimeError is raised — the raw ManifestError never escapes
    to orphan the published manifest."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]
    job_dir = repo.path / "jobs" / job.id

    real_publish = remote.publish_manifest

    def publish_then_plant_fifo(job_id: str, data: bytes) -> None:
        real_publish(job_id, data)
        os.mkfifo(job_dir / "output" / "pipe")  # appears before the re-check

    monkeypatch.setattr(remote, "publish_manifest", publish_then_plant_fifo)

    with pytest.raises(RuntimeError, match="changed during move"):
        repo.move(job.id, "archive")

    assert not remote.exists(job.id)  # published manifest cleaned up, not orphaned
    assert job_dir.exists()  # local job intact
    assert repo._index.get_location(job.id) == "local"


def test_move_aborts_before_upload_if_archive_not_restorable(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """move proves the just-built archive round-trips (safe_extract + verify_directory,
    exactly as fetch does) BEFORE touching the remote. If it does not reconstruct the
    job, move aborts with nothing uploaded and the sole local copy untouched."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]

    def not_restorable(job_dir: Path, manifest: dict) -> None:
        raise r3.manifest.ManifestError("simulated non-round-trip")

    monkeypatch.setattr(r3.manifest, "verify_directory", not_restorable)

    with pytest.raises(RuntimeError, match="round-trip"):
        repo.move(job.id, "archive")

    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "local"
    assert not remote.exists(job.id)  # no manifest published
    assert remote.archive_size(job.id) is None  # nothing uploaded


def test_move_sweeps_stale_fetch_receipt(
    repository_with_remote: Repository,
) -> None:
    """move invalidates any stale fetch receipt up front. A leftover receipt from an
    earlier fetch would otherwise make a later fetch step-0 report a spurious
    remote/receipt disagreement (archive_sha256 is not stable across re-moves)."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    fetch_dir = repo.path / ".fetch"
    fetch_dir.mkdir(exist_ok=True)
    receipt = fetch_dir / f"{job.id}.receipt.json"
    receipt.write_bytes(b"{}")

    repo.move(job.id, "archive")

    assert not receipt.exists()


def test_fetch_step0_persists_receipt_before_finalize(
    repository_with_remote: Repository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Step-0 finalize must persist a recovery receipt before deleting the remote, so
    a crash after the remote delete but before the index flip stays recoverable. This
    reproduces the move step-7↔step-8 window: jobs/<id> present, index remote, no
    receipt (design: fetch recovery)."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    # Snapshot the exact committed bytes, then move (uploads, deletes local, writes no
    # receipt). Recreating jobs/<id> from the snapshot reproduces the state a move
    # crash leaves between the index flip (remote) and the local delete.
    snapshot = tmp_path / "snapshot"
    shutil.copytree(job_dir, snapshot)
    repo.move(job.id, "archive")
    assert not job_dir.exists()
    shutil.copytree(snapshot, job_dir)
    assert repo._index.get_location(job.id) == "archive"

    receipt = repo.path / ".fetch" / f"{job.id}.receipt.json"
    assert not receipt.exists()

    # Simulate a crash after the remote delete but before the index flip on the first
    # finalize; the rerun delegates to the real flip.
    real_set_location = repo._index.set_location
    state = {"tripped": False}

    def flaky_set_location(job_id: str, location: str) -> None:
        if location == "local" and not state["tripped"]:
            state["tripped"] = True
            raise RuntimeError("simulated crash after remote delete")
        real_set_location(job_id, location)

    monkeypatch.setattr(repo._index, "set_location", flaky_set_location)

    with pytest.raises(RuntimeError):
        repo.fetch(job.id)

    # The receipt was persisted before the remote delete, so recovery is possible even
    # though the remote manifest is now gone.
    assert receipt.exists()
    assert repo._index.get_location(job.id) == "archive"

    # Rerun finalizes from the receipt (remote manifest already deleted) — the former
    # dead-end ("Manual intervention required") is gone.
    repo.fetch(job.id)
    assert repo._index.get_location(job.id) == "local"
    assert job_dir.exists()


def test_fetch_write_protects_restored_job(
    repository_with_remote: Repository,
) -> None:
    """A fetched job is write-protected exactly like a committed one: the job dir,
    r3.yaml, and payload files are read-only, while metadata.yaml stays writable."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    repo.fetch(job.id)

    job_dir = repo.path / "jobs" / job.id

    def is_writable(path: Path) -> bool:
        return bool(stat.S_IMODE(os.lstat(path).st_mode) & stat.S_IWUSR)

    assert not is_writable(job_dir)
    assert not is_writable(job_dir / "r3.yaml")
    assert not is_writable(job_dir / "run.py")
    assert is_writable(job_dir / "metadata.yaml")


# --- files-only symmetry: move/fetch reject symlinks, special files, hardlinks ---


def _create_special_entry(parent: Path, external_dir: Path, kind: str):
    """Creates a special filesystem entry of ``kind`` under ``parent``.

    Returns ``(basename, type_keyword)`` for the created entry, or ``None`` when the
    kind cannot be created in this environment (device nodes without privileges, or an
    AF_UNIX socket whose bind() is denied in a restricted sandbox).
    """
    if kind == "fifo":
        os.mkfifo(parent / "fifo")
        return "fifo", "FIFO"
    if kind == "socket":
        sock = socket.socket(socket.AF_UNIX)
        # Bind a short *relative* name from inside ``parent`` so the AF_UNIX sun_path
        # length limit (~108 bytes) is not tripped by a long tmp path; the socket file
        # persists on disk after the socket object is closed. A restricted sandbox can
        # deny bind() with PermissionError; treat that like a device node without
        # privileges and return None so the test skips instead of erroring.
        previous = os.getcwd()
        os.chdir(parent)
        try:
            sock.bind("sock")
        except (PermissionError, OSError):
            sock.close()
            return None
        finally:
            os.chdir(previous)
        sock.close()
        return "sock", "socket"
    if kind == "broken_symlink":
        (parent / "broken").symlink_to(parent / "does-not-exist")
        return "broken", "symbolic link"
    if kind == "file_symlink":
        target = external_dir / "real_target.txt"
        target.write_text("outside payload")
        (parent / "filelink").symlink_to(target)
        return "filelink", "symbolic link"
    if kind == "dir_symlink":
        target = external_dir / "real_dir"
        target.mkdir()
        (target / "inside.txt").write_text("inside")
        (parent / "dirlink").symlink_to(target)
        return "dirlink", "symbolic link"
    if kind == "external_hardlink":
        target = external_dir / "ext_source.txt"
        target.write_text("shared bytes")
        os.link(target, parent / "exthard")
        return "exthard", "hardlink"
    if kind == "internal_hardlink":
        (parent / "inthard_a").write_text("shared bytes")
        os.link(parent / "inthard_a", parent / "inthard_b")
        return "inthard_a", "hardlink"  # sorts first, so it is the named offender
    if kind == "device":
        try:
            os.mknod(parent / "dev0", stat.S_IFCHR | 0o600, os.makedev(1, 3))
        except (PermissionError, OSError):
            return None
        return "dev0", "character device"
    raise AssertionError(f"unknown kind: {kind}")


SPECIAL_ENTRY_KINDS = [
    "fifo",
    "socket",
    "broken_symlink",
    "file_symlink",
    "dir_symlink",
    "external_hardlink",
    "internal_hardlink",
    "device",
]


@pytest.mark.parametrize("kind", SPECIAL_ENTRY_KINDS)
def test_move_refuses_special_or_symlink_or_hardlink_entry(
    repository_with_remote: Repository, tmp_path: Path, kind: str
) -> None:
    """move must refuse a FIFO, socket, (broken/file/dir) symlink, device, or
    hardlink under the job BEFORE publishing a manifest, naming the path and its type,
    and leave the local job — and the offending entry — intact.

    This is the reproduction for the ``is_file()``-filter bug: with the old snapshot,
    such an entry was silently dropped, so move succeeded and then deleted the local
    copy (the FIFO case loses data outright)."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]
    job_dir = repo.path / "jobs" / job.id
    output_dir = job_dir / "output"  # committed jobs keep output/ writable

    external = tmp_path / "external"
    external.mkdir()
    created = _create_special_entry(output_dir, external, kind)
    if created is None:
        pytest.skip(f"cannot create {kind} without elevated privileges")
    basename, type_keyword = created
    entry_path = output_dir / basename

    with pytest.raises(r3.manifest.ManifestError) as excinfo:
        repo.move(job.id, "archive")

    # The error names the offending relative path AND its type.
    message = str(excinfo.value)
    assert f"output/{basename}" in message
    assert type_keyword in message

    # The local job and the offending entry both survive the refusal ...
    assert job_dir.exists()
    assert os.path.lexists(entry_path)  # lexists: a broken symlink still counts
    # ... the index still says local, and NO final manifest was published.
    assert repo._index.get_location(job.id) == "local"
    assert not remote.exists(job.id)


def test_fetch_step0_rejects_special_entry_without_deleting_remote(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """Fetch's step-0 verification (a pre-existing jobs/<id>) must reject an extra
    special entry rather than treat the directory as matching and delete the remote
    authoritative copy.

    Reproduces the move step-7↔step-8 crash window (jobs/<id> present, index remote),
    then plants a FIFO: the old ``is_file()`` walk ignored it, so verification passed
    and fetch deleted the remote."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id
    remote = repo.remotes["archive"]

    # Snapshot the committed bytes, move (uploads + deletes local), then recreate
    # jobs/<id> from the snapshot to reproduce the crash-window state.
    snapshot = tmp_path / "snapshot"
    shutil.copytree(job_dir, snapshot)
    repo.move(job.id, "archive")
    assert not job_dir.exists()
    shutil.copytree(snapshot, job_dir)
    assert repo._index.get_location(job.id) == "archive"
    assert remote.exists(job.id)

    # output/ stays writable on a committed job, so a FIFO can be planted there.
    os.mkfifo(job_dir / "output" / "pipe")

    with pytest.raises((RuntimeError, r3.manifest.ManifestError)):
        repo.fetch(job.id)

    # The remote copy is preserved and the index still points at it: verification
    # refused to accept the tampered directory.
    assert remote.exists(job.id)
    assert repo._index.get_location(job.id) == "archive"


def test_fetch_rejects_canonical_but_different_manifest_job_id(
    repository_with_remote: Repository,
) -> None:
    """Fetch must reject a remote manifest whose ``job_id`` is a *different canonical
    UUID* than the requested job, before downloading any payload or deleting anything:
    the identity binding is checked at the parse boundary, right after ``get_manifest``
    and before ``download_archive``."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = repo.remotes["archive"]

    # Overwrite ONLY the remote manifest's job_id with a different canonical UUID.
    manifest = r3.manifest.loads(remote.get_manifest(job.id))
    manifest["job_id"] = str(uuid.uuid4())
    _put_object(f"{PREFIX}{job.id}/manifest.json", r3.manifest.dumps(manifest))

    keys_before = set(_job_object_keys(job.id, PREFIX))

    with pytest.raises(r3.manifest.ManifestError):
        repo.fetch(job.id)

    # No payload object downloaded or deleted: remote objects intact, no local job dir.
    assert set(_job_object_keys(job.id, PREFIX)) == keys_before
    assert remote.exists(job.id)
    assert not (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"


def test_fetch_step0_rejects_manifest_for_another_job(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """Fetch's step-0 finalize (a pre-existing jobs/<id> while the index says remote)
    must reject a manifest/receipt whose ``job_id`` is a different canonical UUID,
    before the remote is deleted or the index is flipped — so a receipt belonging to
    another job cannot finalize this one. Both local and remote state stay untouched."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id
    remote = repo.remotes["archive"]

    # Reproduce the move step-7<->step-8 crash window: jobs/<id> present, index remote.
    snapshot = tmp_path / "snapshot"
    shutil.copytree(job_dir, snapshot)
    repo.move(job.id, "archive")
    assert not job_dir.exists()
    shutil.copytree(snapshot, job_dir)
    assert repo._index.get_location(job.id) == "archive"

    # Tamper the job_id to a different canonical UUID, and make BOTH the remote manifest
    # and a local receipt carry these identical bytes (so they agree byte-for-byte and
    # the id check — not the disagreement check — is what fires).
    manifest = r3.manifest.loads(remote.get_manifest(job.id))
    manifest["job_id"] = str(uuid.uuid4())
    tampered = r3.manifest.dumps(manifest)
    _put_object(f"{PREFIX}{job.id}/manifest.json", tampered)
    receipt_path = repo.path / ".fetch" / f"{job.id}.receipt.json"
    receipt_path.parent.mkdir(exist_ok=True)
    receipt_path.write_bytes(tampered)

    keys_before = set(_job_object_keys(job.id, PREFIX))

    with pytest.raises(r3.manifest.ManifestError):
        repo.fetch(job.id)

    # Both local and remote state untouched.
    assert job_dir.exists()
    assert set(_job_object_keys(job.id, PREFIX)) == keys_before
    assert remote.exists(job.id)
    assert repo._index.get_location(job.id) == "archive"
    assert receipt_path.exists()


def test_move_fetch_roundtrips_nested_directories(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """Ordinary files and nested directories still round-trip unchanged through
    commit -> move -> fetch."""
    repo = repository_with_remote
    src = tmp_path / "job"
    # Nested payload dirs live under the committed tree (not output/, which R3 drops).
    (src / "code" / "deep" / "nested").mkdir(parents=True)
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text("tags: [roundtrip]\n")
    (src / "top.txt").write_text("top-level payload")
    (src / "code" / "deep" / "mid.bin").write_bytes(b"\x00\x01mid")
    (src / "code" / "deep" / "nested" / "leaf.txt").write_text("leaf payload")

    job = repo.commit(Job(src))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    repo.move(job.id, "archive")
    assert not job_dir.exists()
    repo.fetch(job.id)

    assert (job_dir / "top.txt").read_text() == "top-level payload"
    assert (job_dir / "code" / "deep" / "mid.bin").read_bytes() == b"\x00\x01mid"
    assert (
        job_dir / "code" / "deep" / "nested" / "leaf.txt"
    ).read_text() == "leaf payload"
    assert repo._index.get_location(job.id) == "local"


# --- atomic, bucket-backed, fail-closed rebuild ---


def _publish_job_to_remote(remote, job_dir: Path, job_id: str) -> None:
    """Uploads a complete remote representation (archive + sidecars + manifest) of the
    committed job at ``job_dir`` under ``job_id`` WITHOUT deleting the local copy.

    Mirrors ``move``'s capture/upload steps but skips the local deletion, so a test can
    set up a job that exists both locally and on a remote (local-wins) or the same job
    on two remotes (duplicate detection).
    """
    files = [
        child.relative_to(job_dir)
        for child in job_dir.rglob("*")
        if child.is_file()
    ]
    member_paths = [p for p in files if p.as_posix() not in r3.manifest.SIDECAR_PATHS]

    temp_dir = Path(tempfile.mkdtemp(prefix="r3-test-publish-"))
    try:
        archive_path = temp_dir / "data.tar.zst"
        result = r3.archive.create_archive(job_dir, member_paths, archive_path)
        entries = list(result.entries)
        sidecar_bytes = {}
        for name in r3.manifest.SIDECAR_PATHS:
            data = (job_dir / name).read_bytes()
            sidecar_bytes[name] = data
            entries.append(
                r3.manifest.FileEntry(name, len(data), r3.utils.hash_bytes(data))
            )
        manifest = r3.manifest.build_manifest(
            job_id, entries, result.sha256, result.size
        )
        remote.put_archive(job_id, archive_path)
        for name, data in sidecar_bytes.items():
            remote.put_sidecar(job_id, name, data)
        remote.publish_manifest(job_id, r3.manifest.dumps(manifest))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _assert_index_unchanged(repo: Repository, before: bytes) -> None:
    """Asserts the fail-closed guarantee: old index bytes intact, no stale ``.new``."""
    assert (repo.path / "index.sqlite").read_bytes() == before
    assert not (repo.path / "index.sqlite.new").exists()


@pytest.fixture
def repository_with_two_remotes(tmp_path: Path) -> Generator[Repository, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        repo = Repository.init(tmp_path / "repository")
        config_path = repo.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        config["remotes"] = {
            "archive": {"type": "s3", "bucket": BUCKET, "prefix": "r3/a/"},
            "archive2": {"type": "s3", "bucket": BUCKET, "prefix": "r3/b/"},
        }

        with open(config_path, "w") as f:
            yaml.dump(config, f)

        repo = Repository(repo.path)
        yield repo


def test_rebuild_restores_remote_job_from_bucket(
    repository_with_remote: Repository,
) -> None:
    """After deleting the index, rebuild reconstructs a remote row from the bucket
    alone: metadata, timestamp, dependencies, file list, and location."""
    import sqlite3

    repo = repository_with_remote

    base = repo.commit(get_dummy_job("base"))
    assert base.id is not None

    child = get_dummy_job("base")
    child.metadata["tags"] = ["child"]
    dependency = JobDependency("base_out", base.id)
    child._dependencies = [dependency]
    child._config["dependencies"] = [dependency.to_config()]
    child = repo.commit(child)
    assert child.id is not None

    expected_timestamp = repo.get_job_by_id(child.id).timestamp
    expected_files = sorted(repo.get_job_by_id(child.id).files.keys())

    repo.move(child.id, "archive")

    # Drop the index entirely: the remote row can only come back from the bucket.
    (repo.path / "index.sqlite").unlink()
    repo.rebuild_index()

    results = repo.find({"tags": "child"})
    assert len(results) == 1
    assert results[0].id == child.id

    assert repo._index.get_location(child.id) == "archive"
    assert repo._index.get_location(base.id) == "local"

    assert repo.get_job_by_id(child.id).timestamp == expected_timestamp
    assert "child" in repo.get_job_by_id(child.id).metadata["tags"]

    cached_files = repo._index.get_file_list(child.id)
    assert cached_files is not None
    assert sorted(cached_files) == expected_files

    conn = sqlite3.connect(str(repo.path / "index.sqlite"))
    try:
        edges = conn.execute(
            "SELECT child_id, parent_id FROM job_dependencies"
        ).fetchall()
    finally:
        conn.close()
    assert (child.id, base.id) in edges


def test_rebuild_local_wins_over_complete_remote(
    repository_with_remote: Repository,
) -> None:
    """A job present both as jobs/<id> and as a complete remote manifest is indexed
    once, as local, with no duplicate/IntegrityError."""
    repo = repository_with_remote

    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    _publish_job_to_remote(
        repo.remotes["archive"], repo.path / "jobs" / job.id, job.id
    )

    (repo.path / "index.sqlite").unlink()
    repo.rebuild_index()

    assert len(repo._index) == 1
    assert repo._index.get_location(job.id) == "local"


def test_rebuild_fails_closed_on_corrupt_sidecar(
    repository_with_remote: Repository,
) -> None:
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    repo.remotes["archive"].put_sidecar(job.id, "r3.yaml", b"corrupted!")

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_missing_sidecar(
    repository_with_remote: Repository,
) -> None:
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    client = boto3.client("s3", region_name="us-east-1")
    client.delete_object(Bucket=BUCKET, Key=f"{PREFIX}{job.id}/metadata.yaml")

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_manifest_schema_violation(
    repository_with_remote: Repository,
) -> None:
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}{job.id}/manifest.json", Body=b"not-json"
    )

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_job_id_key_mismatch(
    repository_with_remote: Repository,
) -> None:
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    manifest = r3.manifest.loads(repo.remotes["archive"].get_manifest(job.id))
    manifest["job_id"] = "a-different-job-id"
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX}{job.id}/manifest.json",
        Body=r3.manifest.dumps(manifest),
    )

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_canonical_job_id_mismatch(
    repository_with_remote: Repository,
) -> None:
    """A manifest whose ``job_id`` is a *different canonical UUID* than its object key
    must abort rebuild fail-closed, with a diagnostic naming the remote and job, leaving
    the old index intact. This exercises the identity binding in ``loads`` rather than
    the schema's canonical-UUID check."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    manifest = r3.manifest.loads(repo.remotes["archive"].get_manifest(job.id))
    manifest["job_id"] = str(uuid.uuid4())
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX}{job.id}/manifest.json",
        Body=r3.manifest.dumps(manifest),
    )

    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    message = str(excinfo.value)
    assert job.id in message and "archive" in message
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_missing_archive(
    repository_with_remote: Repository,
) -> None:
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    client = boto3.client("s3", region_name="us-east-1")
    client.delete_object(Bucket=BUCKET, Key=f"{PREFIX}{job.id}/data.tar.zst")

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_oversized_manifest(
    repository_with_remote: Repository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manifest object larger than the rebuild byte cap aborts the rebuild before it
    is slurped and cached, leaving the old index intact. Shrinking the cap so the
    job's own (valid) manifest exceeds it isolates the cap as the sole cause."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    monkeypatch.setattr(r3.index, "MAX_MANIFEST_BYTES", 5, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    message = str(excinfo.value)
    assert job.id in message and "archive" in message
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_oversized_sidecar(
    repository_with_remote: Repository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sidecar object larger than the rebuild byte cap aborts the rebuild before it
    is slurped and cached, leaving the old index intact."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    monkeypatch.setattr(r3.index, "MAX_SIDECAR_BYTES", 3, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    message = str(excinfo.value)
    assert job.id in message and "archive" in message
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_manifest_over_extraction_caps(
    repository_with_remote: Repository,
) -> None:
    """A manifest whose declared file sizes exceed the extraction caps is rejected at
    rebuild — the same bound fetch enforces — so rebuild never caches a manifest a
    later fetch would refuse. The manifest is otherwise valid and consistent, so the
    cap is the sole cause of the abort."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = (repo.path / "index.sqlite").read_bytes()
    manifest = r3.manifest.loads(repo.remotes["archive"].get_manifest(job.id))
    manifest["files"].append(
        {
            "path": "huge.bin",
            "size": r3.archive.DEFAULT_MAX_TOTAL_BYTES + 1,
            "sha256": "0" * 64,
        }
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX}{job.id}/manifest.json",
        Body=r3.manifest.dumps(manifest),
    )

    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    message = str(excinfo.value)
    assert job.id in message and "archive" in message
    _assert_index_unchanged(repo, before)


def test_rebuild_fails_closed_on_local_job_missing_r3yaml(
    repository: Repository,
) -> None:
    repo = repository
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    before = (repo.path / "index.sqlite").read_bytes()
    corrupt = repo.path / "jobs" / "corrupt-job"
    corrupt.mkdir()
    (corrupt / "metadata.yaml").write_text("tags: [corrupt]\n")

    with pytest.raises(RuntimeError):
        repo.rebuild_index()
    _assert_index_unchanged(repo, before)


def test_rebuild_local_wins_over_duplicate_remote_leftovers(
    repository_with_two_remotes: Repository,
) -> None:
    """Local-wins preempts the cross-remote duplicate check.

    A job present locally with leftover complete manifests on two remotes (two move
    attempts each crashing in the publish -> index-flip window) is the exact mess
    rebuild is meant to recover from: the local copy is authoritative and the remote
    leftovers are ignorable, so rebuild must succeed rather than abort on a spurious
    duplicate.
    """
    repo = repository_with_two_remotes
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    _publish_job_to_remote(repo.remotes["archive"], job_dir, job.id)
    _publish_job_to_remote(repo.remotes["archive2"], job_dir, job.id)

    (repo.path / "index.sqlite").unlink()
    repo.rebuild_index()

    assert len(repo._index) == 1
    assert repo._index.get_location(job.id) == "local"


def test_rebuild_rejects_duplicate_job_id_across_remotes(
    repository_with_two_remotes: Repository,
) -> None:
    repo = repository_with_two_remotes
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    _publish_job_to_remote(repo.remotes["archive"], job_dir, job.id)
    _publish_job_to_remote(repo.remotes["archive2"], job_dir, job.id)
    # Delete only the local copy so this is purely a cross-remote duplicate. `remove`
    # would now sweep both remotes too, which would defeat the setup.
    repo._atomic_remove_local(job.id)

    before = (repo.path / "index.sqlite").read_bytes()
    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    message = str(excinfo.value)
    assert "archive" in message and "archive2" in message
    _assert_index_unchanged(repo, before)


def test_rebuild_discards_stale_index_new(repository: Repository) -> None:
    repo = repository
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None

    stale = repo.path / "index.sqlite.new"
    stale.write_bytes(b"garbage-not-a-database")

    repo.rebuild_index()

    assert not stale.exists()
    assert len(repo._index) == 1
    assert repo._index.get_location(job.id) == "local"


# --- remove: gone-everywhere protocol ---


def _job_object_keys(job_id: str, prefix: str) -> list:
    """Returns the keys under a job's prefix on the shared moto bucket."""
    client = boto3.client("s3", region_name="us-east-1")
    response = client.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefix}{job_id}/")
    return [obj["Key"] for obj in response.get("Contents", [])]


def test_remove_deletes_remote_job(repository_with_remote: Repository) -> None:
    """Removing a job archived on a remote deletes its manifest, archive, both
    sidecars, and any staging object; previously remove refused a remote job."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = repo.remotes["archive"]
    assert remote.exists(job.id)

    # Passing the (now stale) Job handle exercises the Union[Job, str] Job branch.
    repo.remove(job)

    assert not remote.exists(job.id)
    assert _job_object_keys(job.id, PREFIX) == []
    with pytest.raises(KeyError):
        repo._index.get_location(job.id)


def test_remove_local_job_sweeps_all_remotes_and_artifacts(
    repository_with_two_remotes: Repository,
) -> None:
    """Removing a local job sweeps EVERY configured remote (not just the indexed one)
    and every local recovery artifact for the job: the receipt, staging dirs, and
    trash dirs."""
    repo = repository_with_two_remotes
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    # A leftover, complete remote copy on a *second* remote (e.g. after a
    # fetch-interruption -> rebuild -> move-to-another-remote sequence).
    _publish_job_to_remote(repo.remotes["archive2"], job_dir, job.id)
    assert repo.remotes["archive2"].exists(job.id)

    # Plant the three recovery-artifact kinds for this job.
    fetch_dir = repo.path / ".fetch"
    trash_dir = repo.path / ".trash"
    fetch_dir.mkdir(exist_ok=True)
    trash_dir.mkdir(exist_ok=True)
    receipt = fetch_dir / f"{job.id}.receipt.json"
    receipt.write_bytes(b"{}")
    # A crash between the atomic receipt write and its os.replace leaves this temp
    # sibling behind; the sweep must catch it too.
    receipt_tmp = fetch_dir / f"{job.id}.receipt.json.tmp-deadbeef"
    receipt_tmp.write_bytes(b"{}")
    staging = fetch_dir / f"{job.id}-deadbeef"
    staging.mkdir()
    (staging / "payload").write_text("stale")
    trashed = trash_dir / f"{job.id}-cafebabe"
    trashed.mkdir()
    (trashed / "payload").write_text("stale")

    repo.remove(job.id)

    assert not job_dir.exists()
    assert not repo.remotes["archive"].exists(job.id)
    assert not repo.remotes["archive2"].exists(job.id)
    assert not receipt.exists()
    assert not receipt_tmp.exists()
    assert not staging.exists()
    assert not trashed.exists()
    with pytest.raises(KeyError):
        repo._index.get_location(job.id)


def test_remove_retry_from_raw_row_tolerates_missing_local_dir(
    repository_with_remote: Repository,
) -> None:
    """A retry from the raw job id completes even when the index row still says
    ``local`` but ``jobs/<id>`` is already gone (the post-step-2 crash state). It must
    not route through Job materialization, which raises the I7 corruption error."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id

    # Fabricate the post-step-2 state: local files gone, index still says local.
    repo._atomic_remove_local(job.id)
    assert not job_dir.exists()
    assert repo._index.get_location(job.id) == "local"

    # Also fabricate the trash entry an interrupted _force_rmtree of THIS remove would
    # leave, so the retry finishes its cleanup too.
    trash_dir = repo.path / ".trash"
    trash_dir.mkdir(exist_ok=True)
    leftover = trash_dir / f"{job.id}-interrupted"
    leftover.mkdir()
    (leftover / "payload").write_text("stale")

    repo.remove(job.id)

    with pytest.raises(KeyError):
        repo._index.get_location(job.id)
    assert not leftover.exists()


def test_remove_refuses_with_dependent_and_deletes_nothing(
    repository_with_remote: Repository,
) -> None:
    """A live dependent makes remove refuse with the dependents-list ValueError before
    any deletion: the referenced-by guard runs ahead of the remote sweep."""
    repo = repository_with_remote
    base = repo.commit(get_dummy_job("base"))
    assert base.id is not None
    job_dir = repo.path / "jobs" / base.id

    # A leftover remote copy that WOULD be swept if the guard did not run first.
    _publish_job_to_remote(repo.remotes["archive"], job_dir, base.id)
    assert repo.remotes["archive"].exists(base.id)

    dependent = get_dummy_job("base")
    dependency = JobDependency("destination", base.id)
    dependent._dependencies = [dependency]
    dependent._config["dependencies"] = [dependency.to_config()]
    dependent = repo.commit(dependent)
    assert dependent.id is not None

    with pytest.raises(ValueError, match="depend on it") as exc_info:
        repo.remove(base.id)
    assert dependent.id in str(exc_info.value)

    # Nothing deleted: local dir, index row, and remote copy all intact.
    assert job_dir.exists()
    assert repo._index.get_location(base.id) == "local"
    assert repo.remotes["archive"].exists(base.id)


def test_remove_aborts_on_remote_delete_error_and_keeps_index_row(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A per-object failure reported in delete_objects' Errors array raises RemoteError
    and aborts remove before the index row is touched (step 3 is never reached)."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    remote = repo.remotes["archive"]
    assert isinstance(remote, S3Remote)

    def failing_delete_objects(**kwargs: object) -> dict:
        return {"Errors": [{"Key": "k", "Code": "AccessDenied", "Message": "no"}]}

    monkeypatch.setattr(remote._client, "delete_objects", failing_delete_objects)

    with pytest.raises(RemoteError):
        repo.remove(job.id)

    assert repo._index.get_location(job.id) == "local"
    assert (repo.path / "jobs" / job.id).exists()


def test_remove_retry_completes_remote_only_job_after_mid_sweep_failure(
    repository_with_remote: Repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A remote-only, un-indexed job whose first delete_job crashed after deleting the
    manifest (archive/sidecars/staging still present) must still be removable on retry.
    The existence probe must detect ANY object under the prefix, not just the manifest,
    so the retry completes the sweep instead of refusing "not contained" — which would
    orphan the leftover objects forever."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    job_dir = repo.path / "jobs" / job.id
    remote = repo.remotes["archive"]
    assert isinstance(remote, S3Remote)

    # Make the job remote-only and un-indexed: publish all objects, then drop both the
    # local copy and the index row.
    _publish_job_to_remote(remote, job_dir, job.id)
    repo._atomic_remove_local(job.id)
    repo._index.remove_by_id(job.id)
    with pytest.raises(KeyError):
        repo._index.get_location(job.id)

    # Fail the SECOND _delete of the first delete_job — the manifest (first _delete) is
    # already gone by then, leaving archive/sidecars/staging behind.
    real_delete = remote._delete
    calls = {"n": 0}

    def flaky_delete(keys: list) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RemoteError("simulated per-object failure on the second batch")
        real_delete(keys)

    monkeypatch.setattr(remote, "_delete", flaky_delete)

    with pytest.raises(RemoteError):
        repo.remove(job.id)

    # Manifest gone (exists() is False now), but objects remain under the prefix.
    assert not remote.exists(job.id)
    assert _job_object_keys(job.id, PREFIX) != []

    # Retry must complete the sweep rather than refuse "not contained".
    repo.remove(job.id)
    assert _job_object_keys(job.id, PREFIX) == []


# --- job-id validation: path-traversal / injection defense ---


# A canonical UUID that is never committed, for "valid shape but absent" cases.
_ABSENT_UUID = "00000000-0000-4000-8000-00000000dead"

# Non-canonical / hostile ids that must be rejected before any mutation.
_MALICIOUS_IDS = [
    "../../victim",              # relative traversal
    "/etc/passwd",              # absolute path
    "a/b",                      # path separator
    "job-*",                    # glob metacharacter
    "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",  # uppercase, non-canonical
    "{00000000-0000-4000-8000-000000000000}",  # braces
    "urn:uuid:00000000-0000-4000-8000-000000000000",  # urn form
    "not-a-uuid",
    "",
]


def test_reproduction_remove_path_traversal_leaves_victim_untouched(
    repository: Repository, tmp_path: Path
) -> None:
    """Reproduction 1 (was RED): ``remove('../../victim')`` must not escape the repo.

    Before the fix, ``jobs/../../victim`` resolved outside the repository, got renamed
    to an escaped trash target, and was deleted. It must now be refused before any
    filesystem mutation, leaving the outside directory fully intact.
    """
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "important.txt").write_text("precious")

    with pytest.raises(ValueError):
        repository.remove("../../victim")

    assert victim.exists()
    assert (victim / "important.txt").read_text() == "precious"


@pytest.mark.parametrize("bad_id", _MALICIOUS_IDS)
def test_remove_rejects_non_canonical_id(
    repository: Repository, tmp_path: Path, bad_id: str
) -> None:
    with pytest.raises(ValueError):
        repository.remove(bad_id)


def test_remove_valid_but_absent_uuid_reports_not_contained(
    repository: Repository,
) -> None:
    """A canonical-but-absent id passes validation and reaches the normal
    'not contained' refusal (ValueError), not the id-validation refusal."""
    with pytest.raises(ValueError, match="not contained"):
        repository.remove(_ABSENT_UUID)


@pytest.mark.parametrize("bad_id", _MALICIOUS_IDS)
def test_get_job_by_id_rejects_non_canonical_id(
    repository: Repository, bad_id: str
) -> None:
    with pytest.raises(ValueError):
        repository.get_job_by_id(bad_id)
    with pytest.raises(ValueError):
        repository[bad_id]


@pytest.mark.parametrize("bad_id", _MALICIOUS_IDS)
def test_move_rejects_non_canonical_id_before_mutation(
    repository_with_remote: Repository, bad_id: str
) -> None:
    repo = repository_with_remote
    with pytest.raises(ValueError):
        repo.move(bad_id, "archive")
    # Nothing was written under the remote prefix for the hostile id.
    assert _all_bucket_keys() == set()


@pytest.mark.parametrize("bad_id", _MALICIOUS_IDS)
def test_fetch_rejects_non_canonical_id_before_mutation(
    repository_with_remote: Repository, bad_id: str
) -> None:
    repo = repository_with_remote
    with pytest.raises(ValueError):
        repo.fetch(bad_id)


def _all_bucket_keys() -> set:
    client = boto3.client("s3", region_name="us-east-1")
    return {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    }


def _put_object(key: str, body: bytes) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=BUCKET, Key=key, Body=body
    )


def _publish_representation_raw(job_dir: Path, job_id: str, prefix: str) -> None:
    """Uploads a complete, self-consistent representation of ``job_dir`` under
    ``{prefix}{job_id}/`` via the RAW S3 client, bypassing the guarded S3Remote
    transport. This is the only way to plant a hostile ``job_id`` on the bucket once
    the transport validates ids, so it is exactly how an attacker's object store —
    or a corrupted one — could present a traversal-shaped key to discovery/rebuild."""
    files = [c.relative_to(job_dir) for c in job_dir.rglob("*") if c.is_file()]
    member_paths = [p for p in files if p.as_posix() not in r3.manifest.SIDECAR_PATHS]
    temp_dir = Path(tempfile.mkdtemp(prefix="r3-test-rawpublish-"))
    try:
        archive_path = temp_dir / "data.tar.zst"
        result = r3.archive.create_archive(job_dir, member_paths, archive_path)
        entries = list(result.entries)
        sidecar_bytes = {}
        for name in r3.manifest.SIDECAR_PATHS:
            data = (job_dir / name).read_bytes()
            sidecar_bytes[name] = data
            entries.append(
                r3.manifest.FileEntry(name, len(data), r3.utils.hash_bytes(data))
            )
        manifest = r3.manifest.build_manifest(
            job_id, entries, result.sha256, result.size
        )
        base = f"{prefix}{job_id}/"
        _put_object(base + "data.tar.zst", archive_path.read_bytes())
        for name, data in sidecar_bytes.items():
            _put_object(base + name, data)
        _put_object(base + "manifest.json", r3.manifest.dumps(manifest))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_reproduction_remote_escape_rebuild_fails_closed(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """Reproduction 2 (was RED): a traversal-shaped remote manifest key must not be
    indexed, and rebuild must FAIL CLOSED, leaving the previous index intact.

    A COMPLETE, self-consistent 4-object representation is published directly under
    ``{prefix}../../escaped/`` (raw client, so the manifest's own ``job_id`` is also
    ``../../escaped``). Before the fix this reconstructed cleanly and indexed an
    escaped job (which ``fetch`` would then write outside the repo). Rebuild must now
    enumerate the manifest key, see a non-canonical job segment, and abort with a
    diagnostic naming it — without replacing the previous index.
    """
    repo = repository_with_remote
    # A legitimate committed job so there is a real index to protect, and a real
    # job_dir to build the (otherwise valid) escaped representation from.
    good = repo.commit(get_dummy_job("base"))
    assert good.id is not None
    before = (repo.path / "index.sqlite").read_bytes()

    _publish_representation_raw(repo.path / "jobs" / good.id, "../../escaped", PREFIX)

    outside = (repo.path / ".." / ".." / "escaped").resolve()

    with pytest.raises(RuntimeError) as excinfo:
        repo.rebuild_index()
    assert "escaped" in str(excinfo.value)

    _assert_index_unchanged(repo, before)
    assert not outside.exists()


def test_remote_check_reports_malformed_manifest_key(
    repository_with_remote: Repository,
) -> None:
    """``remote check`` must surface a traversal/nested manifest key as a finding,
    never silently omit it."""
    repo = repository_with_remote
    _put_object(f"{PREFIX}../../escaped/manifest.json", b"{}")
    _put_object(f"{PREFIX}nested/uuid/manifest.json", b"{}")

    report = repo.remote_check()

    assert report.has_findings
    reported = {finding.job_id for finding in report.malformed_keys}
    assert "../../escaped" in reported
    assert "nested/uuid" in reported


def test_fetch_refuses_escaped_id_even_if_indexed(
    repository_with_remote: Repository, tmp_path: Path
) -> None:
    """Defense in depth: even if a traversal id is force-inserted into the index,
    ``fetch`` refuses via id validation before writing anything outside the repo."""
    import r3.index

    repo = repository_with_remote
    with r3.index.Transaction(repo.path / "index.sqlite") as tx:
        tx.execute(
            "INSERT INTO jobs (id, timestamp, metadata, location)"
            " VALUES (?, ?, ?, ?)",
            ("../../escaped", datetime.now().isoformat(), "{}", "archive"),
        )

    outside = (repo.path / ".." / ".." / "escaped").resolve()
    with pytest.raises(ValueError):
        repo.fetch("../../escaped")
    assert not outside.exists()


def test_valid_uuid_round_trip_unaffected(
    repository_with_remote: Repository,
) -> None:
    """A canonical-UUID job survives the full commit/move/fetch/remove round-trip."""
    repo = repository_with_remote
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    assert r3.utils.is_valid_job_id(job.id)

    repo.move(job.id, "archive")
    assert repo._index.get_location(job.id) == "archive"

    repo.fetch(job.id)
    assert (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "local"

    repo.remove(job.id)
    with pytest.raises(KeyError):
        repo._index.get_location(job.id)
