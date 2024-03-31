"""Unit tests for `r3.index`."""

import datetime
from pathlib import Path

import pytest

from r3.index import Index
from r3.job import Job, JobDependency
from r3.storage import Storage

DATA_PATH = Path(__file__).parent / "data"


# REVIEW: This should be offered centrally.
def get_dummy_job(name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    return Job(path)


@pytest.fixture
def storage(tmp_path) -> Storage:
    return Storage.init(tmp_path / "repository")


@pytest.fixture
def storage_with_jobs(tmp_path) -> Storage:
    storage = Storage.init(tmp_path / "repository")

    job = get_dummy_job("base")
    job.metadata["tags"] = ["test"]
    job.timestamp = datetime.datetime(2021, 1, 1, 0, 0, 0)
    storage.add(job)

    job.metadata["tags"] = ["test", "test-again"]
    job.timestamp = datetime.datetime(2021, 1, 2, 0, 0, 0)
    committed_job = storage.add(job)

    job._config["dependencies"] = [
        JobDependency("previous_job", committed_job).to_config()
    ]
    job.metadata["tags"] = ["test", "test-latest"]
    job.timestamp = datetime.datetime(2021, 1, 3, 0, 0, 0)
    storage.add(job)

    return storage


def test_index_defaults_to_empty(storage: Storage):
    index = Index(storage)
    assert len(index) == 0


def test_index_add_raises_if_job_not_in_storage(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    with pytest.raises(ValueError):
        index.add(job)


def test_index_add_adds_job(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert len(index) == 1
    assert job in index


def test_index_rebuild(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert len(index) == 1
    assert job in index
    index.rebuild()
    assert len(index) == 1
    assert job in index


def test_index_remove(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    assert len(index) == 3

    job = next(iter(storage_with_jobs.jobs()))
    assert job in index

    index.remove(job)
    assert len(index) == 2
    assert job not in index


def test_index_find_requires_all_tags(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    assert len(index.find(["test"])) == 3
    assert len(index.find(["test", "test-again"])) == 1
    assert len(index.find(["test", "test-missing"])) == 0
    assert len(index.find(["test-missing"])) == 0
    assert len(index.find(["test", "test-again", "test-missing"])) == 0
    assert len(index.find([])) == 3


def test_index_find_latest_returns_only_latest_job(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    result = index.find(["test"], latest=True)
    assert len(result) == 1
    assert "test-latest" in result[0].metadata["tags"]


def test_index_find_dependents(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)

    job = index.find(["test-again"], latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 1
    assert "test-latest" in next(iter(dependents)).metadata["tags"]

    job = index.find(["test-latest"], latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 0
