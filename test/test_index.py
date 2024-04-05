"""Unit tests for `r3.index`."""

import datetime
from pathlib import Path
from typing import Any, Dict, List

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
    job.metadata["tags"] = ["test", "test-first"]
    job.metadata["dataset"] = "mnist"
    job.metadata["model"] = "cnn"
    job.metadata["image_size"] = 28
    job.timestamp = datetime.datetime(2021, 1, 1, 0, 0, 0)
    storage.add(job)

    job.metadata["tags"] = ["test", "test-again"]
    job.metadata["dataset"] = "mnist"
    job.metadata["model"] = "cnn"
    job.metadata["image_size"] = 32
    job.timestamp = datetime.datetime(2021, 1, 2, 0, 0, 0)
    committed_job = storage.add(job)

    job._config["dependencies"] = [
        JobDependency("previous_job", committed_job).to_config()
    ]
    job.metadata["tags"] = ["test", "test-latest"]
    job.metadata["dataset"] = "mnist"
    job.metadata["model"] = "resnet"
    job.metadata["image_size"] = 32
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


def test_index_find(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)

    job1 = index.find({"tags": "test-first"}, latest=True)[0]
    job2 = index.find({"tags": "test-again"}, latest=True)[0]
    job3 = index.find({"tags": "test-latest"}, latest=True)[0]

    query: Dict[str, Any] = {"dataset": "mnist"}
    results = index.find(query)
    assert len(results) == 3

    query = {"model": "cnn"}
    results = index.find(query)
    assert len(results) == 2
    assert set(result.id for result in results) == {job1.id, job2.id}

    query = {"$not": {"model": "cnn"}}
    results = index.find(query)
    assert len(results) == 1
    assert results[0].id == job3.id

    query = {"$or": [{"model": "cnn"}, {"model": "resnet"}]}
    results = index.find(query)
    assert len(results) == 3

    query = {"$and": [{"model": "cnn"}, {"image_size": 32}]}
    results = index.find(query)
    assert len(results) == 1
    assert results[0].id == job2.id

    query = {"$or": [{"model": "cnn"}, {"image_size": {"$gt": 28}}]}
    results = index.find(query)
    assert len(results) == 3

    query = {"$and": [{"model": "cnn"}, {"image_size": {"$ne": 32}}]}
    results = index.find(query)
    assert len(results) == 1
    assert results[0].id == job1.id

    query = {"model": {"$in": ["cnn", "transformer"]}}
    results = index.find(query)
    assert len(results) == 2
    assert set(result.id for result in results) == {job1.id, job2.id}


@pytest.mark.parametrize(
    "tags,expected",
    [
        (["test"], 3),
        (["test-again"], 1),
        (["test-missing"], 0),
        (["test", "test-again"], 1),
        (["test", "test-missing"], 0),
        (["test", "test-again", "test-missing"], 0),
        ([], 3),
    ]
)
def test_index_find_all_tags(
    storage_with_jobs: Storage, tags: List[str], expected: int
) -> None:
    index = Index(storage_with_jobs)
    query = {"tags": {"$all": tags}}
    results = index.find(query)
    assert len(results) == expected


def test_index_find_latest_returns_only_latest_job(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    query = {"tags": {"$all": ["test"]}}
    result = index.find(query, latest=True)
    assert len(result) == 1
    assert "test-latest" in result[0].metadata["tags"]


def test_index_find_uses_cached_metadata(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    job = index.find({"tags": "test"}, latest=True)[0]
    assert job.uses_cached_metadata()


def test_index_find_dependents(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)

    job = index.find({"tags": {"$all": ["test-again"]}}, latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 1
    assert "test-latest" in next(iter(dependents)).metadata["tags"]

    job = index.find({"tags": {"$all": ["test-latest"]}}, latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 0


def test_index_find_dependents_uses_cached_metadata(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    job = index.find({"tags": {"$all": ["test-again"]}}, latest=True)[0]
    dependents = index.find_dependents(job)
    assert all(dependent.uses_cached_metadata() for dependent in dependents)
