"""Unit tests for `r3.index`."""

import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

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


def test_index_get(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert len(index) == 1
    assert job in index
    assert job.id is not None
    retrieved_job = index.get(job.id)
    assert retrieved_job.id == job.id


def test_index_update(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job.metadata["updated"] = False
    job = storage.add(job)
    index.add(job)
    assert len(index) == 1
    assert job in index
    assert job.id is not None
    retrieved_job = index.get(job.id)
    assert retrieved_job.metadata["updated"] is False

    job.metadata["updated"] = True
    with open(job.path / "metadata.yaml", "w") as file:
        yaml.dump(job.metadata, file)
    index.update(job)
    assert len(index) == 1
    assert job in index
    assert job.id is not None
    retrieved_job = index.get(job.id)
    assert retrieved_job.metadata["updated"] is True


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


def test_index_find_uses_cached_timestamp(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    job = index.find({"tags": "test"}, latest=True)[0]
    assert job.uses_cached_timestamp()
    assert isinstance(job.timestamp, datetime.datetime)


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


def test_index_find_dependents_uses_cached_timestamp(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    job = index.find({"tags": {"$all": ["test-again"]}}, latest=True)[0]
    dependents = index.find_dependents(job)
    assert all(dependent.uses_cached_timestamp() for dependent in dependents)
    assert all(
        isinstance(dependent.timestamp, datetime.datetime) for dependent in dependents
    )


def test_index_find_dependents_uses_cached_metadata(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()

    job = index.find({"tags": {"$all": ["test-again"]}}, latest=True)[0]
    dependents = index.find_dependents(job)
    assert all(dependent.uses_cached_metadata() for dependent in dependents)


def test_index_add_defaults_location_to_local(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    assert index.get_location(job.id) == "local"


def test_index_set_location(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    index.set_location(job.id, "archive")
    assert index.get_location(job.id) == "archive"
    index.set_location(job.id, "local")
    assert index.get_location(job.id) == "local"


def test_index_rebuild_defaults_location_to_local(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    index.rebuild()
    assert index.get_location(job.id) == "local"


def test_index_find_with_location_filter(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    all_jobs = index.find({})
    assert len(all_jobs) == 3
    job = all_jobs[0]
    assert job.id is not None
    index.set_location(job.id, "archive")
    local_jobs = index.find({}, location="local")
    assert len(local_jobs) == 2
    archived_jobs = index.find({}, location="archive")
    assert len(archived_jobs) == 1
    assert archived_jobs[0].id == job.id
    all_jobs_again = index.find({})
    assert len(all_jobs_again) == 3


def test_index_rebuild_creates_files_column(storage: Storage):
    """The rebuilt schema must include the files column."""
    index = Index(storage)
    index.rebuild()
    import sqlite3
    conn = sqlite3.connect(str(storage.root / "index.sqlite"))
    cursor = conn.execute("PRAGMA table_info(jobs)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()
    assert "files" in columns


def test_index_set_and_get_file_list(storage: Storage):
    """File list round-trips through SQLite as a JSON array of POSIX strings."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("output/result.pt")]
    index.set_file_list(job.id, paths)
    result = index.get_file_list(job.id)
    assert result == paths


def test_index_get_file_list_returns_none_when_unset(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    assert index.get_file_list(job.id) is None


def test_index_find_returns_remote_job_with_cached_file_paths(storage: Storage):
    """When find() returns a remote job, its cached_file_paths come from the index."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    # Simulate the move: set location to remote and store a file list
    index.set_location(job.id, "archive")
    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("run.py")]
    index.set_file_list(job.id, paths)

    # Force the FileNotFoundError fallback by removing the local files
    storage.remove(job)

    results = index.find({"tags": "test"})
    assert len(results) == 1
    found_job = results[0]
    assert set(found_job.files.keys()) == set(paths)
    assert all(v is None for v in found_job.files.values())


def test_index_get_returns_remote_job_with_cached_file_paths(
    storage: Storage,
):
    """Index.get() also applies the FileNotFoundError fallback."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.set_location(job.id, "archive")
    paths = [Path("r3.yaml"), Path("run.py")]
    index.set_file_list(job.id, paths)

    storage.remove(job)

    found = index.get(job.id)
    assert set(found.files.keys()) == set(paths)


def test_index_get_unknown_id_raises_keyerror(storage: Storage):
    index = Index(storage)
    with pytest.raises(KeyError):
        index.get("nonexistent-id")


def test_index_find_remote_job_with_no_cached_files_returns_none(
    storage: Storage,
):
    """When files IS NULL for a remote job, cached_file_paths stays None."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.set_location(job.id, "archive")
    # Note: no set_file_list call — files column stays NULL.
    storage.remove(job)

    results = index.find({"tags": "test"})
    assert len(results) == 1
    found_job = results[0]
    # cached_file_paths is None, so accessing files should raise (not silently
    # succeed with a wrong dict).
    assert found_job._cached_file_paths is None
