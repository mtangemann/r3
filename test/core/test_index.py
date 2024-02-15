"""Unit tests for `r3.index`."""

from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from r3.index import Index
from r3.job import Job, JobDependency
from r3.storage import Storage

DATA_PATH = Path(__file__).parent.parent / "data"


# REVIEW: This should be offered centrally.
def get_dummy_job(fs: FakeFilesystem, name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    fs.add_real_directory(path, read_only=True)
    return Job(path)


@pytest.fixture
def storage(fs: FakeFilesystem) -> Storage:
    fs.create_dir("/repository")
    return Storage.init("/repository")


@pytest.fixture
def storage_with_jobs(fs: FakeFilesystem) -> Storage:
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job.metadata["tags"] = ["test"]
    job.metadata["committed_at"] = "2021-01-01 00:00:00"
    storage.add(job)

    job.metadata["tags"] = ["test", "test-again"]
    job.metadata["committed_at"] = "2021-01-02 00:00:00"
    committed_job = storage.add(job)

    job._config["dependencies"] = [
        JobDependency(committed_job, "previous_job").to_config()
    ]
    job.metadata["tags"] = ["test", "test-latest"]
    job.metadata["committed_at"] = "2021-01-03 00:00:00"
    storage.add(job)

    return storage


def test_index_defaults_to_empty(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    assert len(index._entries) == 0


def test_index_add_raises_if_job_not_in_storage(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    job = get_dummy_job(fs, "base")
    with pytest.raises(ValueError):
        index.add(job)


def test_index_add_adds_job(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    index.add(job)
    assert len(index._entries) == 1
    assert job.id in index._entries


def test_index_add_saves_index_to_disk(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    index.add(job, save=False)
    assert not (storage.root / "index.yaml").exists()
    index.add(job)
    assert (storage.root / "index.yaml").exists()


def test_index_rebuild(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    index.add(job)
    index._entries = dict()
    assert len(index._entries) == 0
    index.rebuild()
    assert len(index._entries) == 1
    assert job.id in index._entries


def test_index_save(fs: FakeFilesystem, storage: Storage):
    index = Index(storage)
    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    index.add(job, save=False)
    assert not (storage.root / "index.yaml").exists()
    index.save()
    assert (storage.root / "index.yaml").exists()


def test_index_remove(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()
    assert len(index._entries) == 3

    job = next(iter(storage_with_jobs.jobs()))
    assert job.id in index._entries

    index.remove(job)
    assert len(index._entries) == 2
    assert job.id not in index._entries


def test_index_find_requires_all_tags(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)
    index.rebuild()
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
    index.rebuild()

    job = index.find(["test-again"], latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 1
    assert "test-latest" in dependents[0].metadata["tags"]

    job = index.find(["test-latest"], latest=True)[0]
    dependents = index.find_dependents(job)
    assert len(dependents) == 0
