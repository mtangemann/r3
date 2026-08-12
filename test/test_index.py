"""Unit tests for `r3.index`."""

import datetime
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from r3.index import Index, Transaction
from r3.job import FilesUnavailableError, Job, JobDependency
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


def test_index_find_location_filter_is_not_sql_injectable(storage_with_jobs: Storage):
    """A crafted location value is a literal, not SQL that bypasses the filter.

    With the value interpolated as a raw string literal, ``missing' OR 1=1 --``
    breaks out of the quotes and the ``OR 1=1`` matches every row. Bound as a
    parameter, it is treated as a location value that matches nothing.
    """
    index = Index(storage_with_jobs)
    # The payload would match all rows if the filter were bypassed.
    assert len(index.find({})) == 3
    results = index.find({}, location="missing' OR 1=1 --")
    assert len(results) == 0


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


def test_index_find_returns_remote_projection(storage: Storage):
    """find() returns a remote job as a metadata-only projection (design §8)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    # Simulate the move: set location to remote, store a file list, drop local files.
    index.set_location(job.id, "archive")
    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("run.py")]
    index.set_file_list(job.id, paths)
    storage.remove(job)

    results = index.find({"tags": "test"})
    assert len(results) == 1
    found_job = results[0]
    assert found_job.id == job.id
    assert isinstance(found_job.metadata, dict)
    # Files are not on the projection; they raise until fetched.
    with pytest.raises(FilesUnavailableError):
        _ = found_job.files
    # The cached file list lives in the index, not on the Job.
    assert index.get_file_list(job.id) == paths


def test_index_get_returns_remote_projection(storage: Storage):
    """Index.get() returns a remote job as a metadata-only projection."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.set_location(job.id, "archive")
    storage.remove(job)

    found = index.get(job.id)
    assert found.id == job.id
    with pytest.raises(FilesUnavailableError):
        _ = found.files


def test_index_get_unknown_id_raises_keyerror(storage: Storage):
    index = Index(storage)
    with pytest.raises(KeyError):
        index.get("nonexistent-id")


def test_index_find_remote_job_without_file_list(
    storage: Storage,
):
    """A remote job with no cached file list is still a projection; files raise."""
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
    assert index.get_file_list(job.id) is None
    with pytest.raises(FilesUnavailableError):
        _ = found_job.files


def test_index_rebuild_local_only_without_remotes(storage_with_jobs: Storage):
    """With no configured remotes, rebuild reconstructs from local storage alone and
    leaves no stale ``index.sqlite.new`` behind."""
    index = Index(storage_with_jobs)
    index.rebuild()
    assert len(index) == 3
    assert not (storage_with_jobs.root / "index.sqlite.new").exists()


def test_index_rebuild_does_not_read_remotes_for_local_jobs(storage: Storage):
    """A remote is only consulted for its listing; local jobs never trigger a read.

    With one job that is local, rebuild must enumerate the remote's job ids (to find
    remote-only jobs) but must not fetch any per-job artifact from it.
    """
    import unittest.mock as mock

    remote = mock.MagicMock()
    remote.list_job_ids.return_value = iter([])

    # Construction auto-rebuilds against empty storage; reset before the real check.
    index = Index(storage, {"archive": remote})
    job = get_dummy_job("base")
    job = storage.add(job)
    remote.reset_mock()
    remote.list_job_ids.return_value = iter([])
    index.rebuild()

    assert len(index) == 1
    remote.list_job_ids.assert_called_once()
    remote.get_manifest.assert_not_called()
    remote.get_sidecar.assert_not_called()
    remote.archive_size.assert_not_called()


def test_index_get_local_missing_directory_raises_corruption(storage: Storage):
    """A local row whose jobs/<id> directory is gone surfaces as a clear corruption
    error rather than a bare FileNotFoundError (F-09)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    job_id = job.id

    # Drop the local directory while the index still records the row as local.
    storage.remove(job)

    with pytest.raises(RuntimeError) as exc_info:
        index.get(job_id)
    message = str(exc_info.value)
    assert job_id in message
    assert "corrupt" in message.lower()
    assert "director" in message.lower()


def test_index_get_local_missing_r3yaml_raises_corruption(storage: Storage):
    """A local row whose directory exists but lacks r3.yaml surfaces as a clear
    corruption error instead of a confusing later failure (F-09)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    job_id = job.id

    job_dir = storage.root / "jobs" / job_id
    os.chmod(job_dir, 0o755)  # committed jobs are read-only; allow the unlink
    (job_dir / "r3.yaml").unlink()

    with pytest.raises(RuntimeError) as exc_info:
        index.get(job_id)
    message = str(exc_info.value)
    assert job_id in message
    assert "corrupt" in message.lower()
    assert "r3.yaml" in message


def test_index_find_local_missing_directory_raises_corruption(storage: Storage):
    """find() raises the clear corruption error for a local row with no directory,
    rather than silently skipping or failing confusingly later (F-09)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    job_id = job.id

    storage.remove(job)

    with pytest.raises(RuntimeError) as exc_info:
        index.find({})
    message = str(exc_info.value)
    assert job_id in message
    assert "corrupt" in message.lower()
    assert "director" in message.lower()


def test_index_find_local_missing_r3yaml_raises_corruption(storage: Storage):
    """find() raises the clear corruption error for a local row whose directory lacks
    r3.yaml (F-09)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    job_id = job.id

    job_dir = storage.root / "jobs" / job_id
    os.chmod(job_dir, 0o755)
    (job_dir / "r3.yaml").unlink()

    with pytest.raises(RuntimeError) as exc_info:
        index.find({})
    message = str(exc_info.value)
    assert job_id in message
    assert "corrupt" in message.lower()
    assert "r3.yaml" in message


def test_index_find_does_not_deserialize_files_column(storage: Storage):
    """find() selects only id/timestamp/metadata/location and loads the file list
    lazily, so non-JSON garbage in the files column must not make find raise (F-10)."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    # Write non-deserializable JSON directly into the files column.
    with Transaction(storage.root / "index.sqlite") as cursor:
        cursor.execute(
            "UPDATE jobs SET files = ? WHERE id = ?",
            ("not valid json {[", job.id),
        )

    results = index.find({})
    assert len(results) == 1
    assert results[0].id == job.id


def test_transaction_rolls_back_on_exception(storage: Storage):
    """A Transaction that raises mid-block must not persist its writes (F-08)."""
    index = Index(storage)
    path = storage.root / "index.sqlite"
    del index

    class _BoomError(Exception):
        pass

    # A write followed by a raise inside the block: the exception must
    # propagate and the write must not be committed.
    with pytest.raises(_BoomError):
        with Transaction(path) as cursor:
            cursor.execute(
                "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)",
                ("rollback-sentinel", "2021-01-01T00:00:00", "{}"),
            )
            raise _BoomError

    # A fresh connection must not see the rolled-back row.
    with Transaction(path) as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM jobs WHERE id = ?", ("rollback-sentinel",)
        )
        assert cursor.fetchone()[0] == 0
