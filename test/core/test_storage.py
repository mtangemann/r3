"""Unit tests for `r3.storage`."""

import filecmp
from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

from r3.job import Job
from r3.storage import Storage

DATA_PATH = Path(__file__).parent.parent / "data"


# REVIEW: This should be offered centrally.
def get_dummy_job(fs: FakeFilesystem, name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    fs.add_real_directory(path, read_only=True)
    return Job(path)


def test_storage_constructor_raises_if_root_does_not_exist(fs: FakeFilesystem):
    with pytest.raises(FileNotFoundError):
        Storage("/does/not/exist")


def test_storage_constructor_raises_if_root_is_not_a_directory(fs: FakeFilesystem):
    fs.create_file("/not/a/directory")
    with pytest.raises(NotADirectoryError):
        Storage("/not/a/directory")


def test_storage_init_creates_directories(fs: FakeFilesystem):
    fs.create_dir("/repository")
    Storage.init("/repository")
    assert Path("/repository/jobs").is_dir()
    assert Path("/repository/git").is_dir()


def test_storage_add_updates_job_path(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.parent == storage.root / "jobs"


def test_storage_add_creates_job_folder(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.is_dir()


def test_storage_add_assigns_job_id(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.id is not None
    assert job.path.name == job.id


def test_storage_add_copies_source_files(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    original_job = get_dummy_job(fs, "base")

    # REVIEW: Job should have a method to return all source files.
    source_files = [
        path.relative_to(original_job.path) for path in original_job.path.rglob("*")
        if path.name not in ["r3.yaml", "metadata.yaml"]
    ]

    committed_job = storage.add(original_job)

    for source_file in source_files:
        assert (committed_job.path / source_file).exists()
        assert filecmp.cmp(
            original_job.path / source_file, committed_job.path / source_file
        )


def test_storage_add_saves_metadata(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job.metadata["test"] = "value"

    job = storage.add(job)
    assert job.metadata["test"] == "value"
    
    assert (job.path / "metadata.yaml").exists()
    with open(job.path / "metadata.yaml", "r") as metadata_file:
        metadata = yaml.safe_load(metadata_file)
    assert metadata["test"] == "value"


def test_storage_add_saves_hashes(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert (job.path / "r3.yaml").exists()
    with open(job.path / "r3.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)
    assert "hashes" in config


def test_storage_contains(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    assert original_job not in storage

    committed_job = storage.add(original_job)
    assert committed_job.id is not None
    assert original_job not in storage
    assert committed_job in storage
    assert committed_job.id in storage


def test_storage_get(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)
    assert committed_job.id is not None

    retrieved_job = storage.get(committed_job.id)
    assert retrieved_job.id == committed_job.id
    assert retrieved_job.path == committed_job.path


def test_storage_get_raises_if_job_id_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    with pytest.raises(FileNotFoundError):
        storage.get("non-existent")


def test_storage_get_raises_if_job_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    with pytest.raises(FileNotFoundError):
        storage.get("non-existent")


def test_storage_jobs_returns_all_jobs(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    jobs = list(storage.jobs())
    assert len(jobs) == 0

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)

    jobs = list(storage.jobs())
    assert len(jobs) == 1
    assert jobs[0].id == committed_job.id
    assert jobs[0].path == committed_job.path

    storage.add(original_job)
    jobs = list(storage.jobs())
    assert len(jobs) == 2


def test_storage_remove_deletes_job_folder(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.exists()
    storage.remove(job)
    assert not job.path.exists()


def test_storage_remove_raises_if_job_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    with pytest.raises(FileNotFoundError):
        storage.remove(job)
