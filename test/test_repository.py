"""Unit tests for ``r3.Repository``."""

import filecmp
import os
import stat
from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

import r3

DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def repository(fs: FakeFilesystem) -> r3.Repository:
    return r3.Repository.init("/test/repository")


def get_dummy_job(fs: FakeFilesystem, name: str) -> r3.Job:
    path = DATA_PATH / "jobs" / name
    fs.add_real_directory(path, read_only=True)
    return r3.Job(path)


def test_init_fails_if_path_exists(fs: FakeFilesystem) -> None:
    path = "/rest/repository"
    fs.create_dir(path)

    with pytest.raises(FileExistsError):
        r3.Repository.init(path)


def test_init_creates_directories(fs: FakeFilesystem) -> None:
    path = Path("/test/repository")
    r3.Repository.init(path)

    assert path.exists()
    assert (path / "git").exists()
    assert (path / "jobs").exists()


def test_init_creates_config_file_with_version(fs: FakeFilesystem) -> None:
    path = Path("/test/repository")
    r3.Repository.init(path)

    assert (path / "r3.yaml").exists()

    with open(path / "r3.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    assert "version" in config


def test_commit_creates_job_folder(
    fs: FakeFilesystem, repository: r3.Repository
) -> None:
    """Unit test for ``r3.Repository.commit``.

    When adding a job, a directory should be created in ``$REPOSITORY_ROOT/jobs``.
    """
    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 0

    job = get_dummy_job(fs, "base")
    repository.commit(job)

    job_paths = list((repository.path / "jobs").iterdir())
    assert len(job_paths) == 1
    assert job_paths[0].is_dir()


def test_commit_returns_the_updated_job(
    fs: FakeFilesystem, repository: r3.Repository
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
    fs: FakeFilesystem, repository: r3.Repository
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
    fs: FakeFilesystem, repository: r3.Repository
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
