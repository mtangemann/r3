"""Unit tests for ``r3.Job``."""

from pathlib import Path

import yaml

import r3

DATA_PATH = Path(__file__).parent.parent / "data"


def test_empty_job_is_valid():
    """Unit test for ``r3.Job``."""
    empty_job = r3.Job()

    assert empty_job.config == dict()
    assert empty_job.metadata == dict()
    assert empty_job.files == dict()
    assert list(empty_job.dependencies()) == []
    assert isinstance(empty_job.hash(), str)


def test_job_constructor_loads_config_file():
    """Unit test for ``r3.Job``."""
    job_path = DATA_PATH / "jobs" / "base"

    with open(job_path / "r3.yaml", "r") as config_file:
        job_config = yaml.safe_load(config_file)

    job = r3.Job(job_path)
    assert job.config == job_config

    job = r3.Job(str(job_path))
    assert job.config == job_config


def test_job_constructor_loads_metadata_file():
    """Unit test for ``r3.Job``."""
    job_path = DATA_PATH / "jobs" / "base"

    with open(job_path / "r3metadata.yaml", "r") as metadata_file:
        job_metadata = yaml.safe_load(metadata_file)

    job = r3.Job(job_path)
    assert job.metadata == job_metadata

    job = r3.Job(str(job_path))
    assert job.metadata == job_metadata


def test_job_doesnt_need_a_path():
    """Unit test for ``r3.Job``.

    It should be possible to construct a job that has not corresponding directory in
    the filesystem.
    """
    config = {
        "commands": {"done": "true"},
    }

    metadata = {
        "tags": ["virtual"],
    }

    files = {
        Path("ls"): Path("/bin/ls"),
    }

    job = r3.Job(None, config, metadata, files)

    assert job.config == config
    assert job.metadata == metadata
    assert job.files == files
    assert list(job.dependencies()) == []
    assert isinstance(job.hash(), str)


def test_changed_root():
    """Unit test for ``r3.Job``."""
    job_path = DATA_PATH / "jobs" / "changed_root" / "config"

    with open(job_path / "r3.yaml", "r") as config_file:
        job_config = yaml.safe_load(config_file)
        del job_config["commit"]

    with open(job_path / "r3metadata.yaml", "r") as metadata_file:
        job_metadata = yaml.safe_load(metadata_file)

    job = r3.Job(job_path)
    assert job.config == job_config
    assert job.metadata == job_metadata
    assert job.files == {
        Path("run.py"): (job_path.parent / "run.py").absolute(),
    }
