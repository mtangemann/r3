"""Unit tests for ``r3.Job``."""

from pathlib import Path

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

import r3

DATA_PATH = Path(__file__).parent.parent / "data"


def test_job_metadata_returns_metadata_yaml_contents():
    job_path = DATA_PATH / "jobs" / "base"

    with open(job_path / "metadata.yaml", "r") as metadata_file:
        job_metadata = yaml.safe_load(metadata_file)

    job = r3.Job(job_path)
    assert job.metadata == job_metadata

    job = r3.Job(str(job_path))
    assert job.metadata == job_metadata


def test_job_metadata_returns_empty_dict_when_metadata_yaml_does_not_exist():
    job_path = DATA_PATH / "jobs" / "no_metadata"

    job = r3.Job(job_path)
    assert job.metadata == {}

    job = r3.Job(str(job_path))
    assert job.metadata == {}


def test_job_hash_does_not_depend_on_metadata(fs: FakeFilesystem) -> None:
    """Unit test for ``r3.Job.hash()``."""
    job_path = DATA_PATH / "jobs" / "base"

    fs.add_real_directory(job_path, read_only=False)
    original_hash = r3.Job(job_path).hash()

    with open(job_path / "metadata.yaml", "w") as metadata_file:
        yaml.dump({"tags": ["changed"]}, metadata_file)

    assert r3.Job(job_path).hash() == original_hash

    fs.remove(job_path / "metadata.yaml")  # type: ignore
    assert r3.Job(job_path).hash() == original_hash
