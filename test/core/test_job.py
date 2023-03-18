"""Unit tests for ``r3.Job``."""

from pathlib import Path

import yaml

import r3

DATA_PATH = Path(__file__).parent.parent / "data"


def test_job_constructor_loads_metadata_file():
    """Unit test for ``r3.Job``."""
    job_path = DATA_PATH / "jobs" / "base"

    with open(job_path / "metadata.yaml", "r") as metadata_file:
        job_metadata = yaml.safe_load(metadata_file)

    job = r3.Job(job_path)
    assert job.metadata == job_metadata

    job = r3.Job(str(job_path))
    assert job.metadata == job_metadata
