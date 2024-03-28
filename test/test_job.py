"""Unit tests for ``r3.Job``."""

import datetime
import uuid
from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

import r3

DATA_PATH = Path(__file__).parent / "data"


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


def test_job_save_metadata_updates_metadata_yaml(fs: FakeFilesystem) -> None:
    job_path = DATA_PATH / "jobs" / "base"

    fs.add_real_directory(job_path, read_only=False)
    job = r3.Job(job_path)

    job.metadata = {"tags": ["changed"]}
    job.save_metadata()

    with open(job_path / "metadata.yaml", "r") as metadata_file:
        assert yaml.safe_load(metadata_file) == job.metadata

    job.metadata["tags"].append("added")
    job.save_metadata()

    with open(job_path / "metadata.yaml", "r") as metadata_file:
        assert yaml.safe_load(metadata_file) == job.metadata


def test_job_save_metadata_creates_metadata_yaml(fs: FakeFilesystem) -> None:
    job_path = DATA_PATH / "jobs" / "no_metadata"

    fs.add_real_directory(job_path, read_only=False)
    job = r3.Job(job_path)

    job.metadata = {"tags": ["changed"]}
    job.save_metadata()

    with open(job_path / "metadata.yaml", "r") as metadata_file:
        assert yaml.safe_load(metadata_file) == job.metadata


def test_job_datetime_returns_datetime_from_metadata_if_id_is_not_none() -> None:
    job_path = DATA_PATH / "jobs" / "base"
    job = r3.Job(job_path, str(uuid.uuid4()))
    job._config["timestamp"] = "2024-02-11 23:29:10"

    assert job.timestamp == datetime.datetime(2024, 2, 11, 23, 29, 10)


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


def test_depedency_from_config() -> None:
    config = {
        "job": str(uuid.uuid4()),
        "source": "output",
        "destination": "data",
        "query": "#query",
    }

    dependency = r3.Dependency.from_config(config)
    assert isinstance(dependency, r3.JobDependency)

    config = {
        "query": "#query",
        "source": "output",
        "destination": "data",
    }

    dependency = r3.Dependency.from_config(config)
    assert isinstance(dependency, r3.QueryDependency)

    config = {
        "query_all": "#query",
        "destination": "data",
    }

    dependency = r3.Dependency.from_config(config)
    assert isinstance(dependency, r3.QueryAllDependency)

    config = {
        "repository": "https://github.com/user/model.git",
        "commit": "2ef52fde13642372a262fd9618159fe72835c813",
        "destination": "model",
    }

    dependency = r3.Dependency.from_config(config)
    assert isinstance(dependency, r3.GitDependency)


def test_job_dependency_from_config_defaults() -> None:
    config = {
        "job": str(uuid.uuid4()),
        "destination": "data",
    }

    dependency = r3.JobDependency.from_config(config)

    assert dependency.job == config["job"]
    assert dependency.destination == Path(config["destination"])
    assert dependency.source == Path(".")
    assert dependency.query is None
    assert dependency.query_all is None


def test_job_dependency_from_config() -> None:
    config = {
        "job": str(uuid.uuid4()),
        "source": "output",
        "destination": "data",
        "query": "#query",
    }

    dependency = r3.JobDependency.from_config(config)

    assert dependency.job == config["job"]
    assert dependency.source == Path(config["source"])
    assert dependency.destination == Path(config["destination"])
    assert dependency.query == config["query"]
    assert dependency.query_all is None

    config = {
        "job": str(uuid.uuid4()),
        "source": ".",
        "destination": "data",
        "query_all": "#query",
    }

    dependency = r3.JobDependency.from_config(config)

    assert dependency.job == config["job"]
    assert dependency.source == Path(config["source"])
    assert dependency.destination == Path(config["destination"])
    assert dependency.query is None
    assert dependency.query_all == config["query_all"]


def test_job_dependency_to_config():
    dependency = r3.JobDependency(str(uuid.uuid4()), Path("data"))

    assert dependency.to_config() == {
        "job": dependency.job,
        "source": ".",
        "destination": str(dependency.destination),
    }

    dependency = r3.JobDependency(
        job=str(uuid.uuid4()),
        source=Path("output"),
        destination=Path("data"),
        query="#query",
    )

    assert dependency.to_config() == {
        "job": dependency.job,
        "source": str(dependency.source),
        "destination": str(dependency.destination),
        "query": dependency.query,
    }

    dependency = r3.JobDependency(
        job=str(uuid.uuid4()),
        destination=Path("data"),
        query_all="#query",
    )

    assert dependency.to_config() == {
        "job": dependency.job,
        "source": ".",
        "destination": str(dependency.destination),
        "query_all": dependency.query_all,
    }


def test_job_dependency_hash_does_not_depend_on_destination() -> None:
    dependency = r3.JobDependency(Path("data"), str(uuid.uuid4()))

    original_hash = dependency.hash()

    dependency.destination = Path("changed")
    assert dependency.hash() == original_hash


def test_job_dependency_hash_does_not_depend_on_query() -> None:
    dependency = r3.JobDependency(
        job=str(uuid.uuid4()),
        source=Path("output"),
        destination=Path("data"),
        query="#query",
    )

    original_hash = dependency.hash()

    dependency.query = "#changed"
    assert dependency.hash() == original_hash

    dependency.query = None
    assert dependency.hash() == original_hash

    dependency.query_all = "#query"
    assert dependency.hash() == original_hash

    dependency.query_all = "#changed"
    assert dependency.hash() == original_hash

    dependency.query_all = None
    assert dependency.hash() == original_hash


def test_query_dependency_from_config() -> None:
    config = {
        "query": "#query",
        "destination": "data",
    }

    dependency = r3.QueryDependency.from_config(config)

    assert dependency.query == config["query"]
    assert dependency.source == Path(".")
    assert dependency.destination == Path(config["destination"])

    config = {
        "query": "#query",
        "source": "output",
        "destination": "data",
    }

    dependency = r3.QueryDependency.from_config(config)

    assert dependency.query == config["query"]
    assert dependency.source == Path(config["source"])
    assert dependency.destination == Path(config["destination"])


def test_query_dependency_to_config():
    dependency = r3.QueryDependency("#query", Path("data"))

    assert dependency.to_config() == {
        "query": dependency.query,
        "source": ".",
        "destination": str(dependency.destination),
    }

    dependency = r3.QueryDependency("#query", Path("data"), Path("output"))

    assert dependency.to_config() == {
        "query": dependency.query,
        "source": str(dependency.source),
        "destination": str(dependency.destination),
    }


def test_query_dependency_hash_raises_error():
    dependency = r3.QueryDependency("#query", "data")

    with pytest.raises(ValueError):
        dependency.hash()


def test_query_all_dependency_from_config() -> None:
    config = {
        "query_all": "#query",
        "destination": "data",
    }

    dependency = r3.QueryAllDependency.from_config(config)

    assert dependency.query_all == config["query_all"]
    assert dependency.destination == Path(config["destination"])


def test_query_all_dependency_to_config():
    dependency = r3.QueryAllDependency("#query", Path("data"))

    assert dependency.to_config() == {
        "query_all": dependency.query_all,
        "destination": str(dependency.destination),
    }


def test_query_all_dependency_hash_raises_error():
    dependency = r3.QueryAllDependency("#query", "data")

    with pytest.raises(ValueError):
        dependency.hash()


def test_git_dependency_from_config() -> None:
    config = {
        "repository": "https://github.com/user/model.git",
        "commit": "2ef52fde13642372a262fd9618159fe72835c813",
        "destination": "model",
    }

    dependency = r3.GitDependency.from_config(config)

    assert dependency.repository == config["repository"]
    assert dependency.commit == config["commit"]
    assert dependency.destination == Path(config["destination"])

    config = {
        "repository": "https://github.com/user/model.git",
        "commit": "2ef52fde13642372a262fd9618159fe72835c813",
        "source": "src/model",
        "destination": "model",
    }

    dependency = r3.GitDependency.from_config(config)

    assert dependency.repository == config["repository"]
    assert dependency.commit == config["commit"]
    assert dependency.source == Path(config["source"])
    assert dependency.destination == Path(config["destination"])


def test_git_dependency_to_config():
    dependency = r3.GitDependency(
        "https://github.com/user/model.git",
        "2ef52fde13642372a262fd9618159fe72835c813",
        Path("model"),
    )

    assert dependency.to_config() == {
        "repository": dependency.repository,
        "commit": dependency.commit,
        "source": ".",
        "destination": str(dependency.destination),
    }

    dependency = r3.GitDependency(
        "https://github.com/user/model.git",
        "2ef52fde13642372a262fd9618159fe72835c813",
        Path("model"),
        Path("src/model"),
    )

    assert dependency.to_config() == {
        "repository": dependency.repository,
        "commit": dependency.commit,
        "source": str(dependency.source),
        "destination": str(dependency.destination),
    }


def test_git_dependency_hash_does_not_depend_on_destination() -> None:
    dependency = r3.GitDependency(
        Path("model"),
        "https://github.com/user/model.git",
        "2ef52fde13642372a262fd9618159fe72835c813",
    )

    original_hash = dependency.hash()

    dependency.destination = Path("changed")
    assert dependency.hash() == original_hash


def test_git_dependency_is_resolved_if_commit_is_not_none() -> None:
    dependency = r3.GitDependency(
        Path("model"),
        "https://github.com/user/model.git",
        "2ef52fde13642372a262fd9618159fe72835c813",
    )
    assert dependency.is_resolved()

    dependency = r3.GitDependency(
        Path("model"),
        "https://github.com/user/model.git",
    )
    assert not dependency.is_resolved()
