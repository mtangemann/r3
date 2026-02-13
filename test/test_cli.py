"""Unit tests for the R3 CLI commands: move, fetch, remote, and find --location."""

from pathlib import Path
from typing import Generator

import boto3
import pytest
import yaml
from click.testing import CliRunner
from moto import mock_aws

from r3.cli import cli
from r3.job import Job
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"

BUCKET = "test-cli-bucket"
PREFIX = "r3/jobs/"


def get_dummy_job(name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    return Job(path)


@pytest.fixture
def repository_with_remote(tmp_path: Path) -> Generator[Repository, None, None]:
    """Creates a repository with an S3 remote named 'archive' backed by moto."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        repo = Repository.init(tmp_path / "repository")
        config_path = repo.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        config["remotes"] = {
            "archive": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}
        }

        with open(config_path, "w") as f:
            yaml.dump(config, f)

        repo = Repository(repo.path)
        yield repo


def test_cli_move(repository_with_remote: Repository) -> None:
    """Commit a job, move it via CLI, verify local files are gone."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job = repo.commit(job)
    assert job.id is not None

    job_path = repo.path / "jobs" / job.id
    assert job_path.exists()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["move", job.id, "archive", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert f"Moved job {job.id}" in result.output

    # Local files should be gone
    assert not job_path.exists()


def test_cli_move_dry_run(repository_with_remote: Repository) -> None:
    """Commit a job, dry-run move via CLI, verify local files still exist."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job = repo.commit(job)
    assert job.id is not None

    job_path = repo.path / "jobs" / job.id
    assert job_path.exists()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["move", job.id, "archive", "--repository", str(repo.path), "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert "Would move job" in result.output

    # Local files should still be there
    assert job_path.exists()


def test_cli_fetch(repository_with_remote: Repository) -> None:
    """Move a job, fetch it via CLI, verify local files are restored."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job = repo.commit(job)
    assert job.id is not None

    repo.move(job.id, "archive")
    job_path = repo.path / "jobs" / job.id
    assert not job_path.exists()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["fetch", job.id, "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert f"Fetched job {job.id}" in result.output

    # Local files should be restored
    assert job_path.exists()


def test_cli_remote_add_and_list(tmp_path: Path) -> None:
    """Add a remote via CLI, list and verify output contains name+type."""
    repo = Repository.init(tmp_path / "repository")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--prefix", "my-prefix/",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Added remote 'archive'" in result.output

    result = runner.invoke(
        cli,
        ["remote", "list", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert "archive" in result.output
    assert "s3" in result.output


def test_cli_remote_remove(tmp_path: Path) -> None:
    """Add then remove a remote, verify it's gone from list."""
    repo = Repository.init(tmp_path / "repository")

    runner = CliRunner()
    # Add
    result = runner.invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output

    # Remove
    result = runner.invoke(
        cli,
        ["remote", "remove", "archive", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert "Removed remote 'archive'" in result.output

    # List should be empty
    result = runner.invoke(
        cli,
        ["remote", "list", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert "archive" not in result.output


def test_cli_remote_add_duplicate(tmp_path: Path) -> None:
    """Adding a remote with an existing name should fail."""
    repo = Repository.init(tmp_path / "repository")

    runner = CliRunner()
    runner.invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--repository", str(repo.path),
        ],
    )

    result = runner.invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "other-bucket",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 1


def test_cli_remote_remove_nonexistent(tmp_path: Path) -> None:
    """Removing a nonexistent remote should fail."""
    repo = Repository.init(tmp_path / "repository")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["remote", "remove", "nonexistent", "--repository", str(repo.path)],
    )
    assert result.exit_code == 1


def test_cli_find_location_filter(repository_with_remote: Repository) -> None:
    """Move a job, find --location archive returns it, --location local doesn't."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job.metadata["tags"] = ["findme"]
    job = repo.commit(job)
    assert job.id is not None

    repo.move(job.id, "archive")

    runner = CliRunner()

    # --location archive should find the job
    result = runner.invoke(
        cli,
        [
            "find", "-t", "findme", "--location", "archive",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert job.id in result.output

    # --location local should NOT find the job
    result = runner.invoke(
        cli,
        ["find", "-t", "findme", "--location", "local", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert job.id not in result.output
