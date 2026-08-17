"""Unit tests for the R3 CLI (init/commit/checkout/edit/remove, move, fetch, remote)."""

from pathlib import Path
from typing import Generator, List

import boto3
import pytest
import yaml
from click.testing import CliRunner
from moto import mock_aws
from pytest_mock.plugin import MockerFixture

from r3.cli import cli
from r3.job import Job, JobDependency
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"

BUCKET = "test-cli-bucket"
PREFIX = "r3/jobs/"

# Canonical UUIDs replacing former shorthand remote ids (ORPHAN,
# DEBRIS): S3Remote validates the id before building an object key.
ORPHAN = "0deada11-0000-4000-8000-00000000abcd"
DEBRIS = "0deb5111-0000-4000-8000-00000000beef"


@pytest.fixture
def repository(tmp_path: Path) -> Repository:
    return Repository.init(tmp_path / "repository")


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


def test_cli_remote_check_clean(repository_with_remote: Repository) -> None:
    """A consistent repository reports no issues and exits 0."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job = repo.commit(job)
    assert job.id is not None
    repo.move(job.id, "archive")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["remote", "check", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert "No issues" in result.output


def test_cli_remote_check_reports_and_exits_nonzero(
    repository_with_remote: Repository,
) -> None:
    """A drifted repository lists the finding and exits non-zero (script-friendly)."""
    repo = repository_with_remote
    # A complete manifest with no index row -> resurrection-risk orphan.
    repo.remotes["archive"].publish_manifest(ORPHAN, b"{}")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["remote", "check", "--repository", str(repo.path)],
    )
    assert result.exit_code != 0
    assert ORPHAN in result.output


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
    """Add then remove a remote whose bucket is empty; removal is clean.

    ``remote remove`` now probes the bucket to refuse orphaning live jobs, so this
    runs under moto with an empty bucket (nothing under the prefix => clean removal).
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        repo = Repository.init(tmp_path / "repository")

        runner = CliRunner()
        # Add
        result = runner.invoke(
            cli,
            [
                "remote", "add", "archive",
                "--type", "s3",
                "--bucket", BUCKET,
                "--prefix", PREFIX,
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


def test_cli_remote_add_warns_when_config_has_comments(tmp_path: Path) -> None:
    """`remote add` warns (but still succeeds) when r3.yaml contains comments."""
    repo = Repository.init(tmp_path / "repository")

    config_path = repo.path / "r3.yaml"
    with open(config_path) as f:
        config_text = f.read()
    with open(config_path, "w") as f:
        f.write("# hand-written note about this repository\n" + config_text)

    result = CliRunner().invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "not preserved" in result.output
    assert "Added remote 'archive'" in result.output


def test_cli_remote_add_does_not_warn_without_comments(tmp_path: Path) -> None:
    """`remote add` does not warn when r3.yaml has no comments."""
    repo = Repository.init(tmp_path / "repository")

    result = CliRunner().invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "not preserved" not in result.output


def test_cli_remote_add_unknown_type_leaves_config_unchanged(tmp_path: Path) -> None:
    """An unknown --type is rejected by validation and never lands in r3.yaml."""
    repo = Repository.init(tmp_path / "repository")

    result = CliRunner().invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "bogus",
            "--bucket", "my-bucket",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)

    with open(repo.path / "r3.yaml") as f:
        config = yaml.safe_load(f)
    assert "archive" not in config.get("remotes", {})


def test_cli_remote_add_s3_without_bucket_leaves_config_unchanged(
    tmp_path: Path,
) -> None:
    """An s3 remote without a bucket is rejected and never lands in r3.yaml."""
    repo = Repository.init(tmp_path / "repository")

    result = CliRunner().invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)

    with open(repo.path / "r3.yaml") as f:
        config = yaml.safe_load(f)
    assert "archive" not in config.get("remotes", {})


def test_cli_remote_add_ceph_flags_round_trip(tmp_path: Path) -> None:
    """The CEPH flags persist into r3.yaml and parse back via from_config."""
    from r3.remote import Remote

    repo = Repository.init(tmp_path / "repository")

    result = CliRunner().invoke(
        cli,
        [
            "remote", "add", "archive",
            "--type", "s3",
            "--bucket", "my-bucket",
            "--prefix", "my-prefix/",
            "--addressing-style", "path",
            "--request-checksum-calculation", "when_required",
            "--response-checksum-validation", "when_required",
            "--repository", str(repo.path),
        ],
    )
    assert result.exit_code == 0, result.output

    with open(repo.path / "r3.yaml") as f:
        config = yaml.safe_load(f)

    remote_config = config["remotes"]["archive"]
    assert remote_config["addressing_style"] == "path"
    assert remote_config["request_checksum_calculation"] == "when_required"
    assert remote_config["response_checksum_validation"] == "when_required"

    # And the persisted config parses back into a usable remote.
    remote = Remote.from_config(remote_config)
    assert remote.addressing_style == "path"  # type: ignore[attr-defined]
    assert remote.request_checksum_calculation == "when_required"  # type: ignore[attr-defined]
    assert remote.response_checksum_validation == "when_required"  # type: ignore[attr-defined]


def test_cli_remote_remove_refuses_with_complete_manifest(
    repository_with_remote: Repository,
) -> None:
    """A complete manifest under the prefix blocks removal, naming the job."""
    repo = repository_with_remote
    repo.remotes["archive"].publish_manifest(ORPHAN, b"{}")

    config_before = (repo.path / "r3.yaml").read_text()

    result = CliRunner().invoke(
        cli,
        ["remote", "remove", "archive", "--repository", str(repo.path)],
    )
    assert result.exit_code != 0
    assert ORPHAN in result.output
    # The config must be untouched when removal is refused.
    assert (repo.path / "r3.yaml").read_text() == config_before


def test_cli_remote_remove_refuses_debris_without_force(
    repository_with_remote: Repository,
) -> None:
    """A manifestless (debris) object blocks removal unless --force is given."""
    repo = repository_with_remote
    repo.remotes["archive"].put_sidecar(DEBRIS, "metadata.yaml", b"data")

    config_before = (repo.path / "r3.yaml").read_text()

    result = CliRunner().invoke(
        cli,
        ["remote", "remove", "archive", "--repository", str(repo.path)],
    )
    assert result.exit_code != 0
    assert DEBRIS in result.output
    assert (repo.path / "r3.yaml").read_text() == config_before


def test_cli_remote_remove_force_removes_debris(
    repository_with_remote: Repository,
) -> None:
    """With --force, debris is reported as unmanaged and the config entry drops."""
    repo = repository_with_remote
    repo.remotes["archive"].put_sidecar(DEBRIS, "metadata.yaml", b"data")

    result = CliRunner().invoke(
        cli,
        ["remote", "remove", "archive", "--force", "--repository", str(repo.path)],
    )
    assert result.exit_code == 0, result.output
    assert DEBRIS in result.output
    assert "Removed remote 'archive'" in result.output

    with open(repo.path / "r3.yaml") as f:
        config = yaml.safe_load(f)
    assert "archive" not in config.get("remotes", {})


def test_cli_remote_remove_reports_corrupt_config(tmp_path: Path) -> None:
    """A hand-corrupted stored remote config gives a clean error, not a traceback."""
    repo = Repository.init(tmp_path / "repository")

    config_path = repo.path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    # Manually broken remote: an s3 remote with no bucket (from_config -> ValueError).
    config["remotes"] = {"archive": {"type": "s3"}}
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    result = CliRunner().invoke(
        cli,
        ["remote", "remove", "archive", "--repository", str(repo.path)],
    )
    assert result.exit_code != 0
    # Reported as a ClickException, not raised as an unhandled traceback.
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Error:" in result.output


def test_cli_edit_refuses_remote_job(repository_with_remote: Repository) -> None:
    """Editing a moved (remote) job is refused with no stray file created."""
    repo = repository_with_remote
    job = get_dummy_job("base")
    job = repo.commit(job)
    assert job.id is not None
    repo.move(job.id, "archive")

    job_path = repo.path / "jobs" / job.id
    assert not job_path.exists()

    result = CliRunner().invoke(
        cli,
        ["edit", job.id, "--repository", str(repo.path)],
    )
    assert result.exit_code != 0
    assert "fetch" in result.output
    # No stray metadata file (or job directory) must be created at the deleted path.
    assert not (job_path / "metadata.yaml").exists()
    assert not job_path.exists()


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


def commit_dependent_job(repository: Repository, job: Job, destination: str) -> Job:
    """Commits a job depending on the given job and returns it."""
    assert job.id is not None

    dependent_job = get_dummy_job("base")
    dependency = JobDependency(destination, job.id)
    dependent_job._dependencies = [dependency]
    dependent_job._config["dependencies"] = [dependency.to_config()]

    return repository.commit(dependent_job)


def test_remove_removes_job(repository: Repository) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    result = CliRunner().invoke(
        cli, ["remove", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code == 0
    assert job not in repository


def test_remove_fails_if_other_jobs_depend_on_job(repository: Repository) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None
    dependent_job = commit_dependent_job(repository, job, "destination")
    assert dependent_job.id is not None

    result = CliRunner().invoke(
        cli, ["remove", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert any(dependent_job.id in line for line in lines)
    # The dependent must be reported with an explanation, not on its own. The exact
    # wording is asserted in ``test_repository.py``.
    assert len(lines) > 1
    assert job in repository


def test_remove_fails_if_job_does_not_exist(repository: Repository) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"

    result = CliRunner().invoke(
        cli, ["remove", job_id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output


def test_edit_updates_metadata_and_index(
    repository: Repository, mocker: MockerFixture
) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None

    def edit_metadata(filename: str) -> None:
        with open(filename, "r") as metadata_file:
            metadata = yaml.safe_load(metadata_file)
        metadata["tags"].append("edited")
        with open(filename, "w") as metadata_file:
            yaml.dump(metadata, metadata_file)

    mocker.patch("r3.cli.click.edit", side_effect=edit_metadata)

    result = CliRunner().invoke(
        cli, ["edit", job.id, "--repository", str(repository.path)]
    )

    assert result.exit_code == 0

    # Read the repository afresh, so that the index is queried rather than any state
    # cached by the repository instance used above.
    jobs = Repository(repository.path).find({"tags": {"$all": ["edited"]}})
    assert [found_job.id for found_job in jobs] == [job.id]


def test_edit_fails_if_job_does_not_exist(repository: Repository) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"

    result = CliRunner().invoke(
        cli, ["edit", job_id, "--repository", str(repository.path)]
    )

    assert result.exit_code != 0
    # The error must be reported, not raised as an unhandled exception.
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output


def test_checkout_checks_out_job(repository: Repository, tmp_path: Path) -> None:
    job = repository.commit(get_dummy_job("base"))
    assert job.id is not None
    target_path = tmp_path / "checkout"

    result = CliRunner().invoke(
        cli,
        ["checkout", job.id, str(target_path), "--repository", str(repository.path)],
    )

    assert result.exit_code == 0
    assert (target_path / "run.py").is_file()


def test_checkout_fails_if_job_does_not_exist(
    repository: Repository, tmp_path: Path
) -> None:
    job_id = "00000000-0000-0000-0000-000000000000"
    target_path = tmp_path / "checkout"

    result = CliRunner().invoke(
        cli,
        ["checkout", job_id, str(target_path), "--repository", str(repository.path)],
    )

    assert result.exit_code != 0
    # The error must be reported, not raised as an unhandled exception.
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert job_id in result.output
    # ``str(KeyError(...))`` is a repr and would wrap the whole message in quotes.
    assert "Error: '" not in result.output
    assert not target_path.exists()


def commands_taking_a_repository(tmp_path: Path) -> List[List[str]]:
    """Returns a minimal invocation of every command that takes a repository.

    The path arguments must exist, so that click's own validation passes and the command
    body is actually reached.
    """
    job_path = tmp_path / "job"
    job_path.mkdir(exist_ok=True)
    job_id = "00000000-0000-0000-0000-000000000000"

    return [
        ["find"],
        ["rebuild-index"],
        ["remove", job_id],
        ["edit", job_id],
        ["checkout", job_id, str(tmp_path / "target")],
        ["commit", str(job_path)],
    ]


def test_commands_report_a_missing_repository(tmp_path: Path) -> None:
    # Looped rather than parametrized, since the commands need `tmp_path`.
    for command in commands_taking_a_repository(tmp_path):
        result = CliRunner().invoke(cli, command, env={"R3_REPOSITORY": None})

        assert result.exit_code != 0, command
        # Reported, not raised as an unhandled exception.
        assert (
            result.exception is None or isinstance(result.exception, SystemExit)
        ), command
        # Whichever way the user meant to pass it, name it.
        assert "--repository" in result.output, command
        assert "R3_REPOSITORY" in result.output, command


def test_commands_report_an_empty_repository_env_var() -> None:
    result = CliRunner().invoke(cli, ["find"], env={"R3_REPOSITORY": ""})

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "R3_REPOSITORY" in result.output


def test_commands_report_a_path_that_is_not_a_repository(tmp_path: Path) -> None:
    not_a_repository = tmp_path / "not-a-repository"
    not_a_repository.mkdir()

    result = CliRunner().invoke(cli, ["find", "--repository", str(not_a_repository)])

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert str(not_a_repository) in result.output


def test_commands_report_an_outdated_repository_version(repository: Repository) -> None:
    with open(repository.path / "r3.yaml", "w") as config_file:
        yaml.dump({"version": "1.0.0-beta.1"}, config_file)

    result = CliRunner().invoke(cli, ["find", "--repository", str(repository.path)])

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "1.0.0-beta.1" in result.output


def test_help_mentions_the_repository_env_var() -> None:
    result = CliRunner().invoke(cli, ["find", "--help"])

    assert result.exit_code == 0
    assert "R3_REPOSITORY" in result.output
