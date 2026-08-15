"""R3 command line interface."""
# ruff: noqa: T201

import sys
from pathlib import Path
from typing import Iterable, Optional

import click
import yaml

import r3


@click.group(
    help=(
        "All functionality is provided by the R3 commands listed below. Use `r3 "
        "<command> --help` for more information about the individual commands."
    )
)
@click.version_option(r3.__version__, message="%(version)s")
def cli() -> None:
    pass


def _get_repository(repository_path: Optional[Path]) -> r3.Repository:
    """Returns the repository at the given path, reporting problems to the user."""
    if repository_path is None:
        raise click.UsageError(
            "No repository given. Use --repository or set the R3_REPOSITORY "
            "environment variable."
        )

    try:
        return r3.Repository(repository_path)
    except (FileNotFoundError, NotADirectoryError, ValueError) as error:
        raise click.ClickException(str(error)) from error


def _get_job(repository: r3.Repository, job_id: str) -> r3.Job:
    """Returns the job with the given ID, reporting a missing job to the user."""
    try:
        return repository.get_job_by_id(job_id)
    except KeyError as error:
        # `str` of a KeyError is the repr of its argument, which would add quotes.
        raise click.ClickException(str(error.args[0])) from error


@cli.command()
@click.argument("path", type=click.Path(file_okay=False, exists=False, path_type=Path))
def init(path: Path):
    """Creates an empty R3 repository at PATH.

    The given PATH must not exist yet.
    """

    try:
        r3.Repository.init(path)
    except FileExistsError as error:
        print(f"Error: {error}")
        sys.exit(1)


@cli.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def commit(path: Path, repository_path: Optional[Path]) -> None:
    """Adds the job at PATH to the repository.

    This command resolves all dependencies of the job and copies the job files to the R3
    repository. If the job was committed successfully this will output the location of
    the job in the R3 repository. For example:

    \b
    ```
    $ ls my/job
    run.py
    r3.yaml
    $ r3 commit my/job
    4b2146f3-5594-4f05-ae13-2e053ef7bfda
    ```
    """
    repository = _get_repository(repository_path)
    job = r3.Job(path)
    job = repository.commit(job)
    print(job.id)


@cli.command()
@click.argument("job_id", type=str)
@click.argument("target_path", type=click.Path(exists=False, path_type=Path))
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def checkout(job_id: str, target_path: Path, repository_path: Optional[Path]) -> None:
    """Checks out the job with JOB_ID to TARGET_PATH.

    This copies all job files from JOB_PATH in the R3 repository to the TARGET_PATH.
    The output folder and all dependencies will by symlinked. Checking out a job is
    required for executing a job, since the dependencies are not explicitely stored in
    the R3 respository. For example:

    \b
    ```
    $ r3 checkout 4b2146f3-5594-4f05-ae13-2e053ef7bfda workdir
    $ ls workdir
    run.py
    data.csv -> /repository/jobs/6b189b64-8c7c-4609-b089-f69c7b3e0548/output/data.csv
    output/ -> /repository/jobs/4b2146f3-5594-4f05-ae13-2e053ef7bfda/output
    ```
    """
    repository = _get_repository(repository_path)
    job = _get_job(repository, job_id)
    repository.checkout(job, target_path)


@cli.command()
@click.argument("job_id", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def remove(job_id: str, repository_path: Optional[Path]) -> None:
    """Removes the job with JOB_ID from the R3 repository.

    If any other job in the R3 repository depends on the job, removing it will fail.
    """
    repository = _get_repository(repository_path)

    # Pass the raw id, not a materialized Job: `remove` must stay usable in the retry
    # state where the index row still says local but jobs/<id> is already gone (which
    # `_get_job` would reject as corruption). An unknown id is reported by `remove`
    # itself, via a ValueError naming the job.
    try:
        repository.remove(job_id)
    except ValueError as error:
        raise click.ClickException(str(error)) from error


@cli.command()
@click.option(
    "--tag", "-t", "tags", multiple=True, type=str,
    help=(
        "Only list jobs that contain the given tag. If this option is specified "
        "multiple times, only jobs with all of the given tags will be listed."
    )
)
@click.option(
    "--latest/--all", default=False,
    help="Whether to list all job matching the given conditions or only the latest job."
)
@click.option("--long/--short", "-l", default=False,
    help=(
        "Whether to list only the job IDs (--short) or also additional job "
        "information (--long)."
    )
)
@click.option(
    "--location", type=str, default=None,
    help="Filter by job location.",
)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def find(
    tags: Iterable[str],
    latest: bool,
    long: bool,
    location: Optional[str],
    repository_path: Optional[Path],
) -> None:
    """Searches the R3 repository for jobs matching the given conditions."""
    repository = _get_repository(repository_path)
    query = {"tags": {"$all": tags}}
    for job in repository.find(query, latest, location=location):
        if long:
            assert job.timestamp is not None
            datetime = job.timestamp.strftime(r"%Y-%m-%d %H:%M:%S")
            tags = " ".join(f"#{tag}" for tag in job.metadata.get("tags", []))
            print(f"{job.id} | {datetime} | {tags}")
        else:
            print(job.id)


@cli.command()
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def rebuild_index(repository_path: Optional[Path]):
    """Rebuild the search index.

    The index is used when querying for jobs. All R3 commands properly update the index.
    When job metadata is modified manually, however, the index needs to be rebuilt in
    order for the changes to take effect.
    """
    repository = _get_repository(repository_path)
    repository.rebuild_index()


@cli.command()
@click.argument(
    "job_id", type=str
)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def edit(job_id: str, repository_path: Optional[Path]) -> None:
    """Edit a jobs metadata."""
    repository = _get_repository(repository_path)
    job = _get_job(repository, job_id)

    # Let user edit the metadata file of the job
    metadata_file_path = job.path / "metadata.yaml"
    click.edit(filename=str(metadata_file_path))

    # Reload metadata from disk before indexing: `job` may have been constructed
    # with cached metadata (get_job_by_id routes through Index.get), so updating the
    # index directly from it would write back the pre-edit metadata.
    job.reload_metadata()

    # Update job in search index (SQLite DB)
    repository._index.update(job)


@cli.command()
@click.argument("job_id", type=str)
@click.argument("remote_name", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
@click.option(
    "--dry-run", is_flag=True,
    help="Show what would be moved without doing it.",
)
def move(job_id: str, remote_name: str, repository_path: Path, dry_run: bool) -> None:
    """Move a job to a remote storage location."""
    repository = r3.Repository(repository_path)
    try:
        if dry_run:
            job = repository.get_job_by_id(job_id)
            dependents = repository.find_dependents(job)
            print(f"Would move job {job_id} to remote '{remote_name}'.")
            if dependents:
                print("The following jobs depend on this job:")
                for dep in dependents:
                    print(f"  - {dep.id}")
            return
        dependents = repository.move(job_id, remote_name)
        print(f"Moved job {job_id} to remote '{remote_name}'.")
        if dependents:
            print("Warning: the following jobs depend on this job:")
            for dep in dependents:
                print(f"  - {dep.id}")
    except (ValueError, KeyError) as error:
        print(f"Error: {error}")
        sys.exit(1)


@cli.command()
@click.argument("job_id", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def fetch(job_id: str, repository_path: Path) -> None:
    """Fetch a job from remote storage back to local."""
    repository = r3.Repository(repository_path)
    try:
        repository.fetch(job_id)
        print(f"Fetched job {job_id} to local storage.")
    except (ValueError, KeyError) as error:
        print(f"Error: {error}")
        sys.exit(1)


@cli.group()
def remote() -> None:
    """Manage remote storage backends."""
    pass


@remote.command("add")
@click.argument("name", type=str)
@click.option(
    "--type", "remote_type", type=str, required=True,
    help="Remote type (e.g. s3).",
)
@click.option("--bucket", type=str, default=None, help="S3 bucket name.")
@click.option("--prefix", type=str, default=None, help="S3 key prefix.")
@click.option("--profile", type=str, default=None, help="AWS profile name.")
@click.option("--endpoint-url", type=str, default=None, help="S3 endpoint URL.")
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_add(
    name: str,
    remote_type: str,
    bucket: Optional[str],
    prefix: Optional[str],
    profile: Optional[str],
    endpoint_url: Optional[str],
    repository_path: Path,
) -> None:
    """Add a remote storage backend."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.get("remotes", {})
    if name in remotes:
        print(f"Error: Remote '{name}' already exists.")
        sys.exit(1)

    remote_config: dict = {"type": remote_type}
    if bucket is not None:
        remote_config["bucket"] = bucket
    if prefix is not None:
        remote_config["prefix"] = prefix
    if profile is not None:
        remote_config["profile"] = profile
    if endpoint_url is not None:
        remote_config["endpoint_url"] = endpoint_url

    remotes[name] = remote_config
    config["remotes"] = remotes

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    print(f"Added remote '{name}' (type: {remote_type}).")


@remote.command("list")
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_list(repository_path: Path) -> None:
    """List configured remote storage backends."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.get("remotes", {})
    if not remotes:
        print("No remotes configured.")
        return

    for name, remote_config in remotes.items():
        print(f"{name} ({remote_config.get('type', 'unknown')})")


@remote.command("remove")
@click.argument("name", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_remove(name: str, repository_path: Path) -> None:
    """Remove a remote storage backend."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.get("remotes", {})
    if name not in remotes:
        print(f"Error: Remote '{name}' does not exist.")
        sys.exit(1)

    del remotes[name]
    config["remotes"] = remotes

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    print(f"Removed remote '{name}'.")


@remote.command("check")
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
    show_envvar=True,
)
def remote_check(repository_path: Optional[Path]) -> None:
    """Reconcile each remote against the index and report drift (read-only).

    This mutates nothing. It reports resurrection-risk manifests, manifestless job
    prefixes, leftover staging manifests, broken remote rows, and incomplete
    multipart uploads. Exits non-zero if any issue is found, so it is script-friendly.
    """
    repository = _get_repository(repository_path)
    report = repository.remote_check()

    if not report.has_findings:
        print("No issues found.")
        return

    def _emit(title: str, findings: list) -> None:
        if not findings:
            return
        print(f"{title}:")
        for finding in findings:
            print(f"  - {finding.job_id} [{finding.remote}]: {finding.detail}")

    _emit(
        "Resurrection-risk orphans / location disagreements",
        report.resurrection_risks,
    )
    _emit("Manifestless job prefixes", report.manifestless_prefixes)
    _emit("Leftover staging manifests", report.staging_manifests)
    _emit("Broken remote rows", report.broken_rows)

    if report.incomplete_multipart_uploads:
        print("Incomplete multipart uploads:")
        for upload in report.incomplete_multipart_uploads:
            print(
                f"  - {upload.key} [{upload.remote}] (upload {upload.upload_id})"
            )

    sys.exit(1)


if __name__ == "__main__":
    cli()
