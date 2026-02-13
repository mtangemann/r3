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
)
def commit(path: Path, repository_path: Path) -> None:
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
    /repository/jobs/4b2146f3-5594-4f05-ae13-2e053ef7bfda
    ```
    """
    repository = r3.Repository(repository_path)
    job = r3.Job(path)
    job = repository.commit(job)
    print(job.path)


@cli.command()
@click.argument(
    "job_path", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("target_path", type=click.Path(exists=False, path_type=Path))
def checkout(job_path: Path, target_path) -> None:
    """Checks out the job at JOB_PATH to TARGET_PATH.

    This copies all job files from JOB_PATH in the R3 repository to the TARGET_PATH.
    The output folder and all dependencies will by symlinked. Checking out a job is
    required for executing a job, since the dependencies are not explicitely stored in
    the R3 respository. For example:

    \b
    ```
    $ r3 checkout /repository/jobs/4b2146f3-5594-4f05-ae13-2e053ef7bfda workdir
    $ ls workdir
    run.py
    data.csv -> /repository/jobs/6b189b64-8c7c-4609-b089-f69c7b3e0548/output/data.csv
    output/ -> /repository/jobs/4b2146f3-5594-4f05-ae13-2e053ef7bfda/output
    ```
    """
    job_path = job_path.resolve()
    repository = r3.Repository(job_path.parent.parent)
    job = r3.Job(job_path, id=job_path.name)
    repository.checkout(job, target_path)


@cli.command()
@click.argument(
    "job_path", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
def remove(job_path: Path) -> None:
    """Removes the job at JOB_PATH from the R3 repository.

    If any other job in the R3 repository depends on the job at JOB_PATH, removing the
    job will fail.
    """
    job_path = job_path.resolve()
    repository = r3.Repository(job_path.parent.parent)
    job = r3.Job(job_path, id=job_path.name)

    try:
        repository.remove(job)
    except ValueError as error:
        print(f"Error removing job: {error}")


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
        "Whether to list only the job paths (--short) or also additional job "
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
)
def find(
    tags: Iterable[str],
    latest: bool,
    long: bool,
    location: Optional[str],
    repository_path: Path,
) -> None:
    """Searches the R3 repository for jobs matching the given conditions."""
    repository = r3.Repository(repository_path)
    query = {"tags": {"$all": tags}}
    for job in repository.find(query, latest, location=location):
        if long:
            assert job.timestamp is not None
            datetime = job.timestamp.strftime(r"%Y-%m-%d %H:%M:%S")
            tags = " ".join(f"#{tag}" for tag in job.metadata.get("tags", []))
            print(f"{job.id} | {datetime} | {tags}")
        else:
            print(job.path)


@cli.command()
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def rebuild_index(repository_path: Path):
    """Rebuild the search index.

    The index is used when querying for jobs. All R3 commands properly update the index.
    When job metadata is modified manually, however, the index needs to be rebuilt in
    order for the changes to take effect.
    """
    repository = r3.Repository(repository_path)
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
)
def edit(job_id: str, repository_path: Path) -> None:
    """Edit a jobs metadata."""
    repository = r3.Repository(repository_path)
    try:
        job = repository[job_id]
    except KeyError:
        print(f"The job with ID {job_id} was not found in the repository.")

    # Let user edit the metadata file of the job
    metadata_file_path = job.path / "metadata.yaml"
    click.edit(filename=metadata_file_path)

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


if __name__ == "__main__":
    cli()
