"""R3 command line interface."""
# ruff: noqa: T201

import sys
from pathlib import Path
from typing import Iterable

import click

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
    print(job.id)


@cli.command()
@click.argument(
    "job_id", type=str
)
@click.argument("target_path", type=click.Path(exists=False, path_type=Path))
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def checkout(job_id: str, target_path: Path, repository_path: Path) -> None:
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
    repository = r3.Repository(repository_path)
    job = repository.get_job_by_id(job_id)
    repository.checkout(job, target_path)


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
def remove(job_id: str, repository_path: Path) -> None:
    """Removes the job at JOB_PATH from the R3 repository.

    If any other job in the R3 repository depends on the job at JOB_PATH, removing the
    job will fail.
    """
    repository = r3.Repository(repository_path)

    try:
        job = repository.get_job_by_id(job_id)
    except KeyError as error:
        print(error)
        return
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
        "Whether to list only the job IDs (--short) or also additional job "
        "information (--long)."
    )
)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def find(tags: Iterable[str], latest: bool, long: bool, repository_path: Path) -> None:
    """Searches the R3 repository for jobs matching the given conditions."""
    repository = r3.Repository(repository_path)
    query = {"tags": {"$all": tags}}
    for job in repository.find(query, latest):
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
    except:
        print(f"The job with ID {job_id} was not found in the repository.")

    # Let user edit the metadata file of the job
    metadata_file_path = job.path / "metadata.yaml"
    click.edit(filename=metadata_file_path)

    # Update job in search index (SQLite DB)
    repository._index.remove(job)
    repository._index.add(job)

if __name__ == "__main__":
    cli()
