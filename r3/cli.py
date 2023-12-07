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
@click.argument(
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
    job = r3.Job(job_path)
    repository = job.repository

    if repository is None:
        raise ValueError("Can only checkout commited jobs.")

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
    job = r3.Job(job_path)
    repository = job.repository

    if repository is None:
        print("Error removing job: Can only remove commited jobs.")
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
        "Whether to list only the job paths (--short) or also additional job "
        "information (--long)."
    )
)
@click.argument(
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def find(tags: Iterable[str], latest: bool, long: bool, repository_path: Path) -> None:
    """Searches the R3 repository for jobs matching the given conditions."""
    repository = r3.Repository(repository_path)
    for job in repository.find(tags, latest):
        if long:
            datetime = job.datetime.strftime(r"%Y-%m-%d %H:%M:%S")
            tags = " ".join(f"#{tag}" for tag in job.metadata.get("tags", []))
            print(f"{job.uuid} | {datetime} | {tags}")
        else:
            print(job.path)


@cli.group()
def dev():
    pass


@dev.command(name="checkout")
@click.argument("path", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument(
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def dev_checkout(path: str, repository_path: str) -> None:
    job = r3.Job(path)
    if job.repository is not None:
        print("ERROR: Can only dev checkout jobs that are not committed.")
        sys.exit(1)

    repository = r3.Repository(repository_path)

    for dependency in job.dependencies:
        if dependency not in repository:
            print(f"--> ERROR: Missing dependency: {dependency}")
            sys.exit(1)

        print(dependency.destination)

        target_path = Path(path) / dependency.destination
        if target_path.exists():
            print(
                "ERROR: Target path exists already. Use --force to override. "
                f"{target_path}"
            )
            sys.exit(1)

        repository.checkout(dependency, path)


@cli.command()
@click.argument(
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


if __name__ == "__main__":
    cli()
