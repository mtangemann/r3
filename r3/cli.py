"""R3 command line interface."""
# ruff: noqa: T201

import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

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
    job = _get_job(repository, job_id)

    try:
        repository.remove(job)
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
    "--path", "-p", "path_glob", type=str, default=None,
    help=(
        "Only list jobs whose `path` matches this glob pattern. The pattern is a "
        "literal SQLite GLOB (`*`, `?`, `[...]` are wildcards) and you add the "
        "wildcards yourself, e.g. -p '*mnist*' or -p 'proj/experiments/*'. "
        "Combined with --tag via AND."
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
    "--tags/--no-tags", "show_tags", default=True,
    help=(
        "Include the tags column in --long output (default: --tags). Use "
        "--no-tags to drop it; the path column already shows where a job lives."
    )
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
    path_glob: Optional[str],
    latest: bool,
    long: bool,
    show_tags: bool,
    repository_path: Optional[Path],
) -> None:
    """Searches the R3 repository for jobs matching the given conditions.

    Results are listed oldest-first by timestamp.
    """
    repository = _get_repository(repository_path)
    query = _build_find_query(tags, path_glob)
    for job in repository.find(query, latest):
        if long:
            assert job.timestamp is not None
            datetime_str = job.timestamp.strftime(r"%Y-%m-%d %H:%M:%S")
            path = job.metadata.get("path", "")
            line = f"{job.id} | {datetime_str} | {path}"
            if show_tags:
                tags_str = " ".join(
                    f"#{tag}" for tag in job.metadata.get("tags", [])
                )
                line += f" | {tags_str}"
            print(line)
        else:
            print(job.id)


def _build_find_query(
    tags: Iterable[str], path_glob: Optional[str]
) -> Dict[str, Any]:
    """Builds the Mongo-style query for `find` from the tag/path options."""
    query: Dict[str, Any] = {"tags": {"$all": list(tags)}}
    if path_glob is not None:
        query["path"] = {"$glob": path_glob}
    return query


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

    # Update job in search index (SQLite DB)
    repository._index.update(job)


if __name__ == "__main__":
    cli()
