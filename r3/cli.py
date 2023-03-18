"""R3 command line interface."""

import sys
from pathlib import Path

import click

import r3


@click.group()
@click.version_option(r3.__version__, message="%(version)s")
def cli() -> None:
    pass


@cli.command()
@click.argument("path", type=click.Path(file_okay=False, exists=False, path_type=Path))
def init(path: Path):
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
    repository = r3.Repository(repository_path)
    job = r3.Job(path)
    job.metadata["source"] = str(path)
    job = repository.commit(job)
    print(job.path)


@cli.command()
@click.argument(
    "job_path", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("target_path", type=click.Path(exists=False, path_type=Path))
def checkout(job_path: Path, target_path) -> None:
    job = r3.Job(job_path)
    repository = job.repository

    if repository is None:
        raise ValueError("Can only checkout commited jobs.")

    repository.checkout(job, target_path)


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
            print(f"ERROR: Missing dependency: {dependency}")
            sys.exit(1)

        target_path = Path(path) / dependency.item
        if target_path.exists():
            print(
                f"ERROR: Target path exists already. Use --force to override. {target_path}"
            )
            sys.exit(1)

        repository.checkout(dependency, path)


@cli.command()
@click.argument(
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def rebuild_cache(repository_path: Path):
    repository = r3.Repository(repository_path)
    repository.rebuild_cache()


if __name__ == "__main__":
    cli()
