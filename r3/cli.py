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
    repository = r3.Repository(path)

    try:
        repository.init()
        print(f"Initialized empty repository in {path}")
    except FileExistsError:
        print(f"Cannot initialize repository in {path}: path exists", file=sys.stderr)
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
    job_path = repository.commit(path)
    print(job_path)


@cli.command()
@click.argument(
    "job_path", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("target_path", type=click.Path(exists=False, path_type=Path))
def checkout(job_path: Path, target_path) -> None:
    repository_path = job_path.parent.parent.parent
    repository = r3.Repository(repository_path)
    repository.checkout(job_path.name, target_path)


@cli.command()
@click.argument(
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def build_indices(repository_path: Path):
    repository = r3.Repository(repository_path)
    repository.build_indices()


if __name__ == "__main__":
    cli()
