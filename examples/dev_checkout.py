#!/usr/bin/env python3
"""Materialize a job's dependencies for local development ("dev checkout").

This is an EXAMPLE, not an r3 command: copy it into your project and adapt it.
r3 core stays unopinionated about the dev loop; the right behavior is personal
(see "Extending this" below). It uses only public r3 API and runs against this
release with no compatibility promise.

    python examples/dev_checkout.py checkout <job_dir>   # materialize deps in place
    python examples/dev_checkout.py cleanup  <job_dir>   # remove them again

`checkout` leaves existing destinations alone, so re-running is safe; `cleanup`
drops them so a later `checkout` can pick up newer upstreams.

Extending this (what fuller tooling typically adds — each is a deliberate
decision left out of this example):
  - skip a dependency based on its upstream job's metadata (e.g. flagged
    obsolete, a test run, or its output cleared)
  - a config list of destinations/repositories to never materialize (e.g. a
    multi-TB dataset already present on the cluster)
  - guard `cleanup` against uncommitted/unpushed changes in a git dependency
"""

import shutil
from pathlib import Path

import click

import r3


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument(
    "job_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--repository", "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY", show_envvar=True,
)
def checkout(job_dir: Path, repository_path: Path) -> None:
    """Materialize JOB_DIR's dependencies into JOB_DIR."""
    repository = r3.Repository(repository_path)
    for dependency in r3.Job(job_dir).dependencies:
        destination = job_dir / dependency.destination

        # decision point: skip an existing destination, or replace it?
        if destination.exists() or destination.is_symlink():
            click.echo(f"{dependency.destination}: already there")
            continue

        # resolves an unresolved dependency, then materializes it
        repository.checkout(dependency, job_dir)
        click.echo(f"{dependency.destination}: checked out")

        # Optional — add `upstream` (the public repo) so you can hack on a git
        # dependency and open a PR. Whole-repo checkouts only (root `source`, so
        # the destination is a real clone). Uncomment and `import subprocess`.
        # is_whole_repo = dependency.source == Path(".")
        # if isinstance(dependency, r3.GitDependency) and is_whole_repo:
        #     subprocess.run(
        #         ["git", "remote", "add", "upstream", dependency.repository],
        #         cwd=destination, check=True,
        #     )


@cli.command()
@click.argument(
    "job_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
def cleanup(job_dir: Path) -> None:
    """Remove JOB_DIR's materialized dependencies (no safety checks)."""
    for dependency in r3.Job(job_dir).dependencies:
        destination = job_dir / dependency.destination
        if destination.is_symlink():
            destination.unlink()          # a job dependency: symlinked in
        elif destination.is_dir():
            shutil.rmtree(destination)    # a git dependency: a real directory
        else:
            continue
        click.echo(f"{dependency.destination}: removed")
    # No guard against changes in a checked-out git dependency — a reasonable
    # personal addition (see the module docstring), left out of this example.


if __name__ == "__main__":
    cli()
