#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.5 to 1.0.0-beta.6."""

import os
from pathlib import Path

import click
import yaml
from executor import ExternalCommandFailed, execute
from tqdm import tqdm

from r3 import Dependency, GitDependency

OLD_VERSION = "1.0.0-beta.5"
NEW_VERSION = "1.0.0-beta.6"


@click.command()
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def migrate(repository_path: Path) -> None:
    if not (repository_path / "r3.yaml").exists():
        click.echo("This is not a valid R3 repository.")
        return

    with open(repository_path / "r3.yaml") as file:
        config = yaml.safe_load(file)
    if config["version"] != OLD_VERSION:
        click.echo(f"This repository is not at version {OLD_VERSION}.")
        return

    click.echo("This script is migrating the following R3 repository:")
    click.echo(f"  {repository_path} ({OLD_VERSION} -> {NEW_VERSION})")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    backup_path = repository_path / "backup" / OLD_VERSION
    click.echo(f"Backup directory: {backup_path}")
    click.echo("You may delete this directory after the migration is complete.")
    click.echo()

    git_repositories = [
        path for path in repository_path.glob("git/*/*/*") if path.is_dir()
    ]

    non_github_repositories = [
        path for path in git_repositories if path.parent.parent.name != "github.com"
    ]
    if len(non_github_repositories) > 0:
        click.echo("This script only supports GitHub repositories.")
        click.echo("Please update the following repositories manually:")
        for path in non_github_repositories:
            click.echo(f"  {path}")

    git_repositories = [
        path for path in git_repositories if path.parent.parent.name == "github.com"
    ]

    for git_repository in git_repositories:
        _, user, repository = git_repository.relative_to(repository_path / "git").parts
        click.echo(f"Updating {user}/{repository}...")

        origin = execute(
            "git config --get remote.origin.url",
            directory=git_repository,
            capture=True,
        ).strip()
        if origin.endswith(".git"):
            origin = origin[:-4]
        click.echo(f"  Origin: {origin}")

        repository_backup_path = backup_path / user / repository
        repository_backup_path.parent.mkdir(parents=True, exist_ok=True)
        click.echo(f"  Backup: {repository_backup_path}")
        os.rename(git_repository, repository_backup_path)

        click.echo("  Cloning...")
        execute(
            f"git clone --bare '{origin}' '{git_repository}'",
            directory=git_repository.parent,
            capture=True,
        )

        click.echo("  Done.")
        click.echo()

    job_paths = [path for path in repository_path.glob("jobs/*") if path.is_dir()]
    click.echo("Adding git tags for commits used by jobs.")
    click.echo(f"Processing {len(job_paths)} jobs ...")

    for job_path in tqdm(job_paths, ncols=80):
        if not (job_path / "r3.yaml").exists():
            click.echo(f"Skipping {job_path} (no r3.yaml)")
            continue

        with open(job_path / "r3.yaml") as config_file:
            job_config = yaml.safe_load(config_file)

        for dependency_config in job_config.get("dependencies", []):
            dependency = Dependency.from_config(dependency_config)

            if isinstance(dependency, GitDependency):
                job_id = job_path.name
                git_repository = repository_path / dependency.repository_path
                try:
                    execute(
                        f"git tag r3/{job_id} {dependency.commit}",
                        directory=git_repository,
                        capture=True,
                    )
                except ExternalCommandFailed:
                    click.echo(f"Failed to add tag for {job_id} in {git_repository}")

    click.echo("Done.")
    click.echo()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)
    click.echo("Done.")
    click.echo()

    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
