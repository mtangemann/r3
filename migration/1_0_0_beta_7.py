#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.6 to 1.0.0-beta.7."""

import os
import shutil
import stat
from datetime import datetime
from pathlib import Path

import click
import yaml
from tqdm import tqdm

from r3 import Repository

OLD_VERSION = "1.0.0-beta.6"
NEW_VERSION = "1.0.0-beta.7"

DATE_FORMAT = r"%Y-%m-%d %H:%M:%S"


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

    jobs = list(repository_path.glob("jobs/*"))
    click.echo(f"Updating {len(jobs)} jobs...")

    for job_path in tqdm(jobs, ncols=80):
        job_backup_path = backup_path / job_path.relative_to(repository_path)
        os.makedirs(job_backup_path, exist_ok=True)
        shutil.copy(job_path / "r3.yaml", job_backup_path / "r3.yaml")
        shutil.copy(job_path / "metadata.yaml", job_backup_path / "metadata.yaml")

        with open(job_path / "metadata.yaml", "r") as metadata_file:
            metadata = yaml.safe_load(metadata_file)
        if "committed_at" not in metadata:
            click.echo(f"Job {job_path.name} does not have a timestamp.")
            continue
        timestamp = datetime.strptime(metadata["committed_at"], DATE_FORMAT)
        del metadata["committed_at"]
        with open(job_path / "metadata.yaml", "w") as metadata_file:
            yaml.safe_dump(metadata, metadata_file)

        with open(job_path / "r3.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)
        config["timestamp"] = timestamp.isoformat()
        _add_write_permission(job_path / "r3.yaml")
        with open(job_path / "r3.yaml", "w") as config_file:
            yaml.safe_dump(config, config_file)
        _remove_write_permissions(job_path / "r3.yaml")

    click.echo("Rebuilding index...")
    repository = Repository(repository_path)
    repository.rebuild_index()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)
    click.echo("Done.")
    click.echo()

    click.echo("Migration complete.")


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)


def _add_write_permission(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode | stat.S_IWOTH | stat.S_IWGRP | stat.S_IWUSR
    os.chmod(path, mode)


if __name__ == "__main__":
    migrate()
