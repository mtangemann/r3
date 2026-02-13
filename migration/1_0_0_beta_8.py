#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.7 to 1.0.0-beta.8."""

from pathlib import Path

import click
import yaml

OLD_VERSION = "1.0.0-beta.7"
NEW_VERSION = "1.0.0-beta.8"


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
    click.echo("Changes:")
    click.echo("  - Update repository format version")
    click.echo("  - Rebuild index with location tracking column")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)

    click.echo("Rebuilding index...")
    from r3 import Repository

    repository = Repository(repository_path)
    repository.rebuild_index()

    click.echo("Done.")
    click.echo()
    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
