#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.8 to 1.0.0-beta.9.

Adds the 'files' column to the index for caching remote-job file lists.
Existing rows get NULL (no cached file list, which behaves the same as
before this version).
"""

import sqlite3
from pathlib import Path

import click
import yaml

OLD_VERSION = "1.0.0-beta.8"
NEW_VERSION = "1.0.0-beta.9"


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
    click.echo("  - Add 'files' column to index (existing rows get NULL)")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)

    click.echo("Adding 'files' column to index...")
    index_path = repository_path / "index.sqlite"
    if index_path.exists():
        conn = sqlite3.connect(str(index_path))
        # SQLite ALTER TABLE ADD COLUMN is idempotent-ish: it raises
        # OperationalError if the column already exists. Catch and continue.
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN files JSON")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                click.echo("  (column already exists, skipping)")
            else:
                raise
        finally:
            conn.close()
    else:
        click.echo("  (no index file; will be created on next access)")

    click.echo("Done.")
    click.echo()
    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
