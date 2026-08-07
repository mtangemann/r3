#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.7 to 1.0.0-beta.8.

Adds the ``location`` column to the index (all existing jobs default to ``local``).
The change is applied directly with SQLite — the migration does not construct the
version-strict ``Repository`` (which would reject the intermediate version) nor reuse
the live ``Index``. The index is backed up first and the new version is written last,
so an interruption leaves the old version and a usable index (design §12).
"""

import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))
import _util  # noqa: E402

OLD_VERSION = "1.0.0-beta.7"
NEW_VERSION = "1.0.0-beta.8"

ADD_LOCATION_COLUMN = (
    "ALTER TABLE jobs ADD COLUMN location TEXT NOT NULL DEFAULT 'local'"
)


def apply(repository_path: Path) -> None:
    """Apply the beta.7 -> beta.8 migration (no prompts; for the CLI and tests)."""
    _util.apply_column_migration(repository_path, ADD_LOCATION_COLUMN, NEW_VERSION)


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

    if _util.read_version(repository_path) != OLD_VERSION:
        click.echo(f"This repository is not at version {OLD_VERSION}.")
        return

    click.echo("This script is migrating the following R3 repository:")
    click.echo(f"  {repository_path} ({OLD_VERSION} -> {NEW_VERSION})")
    click.echo()
    click.echo("Changes:")
    click.echo("  - Add 'location' column to the index (existing jobs -> 'local')")
    click.echo("  - Update repository format version (written last)")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    apply(repository_path)

    click.echo("Done.")
    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
