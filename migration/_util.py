"""Shared, crash-safe helpers for R3 format migrations.

Migrations must not construct the version-strict ``Repository`` (it rejects any
version other than the current one) nor reuse the live ``Index`` (whose ``rebuild``
recreates the *current* HEAD schema, not the era being migrated to). Instead they
perform their SQLite change directly here, back up the index first, and write the new
format version **last** so a failure leaves the old version and a usable index.

See the durable remote-storage design, §12.
"""

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Optional

import yaml


class MigrationError(Exception):
    """Raised when a migration cannot proceed safely."""


def read_version(repository_path: Path) -> str:
    """Returns the repository's current format version from ``r3.yaml``."""
    with open(repository_path / "r3.yaml") as file:
        return yaml.safe_load(file)["version"]


def write_version(repository_path: Path, new_version: str) -> None:
    """Atomically sets the format version, preserving the rest of ``r3.yaml``.

    Written via a temp file + ``os.replace`` so a crash mid-write cannot truncate the
    file (which also holds the ``remotes`` map). Call this **last**, after the
    schema/data change has succeeded.
    """
    config_path = repository_path / "r3.yaml"
    with open(config_path) as file:
        config = yaml.safe_load(file)
    config["version"] = new_version
    tmp_path = config_path.with_name(config_path.name + ".tmp")
    with open(tmp_path, "w") as file:
        yaml.safe_dump(config, file)
    os.replace(tmp_path, config_path)


def backup_index(repository_path: Path) -> Optional[Path]:
    """Copies ``index.sqlite`` aside before a destructive step.

    Refuses to overwrite an existing ``index.sqlite.bak``: a prior interrupted
    migration's *good* backup must not be clobbered by a now-partial index. Returns
    the backup path, or ``None`` if there is no index to back up.

    Raises:
        MigrationError: If a backup already exists (manual recovery required).
    """
    index_path = repository_path / "index.sqlite"
    if not index_path.exists():
        return None

    backup_path = repository_path / "index.sqlite.bak"
    if backup_path.exists():
        raise MigrationError(
            f"A backup already exists at {backup_path}. A previous migration may have "
            f"been interrupted. Confirm index.sqlite is intact and remove the backup, "
            f"or restore it (mv index.sqlite.bak index.sqlite), then re-run."
        )

    shutil.copy2(index_path, backup_path)
    return backup_path


def restore_index(repository_path: Path) -> None:
    """Restores ``index.sqlite`` from its ``.bak`` (used to roll back on failure)."""
    backup_path = repository_path / "index.sqlite.bak"
    if backup_path.exists():
        os.replace(backup_path, repository_path / "index.sqlite")


def discard_backup(repository_path: Path) -> None:
    """Removes the ``.bak`` after a successful migration."""
    backup_path = repository_path / "index.sqlite.bak"
    if backup_path.exists():
        backup_path.unlink()


def add_index_column(repository_path: Path, alter_sql: str) -> None:
    """Runs an idempotent ``ALTER TABLE jobs ADD COLUMN`` on the index if it exists.

    A missing index is a no-op (it will be created with the current schema on next
    access). A duplicate-column error is treated as already-applied, so re-running a
    partially-completed migration is safe.
    """
    index_path = repository_path / "index.sqlite"
    if not index_path.exists():
        return

    connection = sqlite3.connect(str(index_path))
    try:
        connection.execute(alter_sql)
        connection.commit()
    except sqlite3.OperationalError as error:
        if "duplicate column name" not in str(error).lower():
            raise
    finally:
        connection.close()


def apply_column_migration(
    repository_path: Path, alter_sql: str, new_version: str
) -> None:
    """Runs a column-adding migration crash-safely.

    Backs up the index, applies ``alter_sql``, then writes ``new_version`` last. On
    any failure the index is restored from the backup and the error re-raised, so the
    repository is left at its old version with a usable index.
    """
    backup_index(repository_path)
    try:
        add_index_column(repository_path, alter_sql)
        write_version(repository_path, new_version)
    except BaseException:
        restore_index(repository_path)
        raise
    discard_backup(repository_path)
