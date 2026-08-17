"""Shared, crash-safe helpers for R3 format migrations.

Migrations must not construct the version-strict ``Repository`` (it rejects any
version other than the current one) nor reuse the live ``Index`` (whose ``rebuild``
recreates the *current* HEAD schema, not the era being migrated to). Instead they
perform their SQLite change directly here, back up the index first, and write the new
format version **last** so a failure leaves the old version and a usable index.

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


def _is_valid_sqlite_database(path: Path) -> bool:
    """Returns whether ``path`` is a readable, well-formed SQLite database.

    Used before advising an operator to restore a leftover backup: a truncated or
    partial ``.bak`` (e.g. from a crash mid-copy) must never be recommended as a
    replacement for a good index.
    """
    try:
        connection = sqlite3.connect(str(path))
        try:
            result = connection.execute("PRAGMA integrity_check").fetchone()
        finally:
            connection.close()
    except sqlite3.DatabaseError:
        return False
    return result is not None and result[0] == "ok"


def backup_index(repository_path: Path, from_version: str) -> Optional[Path]:
    """Copies ``index.sqlite`` aside before a destructive step.

    The backup name is stamped with ``from_version`` (the version being migrated
    *from*) so backups from different migrations never collide and a restore always
    targets the right era. The copy is written to a temp sibling and then atomically
    moved into place, so a crash mid-copy can never leave a usable-looking, truncated
    backup. Returns the backup path, or ``None`` if there is no index to back up.

    Refuses to overwrite an existing backup for this same version: it belongs to an
    interrupted run of *this* migration and its *good* copy must not be clobbered by a
    now-partial index. If that backup is a valid database the refusal offers to
    restore it (a same-era, safe move); if it is not, the refusal instead advises
    inspecting/removing it and never recommends restoring it over the index.

    Raises:
        MigrationError: If a backup for this version already exists (manual recovery
            required).
    """
    index_path = repository_path / "index.sqlite"
    if not index_path.exists():
        return None

    backup_path = repository_path / f"index.sqlite.{from_version}.bak"
    if backup_path.exists():
        if _is_valid_sqlite_database(backup_path):
            raise MigrationError(
                f"A backup from an interrupted run of this migration already exists "
                f"at {backup_path}. Confirm index.sqlite is intact and remove the "
                f"backup, or restore it (mv {backup_path.name} index.sqlite), then "
                f"re-run."
            )
        raise MigrationError(
            f"A backup at {backup_path} exists but is not a valid SQLite database; a "
            f"previous migration may have crashed while copying it. Do NOT copy it "
            f"over index.sqlite. Inspect index.sqlite, and once you have confirmed it "
            f"is intact, remove {backup_path.name} and re-run."
        )

    tmp_path = backup_path.with_name(backup_path.name + ".tmp")
    shutil.copy2(index_path, tmp_path)
    os.replace(tmp_path, backup_path)
    return backup_path


def restore_index(repository_path: Path, backup_path: Optional[Path]) -> None:
    """Restores ``index.sqlite`` from ``backup_path`` (used to roll back on failure).

    A ``None`` backup path (there was no index to back up) is a no-op.
    """
    if backup_path is not None and backup_path.exists():
        os.replace(backup_path, repository_path / "index.sqlite")


def discard_backup(backup_path: Optional[Path]) -> None:
    """Removes ``backup_path`` after a successful migration (``None`` is a no-op)."""
    if backup_path is not None and backup_path.exists():
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

    The from-version is read up front so the backup is stamped with it and the same
    path can be handed to restore/discard — by discard time ``write_version`` has
    already advanced the version, so it can no longer be re-derived.
    """
    from_version = read_version(repository_path)
    backup_path = backup_index(repository_path, from_version)
    try:
        add_index_column(repository_path, alter_sql)
        write_version(repository_path, new_version)
    except BaseException:
        restore_index(repository_path, backup_path)
        raise
    discard_backup(backup_path)
