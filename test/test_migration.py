"""Tests for the beta.7 -> beta.8 -> beta.9 migration path (design §12, F-02).

The migration scripts have digit-leading filenames, so they are loaded by path.
"""

import importlib.util
import sqlite3
from pathlib import Path
from typing import Any

import pytest
import yaml

import r3

MIGRATION_DIR = Path(__file__).parent.parent / "migration"
DATA_PATH = Path(__file__).parent / "data"


def _load_migration(name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, MIGRATION_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


beta8 = _load_migration("1_0_0_beta_8")
beta9 = _load_migration("1_0_0_beta_9")
MigrationError = beta8._util.MigrationError


def _version(repo_path: Path) -> str:
    return yaml.safe_load((repo_path / "r3.yaml").read_text())["version"]


def _columns(repo_path: Path) -> set:
    connection = sqlite3.connect(str(repo_path / "index.sqlite"))
    try:
        return {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
    finally:
        connection.close()


@pytest.fixture
def beta7_repo(tmp_path: Path) -> Path:
    """A genuine beta.7-shaped repository: version 1.0.0-beta.7 and a jobs table
    without the location/files columns, holding one real committed job row."""
    repo_path = tmp_path / "repo"
    repo = r3.Repository.init(repo_path)
    repo.commit(r3.Job(DATA_PATH / "jobs" / "base"))

    # Rebuild the jobs table with the beta.7 schema (drop location/files).
    connection = sqlite3.connect(str(repo_path / "index.sqlite"))
    try:
        rows = connection.execute("SELECT id, timestamp, metadata FROM jobs").fetchall()
        connection.execute("DROP TABLE jobs")
        connection.execute(
            "CREATE TABLE jobs (id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, "
            "metadata JSON NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)", rows
        )
        connection.commit()
    finally:
        connection.close()

    config_path = repo_path / "r3.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["version"] = "1.0.0-beta.7"
    config_path.write_text(yaml.safe_dump(config))
    return repo_path


def test_beta7_to_beta9_reaches_head_schema(beta7_repo: Path) -> None:
    assert _version(beta7_repo) == "1.0.0-beta.7"
    assert "location" not in _columns(beta7_repo)
    assert "files" not in _columns(beta7_repo)

    beta8.apply(beta7_repo)
    assert _version(beta7_repo) == "1.0.0-beta.8"
    assert "location" in _columns(beta7_repo)

    beta9.apply(beta7_repo)
    assert _version(beta7_repo) == "1.0.0-beta.9"
    assert {"location", "files"} <= _columns(beta7_repo)

    # The repository opens at HEAD (no version-strict error) and the job survives
    # as a local job.
    repo = r3.Repository(beta7_repo)
    jobs = list(repo.jobs())
    assert len(jobs) == 1


def test_migration_preserves_remotes_config(beta7_repo: Path) -> None:
    """The version bump must not drop other r3.yaml keys (e.g. remotes)."""
    config_path = beta7_repo / "r3.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["remotes"] = {"archive": {"type": "s3", "bucket": "b"}}
    config_path.write_text(yaml.safe_dump(config))

    beta8.apply(beta7_repo)

    config = yaml.safe_load(config_path.read_text())
    assert config["version"] == "1.0.0-beta.8"
    assert config["remotes"] == {"archive": {"type": "s3", "bucket": "b"}}


def test_migration_rolls_back_and_keeps_old_version_on_failure(
    beta7_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure between the schema change and the version write leaves the old
    version and a usable index (rolled back), not a half-migrated repository."""

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(beta8._util, "write_version", boom)
    with pytest.raises(RuntimeError):
        beta8.apply(beta7_repo)

    assert _version(beta7_repo) == "1.0.0-beta.7"
    assert "location" not in _columns(beta7_repo)  # ALTER rolled back
    assert not (beta7_repo / "index.sqlite.bak").exists()  # backup consumed

    # A clean re-run then succeeds.
    monkeypatch.undo()
    beta8.apply(beta7_repo)
    assert _version(beta7_repo) == "1.0.0-beta.8"


def test_migration_refuses_to_clobber_existing_backup(beta7_repo: Path) -> None:
    """A stale .bak (from a hard-crashed prior run) must not be overwritten."""
    backup = beta7_repo / "index.sqlite.bak"
    backup.write_bytes(b"prior good backup")

    with pytest.raises(MigrationError):
        beta8.apply(beta7_repo)

    assert _version(beta7_repo) == "1.0.0-beta.7"  # untouched
    assert backup.read_bytes() == b"prior good backup"  # not clobbered


def test_migration_is_idempotent_on_rerun(beta7_repo: Path) -> None:
    """Re-running a completed migration (duplicate column) is a safe no-op-ish."""
    beta8.apply(beta7_repo)
    # Simulate a re-run at the same starting point: the column already exists.
    beta8._util.add_index_column(beta7_repo, beta8.ADD_LOCATION_COLUMN)
    assert "location" in _columns(beta7_repo)
