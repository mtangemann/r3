"""Job index for efficient searching."""

import json
import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Set, Tuple

import r3.manifest
import r3.utils
from r3.archive import (
    DEFAULT_MAX_FILE_BYTES,
    DEFAULT_MAX_FILE_COUNT,
    DEFAULT_MAX_TOTAL_BYTES,
    enforce_extraction_caps,
)
from r3.job import Job, JobDependency
from r3.query import mongo_to_sql
from r3.storage import Storage

if TYPE_CHECKING:
    from r3.remote import Remote

# Defense-in-depth caps on the metadata objects rebuild reads into memory before
# parsing. A manifest and the r3.yaml/metadata.yaml sidecars are normally only a few
# KB, so a few MiB is very generous; rejecting anything larger keeps a corrupt or
# hostile object from exhausting memory during a rebuild, and — together with the
# extraction caps applied to the parsed manifest — makes rebuild fail closed on a job
# a later fetch would refuse rather than caching it.
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_SIDECAR_BYTES = 4 * 1024 * 1024


class Index:
    """Job index for efficient searching."""

    def __init__(
        self,
        storage: Storage,
        remotes: Optional[Mapping[str, "Remote"]] = None,
    ) -> None:
        """Initializes the index.

        Parameters:
            storage: The storage with the jobs to index.
            remotes: The configured remotes, keyed by name. Needed by `rebuild` to
                reconstruct remote job rows from the bucket. Defaults to no remotes,
                in which case `rebuild` reconstructs from local storage alone.
        """
        self.storage = storage
        self.remotes: Mapping[str, "Remote"] = remotes if remotes is not None else {}
        self._path = storage.root / "index.sqlite"

        if not self._path.exists():
            self.rebuild()

    def rebuild(self) -> None:
        """Rebuilds the index from the durable sources: local storage and remotes.

        Local jobs come from `Storage`; remote jobs are reconstructed from each
        configured remote's bucket (the manifest, `r3.yaml`/`metadata.yaml` sidecars,
        parsed exactly as a local job). The rebuild is atomic and fail-closed: it
        builds a fresh `index.sqlite.new`, and only `os.replace`s it over the live
        index once every job reconstructs and validates. Any failure — local
        corruption, a missing/corrupt remote artifact, a manifest/key mismatch, a
        duplicate job id across remotes, or a transient read error — aborts the whole
        rebuild, leaving the previous index untouched and no partial file behind.
        """
        new_path = self.storage.root / "index.sqlite.new"
        # A stale `.new` from an interrupted prior attempt is discarded, never
        # appended to.
        new_path.unlink(missing_ok=True)

        try:
            local_jobs, local_dependencies, local_ids = self._collect_local_jobs()
            remote_jobs, remote_dependencies = self._collect_remote_jobs(local_ids)

            with Transaction(new_path) as transaction:
                _create_schema(transaction)
                transaction.executemany(
                    "INSERT INTO jobs (id, timestamp, metadata, location)"
                    " VALUES (?, ?, ?, 'local')",
                    local_jobs,
                )
                transaction.executemany(
                    "INSERT INTO jobs (id, timestamp, metadata, location, files)"
                    " VALUES (?, ?, ?, ?, ?)",
                    remote_jobs,
                )
                transaction.executemany(
                    "INSERT INTO job_dependencies (child_id, parent_id)"
                    " VALUES (?, ?)",
                    local_dependencies + remote_dependencies,
                )
            # The new database is committed and closed (the Transaction exited)
            # before it is installed over the live index.
            os.replace(new_path, self._path)
        except BaseException:
            # Keep the previous index authoritative and leave no partial file behind.
            # Swallow a secondary unlink error so it cannot mask the original failure.
            try:
                new_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _collect_local_jobs(
        self,
    ) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str]], Set[str]]:
        """Collects local job rows and dependency edges from storage.

        A `jobs/<id>` directory missing its `r3.yaml` is corruption (R3's own write
        paths never produce one) and aborts the rebuild rather than being adopted as a
        valid local job.
        """
        jobs: List[Tuple[str, str, str]] = []
        dependencies: List[Tuple[str, str]] = []
        ids: Set[str] = set()

        for job in self.storage.jobs():
            assert job.id is not None
            if not (job.path / "r3.yaml").is_file():
                raise RuntimeError(
                    f"Corrupt local job {job.id}: jobs/{job.id} has no r3.yaml. "
                    "Aborting rebuild without modifying the existing index."
                )
            assert job.timestamp is not None

            jobs.append(
                (job.id, job.timestamp.isoformat(), json.dumps(job.metadata))
            )
            dependencies.extend(
                (job.id, dependency.job)
                for dependency in job.dependencies
                if isinstance(dependency, JobDependency)
            )
            ids.add(job.id)

        return jobs, dependencies, ids

    def _collect_remote_jobs(
        self, local_ids: Set[str]
    ) -> Tuple[List[Tuple[str, str, str, str, Optional[str]]], List[Tuple[str, str]]]:
        """Collects remote job rows and dependency edges from every remote's bucket.

        Local wins: a job id present locally is skipped and never enters the remote
        candidate set, so a locally-present job's remote leftovers (e.g. after crashed
        move attempts) are treated as ignorable — not as a conflict. Among the
        remaining, non-local jobs, a duplicate id across two remotes has no meaning
        under the one-location model and aborts with a diagnostic naming both remotes.
        """
        # job_id -> (remote_name, remote), collected across all remotes.
        candidates: Dict[str, Tuple[str, "Remote"]] = {}
        for remote_name, remote in self.remotes.items():
            try:
                job_ids = list(remote.list_job_ids())
            except Exception as error:
                raise RuntimeError(
                    f"Failed to list jobs on remote '{remote_name}' during rebuild: "
                    f"{error}. Aborting without modifying the existing index."
                ) from error
            for job_id in job_ids:
                # Fail closed on a manifest key whose job segment is not a canonical
                # UUID (a traversal- or nested-shaped key). Skipping it would let a
                # crafted key hide; adopting it would index an id that later becomes a
                # path/key and escapes. Aborting here leaves the previous index intact.
                if not r3.utils.is_valid_job_id(job_id):
                    raise RuntimeError(
                        f"Remote '{remote_name}' has a manifest under a non-canonical "
                        f"job id {job_id!r} (key "
                        f"'{remote.prefix}{job_id}/manifest.json'); a job id must be a "
                        "canonical UUID. Aborting rebuild without modifying the "
                        "existing index."
                    )
                if job_id in local_ids:
                    continue  # Local wins; the remote copy is an ignorable leftover.
                if job_id in candidates:
                    other_name = candidates[job_id][0]
                    raise RuntimeError(
                        f"Job {job_id} has a complete manifest on both remote "
                        f"'{other_name}' and remote '{remote_name}'. A job can live "
                        "on only one remote; resolve the conflict before rebuilding."
                    )
                candidates[job_id] = (remote_name, remote)

        jobs: List[Tuple[str, str, str, str, Optional[str]]] = []
        dependencies: List[Tuple[str, str]] = []
        for job_id, (remote_name, remote) in candidates.items():
            row, edges = self._reconstruct_remote_job(remote_name, remote, job_id)
            jobs.append(row)
            dependencies.extend(edges)

        return jobs, dependencies

    def _reconstruct_remote_job(
        self, remote_name: str, remote: "Remote", job_id: str
    ) -> Tuple[Tuple[str, str, str, str, Optional[str]], List[Tuple[str, str]]]:
        """Reconstructs and validates a single remote job row from the bucket.

        Structurally validates (without re-downloading the archive) that the manifest
        is well-formed, its `job_id` matches the object key, the fetched sidecars match
        the manifest's recorded sizes/hashes, and the archive exists with the recorded
        size. The `r3.yaml`/`metadata.yaml` sidecars are parsed through the `Job` class
        exactly as a local job, so timestamp/dependencies/metadata come from one code
        path. Any failure raises `RuntimeError` naming the remote and job.
        """
        try:
            manifest = r3.manifest.loads(
                remote.get_manifest(job_id, max_bytes=MAX_MANIFEST_BYTES)
            )

            if not r3.utils.is_valid_job_id(manifest["job_id"]):
                raise RuntimeError(
                    f"manifest job_id {manifest['job_id']!r} is not a canonical UUID."
                )
            if manifest["job_id"] != job_id:
                raise RuntimeError(
                    f"manifest job_id {manifest['job_id']!r} does not match the "
                    f"object key job_id {job_id!r}."
                )

            # Fail closed on a manifest that declares more files or bytes than the
            # extraction caps allow, using the same bound fetch enforces — so rebuild
            # never caches a job a later fetch would refuse (and never touches those
            # declared bytes). Sidecars live outside the archive, so mirror fetch and
            # bound only the archive-resident payload.
            enforce_extraction_caps(
                {
                    entry["path"]: entry["size"]
                    for entry in manifest["files"]
                    if entry["path"] not in r3.manifest.SIDECAR_PATHS
                },
                DEFAULT_MAX_TOTAL_BYTES,
                DEFAULT_MAX_FILE_COUNT,
                DEFAULT_MAX_FILE_BYTES,
            )

            entries = {entry["path"]: entry for entry in manifest["files"]}
            sidecar_bytes: Dict[str, bytes] = {}
            for name in r3.manifest.SIDECAR_PATHS:
                if name not in entries:
                    raise RuntimeError(f"manifest has no entry for sidecar {name!r}.")
                entry = entries[name]
                data = remote.get_sidecar(job_id, name, max_bytes=MAX_SIDECAR_BYTES)
                sidecar_bytes[name] = data
                if len(data) != entry["size"]:
                    raise RuntimeError(
                        f"sidecar {name!r} size {len(data)} != manifest "
                        f"{entry['size']}."
                    )
                if r3.utils.hash_bytes(data) != entry["sha256"]:
                    raise RuntimeError(
                        f"sidecar {name!r} hash does not match manifest."
                    )

            archive_size = remote.archive_size(job_id)
            if archive_size is None:
                raise RuntimeError("archive data.tar.zst is missing.")
            if archive_size != manifest["archive_size"]:
                raise RuntimeError(
                    f"archive size {archive_size} != manifest "
                    f"{manifest['archive_size']}."
                )

            timestamp, metadata, edges = self._parse_remote_sidecars(
                job_id, sidecar_bytes
            )
        except Exception as error:
            raise RuntimeError(
                f"Failed to rebuild job {job_id} from remote '{remote_name}': "
                f"{error}. Aborting without modifying the existing index."
            ) from error

        files_json: Optional[str] = None
        if remote.cache_file_list:
            files_json = json.dumps(
                [path.as_posix() for path in r3.manifest.file_paths(manifest)]
            )

        row = (job_id, timestamp, json.dumps(metadata), remote_name, files_json)
        return row, edges

    def _parse_remote_sidecars(
        self, job_id: str, sidecar_bytes: Mapping[str, bytes]
    ) -> Tuple[str, Dict[str, Any], List[Tuple[str, str]]]:
        """Parses the sidecars through the `Job` class, the same path a local job uses.

        Writes the fetched sidecars into a temporary directory and reads
        timestamp/metadata/dependencies from a plain `Job` (no `remote_location`, so it
        parses rather than projects), yielding a local-identical parse.
        """
        with tempfile.TemporaryDirectory(prefix="r3-rebuild-") as temp_dir:
            temp_path = Path(temp_dir)
            for name in r3.manifest.SIDECAR_PATHS:
                (temp_path / name).write_bytes(sidecar_bytes[name])

            job = Job(temp_path, job_id)
            timestamp = job.timestamp
            assert timestamp is not None
            metadata = job.metadata
            edges = [
                (job_id, dependency.job)
                for dependency in job.dependencies
                if isinstance(dependency, JobDependency)
            ]

        return timestamp.isoformat(), metadata, edges

    def __len__(self) -> int:
        """Returns the number of jobs in the index."""
        with Transaction(self._path) as transaction:
            transaction.execute("SELECT COUNT(*) FROM jobs")
            return transaction.fetchone()[0]

    def __contains__(self, job: Job) -> bool:
        """Checks if a job is in the index.

        Parameters:
            job: The job to check.

        Returns:
            Whether the job is in the index.
        """
        if job.id is None:
            raise ValueError("Job ID is not set")

        with Transaction(self._path) as transaction:
            transaction.execute(
                "SELECT COUNT(*) FROM jobs WHERE id = ?",
                (job.id,)
            )
            return transaction.fetchone()[0] > 0

    def add(self, job: Job) -> None:
        """Adds a job to the index.

        Parameters:
            job: The job to add.
        """
        if job not in self.storage:
            raise ValueError(f"Job not in storage: {job}")

        # Both should be set for jobs in the storage.
        assert job.id is not None
        assert job.timestamp is not None

        with Transaction(self._path) as transaction:
            transaction.execute(
                "INSERT INTO jobs (id, timestamp, metadata, location) VALUES (?, ?, ?, 'local')",  # noqa: E501
                (job.id, job.timestamp.isoformat(), json.dumps(job.metadata))
            )
            transaction.executemany(
                "INSERT INTO job_dependencies (child_id, parent_id) VALUES (?, ?)",
                [
                    (job.id, dependency.job)
                    for dependency in job.dependencies
                    if isinstance(dependency, JobDependency)
                ]
            )

    def _local_job(
        self,
        job_id: str,
        cached_timestamp: datetime,
        cached_metadata: Dict[str, Any],
    ) -> Job:
        """Builds a `Job` for a row indexed as local, verifying it is not corrupt.

        A local row must be backed by a well-formed `jobs/<id>` directory. Two
        corruption cases are caught here, at the index boundary, and reported clearly
        rather than surfacing as a bare `FileNotFoundError` or a confusing later
        failure: the directory is missing entirely, or it exists but has no `r3.yaml`
        (R3's own write paths never produce the latter).
        """
        job_dir = self.storage.root / "jobs" / job_id
        if not job_dir.is_dir():
            raise RuntimeError(
                f"Job {job_id} is indexed as local but its directory jobs/{job_id} "
                "is missing (corruption). Manual intervention required."
            )
        if not (job_dir / "r3.yaml").is_file():
            raise RuntimeError(
                f"Job {job_id} is indexed as local but jobs/{job_id} has no r3.yaml "
                "(corruption). Manual intervention required."
            )
        return self.storage.get(job_id, cached_timestamp, cached_metadata)

    def _row_to_job(
        self,
        job_id: str,
        cached_timestamp: datetime,
        cached_metadata: Dict[str, Any],
        location: str,
    ) -> Job:
        """Builds a `Job` from an index row, location-aware.

        A row indexed as ``local`` is materialized from storage (and verified for
        corruption via `_local_job`); any other location is a metadata-only remote
        projection whose files/hash/dependencies raise `FilesUnavailableError` until
        the job is fetched. Shared by `get`, `find`, and `find_dependents` so the
        projection construction lives in exactly one place.
        """
        if location == "local":
            return self._local_job(job_id, cached_timestamp, cached_metadata)
        return Job(
            self.storage.root / "jobs" / job_id,
            job_id,
            cached_timestamp=cached_timestamp,
            cached_metadata=cached_metadata,
            remote_location=location,
        )

    def get(self, job_id: str) -> Job:
        """Gets a job by ID.

        Parameters:
            job_id: The ID of the job to get.

        Returns:
            The job with the given ID.
        """
        with Transaction(self._path) as transaction:
            transaction.execute(
                "SELECT timestamp, metadata, location FROM jobs WHERE id = ?",
                (job_id,),
            )
            result = transaction.fetchone()

        if result is None:
            raise KeyError(f"Job not found: {job_id}")

        cached_timestamp = datetime.fromisoformat(result[0])
        cached_metadata = json.loads(result[1])
        location = result[2]

        return self._row_to_job(job_id, cached_timestamp, cached_metadata, location)

    def update(self, job: Job) -> None:
        """Updates a job in the index.

        This does not update the dependency graph, since that is not expected to change.

        Parameters:
            job: The job to update.
        """
        if job not in self.storage:
            raise ValueError(f"Job not in storage: {job}")
        assert job.id is not None
        assert job.timestamp is not None

        with Transaction(self._path) as transaction:
            transaction.execute(
                "UPDATE jobs SET timestamp = ?, metadata = ? WHERE id = ?",
                (job.timestamp.isoformat(), json.dumps(job.metadata), job.id)
            )

    def remove(self, job: Job) -> None:
        """Removes a job from the index.

        Parameters:
            job: The job to remove.
        """
        if job.id is None:
            raise ValueError("Job ID is not set")
        self.remove_by_id(job.id)

    def remove_by_id(self, job_id: str) -> None:
        """Removes a job's row and its dependency edges by id.

        Deleting by id lets ``Repository.remove`` finish a crash-interrupted removal
        from the raw index row alone, without materializing a `Job` (which would raise
        when the row says local but the directory is already gone).
        """
        with Transaction(self._path) as transaction:
            transaction.execute(
                "DELETE FROM jobs WHERE id = ?",
                (job_id,)
            )
            transaction.execute(
                "DELETE FROM job_dependencies WHERE child_id = ? OR parent_id = ?",
                (job_id, job_id)
            )

    def set_location(self, job_id: str, location: str) -> None:
        """Sets the location of a job.

        Parameters:
            job_id: The ID of the job.
            location: The location to set.
        """
        with Transaction(self._path) as transaction:
            transaction.execute(
                "UPDATE jobs SET location = ? WHERE id = ?",
                (location, job_id)
            )

    def get_location(self, job_id: str) -> str:
        """Gets the location of a job.

        Parameters:
            job_id: The ID of the job.

        Returns:
            The location of the job.
        """
        with Transaction(self._path) as transaction:
            transaction.execute(
                "SELECT location FROM jobs WHERE id = ?",
                (job_id,)
            )
            result = transaction.fetchone()

        if result is None:
            raise KeyError(f"Job not found: {job_id}")

        return result[0]

    def set_file_list(self, job_id: str, paths: List[Path]) -> None:
        """Sets the cached file list for a job.

        Paths are stored as POSIX strings in a JSON array, regardless of
        platform, so the cached list is portable.
        """
        files_json = json.dumps([p.as_posix() for p in paths])
        with Transaction(self._path) as transaction:
            transaction.execute(
                "UPDATE jobs SET files = ? WHERE id = ?",
                (files_json, job_id),
            )

    def set_remote_location(
        self, job_id: str, location: str, files: Optional[List[Path]]
    ) -> None:
        """Moves a job's location and cached file list in one transaction.

        Both the ``location`` and ``files`` columns are written inside a single
        `Transaction`, so they commit together or not at all: an interruption can
        never leave the row at the new remote location with an old/NULL file list.

        Paths are serialized exactly as `set_file_list` does — POSIX strings in a
        JSON array — so the cached list is portable. Passing ``files=None`` (a
        non-caching remote) stores SQL NULL.
        """
        files_json = (
            None if files is None else json.dumps([p.as_posix() for p in files])
        )
        with Transaction(self._path) as transaction:
            transaction.execute(
                "UPDATE jobs SET location = ? WHERE id = ?",
                (location, job_id),
            )
            transaction.execute(
                "UPDATE jobs SET files = ? WHERE id = ?",
                (files_json, job_id),
            )

    def get_file_list(self, job_id: str) -> Optional[List[Path]]:
        """Returns the cached file list for a job, or None if unset."""
        with Transaction(self._path) as transaction:
            transaction.execute(
                "SELECT files FROM jobs WHERE id = ?", (job_id,)
            )
            result = transaction.fetchone()
        if result is None or result[0] is None:
            return None
        return [Path(s) for s in json.loads(result[0])]

    def find(
        self,
        query: Dict[str, Any],
        latest: bool = False,
        location: Optional[str] = None,
    ) -> List[Job]:
        """Finds jobs by tags.

        Parameters:
            query: The query to match jobs against. The query is specified as a
                MongoDB-style query document.
            latest: Whether to return the latest job or all jobs with the given tags.
            location: Optional location filter. When provided, only jobs with the
                given location are returned.

        Returns:
            The jobs that match the given query.
        """
        sql_query = (
            f"SELECT id, timestamp, metadata, location FROM jobs WHERE "
            f"{mongo_to_sql(query)}"
        )
        params: List[Any] = []
        if location is not None:
            sql_query += " AND location = ?"
            params.append(location)
        if latest:
            sql_query += " ORDER BY timestamp DESC LIMIT 1"

        with Transaction(self._path) as transaction:
            transaction.execute(sql_query, params)
            results = transaction.fetchall()

        jobs = []
        for result in results:
            job_id = result[0]
            cached_timestamp = datetime.fromisoformat(result[1])
            cached_metadata = json.loads(result[2])
            row_location = result[3]
            jobs.append(
                self._row_to_job(
                    job_id, cached_timestamp, cached_metadata, row_location
                )
            )
        return jobs

    def find_dependents(self, job: Job, recursive: bool = False) -> Set[Job]:
        """Finds jobs that directly depend on the given job.

        Parameters:
            job: The job to find dependents for.
            recursive: Whether to find dependents recursively.

        Returns:
            The jobs that directly depend on the given job.
        """
        if job.id is None:
            raise ValueError("Job ID is not set")

        with Transaction(self._path) as transaction:
            transaction.execute(
                """SELECT child_id, timestamp, metadata, location
                FROM job_dependencies JOIN jobs ON child_id = id
                WHERE parent_id = ?""",
                (job.id,)
            )
            results = transaction.fetchall()

        dependents = dict()

        for result in results:
            job_id = result[0]
            cached_timestamp = datetime.fromisoformat(result[1])
            cached_metadata = json.loads(result[2])
            row_location = result[3]

            # A remote dependent has no local directory, so `_row_to_job` builds a
            # metadata-only projection instead of calling storage.get (which would
            # raise FileNotFoundError).
            dependent_job = self._row_to_job(
                job_id, cached_timestamp, cached_metadata, row_location
            )
            dependents[dependent_job.id] = dependent_job

            if recursive:
                dependents.update({
                    job.id: job
                    for job in self.find_dependents(dependent_job, recursive=True)
                })

        return set(dependents.values())


def _create_schema(transaction: sqlite3.Cursor) -> None:
    """Creates the `jobs` and `job_dependencies` tables in a fresh index database."""
    transaction.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            metadata JSON NOT NULL,
            location TEXT NOT NULL DEFAULT 'local',
            files JSON
        )
        """
    )
    transaction.execute(
        """
        CREATE TABLE job_dependencies (
            child_id TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            FOREIGN KEY (child_id) REFERENCES jobs (id),
            FOREIGN KEY (parent_id) REFERENCES jobs (id)
        )
        """
    )


class Transaction:
    def __init__(self, path: Path) -> None:
        self.path = str(path)

    def __enter__(self) -> sqlite3.Cursor:
        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        finally:
            self.connection.close()
