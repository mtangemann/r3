"""Job index for efficient searching."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

from r3.job import Job, JobDependency
from r3.query import mongo_to_sql
from r3.storage import Storage


class Index:
    """Job index for efficient searching."""

    def __init__(self, storage: Storage) -> None:
        """Initializes the index.
        
        Parameters:
            storage: The storage with the jobs to index.
        """
        self.storage = storage
        self._path = storage.root / "index.sqlite"

        if not self._path.exists():
            self.rebuild()

    def rebuild(self) -> None:
        """Rebuilds the index from the storage."""
        if self._path.exists():
            self._path.unlink()

        with Transaction(self._path) as transaction:
            transaction.execute(
                """
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    metadata JSON NOT NULL
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

            job_data = []
            job_dependency_data: list[tuple[str, str]] = []

            for job in self.storage.jobs():
                assert job.id is not None
                assert job.timestamp is not None

                job_data.append(
                    (job.id, job.timestamp.isoformat(), json.dumps(job.metadata))
                )

                job_dependency_data.extend(
                    (job.id, dependency.job)
                    for dependency in job.dependencies
                    if isinstance(dependency, JobDependency)
                )

            transaction.executemany(
                "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)",
                job_data,
            )
            transaction.executemany(
                "INSERT INTO job_dependencies (child_id, parent_id) VALUES (?, ?)",
                job_dependency_data,
            )

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
                "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)",
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

    def get(self, job_id: str) -> Job:
        """Gets a job by ID.
        
        Parameters:
            job_id: The ID of the job to get.
        
        Returns:
            The job with the given ID.
        """
        with Transaction(self._path) as transaction:
            transaction.execute(
                "SELECT timestamp, metadata FROM jobs WHERE id = ?",
                (job_id,)
            )
            result = transaction.fetchone()

        if result is None:
            raise KeyError(f"Job not found: {job_id}")

        cached_timestamp = datetime.fromisoformat(result[0])
        cached_metadata = json.loads(result[1])
        return self.storage.get(job_id, cached_timestamp, cached_metadata)
    
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

        with Transaction(self._path) as transaction:
            transaction.execute(
                "DELETE FROM jobs WHERE id = ?",
                (job.id,)
            )
            transaction.execute(
                "DELETE FROM job_dependencies WHERE child_id = ? OR parent_id = ?",
                (job.id, job.id)
            )

    def find(self, query: Dict[str, Any], latest: bool = False) -> List[Job]:
        """Finds jobs by tags.
        
        Parameters:
            query: The query to match jobs against. The query is specified as a
                MongoDB-style query document.
            latest: Whether to return the latest job or all jobs with the given tags.

        Returns:
            The jobs that match the given query.
        """
        sql_query = f"SELECT id, timestamp, metadata FROM jobs WHERE {mongo_to_sql(query)}"  # noqa: E501
        if latest:
            sql_query += " ORDER BY timestamp DESC LIMIT 1"

        with Transaction(self._path) as transaction:
            transaction.execute(sql_query)
            results = transaction.fetchall()

        jobs = []
        for result in results:
            job_id = result[0]
            cached_timestamp = datetime.fromisoformat(result[1])
            cached_metadata = json.loads(result[2])
            jobs.append(self.storage.get(job_id, cached_timestamp, cached_metadata))
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
                """SELECT child_id, timestamp, metadata
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

            dependent_job = self.storage.get(job_id, cached_timestamp, cached_metadata)
            dependents[dependent_job.id] = dependent_job

            if recursive:
                dependents.update({
                    job.id: job
                    for job in self.find_dependents(dependent_job, recursive=True)
                })

        return set(dependents.values())


class Transaction:
    def __init__(self, path: Path) -> None:
        self.path = str(path)

    def __enter__(self) -> sqlite3.Cursor:
        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.connection.commit()
        self.connection.close()
