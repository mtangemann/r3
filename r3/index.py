"""Job index for efficient searching."""

import datetime
from typing import Any, Dict, Iterable, List, Optional, Set

import yaml

from r3.job import Job
from r3.storage import Storage

DATE_FORMAT = r"%Y-%m-%d %H:%M:%S"


class Index:
    """Job index for efficient searching."""

    def __init__(self, storage: Storage) -> None:
        """Initializes the index.
        
        Parameters:
            storage: The storage with the jobs to index.
        """
        self.storage = storage
        self._path = storage.root / "index.yaml"
        self.__entries: Optional[Dict[str, Any]] = None

    @property
    def _entries(self) -> Dict[str, Any]:
        if self.__entries is None:
            if self._path.exists():
                with open(self._path, "r") as index_file:
                    self.__entries = yaml.safe_load(index_file)
            else:
                self.__entries = dict()

        return self.__entries

    @_entries.setter
    def _entries(self, entries: Dict[str, Any]) -> None:
        self.__entries = entries

    def add(self, job: Job, save: bool = True) -> None:
        """Adds a job to the index.
        
        Parameters:
            job: The job to add.
            save: Whether to save the index to disk after adding the job.
        """
        if job not in self.storage:
            raise ValueError(f"Job not in storage: {job}")

        # Both should be set for jobs in the storage.
        assert job.id is not None
        assert job.datetime is not None

        self._entries[job.id] = {
            "tags": job.metadata.get("tags", []),
            "datetime": job.datetime.strftime(DATE_FORMAT),
            "dependencies": [
                dependency.to_config() for dependency in job.dependencies
            ],
        }

        if save:
            self.save()
    
    def remove(self, job: Job, save: bool = True) -> None:
        """Removes a job from the index.
        
        Parameters:
            job: The job to remove.
            save: Whether to save the index to disk after removing the job.
        """
        if job.id is None:
            raise ValueError("Job ID is not set")

        if job.id in self._entries:
            del self._entries[job.id]

        if save:
            self.save()

    def find(self, tags: Iterable[str], latest: bool = False) -> List[Job]:
        """Finds jobs by tags.
        
        Parameters:
            tags: The tags to search for. Jobs are matched if they contain all the given
                tags.
            latest: Whether to return the latest job or all jobs with the given tags.

        Returns:
            The jobs that match the given tags.
        """
        jobs = list()

        for job_id, job_info in self._entries.items():
            if set(tags).issubset(set(job_info["tags"])):
                jobs.append(self.storage.get(job_id))

        if latest:
            def key(job: Job) -> datetime.datetime:
                assert job.datetime is not None
                return job.datetime
            jobs = sorted(jobs, key=key, reverse=True)
            jobs = [jobs[0]] if len(jobs) > 0 else []

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

        dependents = dict()

        for job_id, job_info in self._entries.items():
            for dependency in job_info["dependencies"]:
                if "job" in dependency and dependency["job"] == job.id:
                    dependents[job_id] = self.storage.get(job_id)

                    if recursive:
                        indirect_dependents = self.find_dependents(
                            dependents[job_id], recursive=True
                        )
                        dependents.update({
                            dependent.id: dependent  # type: ignore
                            for dependent in indirect_dependents
                        })

        return set(dependents.values())

    def rebuild(self) -> None:
        """Rebuilds the index from the storage."""
        self._entries = dict()

        for job in self.storage.jobs():
            self.add(job, save=False)

        self.save()

    def save(self) -> None:
        """Saves the index to disk."""
        with open(self._path, "w") as index_file:
            yaml.dump(self._entries, index_file)
