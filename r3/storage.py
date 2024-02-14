"""Storage component for R3 repositories."""

import os
import shutil
import stat
import uuid
from pathlib import Path
from typing import Iterator, Union

import yaml

from r3.job import Job


class Storage:
    def __init__(self, root: Union[str, os.PathLike]) -> None:
        """Initializes a storage.
        
        Parameters:
            root: The root directory of the storage (the repository root).
        """
        self.root = Path(root)

        if not self.root.exists():
            raise FileNotFoundError(f"Root directory does not exist: {self.root}")

        if not self.root.is_dir():
            raise NotADirectoryError(f"Root path is not a directory: {self.root}")

    @staticmethod
    def init(root: Union[str, os.PathLike]) -> "Storage":
        """Initializes a new storage at the given root directory.
        
        Parameters:
            root: The root directory of the storage (the repository root).
        
        Returns:
            The initialized storage.
        """
        root = Path(root)
        os.makedirs(root / "git")
        os.makedirs(root / "jobs")
        return Storage(root)

    def __contains__(self, job_or_job_id: Union[Job, str]) -> bool:
        """Checks whether a job is in the storage.
        
        Parameters:
            job_or_job_id: The job or job ID to check for.
        """
        if isinstance(job_or_job_id, str):
            return (self.root / "jobs" / job_or_job_id).exists()

        if isinstance(job_or_job_id, Job):
            job_path = job_or_job_id.path.resolve()
            return job_path.parent.parent == self.root

        raise TypeError(f"Expected Job or str, got {type(job_or_job_id)}")

    def get(self, job_id: str) -> Job:
        """Retrieves a job from the storage.
        
        Parameters:
            job_id: The ID of the job to retrieve.
        
        Returns:
            The job with the given ID.
        """
        if job_id not in self:
            raise FileNotFoundError(f"Job not found: {job_id}")
        return Job(self.root / "jobs" / job_id, job_id)

    def jobs(self) -> Iterator[Job]:
        """Returns an iterator over all jobs in the storage."""
        for path in (self.root / "jobs").iterdir():
            if path.is_dir():
                yield Job(path, path.name)

    def add(self, job: Job) -> Job:
        """Adds a job to the storage.
        
        This method does not check whether all dependencies of the job are satisfied but
        copies the job to the storage as is.

        Parameters:
            job: The job to add to the storage.
        
        Returns:
            The job with updated path and ID.
        """
        job_id = str(uuid.uuid4())

        job_path = self.root / "jobs" / job_id
        if job_path.exists():
            raise FileExistsError(f"Congrats, you found a UUID collision: {job_id}")

        job.hash(recompute=True)

        os.mkdir(job_path)
        os.mkdir(job_path / "output")

        with open(job_path / "r3.yaml", "w") as config_file:
            # REVIEW: Any way to avoid using the private attribute?
            yaml.dump(job._config, config_file)
        _remove_write_permissions(job_path / "r3.yaml")

        with open(job_path / "metadata.yaml", "w") as metadata_file:
            yaml.dump(job.metadata, metadata_file)

        for destination, source in job.files.items():
            if destination in [Path("r3.yaml"), Path("metadata.yaml")]:
                continue

            target = job_path / destination

            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(source, target)
            _remove_write_permissions(target)

        _remove_write_permissions(job_path)

        return Job(job_path, job_id)

    def remove(self, job: Job) -> None:
        """Removes a job from the storage.

        This method does not check whether the job is still referenced by other jobs but
        removes the job from the storage as is.

        Parameters:
            job: The job to remove from the storage.
        """
        if job not in self:
            raise FileNotFoundError(f"Job not found: {job}")
    
        for path in job.files:
            _add_write_permission(job.path / path)
        _add_write_permission(job.path)

        shutil.rmtree(job.path)

    def __repr__(self):
        """Returns a string representation of the storage."""
        return f"Storage({self.root})"


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)


def _add_write_permission(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode | stat.S_IWOTH | stat.S_IWGRP | stat.S_IWUSR
    os.chmod(path, mode)
