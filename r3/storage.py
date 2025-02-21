"""Storage component for R3 repositories."""

import os
import shutil
import stat
import tempfile
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

import yaml
from executor import execute

from r3.job import Dependency, GitDependency, Job, JobDependency


class Storage:
    def __init__(self, root: Union[str, os.PathLike]) -> None:
        """Initializes a storage.

        Parameters:
            root: The root directory of the storage (the repository root).
        """
        self.root = Path(root).resolve()

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

    def get(
        self,
        job_id: str,
        cached_timestamp: Optional[datetime] = None,
        cached_metadata: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """Retrieves a job from the storage.

        Parameters:
            job_id: The ID of the job to retrieve.
            cached_timestamp: The timestamp of the job to retrieve, if available in the
                cache.
            cached_metadata: The metadata of the job to retrieve, if available in the
                cache.

        Returns:
            The job with the given ID.
        """
        if job_id not in self:
            raise FileNotFoundError(f"Job not found: {job_id}")

        return Job(
            self.root / "jobs" / job_id,
            job_id,
            cached_timestamp=cached_timestamp,
            cached_metadata=cached_metadata,
        )

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

        job.timestamp = datetime.now()
        job.hash(recompute=True)

        for dependency in job.dependencies:
            if isinstance(dependency, GitDependency):
                repository_path = self.root / dependency.repository_path
                execute(
                    f"git tag r3/{job_id} {dependency.commit}",
                    directory=repository_path,
                )

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

    def checkout(
        self, item: Union[Job, Dependency], path: Union[str, os.PathLike]
    ) -> None:
        """Checks out a job or dependency to a destination directory.

        Parameters:
            item: The job or dependency to check out.
            path: The directory to check out the item to.
        """
        if not item.is_resolved():
            raise ValueError(f"Cannot checkout unresolved item: {item}")

        if isinstance(item, Job):
            self.checkout_job(item, path)
        elif isinstance(item, JobDependency):
            self.checkout_job_dependency(item, path)
        elif isinstance(item, GitDependency):
            self.checkout_git_dependency(item, path)
        else:
            raise TypeError(
                f"Expected Job, JobDependency or GitDependency, got {type(item)}"
            )

    def checkout_job(self, job: Job, destination: Union[str, os.PathLike]) -> None:
        """Checks out a job to a destination directory.

        Parameters:
            job: The job to check out.
            destination: The directory to check out the job to.
        """
        if job not in self:
            raise FileNotFoundError(f"Cannot find job: {job.path}")

        destination = Path(destination)
        os.makedirs(destination)

        for child in job.path.iterdir():
            if child.name not in ["r3.yaml", "metadata.yaml", "output"]:
                if child.is_dir():
                    shutil.copytree(child, destination / child.name)
                else:
                    shutil.copy(child, destination / child.name)

        os.symlink(job.path / "output", destination / "output")

        for dependency in job.dependencies:
            self.checkout(dependency, destination)

    def checkout_job_dependency(
        self, dependency: JobDependency, destination: Union[str, os.PathLike]
    ) -> None:
        """Checks out a job dependency to a destination directory.

        Parameters:
            dependency: The job dependency to check out.
            destination: The directory to check out the job dependency to.
        """
        destination = destination / dependency.destination

        if str(dependency.source) == "." and dependency.recursive_checkout:
            job = self.get(dependency.job)
            self.checkout_job(job, destination)
            return

        source = self.root / "jobs" / dependency.job / dependency.source

        os.makedirs(destination.parent, exist_ok=True)
        os.symlink(source, destination)

    def checkout_git_dependency(
        self, dependency: GitDependency, destination: Union[str, os.PathLike]
    ) -> None:
        """Checks out a git dependency to a destination directory.

        Parameters:
            dependency: The git dependency to check out.
            destination: The directory to check out the git dependency to.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            git_version_str = execute("git --version", capture=True).rsplit(" ", 1)[-1]
            git_version = tuple(int(part) for part in git_version_str.split("."))

            if git_version < (2, 5):
                warnings.warn(
                    f"Git is outdated ({git_version_str}). Falling back to cloning the "
                    "entire repository for git dependencies.",
                    stacklevel=1,
                )
                clone_path = Path(tempdir) / "clone"
                execute(
                    f"git clone {self.root / dependency.repository_path} {clone_path}"
                )
                execute(
                    f"git checkout {dependency.commit}", directory=clone_path
                )
                shutil.move(
                    clone_path / dependency.source,
                    destination / dependency.destination,
                )

            else:
                # https://stackoverflow.com/a/43136160
                origin = str(self.root / dependency.repository_path)
                commands = " && ".join([
                    "git init",
                    f"git remote add origin {origin}",
                    f"git fetch --depth=1 origin {dependency.commit}",
                    "git checkout FETCH_HEAD",
                ])
                execute(commands, directory=tempdir)
                shutil.move(
                    Path(tempdir) / dependency.source,
                    destination / dependency.destination,
                )

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
