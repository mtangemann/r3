"""R3 core functionality.

This module provides the core functionality of R3. This module should not be used
directly, but rather the public API exported by the top-level ``r3`` module.
"""

import os
import shutil
import stat
import tempfile
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import yaml
from executor import execute

import r3
import r3.utils
from r3.job import (
    Dependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)

R3_FORMAT_VERSION = "1.0.0-beta.5"

DATE_FORMAT = r"%Y-%m-%d %H:%M:%S"


class Repository:
    def __init__(self, path: Union[str, os.PathLike]) -> None:
        """Initializes the repository instance.

        Raises:
            FileNotFoundError: If the given path does not exist.
            NotADirectoryError: If the given path exists but is not a directory.
        """
        self.path = Path(path)

        if not self.path.exists():
            raise FileNotFoundError(f"No such directory: {self.path}")

        if not self.path.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.path}")

        if not (self.path / "r3.yaml").exists():
            raise ValueError(f"Invalid repository: {self.path}")

        self._index_path: Path = self.path / "index.yaml"
        self.__index: Dict[str, Dict[str, Union[str, List[str]]]] | None = None

    @staticmethod
    def init(path: Union[str, os.PathLike]) -> "Repository":
        """Creates a repository at the given path.

        Raises:
            FileExistsError: If the given path exists alreay.
        """
        path = Path(path)

        if path.exists():
            raise FileExistsError(f"Path exists already: {path}")

        os.makedirs(path)
        os.makedirs(path / "git")
        os.makedirs(path / "jobs")

        r3config = {"version": R3_FORMAT_VERSION}

        with open(path / "r3.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

        return Repository(path)

    def jobs(self) -> Iterable[Job]:
        """Returns an iterator over all jobs in this repository."""
        for path in (self.path / "jobs").iterdir():
            yield Job(path, path.name)

    def commit(self, job: Job) -> Job:
        job = self.resolve(job)  # type: ignore
        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        job_id = uuid.uuid4()
        target_path = self.path / "jobs" / str(job_id)

        job.hash(recompute=True)

        if "committed_at" in job.metadata:
            warnings.warn("Overwriting `committed_at` in job metadata.", stacklevel=2)
        job.metadata["committed_at"] = datetime.now().strftime(DATE_FORMAT)

        os.makedirs(target_path)
        os.makedirs(target_path / "output")

        with open(target_path / "r3.yaml", "w") as config_file:
            yaml.dump(job._config, config_file)
        _remove_write_permissions(target_path / "r3.yaml")

        with open(target_path / "metadata.yaml", "w") as metadata_file:
            yaml.dump(job.metadata, metadata_file)

        for destination, source in job.files.items():
            if destination in [Path("r3.yaml"), Path("metadata.yaml")]:
                continue

            target = target_path / destination

            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(source, target)
            _remove_write_permissions(target)

        _remove_write_permissions(target_path)

        committed_job = Job(target_path, target_path.name)

        self._add_job_to_index(committed_job)
        self._save_index()

        return committed_job

    def checkout(
        self, item: Union[Dependency, Job], path: Union[str, os.PathLike]
    ) -> None:
        path = Path(path)

        if isinstance(item, Job):
            return self._checkout_job(item, path)
        if isinstance(item, QueryDependency):
            item = self.resolve(item)  # type: ignore
        if isinstance(item, JobDependency):
            return self._checkout_job_dependency(item, path)
        if isinstance(item, GitDependency):
            return self._checkout_git_dependency(item, path)

    def _checkout_job(self, job: Job, path: Path) -> None:
        if job not in self:
            raise FileNotFoundError(f"Cannot find job: {job.path}")

        if job.path is None:
            raise RuntimeError("Job is committed but doesn't have a path.")

        os.makedirs(path)

        # Copy files
        for child in job.path.iterdir():
            if child.name not in ["r3.yaml", "metadata.yaml", "output"]:
                if child.is_dir():
                    shutil.copytree(child, path / child.name)
                else:
                    shutil.copy(child, path / child.name)

        # Symlink output directory
        os.symlink(job.path / "output", path / "output")

        for dependency in job.dependencies:
            self.checkout(dependency, path)

    def _checkout_job_dependency(self, dependency: JobDependency, path: Path) -> None:
        source = self.path / "jobs" / dependency.job / dependency.source
        destination = path / dependency.destination

        os.makedirs(destination.parent, exist_ok=True)
        os.symlink(source, destination)

    def _checkout_git_dependency(self, dependency: GitDependency, path: Path) -> None:
        origin = str(self.path / dependency.repository_path / ".git")

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
                    f"git clone {self.path / dependency.repository_path} {clone_path}"
                )
                execute(
                    f"git checkout {dependency.commit}", directory=clone_path
                )
                shutil.move(
                    clone_path / dependency.source,
                    path / dependency.destination,
                )

            else:
                # https://stackoverflow.com/a/43136160
                commands = " && ".join([
                    "git init",
                    f"git remote add origin {origin}",
                    f"git fetch --depth=1 origin {dependency.commit}",
                    "git checkout FETCH_HEAD",
                ])
                execute(commands, directory=tempdir)
                shutil.move(
                    Path(tempdir) / dependency.source,
                    path / dependency.destination,
                )

    def remove(self, job: Job) -> None:
        if job not in self:
            raise ValueError("Job is not contained in this repository.")

        assert job.id is not None

        for job_id, metadata in self._index.items():
            for dependency in metadata.get("dependencies", []):
                if dependency.get("job", None) == job.id:
                    raise ValueError(f"Another job depends on this job: {job_id}")

        for path in job.files:
            _add_write_permission(job.path / path)
        _add_write_permission(job.path)

        shutil.rmtree(job.path)

        del self._index[job.id]
        self._save_index()

    def __contains__(self, item: Union[Job, Dependency]) -> bool:
        """Checks if the given item is contained in this repository."""
        if isinstance(item, Job):
            return (
                item.id is not None and (self.path / "jobs" / item.id).is_dir()
            )

        if isinstance(item, QueryDependency):
            item = self.resolve(item)  # type: ignore

        if isinstance(item, JobDependency):
            return (self.path / "jobs" / item.job / item.source).exists()

        if isinstance(item, GitDependency):
            return r3.utils.git_path_exists(
                self.path / item.repository_path, item.commit, item.source
            )

        return False

    def find(self, tags: Iterable[str], latest: bool = False) -> List[Job]:
        """Searches for jobs with the given tags.

        Parameters:
            tags: Return jobs that include all of this tags.
            latest: If true, only return the latest matching job. Otherwise, return all
                jobs.

        Returns:
            List of job matching the search parameters.
        """
        tags = set(tags)
        results = []

        for job_id, metadata in self._index.items():
            if tags.issubset(metadata["tags"]):
                results.append(Job(self.path / "jobs" / job_id, job_id))

        if latest:
            return [max(results, key=lambda job: job.datetime or datetime.min)]
        else:
            return sorted(results, key=lambda job: job.datetime or datetime.min)

    @property
    def _index(self) -> Dict[str, Any]:
        if self.__index is None:
            if self._index_path.exists():
                with open(self._index_path, "r") as index_file:
                    self.__index = yaml.safe_load(index_file)
            else:
                self.__index = dict()

        return self.__index

    @_index.setter
    def _index(self, index: Dict[str, Any]) -> None:
        self.__index = index

    def _save_index(self) -> None:
        with open(self._index_path, "w") as index_file:
            yaml.dump(self._index, index_file)

    def _add_job_to_index(self, job: "Job") -> None:
        if job.id is None:
            raise ValueError("Job id not set. Cannot add to index.")

        assert job.datetime is not None

        self._index[job.id] = {
            "tags": job.metadata.get("tags", []),
            "datetime": job.datetime.strftime(DATE_FORMAT),
            "dependencies": job._config["dependencies"],
        }

    def rebuild_index(self):
        """Rebuilds the job index.

        The job index is used to efficiently query for jobs. The index is automatically
        updated when committing job, so explicitely calling this should not be
        necessary.
        """
        self._index = dict()

        for job in self.jobs():
            self._add_job_to_index(job)

        self._save_index()

    def resolve(
        self,
        item: Union[Job, Dependency],
    ) -> Union[Job, Dependency, List[JobDependency]]:
        if isinstance(item, Job):
            return self._resolve_job(item)
        if isinstance(item, QueryDependency):
            return self._resolve_query_dependency(item)
        if isinstance(item, QueryAllDependency):
            return self._resolve_query_all_dependency(item)

        raise ValueError(f"Cannot resolve {item}")

    def _resolve_job(self, job: Job) -> Job:
        if not isinstance(job.dependencies, list):
            raise ValueError("Dependencies are not writeable.")

        resolved_dependencies = []

        for index in range(len(job.dependencies)):
            if isinstance(job.dependencies[index], QueryDependency):
                dependency = self._resolve_query_dependency(job.dependencies[index])
                resolved_dependencies.append(dependency)
            
            elif isinstance(job.dependencies[index], QueryAllDependency):
                dependencies = self._resolve_query_all_dependency(
                    job.dependencies[index]
                )
                resolved_dependencies.extend(dependencies)

            else:
                resolved_dependencies.append(job.dependencies[index])

        job._dependencies = resolved_dependencies
        job._config["dependencies"] = [  # type: ignore
            dependency.to_config() for dependency in job.dependencies
        ]
        return job

    def _resolve_query_dependency(
        self,
        dependency: QueryDependency,
    ) -> JobDependency:
        tags = dependency.query.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query}")

        tags = [tag[1:] for tag in tags]
        result = self.find(tags, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        return JobDependency(
            result[0], dependency.destination, dependency.source, dependency.query
        )

    def _resolve_query_all_dependency(
        self,
        dependency: QueryAllDependency,
    ) -> List[JobDependency]:
        tags = dependency.query_all.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query_all}")

        tags = [tag[1:] for tag in tags]
        result = self.find(tags)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query_all}")

        resolved_dependencies = []
        for job in result:
            assert job.id is not None
            resolved_dependencies.append(JobDependency(
                job, dependency.destination, query_all=dependency.query_all)
            )

        return resolved_dependencies


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)


def _add_write_permission(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode | stat.S_IWOTH | stat.S_IWGRP | stat.S_IWUSR
    os.chmod(path, mode)
