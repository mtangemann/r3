"""R3 core functionality.

This module provides the core functionality of R3. This module should not be used
directly, but rather the public API exported by the top-level ``r3`` module.
"""

import os
import warnings
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Union

import yaml

import r3
import r3.utils
from r3.index import Index
from r3.job import (
    Dependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.storage import Storage

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

        self._storage = Storage(self.path)
        self._index = Index(self._storage)

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
        Storage.init(path)

        r3config = {"version": R3_FORMAT_VERSION}

        with open(path / "r3.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

        return Repository(path)

    def jobs(self) -> Iterable[Job]:
        """Returns an iterator over all jobs in this repository."""
        yield from self._storage.jobs()

    def commit(self, job: Job) -> Job:
        job = self.resolve(job)  # type: ignore
        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        if "committed_at" in job.metadata:
            warnings.warn("Overwriting `committed_at` in job metadata.", stacklevel=2)
        job.metadata["committed_at"] = datetime.now().strftime(DATE_FORMAT)

        job = self._storage.add(job)
        self._index.add(job)

        return job

    def checkout(
        self, item: Union[Dependency, Job], path: Union[str, os.PathLike]
    ) -> None:
        path = Path(path)
        resolved_item = self.resolve(item)

        if isinstance(resolved_item, list):
            for dependency in resolved_item:
                self._storage.checkout(dependency, path)
        else:
            self._storage.checkout(resolved_item, path)

    def remove(self, job: Job) -> None:
        if job not in self:
            raise ValueError("Job is not contained in this repository.")

        assert job.id is not None

        dependents = self._index.find_dependents(job)
        if len(dependents) > 0:
            raise ValueError(
                "Cannot remove job since other jobs depend on it: \n"
                "\n".join(f"  - {dependent.id}" for dependent in dependents)
            )

        self._storage.remove(job)
        self._index.remove(job)

    def __contains__(self, item: Union[Job, Dependency]) -> bool:
        """Checks if the given item is contained in this repository."""
        if isinstance(item, Job):
            return item in self._storage

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
        """Finds jobs by tags.
        
        Parameters:
            tags: The tags to search for. Jobs are matched if they contain all the given
                tags.
            latest: Whether to return the latest job or all jobs with the given tags.

        Returns:
            The jobs that match the given tags.
        """
        return self._index.find(tags, latest)

    def rebuild_index(self):
        """Rebuilds the job index.

        The job index is used to efficiently query for jobs. The index is automatically
        updated when jobs are added or removed. This method has to be called manually
        if the metadata of a job is changed.
        """
        self._index.rebuild()

    def resolve(
        self,
        item: Union[Job, Dependency],
    ) -> Union[Job, Dependency, List[JobDependency]]:
        if item.is_resolved():
            return item

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
