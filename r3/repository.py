"""High-level interface to R3 repositories.

The `Repository` class should be imported not from this module but from the top-level
`r3` package.
"""

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Union

import yaml
from executor import execute

import r3
import r3.utils
from r3.index import Index
from r3.job import (
    Dependency,
    FindAllDependency,
    FindLatestDependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.storage import Storage

R3_FORMAT_VERSION = "1.0.0-beta.7"


class Repository:
    """A repository of jobs."""

    def __init__(self, path: Union[str, os.PathLike]) -> None:
        """Initializes the repository instance.

        Parameters:
            path: The path to the repository.

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

        with open(self.path / "r3.yaml") as config_file:
            config = yaml.safe_load(config_file)
            if config["version"] != R3_FORMAT_VERSION:
                raise ValueError(
                    f"Invalid repository version: {config['version']}. Please migrate "
                    f"to {R3_FORMAT_VERSION}."
                )

        self._storage = Storage(self.path)
        self._index = Index(self._storage)

    @staticmethod
    def init(path: Union[str, os.PathLike]) -> "Repository":
        """Creates a new repository at the given path.

        Returns:
            The newly created repository.

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

    def __contains__(self, item: Union[Job, Dependency]) -> bool:
        """Checks whether a job or dependency is contained in this repository.
        
        Parameters:
            item: The job or dependency to check for.
        
        Returns:
            Whether the given job or dependency is contained in this repository.
        """
        if isinstance(item, Job):
            return item in self._storage

        try:
            resolved_item = self.resolve(item)
        except ValueError:
            return False

        if isinstance(resolved_item, list):
            return all(dependency in self for dependency in resolved_item)

        if isinstance(resolved_item, JobDependency):
            target = self.path / "jobs" / resolved_item.job / resolved_item.source
            return target.exists()

        if isinstance(resolved_item, GitDependency):
            assert resolved_item.commit is not None
            repository_path = self.path / resolved_item.repository_path

            if not repository_path.exists():
                execute(
                    f"git clone --bare {resolved_item.repository} {repository_path}"
                )

            if not r3.utils.git_commit_exists(repository_path, resolved_item.commit):
                execute("git fetch origin *:* --force", directory=repository_path)

            return r3.utils.git_path_exists(
                repository_path,
                resolved_item.commit,
                resolved_item.source,
            )

        return False

    def commit(self, job: Job) -> Job:
        """Commits a job to the repository.
        
        Parameters:
            job: The job to commit.
        
        Returns:
            The committed job. Compared to the original job, the returned job has an id
            and the path is changed to the location in the repository.
        """
        job = self.resolve(job)  # type: ignore

        # REVIEW It would be nice if `resolve` would check whether the dependencies
        #        exist in the repository.
        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        job = self._storage.add(job)
        self._index.add(job)

        return job

    def checkout(
        self, item: Union[Dependency, Job], path: Union[str, os.PathLike]
    ) -> None:
        """Checks out a job or dependency to the given path.
        
        Parameters:
            item: The job or dependency to check out.
            path: The path to check out the job or dependency to.
        """
        resolved_item = self.resolve(item)

        if isinstance(resolved_item, list):
            for dependency in resolved_item:
                self._storage.checkout(dependency, path)
        else:
            self._storage.checkout(resolved_item, path)

    def remove(self, job: Job) -> None:
        """Removes a job from the repository.
        
        Parameters:
            job: The job to remove.
        
        Raises:
            ValueError: If the job is not contained in this repository or if other jobs
                depend on it.
        """
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

    def find(self, query: Dict[str, Any], latest: bool = False) -> List[Job]:
        """Finds jobs by a query.
        
        Parameters:
            query: The mongo-style query document to find jobs by.
            latest: Whether to return the latest job or all jobs with the given tags.

        Returns:
            The jobs that match the given tags.
        """
        return self._index.find(query, latest)

    def find_dependents(self, job: Job, recursive: bool = False) -> Set[Job]:
        """Finds jobs that depend on the given job.
        
        Parameters:
            job: The job to find dependents for.
            recursive: Whether to find dependents recursively.
        
        Returns:
            The jobs that depend on the given job.
        """
        return self._index.find_dependents(job, recursive)

    def rebuild_index(self):
        """Rebuilds the job index.

        The job index is used to efficiently query for jobs. The index is automatically
        updated when jobs are added or removed. This method has to be called manually
        if the metadata file of a job has been updated manually.
        """
        self._index.rebuild()

    def resolve(
        self,
        item: Union[Job, Dependency],
    ) -> Union[Job, Dependency, List[JobDependency]]:
        """Resolves a job or dependency.
        
        A job or dependency is resolved by replacing query dependencies with concrete
        dependencies.

        Parameters:
            item: The job or dependency to resolve.
        
        Returns:
            The resolved job or dependency. A query dependency might resolve to multiple
            concrete dependencies, in which case a list of dependencies is returned.
        """
        if item.is_resolved():
            return item

        if isinstance(item, Job):
            return self._resolve_job(item)
        if isinstance(item, FindLatestDependency):
            return self._resolve_find_latest_dependency(item)
        if isinstance(item, FindAllDependency):
            return self._resolve_find_all_dependency(item)
        if isinstance(item, QueryDependency):
            return self._resolve_query_dependency(item)
        if isinstance(item, QueryAllDependency):
            return self._resolve_query_all_dependency(item)
        if isinstance(item, GitDependency):
            return self._resolve_git_dependency(item)

        raise ValueError(f"Cannot resolve {item}")

    def _resolve_job(self, job: Job) -> Job:
        if not isinstance(job.dependencies, list):
            raise ValueError("Dependencies are not writeable.")

        resolved_dependencies: List[Dependency] = []

        for dependency in job.dependencies:
            resolved_dependency = self.resolve(dependency)
            if isinstance(resolved_dependency, list):
                resolved_dependencies.extend(resolved_dependency)
            else:
                assert isinstance(resolved_dependency, Dependency)
                resolved_dependencies.append(resolved_dependency)

        job._dependencies = resolved_dependencies
        job._config["dependencies"] = [  # type: ignore
            dependency.to_config() for dependency in job.dependencies
        ]
        return job
    
    def _resolve_find_latest_dependency(
        self,
        dependency: FindLatestDependency,
    ) -> JobDependency:
        result = self.find(dependency.query, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        return JobDependency(
            destination=dependency.destination,
            job=result[0],
            source=dependency.source,
            find_latest=dependency.query,
        )

    def _resolve_find_all_dependency(
        self, dependency: FindAllDependency
    ) -> List[JobDependency]:
        result = self.find(dependency.query)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        resolved_dependencies = []
        for job in result:
            assert job.id is not None
            resolved_dependencies.append(JobDependency(
                dependency.destination / job.id, job, find_all=dependency.query
            ))

        return resolved_dependencies

    def _resolve_query_dependency(
        self,
        dependency: QueryDependency,
    ) -> JobDependency:
        tags = dependency.query.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query}")

        tags = [tag[1:] for tag in tags]
        query = { "tags": { "$all": tags } }
        result = self.find(query, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        return JobDependency(
            dependency.destination, result[0], dependency.source, query=dependency.query
        )

    def _resolve_query_all_dependency(
        self,
        dependency: QueryAllDependency,
    ) -> List[JobDependency]:
        tags = dependency.query_all.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query_all}")

        tags = [tag[1:] for tag in tags]
        query = { "tags": { "$all": tags } }
        result = self.find(query)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query_all}")

        resolved_dependencies = []
        for job in result:
            assert job.id is not None
            resolved_dependencies.append(JobDependency(
                dependency.destination / job.id, job, query_all=dependency.query_all)
            )

        return resolved_dependencies

    def _resolve_git_dependency(self, dependency: GitDependency) -> GitDependency:
        repository_path = self.path / dependency.repository_path
        if not repository_path.exists():
            execute(f"git clone --bare {dependency.repository} {repository_path}")
        
        if dependency.branch is not None:
            commit = r3.utils.git_get_remote_branch_head(
                repository_path, dependency.branch
            )
            if commit is None:
                raise ValueError(f"Branch not found: {dependency.branch}")
        elif dependency.tag is not None:
            commit = r3.utils.git_get_remote_tag_head(repository_path, dependency.tag)
            if commit is None:
                raise ValueError(f"Tag not found: {dependency.tag}")
        else:
            commit = r3.utils.git_get_remote_head(repository_path)
        
        return GitDependency(
            dependency.destination,
            dependency.repository,
            commit,
            source=dependency.source,
        )
