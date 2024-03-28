import abc
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Union

import yaml

import r3.utils


class Job:
    """A computational job."""

    def __init__(self, path: Union[str, os.PathLike], id: Optional[str] = None) -> None:
        """Initializes a job instance.

        Parameters:
            path: Path to the job's root directory.
            id: Job id for committed jobs. This is set automatically for jobs retrieved
                from a repository.
        """
        self._path = Path(path).absolute()
        self.id = id

        self._metadata: Optional[Dict[str, Any]] = None
        self._files: Optional[Dict[Path, Path]] = None
        self.__config: Optional[Dict[str, Any]] = None
        self._dependencies: Optional[Sequence["Dependency"]] = None
        self._hash: Optional[str] = None

    @property
    def path(self) -> Path:
        """Path to the job's root directory."""
        return self._path

    @property
    def metadata(self) -> Dict[str, Any]:
        """Job metadata.

        Changes to this dictionary are not automatically written to the job's metadata
        file. Use `save_metadata` to save changes to the metadata file.
        """
        if self._metadata is None:
            if (self.path / "metadata.yaml").is_file():
                with open(self.path / "metadata.yaml", "r") as metadata_file:
                    self._metadata = yaml.safe_load(metadata_file)
            else:
                self._metadata = dict()

        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Dict[str, Any]) -> None:
        self._metadata = metadata

    def save_metadata(self) -> None:
        """Saves the job metadata to the metadata file.
        
        This method has to be called after modifying the metadata dictionary.
        """
        with open(self.path / "metadata.yaml", "w") as metadata_file:
            yaml.dump(self.metadata, metadata_file)

    @property
    def timestamp(self) -> Optional[datetime]:
        """Returns the date and time when this job was committed.
        
        Returns:
            A datetime object representing the date and time when this job was
            committed. If the job is not committed, this returns `None`.
        """
        if "timestamp" in self._config:
            return datetime.fromisoformat(self._config["timestamp"])
        
        return None

    @timestamp.setter
    def timestamp(self, timestamp: datetime) -> None:
        self._config["timestamp"] = timestamp.isoformat()

    # REVIEW: Replace with a method that returns an iterator?
    @property
    def files(self) -> Mapping[Path, Path]:
        """Files belonging to this job."""
        if self._files is None:
            ignore = self._config.get("ignore", [])

            for dependency in self.dependencies:
                ignore.append(f"/{dependency.destination}")

            self._files = {
                file: (self.path / file).absolute()
                for file in r3.utils.find_files(self.path, ignore)
            }

        return self._files

    @property
    def dependencies(self) -> Sequence["Dependency"]:
        """Dependencies of this job."""
        if self._dependencies is None:
            self._dependencies = [
                Dependency.from_config(config)
                for config in self._config["dependencies"]
            ]

        return self._dependencies

    def is_resolved(self) -> bool:
        """Returns `True` if all dependencies are resolved."""
        return all(dependency.is_resolved() for dependency in self.dependencies)

    @property
    def _config(self) -> Dict[str, Any]:
        if self.__config is None:
            if (self.path / "r3.yaml").is_file():
                with open(self.path / "r3.yaml", "r") as config_file:
                    self.__config = yaml.safe_load(config_file)
            else:
                self.__config = dict()

            self.__config.setdefault("dependencies", [])

        return self.__config

    @_config.setter
    def _config(self, config: Dict[str, Any]) -> None:
        self.__config = config

    def hash(self, recompute: bool = False) -> str:
        """Returns the hash of this job.

        Parameters:
            recompute: This method uses cashing to compute the job hash only when
                necessary. If set to `True`, this will recompute the job hash in any
                case.
        """
        if self._hash is None or recompute:
            hashes = dict()

            for destination, source in self.files.items():
                if destination in (Path("r3.yaml"), Path("metadata.yaml")):
                    continue

                hashes[str(destination)] = r3.utils.hash_file(source)

            for dependency in self.dependencies:
                hashes[str(dependency.destination)] = dependency.hash()

            index = "\n".join(f"{path} {hashes[path]}" for path in sorted(hashes))
            hashes["."] = r3.utils.hash_str(index)

            self._config["hashes"] = hashes  # type: ignore
            self._hash = hashes["."]

        return self._hash


class Dependency(abc.ABC):
    """Dependency base class."""

    def __init__(self, destination: Union[os.PathLike, str]) -> None:
        """Initializes the dependency.

        Parameters:
            destination: Path relative to the job to which the dependency will be
                checked out.
        """
        self.destination = Path(destination)

    @staticmethod
    def from_config(config: Dict[str, str]) -> "Dependency":
        """Returns a dependency instance from a config dictionary.

        This method determines the type of dependency from config and delegates the
        instantiation to the appropriate class.

        Parameters:
            config: A dictionary representing the dependency. The format of the
                dictionary depends on the type of dependency. See the documentation of
                the specific dependency class for more information.
        """
        if "job" in config:
            return JobDependency.from_config(config)
        if "query" in config:
            return QueryDependency.from_config(config)
        if "query_all" in config:
            return QueryAllDependency.from_config(config)
        if "repository" in config:
            return GitDependency.from_config(config)

        raise ValueError(f"Unrecognized dependency config: {config}")

    @abc.abstractmethod
    def to_config(self) -> Dict[str, str]:
        """Returns a config dictionary representing the dependency."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_resolved(self) -> bool:
        """Returns `True` if the dependency is resolved.
        
        A dependency is resolved if it references a specific job or commit.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def hash(self) -> str:
        """Returns the hash of the dependency."""
        raise NotImplementedError


class JobDependency(Dependency):
    """A dependency on another job."""

    def __init__(
        self,
        job: Union[Job, str],
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = ".",
        query: Optional[str] = None,
        query_all: Optional[str] = None,
    ) -> None:
        """Initializes the job dependency.
        
        Parameters:
            job: Job instance or job id.
            destination: Path relative to the job to which the dependency will be
                checked out.
            source: Path relative to the source job to be checked out.
            query: If this job was resolved from a QueryDependency, this is the query
                that was used.
            query_all: If this job was resolved from a QueryAllDependency, this is the
                query that was used.
        """
        super().__init__(destination)

        # REVIEW: Should we allow job to be a Job instance?
        if isinstance(job, Job):
            if job.id is None:
                raise ValueError("Job is not committed.")
            self.job = job.id
        else:
            self.job = job

        self.source = Path(source)
        self.query = query
        self.query_all = query_all

    @staticmethod
    def from_config(config: Dict[str, str]) -> "JobDependency":
        """Creates a JobDependency instance from a config dictionary.
        
        Example:

            config = {
                "job": "123abc...",     # Job id
                "source": "output",     # Checkout <source_job>/output (default: .)
                "destination": "data",  # to <dependent_job>/data
                "query": "#data/xyz",   # Query used when committing the job (optional)
            }

            dependency = JobDependency.from_config(config)

        Parameters:
            config: A dictionary representing the dependency. See the example above for
                the format of the dictionary.
        
        Returns:
            A JobDependency instance.
        """
        return JobDependency(**config)

    def to_config(self) -> Dict[str, str]:
        """Returns a config dictionary representing the dependency.
        
        See `from_config` for an example.
        """
        config = {
            "job": self.job,
            "source": str(self.source),
            "destination": str(self.destination),
        }

        if self.query is not None:
            config["query"] = self.query

        if self.query_all is not None:
            config["query_all"] = self.query_all

        return config

    def is_resolved(self) -> bool:
        """Returns `True` if the dependency is resolved."""
        return True

    def hash(self) -> str:
        """Returns the hash of the dependency."""
        return r3.utils.hash_str(f"jobs/{self.job}/{self.source}")


class QueryDependency(Dependency):
    """A dependency to the latest job determined by a query."""

    def __init__(
        self,
        query: str,
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = ".",
    ) -> None:
        """Initializes the query dependency.

        Parameters:
            query: A query that will be used to determine the job.
            destination: Path relative to the job to which the dependency will be
                checked out.
            source: Path relative to the source job to be checked out.
        """
        super().__init__(destination)
        self.source = Path(source)
        self.query = query

    @staticmethod
    def from_config(config: Dict[str, str]) -> "QueryDependency":
        """Creates a QueryDependency instance from a config dictionary.
        
        Example:

            config = {
                "query": "#data/xyz",
                "source": "output",
                "destination": "data",
            }

            dependency = QueryDependency.from_config(config)
        
        Parameters:
            config: A dictionary representing the dependency. See the example above for
                the format of the dictionary.
        """
        return QueryDependency(**config)

    def to_config(self) -> Dict[str, str]:
        """Returns a config dictionary representing the dependency.
        
        See `from_config` for an example.
        """
        return {
            "query": self.query,
            "source": str(self.source),
            "destination": str(self.destination),
        }

    def is_resolved(self) -> bool:
        """Returns `True` if the dependency is resolved."""
        return False

    def hash(self) -> str:
        """Raises an error.
        
        QueryDependencies cannot be hashed because the hash would depend on the result
        of the query, which is not known at the time of creating the dependency.

        Raises:
            ValueError: Always.
        """
        raise ValueError("Cannot hash QueryDependency")


class QueryAllDependency(Dependency):
    """A dependency to all jobs determined by a query."""

    def __init__(
        self,
        query_all: str,
        destination: Union[os.PathLike, str]
    ) -> None:
        """Initializes the query all dependency.

        This does not specifying a source, since all jobs need to be checked out to
        directories with different names. The source is always the root of the job,
        and the destination directory name is always the job id.

        Parameters:
            query_all: A query that will be used to determine the jobs.
            destination: Base path relative to the job to which the jobs will be checked
                out. Each job will be checked out to a subdirectory of this path with
                the job id as the name of the subdirectory.
        """
        super().__init__(destination)
        self.query_all = query_all

    @staticmethod
    def from_config(config: Dict[str, str]) -> "QueryAllDependency":
        """Creates a QueryAllDependency instance from a config dictionary.
        
        Example:

            config = {
                "query_all": "#data/xyz",
                "destination": "data",
            }

            dependency = QueryAllDependency.from_config(config)
        
        Parameters:
            config: A dictionary representing the dependency. See the example above for
                the format of the dictionary.
        """
        return QueryAllDependency(**config)

    def to_config(self) -> Dict[str, str]:
        """Returns a config dictionary representing the dependency.

        See `from_config` for an example.
        """
        return {
            "query_all": self.query_all,
            "destination": str(self.destination),
        }

    def is_resolved(self) -> bool:
        """Returns `True` if the dependency is resolved."""
        return False

    def hash(self) -> str:
        """Raises an error.

        QueryAllDependencies cannot be hashed because the hash would depend on the
        result of the query, which is not known at the time of creating the dependency.

        Raises:
            ValueError: Always.
        """
        raise ValueError("Cannot hash QueryAllDependency")


class GitDependency(Dependency):
    """A dependency to a git repository."""

    def __init__(
        self,
        repository: str,
        commit: Optional[str],
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = "",
        branch: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> None:
        """Initializes the git dependency.
        
        Parameters:
            repository: URL of the git repository. Currently, only github.com is
                supported.
            commit: Commit hash.
            destination: Path relative to the job to which the repository will be
                checked out.
            source: Path relative to the repository root to be checked out.
            branch: Branch name. If no commit id is given, the dependency will be
                resolved to the latest commit on this branch.
            tag: Tag name. If no commit id is given, the dependency will be resolved to
                the commit pointed to by this tag.
        """
        if branch is not None and tag is not None:
            raise ValueError("Cannot specify both branch and tag.")

        super().__init__(destination)
        self.source = Path(source)
        self.repository = repository
        self.commit = commit
        self.branch = branch
        self.tag = tag


    # REVIEW: This should not be a method of this class. Instead, the git manager in the
    #         repository class should be responsible for this.
    @property
    def repository_path(self) -> Path:
        """Returns the path where the repository will stored in R3."""
        https_pattern = r"^https://github\.com/([^/]+)/([^/\.]+)(?:\.git)?$"
        match = re.match(https_pattern, self.repository)
        if match:
            return Path("git") / "github.com" / match.group(1) / match.group(2)

        ssh_pattern = r"^git@github\.com:([^/]+)/([^/\.]+)(?:\.git)?$"
        match = re.match(ssh_pattern, self.repository)
        if match:
            return Path("git") / "github.com" / match.group(1) / match.group(2)

        raise ValueError(f"Unrecognized git url: {self.repository}")

    @staticmethod
    def from_config(config: Dict[str, str]) -> "GitDependency":
        """Creates a GitDependency instance from a config dictionary.
        
        Example:

            config = {
                "repository": "https://github.com/user/model.git",
                "commit": "123abc...",
                "source": "src/model",
                "destination": "model",
            }
        
            dependency = GitDependency.from_config(config)
        
        Parameters:
            config: A dictionary representing the dependency. See the example above for
                the format of the dictionary.

        Returns:
            A GitDependency instance.        
        """
        return GitDependency(**config)

    def to_config(self) -> Dict[str, str]:
        """Returns a config dictionary representing the dependency.

        See `from_config` for an example.
        """
        config = {
            "repository": self.repository,
            "source": str(self.source),
            "destination": str(self.destination),
        }
        if self.commit is not None:
            config["commit"] = self.commit
        return config

    def is_resolved(self) -> bool:
        """Returns `True` if the dependency is resolved."""
        return self.commit is not None

    def hash(self) -> str:
        """Returns the hash of the dependency."""
        return r3.utils.hash_str(f"{self.repository_path}@{self.commit}/{self.source}")
