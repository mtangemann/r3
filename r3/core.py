"""R3 core functionality.

This module provides the core functionality of R3. This module should not be used
directly, but rather the public API exported by the top-level ``r3`` module.
"""

import abc
import os
import re
import shutil
import stat
import tempfile
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Union

import yaml
from executor import execute

import r3
import r3.utils

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

        self._index_path: Path = self.path / "index.yaml"
        self._load_index()

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

    def jobs(self) -> Iterable["Job"]:
        """Returns an iterator over all jobs in this repository."""
        for path in (self.path / "jobs").iterdir():
            yield Job(path)

    def commit(self, job: "Job") -> "Job":
        job.resolve(self)
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

        committed_job = Job(target_path)

        self._add_job_to_index(committed_job)
        self._save_index()

        return committed_job

    def checkout(
        self, item: Union["Dependency", "Job"], path: Union[str, os.PathLike]
    ) -> None:
        path = Path(path)

        if isinstance(item, Job):
            return self._checkout_job(item, path)
        if isinstance(item, QueryDependency):
            item = item.resolve(self)
        if isinstance(item, JobDependency):
            return self._checkout_job_dependency(item, path)
        if isinstance(item, GitDependency):
            return self._checkout_git_dependency(item, path)

    def _checkout_job(self, job: "Job", path: Path) -> None:
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

    def _checkout_job_dependency(self, dependency: "JobDependency", path: Path) -> None:
        source = self.path / "jobs" / dependency.job / dependency.source
        destination = path / dependency.destination

        os.makedirs(destination.parent, exist_ok=True)
        os.symlink(source, destination)

    def _checkout_git_dependency(self, dependency: "GitDependency", path: Path) -> None:
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

    def remove(self, job: "Job") -> None:
        if job not in self:
            raise ValueError("Job is not contained in this repository.")

        for job_id, metadata in self._index.items():
            for dependency in metadata.get("dependencies", []):
                if dependency.get("job", None) == str(job.uuid):
                    raise ValueError(f"Another job depends on this job: {job_id}")

        for path in job.files:
            _add_write_permission(job.path / path)
        _add_write_permission(job.path)

        shutil.rmtree(job.path)

        del self._index[str(job.uuid)]
        self._save_index()

    def __contains__(self, item: Union["Job", "Dependency"]) -> bool:
        """Checks if the given item is contained in this repository."""
        if isinstance(item, Job):
            return (
                item.uuid is not None and (self.path / "jobs" / str(item.uuid)).is_dir()
            )

        if isinstance(item, QueryDependency):
            item = item.resolve(self)

        if isinstance(item, JobDependency):
            return (self.path / "jobs" / item.job / item.source).exists()

        if isinstance(item, GitDependency):
            return r3.utils.git_path_exists(
                self.path / item.repository_path, item.commit, item.source
            )

        return False

    def find(self, tags: Iterable[str], latest: bool = False) -> List["Job"]:
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
                results.append(Job(self.path / "jobs" / job_id))

        if latest:
            return [max(results, key=lambda job: job.datetime)]
        else:
            return sorted(results, key=lambda job: job.datetime)

    def _load_index(self) -> None:
        if self._index_path.exists():
            with open(self._index_path, "r") as index_file:
                self._index = yaml.safe_load(index_file)
        else:
            self._index = dict()

    def _save_index(self) -> None:
        with open(self._index_path, "w") as index_file:
            self._index = yaml.dump(self._index, index_file)

    def _add_job_to_index(self, job: "Job") -> None:
        self._index[str(job.uuid)] = {
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


class Job:
    """A job that may or may not be part of a repository."""

    def __init__(self, path: Union[str, os.PathLike]) -> None:
        """Initializes a job instance.

        Parameters:
            path: Path to the job's root directory.
        """
        self._path = Path(path).absolute()

        self._repository: Optional[Repository] = None
        self._hash: Optional[str] = None
        self._uuid: Optional[uuid.UUID] = None

        if (self._path.parent.parent / "r3.yaml").is_file():
            self._repository = Repository(self._path.parent.parent)
            self._uuid = uuid.UUID(self._path.name)

        self._files: Mapping[Path, Path] | None = None
        self._metadata: Dict[str, str] | None = None

        self._load_config()
        self._load_dependencies()

    def _load_config(self) -> None:
        if (self.path / "r3.yaml").is_file():
            with open(self.path / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)
        else:
            config = dict()

        config.setdefault("dependencies", [])

        self._config = config if self._repository is None else MappingProxyType(config)

    def _load_dependencies(self) -> None:
        dependencies = [
            Dependency.from_dict(kwargs) for kwargs in self._config["dependencies"]
        ]

        self._dependencies = (
            dependencies if self._repository is None else tuple(dependencies)
        )

    def _load_files(self) -> None:
        ignore = self._config.get("ignore", [])

        for dependency in self.dependencies:
            ignore.append(f"/{dependency.destination}")

        files = {
            file: (self.path / file).absolute()
            for file in r3.utils.find_files(self.path, ignore)
        }

        self._files = files if self._repository is None else MappingProxyType(files)

    @property
    def uuid(self) -> Optional[uuid.UUID]:
        return self._uuid

    @property
    def path(self) -> Path:
        return self._path

    @property
    def repository(self) -> Optional[Repository]:
        """Optionally returns the repository in which this job is contained.

        Returns:
            This returns the repository in which this job is contained. If this job is
                not part of any repository, this returns ``None``.
        """
        return self._repository

    @property
    def files(self) -> Mapping[Path, Path]:
        """Files belonging to this job."""
        if self._files is None:
            self._load_files()
        return self._files  # type: ignore

    @property
    def dependencies(self) -> Sequence["Dependency"]:
        """Dependencies of this job."""
        return self._dependencies

    @property
    def metadata(self) -> Dict[str, str]:
        """Job metadata.

        Changes to this dictionary are not written to the job's metadata file.
        """
        if self._metadata is None:
            if (self.path / "metadata.yaml").is_file():
                with open(self.path / "metadata.yaml", "r") as metadata_file:
                    self._metadata = yaml.safe_load(metadata_file)
            else:
                self._metadata = dict()

        return self._metadata

    def resolve(self, repository: Repository) -> None:
        if not isinstance(self.dependencies, list):
            raise ValueError("Dependencies are not writeable.")

        resolved_dependencies = []

        for index in range(len(self.dependencies)):
            if isinstance(self.dependencies[index], QueryDependency):
                dependency = self.dependencies[index].resolve(repository)
                resolved_dependencies.append(dependency)
            
            elif isinstance(self.dependencies[index], QueryAllDependency):
                dependencies = self.dependencies[index].resolve(repository)
                resolved_dependencies.extend(dependencies)
            
            else:
                resolved_dependencies.append(self.dependencies[index])

        self._dependencies = resolved_dependencies
        self._config["dependencies"] = [
            dependency.to_dict() for dependency in self.dependencies
        ]

    @property
    def datetime(self) -> datetime:
        """Returns the date and time when this job was created (committed)."""
        if "committed_at" in self.metadata:
            return datetime.strptime(self.metadata["committed_at"], DATE_FORMAT)
        else:
            warnings.warn(
                "Job metadata doesn't include `datetime`. Falling back to using the "
                "directory creation data (deprecated).",
                stacklevel=2,
            )
            timestamp = self.path.stat().st_ctime
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)

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

            for dependency in self._dependencies:
                hashes[str(dependency.destination)] = dependency.hash()

            index = "\n".join(f"{path} {hashes[path]}" for path in sorted(hashes))
            hashes["."] = r3.utils.hash_str(index)

            self._config["hashes"] = hashes
            self._hash = hashes["."]

        return self._hash


class Dependency(abc.ABC):
    """Dependency base class."""

    def __init__(
        self,
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = ".",
    ) -> None:
        """Initializes the dependency.

        Parameters:
            source: Path relative to the item (job / git repository) that is referenced
                by the dependecy. Defaults to "." if no query is given.
            destination: Path relative to the job to which the dependency will be
                checked out.
        """
        self.source = Path(source)
        self.destination = Path(destination)

    @abc.abstractmethod
    def to_dict(self) -> Dict[str, str]:
        raise NotImplementedError

    @staticmethod
    def from_dict(dict_: Dict[str, str]) -> "Dependency":
        if "job" in dict_:
            return JobDependency(**dict_)
        if "query" in dict_:
            return QueryDependency(**dict_)
        if "query_all" in dict_:
            return QueryAllDependency(**dict_)
        if "repository" in dict_:
            return GitDependency(**dict_)

        raise ValueError(f"Invalid dependency dict: {dict_}")

    @abc.abstractmethod
    def hash(self) -> str:
        raise NotImplementedError


class JobDependency(Dependency):
    def __init__(
        self,
        job: Union[Job, str],
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = "",
        query: Optional[str] = None,
        query_all: Optional[str] = None,
    ) -> None:
        super().__init__(destination, source)
        self.job = job if isinstance(job, str) else str(job.uuid)
        self.query = query
        self.query_all = query_all

    def to_dict(self) -> Dict[str, str]:
        dict_ = {
            "job": self.job,
            "source": str(self.source),
            "destination": str(self.destination),
        }

        if self.query is not None:
            dict_["query"] = self.query
        
        if self.query_all is not None:
            dict_["query_all"] = self.query_all

        return dict_

    def hash(self) -> str:
        return r3.utils.hash_str(f"jobs/{self.job}/{self.source}")


class GitDependency(Dependency):
    def __init__(
        self,
        repository: str,
        commit: str,
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = "",
    ) -> None:
        super().__init__(destination, source)
        self.repository = repository
        self.commit = commit

    @property
    def repository_path(self) -> Path:
        https_pattern = r"^https://github\.com/([^/]+)/([^/\.]+)(?:\.git)?$"
        match = re.match(https_pattern, self.repository)
        if match:
            return Path("git") / "github.com" / match.group(1) / match.group(2)

        ssh_pattern = r"^git@github\.com:([^/]+)/([^/\.]+)(?:\.git)?$"
        match = re.match(ssh_pattern, self.repository)
        if match:
            return Path("git") / "github.com" / match.group(1) / match.group(2)

        raise ValueError(f"Unrecognized git url: {self.repository}")

    def to_dict(self) -> Dict[str, str]:
        return {
            "repository": self.repository,
            "commit": self.commit,
            "source": str(self.source),
            "destination": str(self.destination),
        }

    def hash(self) -> str:
        return r3.utils.hash_str(f"{self.repository_path}@{self.commit}/{self.source}")


class QueryDependency(Dependency):
    def __init__(
        self,
        query: str,
        destination: Union[os.PathLike, str],
        source: Union[os.PathLike, str] = ".",
    ) -> None:
        super().__init__(destination, source)
        self.query = query

    def to_dict(self) -> Dict[str, str]:
        return {
            "query": self.query,
            "source": str(self.source),
            "destination": str(self.destination),
        }

    def resolve(self, repository: Repository) -> JobDependency:
        tags = self.query.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {self.query}")

        tags = [tag[1:] for tag in tags]
        result = repository.find(tags, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {self.query}")

        return JobDependency(result[0], self.destination, self.source, self.query)

    def hash(self) -> str:
        raise ValueError("Cannot hash QueryDependency")


class QueryAllDependency(Dependency):
    def __init__(
        self,
        query_all: str,
        destination: Union[os.PathLike, str],
    ) -> None:
        super().__init__(destination, ".")
        self.query_all = query_all
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "query_all": self.query_all,
            "destination": str(self.destination),
        }

    def resolve(self, repository: Repository) -> List[JobDependency]:
        tags = self.query_all.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {self.query_all}")

        tags = [tag[1:] for tag in tags]
        result = repository.find(tags)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {self.query_all}")

        return [
            JobDependency(
                job, self.destination / str(job.uuid), query_all=self.query_all
            )
            for job in result
        ]

    def hash(self) -> str:
        raise ValueError("Cannot hash QueryAllDependency")


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)


def _add_write_permission(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode | stat.S_IWOTH | stat.S_IWGRP | stat.S_IWUSR
    os.chmod(path, mode)
