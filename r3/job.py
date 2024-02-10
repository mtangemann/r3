import abc
import os
import re
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Union

import yaml

import r3.utils

DATE_FORMAT = r"%Y-%m-%d %H:%M:%S"


class Job:
    """A job that may or may not be part of a repository."""

    def __init__(self, path: Union[str, os.PathLike], id: str | None = None) -> None:
        """Initializes a job instance.

        Parameters:
            path: Path to the job's root directory.
            id: Job id for committed jobs.
        """
        self._path = Path(path).absolute()
        self.id = id

        self._hash: Optional[str] = None
        self._files: Mapping[Path, Path] | None = None
        self._metadata: Dict[str, str] | None = None
        self.__config: Mapping[str, Any] | None = None
        self._dependencies: Sequence["Dependency"] | None = None

    @property
    def _config(self) -> Mapping[str, Any]:
        if self.__config is None:
            self._load_config()
        return self.__config  # type: ignore

    @_config.setter
    def _config(self, config: Mapping[str, Any]) -> None:
        self.__config = config

    def _load_config(self) -> None:
        if (self.path / "r3.yaml").is_file():
            with open(self.path / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)
        else:
            config = dict()

        config.setdefault("dependencies", [])

        self._config = config

    def _load_dependencies(self) -> None:
        self._dependencies = [
            Dependency.from_dict(kwargs) for kwargs in self._config["dependencies"]
        ]

    def _load_files(self) -> None:
        ignore = self._config.get("ignore", [])

        for dependency in self.dependencies:
            ignore.append(f"/{dependency.destination}")

        self._files = {
            file: (self.path / file).absolute()
            for file in r3.utils.find_files(self.path, ignore)
        }

    @property
    def path(self) -> Path:
        return self._path

    @property
    def files(self) -> Mapping[Path, Path]:
        """Files belonging to this job."""
        if self._files is None:
            self._load_files()
        return self._files  # type: ignore

    @property
    def dependencies(self) -> Sequence["Dependency"]:
        """Dependencies of this job."""
        if self._dependencies is None:
            self._load_dependencies()
        return self._dependencies  # type: ignore

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

            for dependency in self.dependencies:
                hashes[str(dependency.destination)] = dependency.hash()

            index = "\n".join(f"{path} {hashes[path]}" for path in sorted(hashes))
            hashes["."] = r3.utils.hash_str(index)

            self._config["hashes"] = hashes  # type: ignore
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

        if isinstance(job, Job):
            if job.id is None:
                raise ValueError("Job is not committed.")
            self.job = job.id
        else:
            self.job = job

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

    def hash(self) -> str:
        raise ValueError("Cannot hash QueryAllDependency")
