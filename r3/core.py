"""R3 core functionality.

This module provides the core functionality of R3. This module should not be used
directly, but rather the public API exported by the top-level ``r3`` module.
"""

import hashlib
import json
import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping, Optional, Union

import yaml
from executor import ExternalCommandFailed, execute

import r3
import r3.utils

R3_FORMAT_VERSION = "1.0.0-beta.2"


class Repository:
    def __init__(self, path: Union[str, os.PathLike]) -> None:
        """Initializes the repository instance.

        Raises
        ------
        FileNotFoundError
            If the given path does not exist.
        NotADirectoryError
            If the given path exists but is not a directory.
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

        Raises
        ------
        FileExistsError
            If the given path exists alreay.
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
        target_path = self.path / "jobs" / job.hash()

        if (self.path / target_path).is_dir():
            print(f"Job exists already: {target_path}")
            return Job(target_path)

        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

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
        if isinstance(item, Dependency):
            return self._checkout_dependency(item, path)
        else:
            return self._checkout_job(item, path)

    def _checkout_dependency(
        self, dependency: "Dependency", path: Union[str, os.PathLike]
    ) -> None:
        if dependency.source_path != ".":
            raise NotImplementedError

        source = self.path / dependency.item
        destination = path / dependency.target_path

        os.makedirs(destination.parent, exist_ok=True)

        if dependency.commit is None:
            os.symlink(source / dependency.source_path, destination)
        else:
            with tempfile.TemporaryDirectory() as tempdir:
                execute(f"git clone {source} {tempdir}")
                execute(f"git checkout {dependency.commit}", directory=tempdir)
                shutil.move(Path(tempdir) / dependency.source_path, destination)

    def _checkout_job(self, job: "Job", path: Union[str, os.PathLike]) -> None:
        if job not in self:
            raise FileNotFoundError(f"Cannot find job: {job.path}")

        if job.path is None:
            raise RuntimeError("Job is committed but doesn't have a path.")

        path = Path(path)
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

    def __contains__(self, item: Union["Job", "Dependency"]) -> bool:
        if isinstance(item, Job):
            return (self.path / "jobs" / item.hash()).is_dir()

        if not (self.path / item.item).exists():
            return False

        elif item.commit is None:
            return True

        else:
            try:
                object_type = execute(
                    f"git cat-file -t {item.commit}",
                    directory=self.path / item.item,
                    capture=True,
                )
            except ExternalCommandFailed:
                return False
            else:
                return object_type == "commit"

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
        self._index[job.hash()] = {
            "tags": job.metadata.get("tags", []),
            "datetime": job.datetime,
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

        Parameters
        ----------
        path
            Path to the job's root directory.
        """
        self._path = Path(path).absolute()

        self._repository: Optional[Repository] = None
        self._hash: Optional[str] = None

        if (self._path.parent.parent / "r3.yaml").is_file():
            self._repository = Repository(self._path.parent.parent)
            self._hash = self._path.name

        self._load_config()
        self._load_dependencies()
        self._load_files()
        self._load_metadata()

    def _load_config(self) -> None:
        if (self.path / "r3.yaml").is_file():
            with open(self.path / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)
        else:
            config = dict()

        self._config = config if self._repository is None else MappingProxyType(config)

    def _load_dependencies(self) -> None:
        dependencies = [
            Dependency(**kwargs) for kwargs in self._config.get("dependencies", [])
        ]

        self._dependencies = (
            dependencies if self._repository is None else tuple(dependencies)
        )

    def _load_files(self) -> None:
        ignore = self._config.get("ignore", [])

        for dependency in self.dependencies:
            ignore.append(f"/{dependency.item}")

        files = {
            file: (self.path / file).absolute()
            for file in r3.utils.find_files(self.path, ignore)
        }

        self._files = files if self._repository is None else MappingProxyType(files)

    def _load_metadata(self) -> None:
        if (self.path / "metadata.yaml").is_file():
            with open(self.path / "metadata.yaml", "r") as metadata_file:
                metadata = yaml.safe_load(metadata_file)
        else:
            metadata = dict()

        self.metadata = metadata

    @property
    def path(self) -> Path:
        return self._path

    @property
    def repository(self) -> Optional[Repository]:
        """Optionally returns the repository in which this job is contained.

        Returns
        -------
        Repository or None
            This returns the repository in which this job is contained. If this job is
            not part of any repository, this returns ``None``.
        """
        return self._repository

    @property
    def files(self) -> Mapping[Path, Path]:
        """Files belonging to this job."""
        return self._files

    @property
    def dependencies(self) -> Iterable["Dependency"]:
        """Dependencies of this job."""
        return self._dependencies

    @property
    def datetime(self) -> datetime:
        """Returns the date and time when this job was created (committed)."""
        timestamp = self.path.stat().st_ctime
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def hash(self, recompute: bool = False) -> str:
        """Returns the hash of this job.

        Parameters
        ----------
        recompute
            This method uses cashing to compute the job hash only when necessary. If set
            to `True`, this will recompute the job hash in any case.
        """
        if self._hash is None or recompute:
            hashes = {
                str(destination): self._hash_file(source)
                for destination, source in self.files.items()
                if destination not in (Path("r3.yaml"), Path("metadata.yaml"))
            }
            hashes["r3.yaml"] = self._hash_config()

            index = "\n".join(
                "{} {}".format(hashes[file], file) for file in sorted(hashes)
            )

            self._hash = hashlib.sha256(index.encode("utf-8")).hexdigest()

        return self._hash

    def _hash_config(self) -> str:
        dependencies = []
        for dependency in self._config.get("dependencies", []):
            dependencies.append({k: v for k, v in dependency.items() if k != "query"})

        config = {"dependencies": dependencies}

        config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()

    @staticmethod
    def _hash_file(path: Path, chunk_size: int = 2**16) -> str:
        hash = hashlib.sha256()

        with open(path, "rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                hash.update(chunk)

        return hash.hexdigest()


class Dependency:
    def __init__(
        self,
        item: Union[os.PathLike, str],
        commit: Optional[str] = None,
        source_path: Optional[Union[os.PathLike, str]] = None,
        target_path: Optional[Union[os.PathLike, str]] = None,
        origin: Optional[str] = None,
    ) -> None:
        self.item = Path(item)
        self.commit = commit
        self.source_path = Path(".") if source_path is None else Path(source_path)

        if target_path is None:
            self.target_path = self.item / self.source_path
        else:
            self.target_path = Path(target_path)

        self.origin = origin

    @staticmethod
    def from_string(string: str) -> "Dependency":
        parts = string.split("@", maxsplit=1)
        path = Path(parts[0])
        commit = None if len(parts) < 2 else parts[1]
        return Dependency(path, commit)


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)
