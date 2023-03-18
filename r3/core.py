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
from datetime import datetime
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

    @staticmethod
    def create(path: Union[str, os.PathLike]) -> "Repository":
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

    def add(self, job: "Job") -> "Job":
        target_path = self.path / "jobs" / job.hash()

        if (self.path / target_path).is_dir():
            print(f"Job exists already: {target_path}")
            return Job(target_path)

        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        os.makedirs(target_path)
        os.makedirs(target_path / "output")

        job.metadata["date"] = datetime.now().replace(microsecond=0).isoformat()

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

        return Job(target_path)

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

    def rebuild_cache(self):
        """Aggregates all job metadata into 'metadata.json'."""
        metadata = dict()

        for job in (self.path / "jobs").iterdir():
            with open(job / "metadata.yaml", "r") as metadata_file:
                job_metadata = yaml.safe_load(metadata_file)

            metadata[job.name] = job_metadata

        with open(self.path / "index.yaml", "w") as cache_file:
            yaml.dump(metadata, cache_file)


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
        if (self._path.parent.parent / "r3.yaml").is_file():
            self._repository = Repository(self._path.parent.parent)

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

    def hash(self, recompute: bool = False) -> str:
        if self.repository is not None and not recompute:
            return self.path.name

        return _hash_job(self._config, self.files, self.path)


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


def _hash_job(config: Mapping, files: Mapping[Path, Path], root: Optional[Path]) -> str:
    hashes = {
        str(destination): _hash_file(source)
        for destination, source in files.items()
        if destination != Path("r3.yaml")
    }
    hashes["r3.yaml"] = _hash_config(config)

    index = "\n".join("{} {}".format(hashes[file], file) for file in sorted(hashes))

    return hashlib.sha256(index.encode("utf-8")).hexdigest()


def _hash_file(path: Path, chunk_size: int = 2**16) -> str:
    hash = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            hash.update(chunk)

    return hash.hexdigest()


def _hash_config(config: Mapping) -> str:
    ignored_keys = {"environment", "ignore", "metadata"}
    config = {key: value for key, value in config.items() if key not in ignored_keys}
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)
