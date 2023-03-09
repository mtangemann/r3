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
from typing import Any, Dict, Iterable, Mapping, Optional, Union

import yaml
from executor import ExternalCommandFailed, execute

import r3
import r3.utils

R3_FORMAT_VERSION = "1.0.0-beta.1"


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

        with open(path / "r3repository.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

        return Repository(path)

    def add(self, job: "Job") -> "Job":
        target_path = self.path / "jobs" / job.hash()

        if (self.path / target_path).is_dir():
            print(f"Job exists already: {target_path}")
            return Job(target_path)

        for dependency in job.dependencies():
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        os.makedirs(target_path)
        os.makedirs(target_path / "output")

        job.metadata["date"] = datetime.now().replace(microsecond=0).isoformat()

        with open(target_path / "r3.yaml", "w") as config_file:
            yaml.dump(job.config, config_file)
        _remove_write_permissions(target_path / "r3.yaml")

        with open(target_path / "r3metadata.yaml", "w") as metadata_file:
            yaml.dump(job.metadata, metadata_file)

        for destination, source in job.files.items():
            if destination in [Path("r3.yaml"), Path("r3metadata.yaml")]:
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
            if not child.name == "output":
                if child.is_dir():
                    shutil.copytree(child, path / child.name)
                else:
                    shutil.copy(child, path / child.name)

        # Symlink output directory
        os.symlink(job.path / "output", path / "output")

        for dependency in job.dependencies():
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
            with open(job / "r3metadata.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)

            metadata[job.name] = config.get("metadata", dict())

        with open(self.path / "metadata.json", "w") as metadata_file:
            json.dump(metadata, metadata_file)


class Job:
    """A job that may or may not be part of a repository

    This class provides an API to access and/or modifiy jobs. The behavior of this class
    depends on whether the job is committed to a repository or not. If this job is part
    of a repository, all properties except the metadata are read-only. If not yet
    committed, all properties may be modified.
    """

    def __init__(
        self,
        path: Optional[Union[str, os.PathLike]] = None,
        config: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[Path, Path]] = None,
    ) -> None:
        """Initializes a job instance.

        Parameters
        ----------
        path
            Path to the job's root directory. This may be ``None`` if the job is
            constructed entirely via the Python API and no corresponding directory
            exists.
        config
            Job configuration override. Per default, the job config is read from a
            config file named ``r3.yaml`` relative to the job path if it exists or set
            to the empty dict otherwise. If a config is specified here, the default
            config is entirely ignored. If the job is part of an R3 repository, this
            parameter must be None.
        metadata
            Job metadata override. Per default, the job metadata is read from a config
            file named ``r3metadata.yaml`` relative to the job path if it exists or set
            to the empty dict otherwise. If a metadata dict is specified here, the
            default metadata is entirely ignored. If the job is not committed yet, the
            metadata may also specified in the ``r3.yaml`` config file. If present, this
            will override the metadata file but not the given metadata.
        files
            A dict of files belonging to the job. Keys are the destination paths of the
            files relative to the job directory which don't have to exist yet. The
            values are the absolute file paths which have to exist. Per default, this
            list is created by recursively scanning the job path if given or set to the
            empty list otherwise. This respects the ignore rules from the job
            configuration, see the user guide for more information. If a list of files
            is specified here, the default list is entirely ignored. If the job is part
            of an R3 repository, this parameter must be None.

        Raises
        ------
        ValueError
            If a job path is given that is contained in an R3 repository and ``config``
            or ``files`` is not None. If the ``r3.yaml`` config file of a committed job
            contains a ``metadata`` or ``commit`` key.
        """
        self._repository: Optional[Repository] = None

        if path is None:
            self.path = None

        else:
            self.path = Path(path).absolute()

            if (self.path.parent.parent / "r3repository.yaml").is_file():
                self._repository = Repository(self.path.parent.parent)

        if self._repository is not None and config is not None:
            raise ValueError(
                "Overriding the config is not allowed for committed jobs "
                f"(path={path})."
            )

        if self._repository is not None and files is not None:
            raise ValueError(
                "Overriding the config is not allowed for committed jobs "
                f"(path={path})."
            )

        self._config = config or self._load_config()

        if "metadata" in self._config:
            if self._repository is not None:
                raise ValueError(
                    "The r3.yaml file may not contain metadata for committed jobs."
                )

            if metadata is None:
                metadata = self._config["metadata"]

            assert isinstance(self._config, dict)
            del self._config["metadata"]

        if "commit" in self._config:
            if self._repository is not None:
                raise ValueError(
                    "The r3.yaml config file may not contain a commit config for jobs "
                    "that are committed already."
                )

            assert isinstance(self._config, dict)
            self._commit_config = self._config.pop("commit")

        else:
            self._commit_config = dict()

        self.metadata = metadata or self._load_metadata()
        self._files = files or self._load_files()

    def _load_config(self) -> Mapping[str, Any]:
        if self.path is not None and (self.path / "r3.yaml").is_file():
            with open(self.path / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)
        else:
            config = dict()

        if self._repository is None:
            return config
        else:
            return MappingProxyType(config)

    def _load_metadata(self) -> Dict[str, Any]:
        if self.path is not None and (self.path / "r3metadata.yaml").is_file():
            with open(self.path / "r3metadata.yaml", "r") as config_file:
                return yaml.safe_load(config_file)
        else:
            return dict()

    def _load_files(self) -> Mapping[Path, Path]:
        if self.path is None:
            return dict()

        ignore_paths = self._commit_config.get("ignore", [])

        for dependency in self.dependencies():
            ignore_paths.append(f"/{dependency.item}")

        files = r3.utils.find_files(self.path, ignore_paths)
        filedict = {file: (self.path / file).absolute() for file in files}

        if self._repository is None:
            return filedict
        else:
            return MappingProxyType(filedict)

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
    def config(self) -> Mapping[str, Any]:
        """Returns the config of this job."""
        return self._config

    @config.setter
    def config(self, config: Dict[str, Any]) -> None:
        """Sets the config of this jobs.

        This operation is only allowed if this job is not committed. Otherwise this will
        raise an exception.

        Raises
        ------
        ValueError
            If this job is contained in an R3 repository.
        """
        if self._repository is not None:
            raise ValueError("The config is read-only for committed jobs.")

        self._config = config

    @property
    def files(self) -> Mapping[Path, Path]:
        """Files belonging to this job."""
        return self._files

    @files.setter
    def files(self, files: Dict[Path, Path]) -> None:
        """Sets the list of files belonging to this job.

        This operation is only allowed if this job is not committed. Otherwise this will
        raise an exception.
        """
        if self._repository is not None:
            raise ValueError("The files list is read-only for committed jobs.")

        self._files = files

    def dependencies(self) -> Iterable["Dependency"]:
        for dependency_dict in self.config.get("dependencies", []):
            yield Dependency(**dependency_dict)

    def hash(self, recompute: bool = False) -> str:
        if self.path is not None and self.repository is not None and not recompute:
            return self.path.name

        return _hash_job(self.config, self.files, self.path)


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
