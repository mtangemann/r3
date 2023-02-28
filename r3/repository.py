import hashlib
import json
import os
import shutil
import stat
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import yaml
from executor import ExternalCommandFailed, execute

import r3
import r3.utils

R3_FORMAT_VERSION = "1.0.0-beta.1"


class Repository:
    def __init__(self, path: Union[str, os.PathLike]) -> None:
        self.path = Path(path)

    def init(self) -> None:
        """Initializes the Repository in the file system.

        Raises
        ------
        FileExistsError
            If the repository path exists alreay.
        """
        if self.path.exists():
            raise FileExistsError(f"The repository path exists already: {self.path}")

        os.makedirs(self.path)
        os.makedirs(self.path / "git")
        os.makedirs(self.path / "jobs")

        r3config = {"version": R3_FORMAT_VERSION}

        with open(self.path / "r3repository.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

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

        config = job.config
        metadata = job.config.pop("metadata", dict())
        metadata = {**metadata, **job.metadata}
        metadata["date"] = datetime.now().replace(microsecond=0).isoformat()
        metadata["source"] = str(job.path)

        files = job.files()
        config.pop("ignore", None)

        with open(target_path / "r3.yaml", "w") as config_file:
            yaml.dump(config, config_file)
        _remove_write_permissions(target_path / "r3.yaml")

        with open(target_path / "r3metadata.yaml", "w") as metadata_file:
            yaml.dump(metadata, metadata_file)

        for file in files:
            if file == Path("r3.yaml") or file == Path("r3metadata.yaml"):
                continue

            target = target_path / file
            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(job.path / file, target)
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
    def __init__(self, path: Union[str, os.PathLike]) -> None:
        self.path = Path(path).absolute()

        if (self.path.parent.parent / "r3repository.yaml").is_file():
            self._repository: Optional[Repository] = Repository(self.path.parent.parent)
        else:
            self._repository = None

        self._config: Optional[Dict[str, Any]] = None
        self._metadata: Optional[Dict[str, Any]] = None

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
    def config(self) -> Dict[str, Any]:
        if self._config is None:
            with open(self.path / "r3.yaml", "r") as config_file:
                self._config = yaml.safe_load(config_file)

        return self._config

    @property
    def metadata(self) -> Dict[str, Any]:
        if self._metadata is None:
            metadata_path = self.path / "r3metadata.yaml"

            if not metadata_path.exists():
                self._metadata = dict()
            else:
                with open(metadata_path, "r") as metadata_file:
                    self._metadata = yaml.safe_load(metadata_file)

        return self._metadata

    def dependencies(self) -> Iterable["Dependency"]:
        for dependency_dict in self.config.get("dependencies", []):
            yield Dependency(**dependency_dict)

    def files(self) -> Iterable[Path]:
        ignore_paths = self.config.get("ignore", [])

        for dependency in self.dependencies():
            ignore_paths.append(f"/{dependency.item}")

        return r3.utils.find_files(self.path, ignore_paths)

    def hash(self, recompute: bool = False) -> str:
        if self.repository is not None and not recompute:
            return self.path.name

        return _hash_job(self.config, self.files(), self.path)


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


def _hash_job(config: Dict, files: Iterable[Path], root: Path) -> str:
    hashes = {
        str(file): _hash_file(root / file) for file in files if file != Path("r3.yaml")
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


def _hash_config(config: Dict) -> str:
    ignored_keys = {"environment", "ignore", "metadata"}
    config = {key: value for key, value in config.items() if key not in ignored_keys}
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()


def _remove_write_permissions(path: Path) -> None:
    mode = stat.S_IMODE(os.lstat(path).st_mode)
    mode = mode & ~stat.S_IWOTH & ~stat.S_IWGRP & ~stat.S_IWUSR
    os.chmod(path, mode)
