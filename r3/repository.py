import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import yaml
from executor import ExternalCommandFailed, execute

import r3
import r3.utils


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
        os.makedirs(self.path / "code")
        os.makedirs(self.path / "data")
        os.makedirs(self.path / "jobs")

        r3config = {"version": r3.__version__}

        with open(self.path / "r3.yaml", "w") as config_file:
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
        config["metadata"]["date"] = datetime.now().replace(microsecond=0).isoformat()
        config["metadata"]["source"] = str(job.path)

        with open(target_path / "r3.yaml", "w") as config_file:
            yaml.dump(config, config_file)

        for file in job.files():
            if file == Path("r3.yaml"):
                continue

            target = target_path / file
            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(file, target)

        return Job(target_path)

    def checkout(self, job: "Job", path: Union[str, os.PathLike]) -> None:
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

        # Symlink / clone dependencies
        for dependency in job.dependencies():
            source = self.path / dependency.path
            destination = path / dependency.path

            os.makedirs(destination.parent, exist_ok=True)

            if dependency.commit is None:
                os.symlink(source, destination)
            else:
                execute(f"git clone {source} {destination}")
                execute(f"git checkout {dependency.commit}", directory=destination)

    def __contains__(self, item: Union["Job", "Dependency"]) -> bool:
        if isinstance(item, Job):
            return (self.path / "jobs" / item.hash()).is_dir()

        if not (self.path / item.path).exists():
            return False

        elif item.commit is None:
            return True

        else:
            try:
                object_type = execute(
                    f"git cat-file -t {item.commit}",
                    directory=self.path / item.path,
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
            with open(job / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)

            metadata[job.name] = config.get("metadata", dict())

        with open(self.path / "metadata.json", "w") as metadata_file:
            json.dump(metadata, metadata_file)


class Job:
    def __init__(self, path: Union[str, os.PathLike]) -> None:
        self.path = Path(path).absolute()

        if (self.path.parent.parent / "r3.yaml").is_file():
            self._repository: Optional[Repository] = Repository(self.path.parent.parent)
        else:
            self._repository = None

        self._config: Optional[Dict[str, Any]] = None

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

            self._config.setdefault("metadata", dict())

        return self._config

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.config["metadata"]

    def dependencies(self) -> Iterable["Dependency"]:
        for dependency_string in self.config.get("dependencies", []):
            yield Dependency.from_string(dependency_string)

    def files(self) -> Iterable[Path]:
        ignore_paths = self.config.get("ignore", [])

        for dependency in self.dependencies():
            ignore_paths.append(f"/{dependency.path}")

        return r3.utils.find_files(self.path, ignore_paths)

    def hash(self, recompute: bool = False) -> str:
        if self.repository is not None and not recompute:
            return self.path.name

        return _hash_job(self.config, self.files())


class Dependency:
    def __init__(self, path: Path, commit: Optional[str] = None) -> None:
        self.path = path
        self.commit = commit

    @staticmethod
    def from_string(string: str) -> "Dependency":
        parts = string.split("@", maxsplit=1)
        path = Path(parts[0])
        commit = None if len(parts) < 2 else parts[1]
        return Dependency(path, commit)


def _hash_job(config: Dict, files: Iterable[Path]) -> str:
    hashes = {str(file): _hash_file(file) for file in files if file != Path("r3.yaml")}
    hashes["r3.yaml"] = _hash_config(config)

    index = "\n".join("{} {}".format(hashes[file], file) for file in sorted(hashes))

    print(index)

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
