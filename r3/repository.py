import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import yaml
from executor import ExternalCommandFailed, execute

import r3
import r3._indices


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
        os.makedirs(self.path / "jobs" / "by_hash")

        r3config = {"version": r3.__version__}

        with open(self.path / "r3.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

    def commit(self, path: Union[str, os.PathLike]) -> Path:
        """Adds the job from the given path to the repository.

        Returns
        -------
        Path
            The path to the job within the repository.
        """
        path = Path(path)

        config = _read_config(path / "config.yaml")

        for dependency in config.get("dependencies", []):
            parts = dependency.split("@", maxsplit=1)
            dependency_path = self.path / parts[0]
            dependency_commit = None if len(parts) < 2 else parts[1]

            if not dependency_path.exists():
                raise FileNotFoundError(f"Missing dependency: {dependency}")

            if dependency_commit is not None:
                try:
                    object_type = execute(
                        f"git cat-file -t {dependency_commit}",
                        directory=dependency_path,
                        capture=True,
                    )
                except ExternalCommandFailed:
                    dependency_commit_exists = False
                else:
                    dependency_commit_exists = object_type == "commit"

                if not dependency_commit_exists:
                    raise FileNotFoundError(f"Missing dependecy (commit): {dependency}")

        files = _find_files(path)

        job_hash = _hash_job(config, files)
        job_path = self.path / "jobs" / "by_hash" / job_hash

        if job_path.exists():
            print(f"Job exists already: {job_path}")
            return job_path

        config.setdefault("metadata", dict())
        config["metadata"]["createdAt"] = (
            datetime.now().replace(microsecond=0).isoformat()
        )

        os.makedirs(job_path)

        with open(job_path / "config.yaml", "w") as config_file:
            yaml.dump(config, config_file)

        for file in files:
            if file == Path("config.yaml"):
                continue

            source = path / file
            target = job_path / file
            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(source, target)

        # Add to by_date index.
        iso_format = r"%Y-%m-%dT%H:%M:%S"
        createdAt = datetime.strptime(config["metadata"]["createdAt"], iso_format)
        date = str(createdAt.date())
        by_date_path = self.path / "jobs" / "by_date" / date / job_hash
        os.makedirs(by_date_path.parent, exist_ok=True)
        os.symlink(job_path, by_date_path)

        # Add to by_tag index.
        for tag in config["metadata"].get("tags", []):
            by_tag_path = self.path / "jobs" / "by_tag" / tag / job_hash
            os.makedirs(by_tag_path.parent, exist_ok=True)
            os.symlink(job_path, by_tag_path)

        return job_path

    def checkout(self, hash: str, path: Union[str, os.PathLike]) -> None:
        path = Path(path)

        job_path = self.path / "jobs" / "by_hash" / hash
        if not job_path.exists():
            raise FileNotFoundError(f"Cannot find job: {hash}")

        os.makedirs(path)

        # Copy files
        for child in job_path.iterdir():
            if not child.name == "output":
                if child.is_dir():
                    shutil.copytree(child, path / child.name)
                else:
                    shutil.copy(child, path / child.name)

        # Symlink output directory
        os.symlink(job_path / "output", path / "output")

        # Symlink dependencies
        config = _read_config(job_path / "config.yaml")
        for dependency in config.get("dependencies", []):
            parts = dependency.split("@", maxsplit=1)
            dependency_path = parts[0]
            dependency_commit = None if len(parts) < 2 else parts[1]

            source = self.path / dependency_path
            destination = path / dependency_path

            os.makedirs(destination.parent, exist_ok=True)

            if dependency_commit is None:
                os.symlink(source, destination)
            else:
                execute(f"git clone {source} {destination}")
                execute(f"git checkout {dependency_commit}", directory=destination)

    def build_indices(self):
        """Builds the `by_date` and `by_tag` indices."""
        r3._indices.build_by_date_index(self.path)
        r3._indices.build_by_tag_index(self.path)


def _read_config(path: Path) -> Dict:
    if not path.is_file():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r") as config_file:
        return yaml.safe_load(config_file)


def _find_files(path: Path) -> List[Path]:
    return [child.relative_to(path) for child in path.rglob("*")]


def _hash_job(config: Dict, files: List[Path]) -> str:
    hashes = {
        str(file): _hash_file(file) for file in files if file != Path("config.yaml")
    }
    hashes["config.yaml"] = _hash_config(config)

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
    ignored_keys = {"environment", "metadata"}
    config = {key: value for key, value in config.items() if key not in ignored_keys}
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()
