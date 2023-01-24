import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Union

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

    def commit(self, path: Union[str, os.PathLike]) -> Path:
        """Adds the job from the given path to the repository.

        Returns
        -------
        Path
            The path to the job within the repository.
        """
        path = Path(path)

        config = _read_config(path / "r3.yaml")

        ignore_paths = set(config.get("ignore", []))

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

            ignore_paths.add(f"/{dependency}")
            if (path / parts[0]).exists() and (
                path / parts[0]
            ).resolve() != dependency_path:
                print(f"WARNING: Ignoring {parts[0]}")

        files = r3.utils.find_files(path, ignore_paths)

        job_hash = _hash_job(config, files)
        job_path = self.path / "jobs" / job_hash

        if job_path.exists():
            print(f"Job exists already: {job_path}")
            return job_path

        config.setdefault("metadata", dict())
        config["metadata"]["date"] = datetime.now().replace(microsecond=0).isoformat()
        config["metadata"]["source"] = str(path.absolute())

        os.makedirs(job_path)

        with open(job_path / "r3.yaml", "w") as config_file:
            yaml.dump(config, config_file)

        for file in files:
            if file == Path(path / "r3.yaml"):
                continue

            target = job_path / file
            os.makedirs(target.parent, exist_ok=True)
            shutil.copy(file, target)

        return job_path

    def checkout(self, hash: str, path: Union[str, os.PathLike]) -> None:
        path = Path(path)

        job_path = self.path / "jobs" / hash
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
        config = _read_config(job_path / "r3.yaml")
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

    def rebuild_cache(self):
        """Aggregates all job metadata into 'metadata.json'."""
        metadata = dict()

        for job in (self.path / "jobs").iterdir():
            with open(job / "r3.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)

            metadata[job.name] = config.get("metadata", dict())

        with open(self.path / "metadata.json", "w") as metadata_file:
            json.dump(metadata, metadata_file)


def _read_config(path: Path) -> Dict:
    if not path.is_file():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r") as config_file:
        return yaml.safe_load(config_file)


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
    ignored_keys = {"environment", "metadata"}
    config = {key: value for key, value in config.items() if key not in ignored_keys}
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()
