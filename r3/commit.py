import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import click
import yaml


@click.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument(
    "repository",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def commit(path: Path, repository: Path) -> None:
    config = _read_config(path / "config.yaml")

    for dependency in config.get("dependencies", []):
        if not (repository / dependency).exists():
            raise FileNotFoundError(f"Missing dependency: {dependency}")

    files = _find_files(path)

    job_hash = _hash_job(config, files)
    job_path = repository / "jobs" / "by_hash" / job_hash

    if job_path.exists():
        print(f"Job exists already: {job_path}")
        return

    config.setdefault("metadata", dict())
    config["metadata"]["createdAt"] = datetime.now().replace(microsecond=0).isoformat()

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
    by_date_path = repository / "jobs" / "by_date" / date / job_hash
    os.makedirs(by_date_path.parent, exist_ok=True)
    os.symlink(job_path, by_date_path)

    # Add to by_tag index.
    for tag in config["metadata"].get("tags", []):
        by_tag_path = repository / "jobs" / "by_tag" / tag / job_hash
        os.makedirs(by_tag_path.parent, exist_ok=True)
        os.symlink(job_path, by_tag_path)

    print(job_path)


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
