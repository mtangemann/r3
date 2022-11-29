import hashlib
import json
import os
import shutil
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
    job_path = repository / "jobs" / job_hash

    if job_path.exists():
        print(f"Job exists already: {job_path}")
        return

    for file in files:
        source = path / file
        target = job_path / file
        os.makedirs(target.parent, exist_ok=True)
        shutil.copy(source, target)

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
    config = {key: value for key, value in config.items() if key != "environment"}
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(config_json, encoding="utf-8")).hexdigest()
