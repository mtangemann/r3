import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Set

import yaml

_IndexType = Dict[str, Set[str]]


def build_by_date_index(repository: Path) -> None:
    print("Building index: by_date")
    index: _IndexType = dict()

    for job in (repository / "jobs" / "by_hash").iterdir():
        with open(job / "config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)

        if "createdAt" not in config.get("metadata", dict()):
            date = "unknown"
        else:
            iso_format = r"%Y-%m-%dT%H:%M:%S"
            createdAt = datetime.strptime(config["metadata"]["createdAt"], iso_format)
            date = str(createdAt.date())

        if date not in index:
            index[date] = set()

        index[date].add(job.name)

    _write_index(repository, "by_date", index)


def build_by_tag_index(repository: Path) -> None:
    print("Building index: by_tag")
    index: _IndexType = dict()

    for job in (repository / "jobs" / "by_hash").iterdir():
        with open(job / "config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)

        tags = config.get("metadata", dict()).get("tags", [])

        for tag in tags:
            if tag not in index:
                index[tag] = set()

            index[tag].add(job.name)

        _write_index(repository, "by_tag", index)


def _write_index(repository: Path, index_name: str, index: _IndexType) -> None:
    jobs_path = repository / "jobs" / "by_hash"
    index_path = repository / "jobs" / index_name

    if index_path.exists():
        print(f"Deleting existing index: {index_name}")
        shutil.rmtree(index_path)

    os.mkdir(index_path)

    for key, hashes in index.items():
        os.makedirs(index_path / key, exist_ok=True)

        for hash_ in hashes:
            os.symlink(jobs_path / hash_, index_path / key / hash_)
