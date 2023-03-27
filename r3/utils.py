import hashlib
import json
from pathlib import Path
from typing import Iterable, List, Optional

from executor import ExternalCommandFailed, execute


def find_files(path: Path, ignore_patterns: Iterable[str]) -> List[Path]:
    return [child.relative_to(path) for child in _find_files(path, ignore_patterns)]


def _find_files(path: Path, ignore_patterns: Iterable[str]) -> Iterable[Path]:
    if not all(pattern.startswith("/") for pattern in ignore_patterns):
        raise NotImplementedError(
            "Only absolute ignore patterns (starting with /) are supported for now."
        )

    for child in path.iterdir():
        if _is_ignored(child, ignore_patterns):
            continue

        if child.is_file():
            yield child

        elif child.is_dir():
            prefix = f"/{child.name}"
            ignore_patterns = [
                pattern[len(prefix) :]
                for pattern in ignore_patterns
                if pattern.startswith(prefix)
            ]
            yield from _find_files(child, ignore_patterns)


def _is_ignored(path: Path, ignore_patterns: Iterable[str]):
    return any(pattern == f"/{path.name}" for pattern in ignore_patterns)


def hash_dict(dict_) -> str:
    dict_json = json.dumps(dict_, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(bytes(dict_json, encoding="utf-8")).hexdigest()


def hash_file(path: Path, chunk_size: int = 2**16) -> str:
    hash = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            hash.update(chunk)

    return hash.hexdigest()


def git_path_exists(
    repository: Path,
    commit: Optional[str] = None,
    path: Optional[Path] = None,
) -> bool:
    commit = commit or "HEAD~1"
    path = path or Path(".")

    if not repository.is_dir():
        return False

    if path == Path("."):
        try:
            object_type = execute(
                f"git cat-file -t {commit}", directory=repository, capture=True
            )
        except ExternalCommandFailed:
            return False
        else:
            return object_type == "commit"

    else:
        try:
            execute(
                f"git ls-tree -r {commit} --name-only | grep '^{path}'",
                directory=repository,
                capture=True,
            )
        except ExternalCommandFailed:
            return False

        return True
