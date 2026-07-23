"""Repository for Reliable Research."""

from pathlib import Path

from r3.job import (
    Dependency,
    FindAllDependency,
    FindLatestDependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.repository import Repository


def _read_version() -> str:
    # Editable install / source tree: VERSION sits next to the package, so read it
    # live. `git pull` bumping VERSION then takes effect without reinstalling.
    version_file = Path(__file__).parent.parent / "VERSION"
    if version_file.is_file():
        return version_file.read_text().strip()

    # Non-editable (wheel) install: VERSION is not shipped, so fall back to the
    # version baked into the distribution metadata at build time.
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("r3")
    except PackageNotFoundError:
        return "unknown"


__version__ = _read_version()

__all__ = [
    "Dependency",
    "FindAllDependency",
    "FindLatestDependency",
    "GitDependency",
    "Job",
    "JobDependency",
    "QueryAllDependency",
    "QueryDependency",
    "Repository",
]
