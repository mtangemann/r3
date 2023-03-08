"""Repository of Reproducible Research."""

from pathlib import Path

from r3.core import Dependency, Job, Repository

with open(Path(__file__).parent.parent / "VERSION", "r") as version_file:
    __version__ = version_file.read().strip()

__all__ = ["Dependency", "Job", "Repository"]
