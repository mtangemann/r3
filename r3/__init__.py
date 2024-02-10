"""Repository of Reproducible Research."""

from pathlib import Path

from r3.job import Dependency, Job
from r3.repository import Repository

with open(Path(__file__).parent.parent / "VERSION", "r") as version_file:
    __version__ = version_file.read().strip()

__all__ = ["Dependency", "Job", "Repository"]
