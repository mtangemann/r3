"""Repository of Reproducible Research."""

from pathlib import Path

with open(Path(__file__).parent.parent / "VERSION", "r") as version_file:
    __version__ = version_file.read().strip()
