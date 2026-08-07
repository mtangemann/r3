"""Tests for package version resolution."""

from pathlib import Path

import r3

VERSION_FILE = Path(__file__).parent.parent / "VERSION"


def test_version_matches_version_file() -> None:
    """In a source tree / editable install, ``__version__`` reflects VERSION live.

    This is the path exercised by the CLI's ``--version`` and by tests running
    against an editable install, so it must track the on-disk VERSION file without
    requiring a reinstall.
    """
    assert r3.__version__ == VERSION_FILE.read_text().strip()
