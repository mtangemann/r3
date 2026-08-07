"""Unit tests for ``r3.utils``."""

from pathlib import Path

from pyfakefs.fake_filesystem import FakeFilesystem

import r3.utils


def test_find_files_ignores_sibling_dirs_after_recursing_into_earlier_sibling(
    fs: FakeFilesystem,
) -> None:
    """Ignore patterns must not be lost after recursing into a subdirectory.

    Regression test: when _find_files recurses into a non-ignored subdirectory, it
    was reassigning the `ignore_patterns` local variable. Subsequent siblings at the
    same level were then checked against the stripped (possibly empty) sub-patterns
    instead of the original ones, causing ignored dirs to be traversed.
    """
    fs.create_file("/job/run.py")
    fs.create_file("/job/subdir/helper.py")   # non-ignored subdir comes first
    fs.create_file("/job/cache/big_file.bin") # should be ignored

    files = r3.utils.find_files(Path("/job"), ["/cache"])

    assert Path("cache/big_file.bin") not in files
