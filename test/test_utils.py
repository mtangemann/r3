"""Unit tests for ``r3.utils``."""

import uuid
from pathlib import Path

import pytest
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


def test_is_valid_job_id_accepts_canonical_uuids() -> None:
    for _ in range(50):
        job_id = str(uuid.uuid4())
        assert r3.utils.is_valid_job_id(job_id)
    # The nil UUID is canonical too.
    assert r3.utils.is_valid_job_id("00000000-0000-0000-0000-000000000000")


@pytest.mark.parametrize(
    "bad",
    [
        "../../victim",                       # relative traversal
        "..",                                 # bare parent ref
        "/etc/passwd",                        # absolute path
        "a/b",                                # path separator
        "jobs\\evil",                         # backslash separator
        "job-*",                              # glob metacharacter
        "job?",                               # glob metacharacter
        "job[a-z]",                           # glob metacharacter
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",  # uppercase (non-canonical)
        "{00000000-0000-4000-8000-000000000000}",  # braces
        "urn:uuid:00000000-0000-4000-8000-000000000000",  # urn form
        "00000000000040008000000000000000",   # hex without hyphens (non-canonical)
        "not-a-uuid",
        "",
        "   ",
    ],
)
def test_is_valid_job_id_rejects_non_canonical(bad: str) -> None:
    assert not r3.utils.is_valid_job_id(bad)


@pytest.mark.parametrize("bad", [None, 123, 1.0, b"bytes", uuid.uuid4(), ["x"]])
def test_is_valid_job_id_rejects_non_str(bad: object) -> None:
    assert not r3.utils.is_valid_job_id(bad)


def test_validate_job_id_passes_for_canonical() -> None:
    r3.utils.validate_job_id(str(uuid.uuid4()))  # no raise


def test_validate_job_id_raises_for_non_canonical() -> None:
    with pytest.raises(ValueError):
        r3.utils.validate_job_id("../../escaped")
