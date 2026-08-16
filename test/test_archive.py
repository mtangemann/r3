"""Unit tests for r3.archive — the files-only tar.zst builder and safe extraction.

The extraction tests are adversarial: they cover path-traversal attacks, where a
crafted archive member tries to write outside the destination directory.
"""

import io
import tarfile
from pathlib import Path

import pytest
import pyzstd

from r3.archive import ArchiveError, create_archive, safe_extract


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    d = tmp_path / "job"
    (d / "output" / "sub").mkdir(parents=True)
    (d / "input.txt").write_text("input data")
    (d / "output" / "result.txt").write_text("result data")
    (d / "output" / "sub" / "deep.bin").write_bytes(b"\x00\x01\x02deep")
    return d


def _member_paths() -> list:
    # archive members = job files EXCEPT the r3.yaml/metadata.yaml sidecars
    return [Path("input.txt"), Path("output/result.txt"), Path("output/sub/deep.bin")]


# ---------------------------------------------------------------- create_archive


def test_create_archive_entries_and_hashes(tmp_path: Path, job_dir: Path) -> None:
    import r3.utils

    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, _member_paths(), archive_path)

    assert result.path == archive_path
    assert archive_path.exists()
    assert result.size == archive_path.stat().st_size
    assert result.sha256 == r3.utils.hash_file(archive_path)

    by_path = {e.path: e for e in result.entries}
    assert set(by_path) == {"input.txt", "output/result.txt", "output/sub/deep.bin"}
    assert by_path["input.txt"].size == len("input data")
    assert by_path["input.txt"].sha256 == r3.utils.hash_file(job_dir / "input.txt")


def test_create_archive_round_trips_via_safe_extract(
    tmp_path: Path, job_dir: Path
) -> None:
    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, _member_paths(), archive_path)

    staging = tmp_path / "staging"
    expected = {e.path: e.size for e in result.entries}
    safe_extract(archive_path, staging, expected)

    assert (staging / "input.txt").read_text() == "input data"
    assert (staging / "output" / "result.txt").read_text() == "result data"
    assert (staging / "output" / "sub" / "deep.bin").read_bytes() == b"\x00\x01\x02deep"
    # output/ is (re)created even though it only contained files here
    assert (staging / "output").is_dir()


def test_create_archive_rejects_symlink_member(tmp_path: Path, job_dir: Path) -> None:
    (job_dir / "link.txt").symlink_to(job_dir / "input.txt")
    archive_path = tmp_path / "a.tar.zst"
    with pytest.raises(ArchiveError):
        create_archive(job_dir, [Path("link.txt")], archive_path)


def test_safe_extract_creates_output_dir_when_absent(
    tmp_path: Path, job_dir: Path
) -> None:
    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, [Path("input.txt")], archive_path)
    staging = tmp_path / "staging"
    safe_extract(archive_path, staging, {e.path: e.size for e in result.entries})
    assert (staging / "output").is_dir()


# ---------------------------------------------------------------- safe_extract: attacks


def _write_evil_archive(path: Path, members: list) -> None:
    """Writes a tar.zst with the given (TarInfo, payload-bytes) members verbatim."""
    with pyzstd.SeekableZstdFile(str(path), "w") as zfh:
        with tarfile.open(fileobj=zfh, mode="w|") as tar:
            for info, payload in members:
                tar.addfile(info, io.BytesIO(payload) if payload is not None else None)


def _regular(name: str, data: bytes) -> tuple:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.type = tarfile.REGTYPE
    return info, data


def test_safe_extract_rejects_parent_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("../escaped.txt", b"pwned")])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"../escaped.txt": 5})
    assert not (tmp_path / "escaped.txt").exists()


def test_safe_extract_rejects_absolute_path(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("/tmp/r3-abs-escape", b"pwned")])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"/tmp/r3-abs-escape": 5})
    assert not Path("/tmp/r3-abs-escape").exists()


def test_safe_extract_rejects_symlink_member(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    info = tarfile.TarInfo("link")
    info.type = tarfile.SYMTYPE
    info.linkname = "/etc/passwd"
    _write_evil_archive(archive, [(info, None)])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"link": 0})
    assert not (staging / "link").exists()


def test_safe_extract_rejects_hardlink_member(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    info = tarfile.TarInfo("hard")
    info.type = tarfile.LNKTYPE
    info.linkname = "input.txt"
    _write_evil_archive(archive, [(info, None)])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"hard": 0})


def test_safe_extract_rejects_directory_member(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    info = tarfile.TarInfo("adir")
    info.type = tarfile.DIRTYPE
    _write_evil_archive(archive, [(info, None)])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"adir": 0})


def test_safe_extract_rejects_duplicate_member(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(
        archive, [_regular("a.txt", b"one"), _regular("a.txt", b"two")]
    )
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"a.txt": 3})


def test_safe_extract_rejects_member_not_in_allowed(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("surprise.txt", b"x")])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"expected.txt": 1})
    assert not (staging / "surprise.txt").exists()


def test_safe_extract_rejects_sidecar_member(tmp_path: Path) -> None:
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("r3.yaml", b"version: 1")])
    staging = tmp_path / "staging"
    # even if it were "allowed", a sidecar name inside the archive is rejected
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"r3.yaml": 10})


def test_safe_extract_rejects_size_mismatch(tmp_path: Path) -> None:
    # The linchpin manifest-bounded check: a member whose on-tape size disagrees
    # with the manifest is rejected before it is written.
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("a.txt", b"three")])  # 5 bytes
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"a.txt": 4})
    assert not (staging / "a.txt").exists()


def test_safe_extract_rejects_contiguous_member(tmp_path: Path) -> None:
    # CONTTYPE (and GNUTYPE_SPARSE) satisfy tarfile.isreg() but are not plain
    # regular files; the tightened type gate rejects them.
    archive = tmp_path / "evil.tar.zst"
    data = b"contig"
    info = tarfile.TarInfo("cont.txt")
    info.size = len(data)
    info.type = tarfile.CONTTYPE
    _write_evil_archive(archive, [(info, data)])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"cont.txt": len(data)})
    assert not (staging / "cont.txt").exists()


def test_safe_extract_rejects_file_then_dir_parent(tmp_path: Path) -> None:
    # Member 'a' (a file) followed by 'a/b' (which needs 'a' to be a directory). The
    # stdlib data filter's realpath lstats the path and raises an unwrapped OSError
    # (NotADirectoryError); safe_extract must still surface a clean ArchiveError.
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("a", b"x"), _regular("a/b", b"y")])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"a": 1, "a/b": 1})


def test_safe_extract_wraps_write_collision(tmp_path: Path) -> None:
    # Member 'a/b' (a file) creates directory 'a'; the later member 'a' (a file) then
    # collides with that directory, so opening it for writing raises IsADirectoryError,
    # which the per-member write wrap converts into a clean ArchiveError.
    archive = tmp_path / "evil.tar.zst"
    _write_evil_archive(archive, [_regular("a/b", b"x"), _regular("a", b"y")])
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive, staging, {"a/b": 1, "a": 1})


def _staging_is_empty(staging: Path) -> bool:
    return not staging.exists() or not any(staging.iterdir())


def test_safe_extract_rejects_total_bytes_cap(tmp_path: Path, job_dir: Path) -> None:
    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, _member_paths(), archive_path)
    expected = {e.path: e.size for e in result.entries}
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive_path, staging, expected, max_total_bytes=1)
    assert _staging_is_empty(staging)


def test_safe_extract_rejects_file_count_cap(tmp_path: Path, job_dir: Path) -> None:
    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, _member_paths(), archive_path)
    expected = {e.path: e.size for e in result.entries}
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive_path, staging, expected, max_file_count=1)
    assert _staging_is_empty(staging)


def test_safe_extract_rejects_single_file_cap(tmp_path: Path, job_dir: Path) -> None:
    archive_path = tmp_path / "a.tar.zst"
    result = create_archive(job_dir, _member_paths(), archive_path)
    expected = {e.path: e.size for e in result.entries}
    staging = tmp_path / "staging"
    with pytest.raises(ArchiveError):
        safe_extract(archive_path, staging, expected, max_file_bytes=1)
    assert _staging_is_empty(staging)


def test_create_archive_rejects_sidecar_member(tmp_path: Path, job_dir: Path) -> None:
    archive_path = tmp_path / "a.tar.zst"
    with pytest.raises(ArchiveError):
        create_archive(job_dir, [Path("input.txt"), Path("r3.yaml")], archive_path)
