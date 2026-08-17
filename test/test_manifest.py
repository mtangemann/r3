"""Unit tests for r3.manifest (the integrity + listing record)."""

import os
from pathlib import Path

import pytest

from r3.manifest import (
    MANIFEST_VERSION,
    FileEntry,
    ManifestError,
    build_manifest,
    dumps,
    file_paths,
    loads,
    validate,
    verify_directory,
    walk_regular_files,
)

# A canonical job UUID, required by validate() for any manifest that is parsed. It
# contains hex letters so uppercasing yields a distinct (non-canonical) spelling.
JOB_ID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"


def _entries() -> list[FileEntry]:
    return [
        FileEntry("output/result.pt", 5, "c" * 64),
        FileEntry("r3.yaml", 3, "a" * 64),
        FileEntry("metadata.yaml", 4, "b" * 64),
    ]


def test_build_manifest_structure() -> None:
    manifest = build_manifest(
        job_id="job-1",
        files=_entries(),
        archive_sha256="d" * 64,
        archive_size=123,
    )
    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["job_id"] == "job-1"
    assert manifest["representation"] == "tar.zst"
    assert manifest["archive_sha256"] == "d" * 64
    assert manifest["archive_size"] == 123
    # No derived provenance in the manifest (deps/timestamp live in r3.yaml).
    assert "dependencies" not in manifest
    assert "timestamp" not in manifest
    # files are sorted by path for deterministic serialization.
    assert [e["path"] for e in manifest["files"]] == [
        "metadata.yaml",
        "output/result.pt",
        "r3.yaml",
    ]
    assert manifest["files"][0] == {
        "path": "metadata.yaml",
        "size": 4,
        "sha256": "b" * 64,
    }


def test_dumps_is_deterministic_and_json() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    a = dumps(manifest)
    b = dumps(build_manifest(JOB_ID, list(reversed(_entries())), "d" * 64, 1))
    assert isinstance(a, bytes)
    assert a == b  # order of input entries must not change the bytes


def test_loads_round_trip() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    assert loads(dumps(manifest)) == manifest


def test_loads_rejects_unknown_version() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["manifest_version"] = 999
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_bad_json() -> None:
    with pytest.raises(ManifestError):
        loads(b"not json{{{")


def test_loads_rejects_missing_key() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    del manifest["archive_sha256"]
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_unknown_top_level_key() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["surprise"] = "value"
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_unknown_file_entry_key() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["files"][0]["extra"] = 1
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_duplicate_path() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["files"].append({"path": "r3.yaml", "size": 3, "sha256": "a" * 64})
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


@pytest.mark.parametrize(
    "bad", ["/abs/path", "../escape", "./leading", "a/../b", ".", "a/./b"]
)
def test_loads_rejects_unsafe_path(bad: str) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["files"].append({"path": bad, "size": 1, "sha256": "e" * 64})
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_missing_job_id() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    del manifest["job_id"]
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


@pytest.mark.parametrize("bad", [123, None, ["x"], {"a": 1}])
def test_loads_rejects_non_string_job_id(bad: object) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["job_id"] = bad
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


@pytest.mark.parametrize(
    "bad",
    [
        "not-a-uuid",
        "job-1",
        JOB_ID.upper(),  # non-canonical form: uppercase
        JOB_ID.replace("-", ""),  # non-canonical form: no hyphens
        "urn:uuid:" + JOB_ID,  # non-canonical form: urn prefix
    ],
)
def test_loads_rejects_non_canonical_job_id(bad: str) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["job_id"] = bad
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_mismatched_expected_job_id() -> None:
    other = "22222222-2222-2222-2222-222222222222"
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    with pytest.raises(ManifestError):
        loads(dumps(manifest), expected_job_id=other)


def test_loads_accepts_matching_expected_job_id() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    assert loads(dumps(manifest), expected_job_id=JOB_ID) == manifest


@pytest.mark.parametrize("bad", ["z" * 64, "a" * 63, "a" * 65, "A" * 64, ""])
def test_validate_rejects_bad_archive_sha256(bad: str) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["archive_sha256"] = bad
    with pytest.raises(ManifestError):
        validate(manifest)


@pytest.mark.parametrize("bad", ["z" * 64, "a" * 63, "A" * 64])
def test_validate_rejects_bad_entry_sha256(bad: str) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["files"][0]["sha256"] = bad
    with pytest.raises(ManifestError):
        validate(manifest)


@pytest.mark.parametrize("bad", [-1, True, False])
def test_validate_rejects_bad_archive_size(bad: object) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["archive_size"] = bad
    with pytest.raises(ManifestError):
        validate(manifest)


@pytest.mark.parametrize("bad", [-1, True, False])
def test_validate_rejects_bad_entry_size(bad: object) -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    manifest["files"][0]["size"] = bad
    with pytest.raises(ManifestError):
        validate(manifest)


def test_file_paths() -> None:
    manifest = build_manifest(JOB_ID, _entries(), "d" * 64, 1)
    assert file_paths(manifest) == [
        Path("metadata.yaml"),
        Path("output/result.pt"),
        Path("r3.yaml"),
    ]


def _manifest_for_dir(job_dir: Path) -> dict:
    import r3.utils

    entries = []
    for child in sorted(job_dir.rglob("*")):
        if child.is_file():
            rel = child.relative_to(job_dir)
            entries.append(
                FileEntry(
                    rel.as_posix(),
                    child.stat().st_size,
                    r3.utils.hash_file(child),
                )
            )
    return build_manifest(JOB_ID, entries, "d" * 64, 1)


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    d = tmp_path / "job"
    (d / "output").mkdir(parents=True)
    (d / "r3.yaml").write_text("version: 1\n")
    (d / "metadata.yaml").write_text("tags: []\n")
    (d / "output" / "result.txt").write_text("result")
    return d


def test_verify_directory_passes(job_dir: Path) -> None:
    verify_directory(job_dir, _manifest_for_dir(job_dir))  # no raise


def test_verify_directory_missing_file(job_dir: Path) -> None:
    manifest = _manifest_for_dir(job_dir)
    (job_dir / "r3.yaml").unlink()
    with pytest.raises(ManifestError):
        verify_directory(job_dir, manifest)


def test_verify_directory_extra_file(job_dir: Path) -> None:
    manifest = _manifest_for_dir(job_dir)
    (job_dir / "extra.txt").write_text("x")
    with pytest.raises(ManifestError):
        verify_directory(job_dir, manifest)


def test_verify_directory_size_or_checksum_mismatch(job_dir: Path) -> None:
    manifest = _manifest_for_dir(job_dir)
    (job_dir / "output" / "result.txt").write_text("tampered!")
    with pytest.raises(ManifestError):
        verify_directory(job_dir, manifest)


def test_verify_directory_tolerates_empty_directory(tmp_path: Path) -> None:
    """A directory legitimately absent from the manifest (an empty ``output/``) must
    not be flagged; only special *files* are rejected."""
    d = tmp_path / "job"
    (d / "output").mkdir(parents=True)  # empty, leaves no manifest entry
    (d / "r3.yaml").write_text("version: 1\n")
    (d / "metadata.yaml").write_text("tags: []\n")
    verify_directory(d, _manifest_for_dir(d))  # no raise


def test_verify_directory_rejects_special_entry(job_dir: Path) -> None:
    """An extra special entry (a FIFO) must be rejected rather than ignored: fetch's
    step-0 verification must not pass and go on to delete the remote copy."""
    manifest = _manifest_for_dir(job_dir)
    os.mkfifo(job_dir / "output" / "pipe")
    with pytest.raises(ManifestError, match="output/pipe"):
        verify_directory(job_dir, manifest)


def test_verify_directory_rejects_symlink_at_expected_path(job_dir: Path) -> None:
    """A symlink placed where a regular file is expected must be rejected rather than
    followed, even if it resolves to matching content."""
    manifest = _manifest_for_dir(job_dir)
    target = job_dir.parent / "elsewhere.txt"
    target.write_text("result")  # same bytes as output/result.txt
    (job_dir / "output" / "result.txt").unlink()
    (job_dir / "output" / "result.txt").symlink_to(target)
    with pytest.raises(ManifestError, match="output/result.txt"):
        verify_directory(job_dir, manifest)


# --------------------------------------------------------------- walk_regular_files


def test_walk_regular_files_returns_sorted_nested_files(tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "a" / "b").mkdir(parents=True)
    (root / "top.txt").write_text("top")
    (root / "a" / "mid.txt").write_text("middle")
    (root / "a" / "b" / "deep.bin").write_bytes(b"xyz")
    (root / "empty").mkdir()  # empty dir tolerated, contributes nothing

    walked = walk_regular_files(root)

    assert [w.path.as_posix() for w in walked] == [
        "a/b/deep.bin",
        "a/mid.txt",
        "top.txt",
    ]
    by_path = {w.path.as_posix(): w for w in walked}
    assert by_path["top.txt"].size == len("top")
    assert by_path["a/b/deep.bin"].size == 3
    assert all(w.mtime_ns > 0 for w in walked)


def test_walk_regular_files_rejects_fifo(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "keep.txt").write_text("keep")
    os.mkfifo(root / "pipe")
    with pytest.raises(ManifestError, match="pipe"):
        walk_regular_files(root)


def test_walk_regular_files_rejects_file_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "real.txt").write_text("real")
    (root / "link.txt").symlink_to(root / "real.txt")
    with pytest.raises(ManifestError, match="symbolic link"):
        walk_regular_files(root)


def test_walk_regular_files_rejects_dir_symlink_without_following(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    (root / "realdir").mkdir(parents=True)
    (root / "realdir" / "inside.txt").write_text("inside")
    external = tmp_path / "external"
    external.mkdir()
    (root / "dirlink").symlink_to(external)
    with pytest.raises(ManifestError, match="symbolic link"):
        walk_regular_files(root)


def test_walk_regular_files_rejects_broken_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "broken").symlink_to(root / "does-not-exist")
    with pytest.raises(ManifestError, match="symbolic link"):
        walk_regular_files(root)


def test_walk_regular_files_rejects_internal_hardlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("shared")
    os.link(root / "a.txt", root / "b.txt")  # two names, one inode
    with pytest.raises(ManifestError, match="hardlink"):
        walk_regular_files(root)


def test_walk_regular_files_rejects_external_hardlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    external = tmp_path / "external.txt"
    external.write_text("shared")
    os.link(external, root / "hard.txt")  # link count > 1, twin outside the tree
    with pytest.raises(ManifestError, match="hardlink"):
        walk_regular_files(root)
