"""Unit tests for r3.manifest (the integrity + listing record; design §4.1)."""

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
    verify_directory,
)


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
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    a = dumps(manifest)
    b = dumps(build_manifest("j", list(reversed(_entries())), "d" * 64, 1))
    assert isinstance(a, bytes)
    assert a == b  # order of input entries must not change the bytes


def test_loads_round_trip() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    assert loads(dumps(manifest)) == manifest


def test_loads_rejects_unknown_version() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    manifest["manifest_version"] = 999
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_bad_json() -> None:
    with pytest.raises(ManifestError):
        loads(b"not json{{{")


def test_loads_rejects_missing_key() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    del manifest["archive_sha256"]
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_unknown_top_level_key() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    manifest["surprise"] = "value"
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_unknown_file_entry_key() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    manifest["files"][0]["extra"] = 1
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_loads_rejects_duplicate_path() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    manifest["files"].append({"path": "r3.yaml", "size": 3, "sha256": "a" * 64})
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


@pytest.mark.parametrize("bad", ["/abs/path", "../escape", "./leading", "a/../b"])
def test_loads_rejects_unsafe_path(bad: str) -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
    manifest["files"].append({"path": bad, "size": 1, "sha256": "e" * 64})
    with pytest.raises(ManifestError):
        loads(dumps(manifest))


def test_file_paths() -> None:
    manifest = build_manifest("j", _entries(), "d" * 64, 1)
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
    return build_manifest("j", entries, "d" * 64, 1)


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
