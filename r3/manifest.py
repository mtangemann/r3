"""The job manifest: a remote job's integrity + listing record.

The manifest is a pure integrity/listing record — it deliberately carries no
dependency graph or timestamp (those live in the authoritative ``r3.yaml`` sidecar)
and no absolute object keys (keys are derived from ``{prefix}{job_id}`` at read time).

Per-file hashes are computed by the caller in the same pass that writes the archive;
this module only assembles, (de)serialises, and verifies — it does not walk the job
directory to build a manifest.
"""

import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import r3.utils

MANIFEST_VERSION = 1
REPRESENTATION_TAR_ZST = "tar.zst"

#: A SHA-256 digest as stored/compared everywhere in the manifest: 64 lowercase hex.
_SHA256_RE = re.compile(r"[0-9a-f]{64}")

#: Logical files stored as their own sidecar objects rather than inside the archive.
SIDECAR_PATHS = ("r3.yaml", "metadata.yaml")


class ManifestError(Exception):
    """Raised when a manifest is malformed, unsupported, or does not match a job."""


@dataclass(frozen=True)
class FileEntry:
    """One logical file: a relative POSIX path with its byte size and SHA-256."""

    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class WalkedFile:
    """One regular file found by :func:`walk_regular_files`.

    ``path`` is relative to the walk root; ``size`` and ``mtime_ns`` come from the same
    ``os.lstat`` that classified the entry, so a caller needing a quiescence snapshot
    does not have to lstat a second time.
    """

    path: Path
    size: int
    mtime_ns: int


def walk_regular_files(root: Path) -> List[WalkedFile]:
    """Enumerates every regular file under ``root``, files-only, via ``os.lstat``.

    This is the single files-only gate shared by ``move``'s capture and
    :func:`verify_directory`, so both agree on exactly which on-disk entries count as
    payload. It never follows a symlink: it recurses only into real directories
    (``S_ISDIR`` on the lstat) and accepts only regular files with a single hard link
    (``S_ISREG`` and ``st_nlink == 1``). Empty directories (e.g. an empty ``output/``)
    are tolerated — they simply contribute no files.

    Returns:
        The regular files found, as :class:`WalkedFile` records, sorted by path.

    Raises:
        ManifestError: On the first symlink (broken, to a file, or to a directory),
            FIFO, socket, device node, other special entry, or hardlinked regular file,
            naming the offending relative path and its type. Such an entry is a
            data-integrity hazard the move/fetch model does not support, so this fails
            closed rather than following it (a symlink) or silently dropping it (a
            special file), either of which could lose data or corrupt a round-trip.
        OSError: Filesystem errors from ``os.scandir``/``os.lstat`` (e.g. an unreadable
            subdirectory, or an entry vanishing mid-walk) propagate unchanged rather
            than being wrapped as ``ManifestError``.
    """
    results: List[WalkedFile] = []
    _walk_regular_files(root, root, results)
    return sorted(results, key=lambda walked: walked.path.as_posix())


def _walk_regular_files(root: Path, current: Path, results: List[WalkedFile]) -> None:
    with os.scandir(current) as entries:
        children = sorted(entries, key=lambda entry: entry.name)
    for child in children:
        stat_result = os.lstat(child.path)
        mode = stat_result.st_mode
        relative = Path(child.path).relative_to(root)
        if stat.S_ISDIR(mode):
            _walk_regular_files(root, Path(child.path), results)
        elif stat.S_ISREG(mode):
            if stat_result.st_nlink > 1:
                raise ManifestError(
                    f"Refusing hardlinked file: {relative.as_posix()} has "
                    f"{stat_result.st_nlink} links (only one name per file is "
                    "supported)."
                )
            results.append(
                WalkedFile(relative, stat_result.st_size, stat_result.st_mtime_ns)
            )
        else:
            raise ManifestError(
                f"Refusing special filesystem entry: {relative.as_posix()} is a "
                f"{_describe_special_mode(mode)}."
            )


def _describe_special_mode(mode: int) -> str:
    if stat.S_ISLNK(mode):
        return "symbolic link"
    if stat.S_ISFIFO(mode):
        return "FIFO (named pipe)"
    if stat.S_ISSOCK(mode):
        return "socket"
    if stat.S_ISCHR(mode):
        return "character device"
    if stat.S_ISBLK(mode):
        return "block device"
    return "special file"


def build_manifest(
    job_id: str,
    files: List[FileEntry],
    archive_sha256: str,
    archive_size: int,
    representation: str = REPRESENTATION_TAR_ZST,
) -> Dict[str, Any]:
    """Assembles a manifest dict from precomputed file entries.

    ``files`` is the whole *logical* file set (including the ``r3.yaml`` and
    ``metadata.yaml`` sidecars). Entries are sorted by path so serialization is
    deterministic.
    """
    return {
        "manifest_version": MANIFEST_VERSION,
        "job_id": job_id,
        "representation": representation,
        "archive_sha256": archive_sha256,
        "archive_size": archive_size,
        "files": [
            {"path": entry.path, "size": entry.size, "sha256": entry.sha256}
            for entry in sorted(files, key=lambda entry: entry.path)
        ],
    }


def dumps(manifest: Dict[str, Any]) -> bytes:
    """Serialises a manifest to deterministic UTF-8 JSON bytes.

    Determinism matters: ``move`` publishes the manifest by byte-comparing a
    downloaded staging copy against these bytes, and ``fetch`` compares the remote
    manifest against a local receipt.
    """
    return json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8")


def loads(data: bytes, *, expected_job_id: Optional[str] = None) -> Dict[str, Any]:
    """Parses and structurally validates manifest bytes.

    ``expected_job_id`` is the single identity-binding hook for consumers that already
    know which job the bytes should describe (fetch and rebuild derive the key from a
    requested/enumerated id): when given, the parsed ``job_id`` must equal it, else the
    bytes describe a different job and are rejected. This keeps the identity check in
    one place rather than each consumer re-deriving it.

    Raises:
        ManifestError: If the bytes are not valid JSON, violate the schema, or (when
            ``expected_job_id`` is given) name a different job.
    """
    try:
        manifest = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ManifestError(f"Manifest is not valid JSON: {error}") from error
    validate(manifest)
    if expected_job_id is not None and manifest["job_id"] != expected_job_id:
        raise ManifestError(
            f"Manifest 'job_id' {manifest['job_id']!r} does not match expected "
            f"{expected_job_id!r}"
        )
    return manifest


def validate(manifest: Any) -> None:
    """Structurally validates a manifest object (schema, version, safe paths).

    This is the boundary ``rebuild`` uses to fail closed rather than accepting a
    malformed remote job.

    Raises:
        ManifestError: If the manifest is malformed or its version unsupported.
    """
    if not isinstance(manifest, dict):
        raise ManifestError("Manifest must be a JSON object.")

    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        raise ManifestError(f"Unsupported manifest_version: {version!r}")

    # A manifest is a closed schema: reject unknown keys rather than ignore them, so
    # corruption/tampering/version drift fails closed (the schema is version-gated
    # above, so additions come with a version bump).
    allowed = {
        "manifest_version",
        "job_id",
        "representation",
        "archive_sha256",
        "archive_size",
        "files",
    }
    missing = allowed - set(manifest)
    if missing:
        raise ManifestError(f"Manifest missing required keys: {sorted(missing)}")
    unknown = set(manifest) - allowed
    if unknown:
        raise ManifestError(f"Manifest has unknown keys: {sorted(unknown)}")

    if not r3.utils.is_valid_job_id(manifest["job_id"]):
        raise ManifestError(
            f"Manifest 'job_id' must be a canonical UUID: {manifest['job_id']!r}"
        )

    if manifest["representation"] != REPRESENTATION_TAR_ZST:
        raise ManifestError(f"Unknown representation: {manifest['representation']!r}")

    if not _is_sha256(manifest["archive_sha256"]):
        raise ManifestError(
            "Manifest 'archive_sha256' must be 64 lowercase hex chars: "
            f"{manifest['archive_sha256']!r}"
        )

    if not _is_valid_size(manifest["archive_size"]):
        raise ManifestError(
            "Manifest 'archive_size' must be a non-negative integer: "
            f"{manifest['archive_size']!r}"
        )

    if not isinstance(manifest["files"], list):
        raise ManifestError("Manifest 'files' must be a list.")

    seen = set()
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or set(entry) != {"path", "size", "sha256"}:
            raise ManifestError(f"Malformed file entry: {entry!r}")
        if not _is_valid_size(entry["size"]):
            raise ManifestError(
                f"File entry size must be a non-negative integer: {entry!r}"
            )
        if not _is_sha256(entry["sha256"]):
            raise ManifestError(
                f"File entry sha256 must be 64 lowercase hex chars: {entry!r}"
            )
        path = entry["path"]
        _validate_path(path)
        if path in seen:
            raise ManifestError(f"Duplicate file entry: {path!r}")
        seen.add(path)


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_valid_size(value: Any) -> bool:
    # ``bool`` is a subclass of ``int``; reject True/False as sizes explicitly.
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_path(path: Any) -> None:
    if not isinstance(path, str) or not path:
        raise ManifestError(f"Invalid manifest path: {path!r}")
    if path.startswith("./") or path.startswith("/"):
        raise ManifestError(f"Unsafe manifest path: {path!r}")
    # Reject degenerate segments directly. Splitting on "/" catches absolute paths
    # (leading empty segment), "..", ".", and empty/collapsed segments ("a//b") that
    # a PurePosixPath would silently drop — any of which would later confuse
    # extraction (e.g. an IsADirectoryError on a "." component).
    if any(segment in ("", ".", "..") for segment in path.split("/")):
        raise ManifestError(f"Unsafe manifest path: {path!r}")


def file_paths(manifest: Dict[str, Any]) -> List[Path]:
    """Returns the manifest's logical file set as relative ``Path`` objects."""
    return [Path(entry["path"]) for entry in manifest["files"]]


def verify_directory(job_dir: Path, manifest: Dict[str, Any]) -> None:
    """Verifies that ``job_dir`` matches ``manifest`` exactly.

    Checks that the set of files on disk equals the manifest's file set and that
    every file's size and SHA-256 agree. Directories (including an empty ``output/``)
    are not compared — the manifest lists files only. The on-disk set is enumerated by
    the shared files-only walker (:func:`walk_regular_files`), so a symlink at an
    expected path or an extra special entry is rejected rather than followed or ignored
    — a special entry must never let fetch's step-0 verification pass and delete the
    remote authoritative copy.

    Raises:
        ManifestError: On any missing file, extra file, size/checksum mismatch, or a
            symlink/special/hardlinked entry on disk.
    """
    expected = {entry["path"]: entry for entry in manifest["files"]}
    actual = {
        walked.path.as_posix(): (job_dir / walked.path)
        for walked in walk_regular_files(job_dir)
    }

    missing = sorted(set(expected) - set(actual))
    if missing:
        raise ManifestError(f"Missing files vs manifest: {missing}")

    extra = sorted(set(actual) - set(expected))
    if extra:
        raise ManifestError(f"Unexpected files vs manifest: {extra}")

    for path, entry in expected.items():
        target = actual[path]
        size = target.stat().st_size
        if size != entry["size"]:
            raise ManifestError(
                f"Size mismatch for {path}: {size} != {entry['size']}"
            )
        if r3.utils.hash_file(target) != entry["sha256"]:
            raise ManifestError(f"Checksum mismatch for {path}")
