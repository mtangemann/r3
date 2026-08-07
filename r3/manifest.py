"""The job manifest: a remote job's integrity + listing record.

See the durable remote-storage design, §4.1. The manifest is a pure
integrity/listing record — it deliberately carries no dependency graph or timestamp
(those live in the authoritative ``r3.yaml`` sidecar) and no absolute object keys
(keys are derived from ``{prefix}{job_id}`` at read time).

Per-file hashes are computed by the caller in the same pass that writes the archive
(design §5 step 1); this module only assembles, (de)serialises, and verifies — it
does not walk the job directory to build a manifest.
"""

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List

import r3.utils

MANIFEST_VERSION = 1
REPRESENTATION_TAR_ZST = "tar.zst"

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


def loads(data: bytes) -> Dict[str, Any]:
    """Parses and structurally validates manifest bytes.

    Raises:
        ManifestError: If the bytes are not valid JSON or violate the schema.
    """
    try:
        manifest = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ManifestError(f"Manifest is not valid JSON: {error}") from error
    validate(manifest)
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

    if manifest["representation"] != REPRESENTATION_TAR_ZST:
        raise ManifestError(f"Unknown representation: {manifest['representation']!r}")

    if not isinstance(manifest["archive_size"], int):
        raise ManifestError("Manifest 'archive_size' must be an integer.")

    if not isinstance(manifest["files"], list):
        raise ManifestError("Manifest 'files' must be a list.")

    seen = set()
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or set(entry) != {"path", "size", "sha256"}:
            raise ManifestError(f"Malformed file entry: {entry!r}")
        if not isinstance(entry["size"], int):
            raise ManifestError(f"File entry size must be an integer: {entry!r}")
        path = entry["path"]
        _validate_path(path)
        if path in seen:
            raise ManifestError(f"Duplicate file entry: {path!r}")
        seen.add(path)


def _validate_path(path: Any) -> None:
    if not isinstance(path, str) or not path:
        raise ManifestError(f"Invalid manifest path: {path!r}")
    if path.startswith("./") or path.startswith("/"):
        raise ManifestError(f"Unsafe manifest path: {path!r}")
    pure = PurePosixPath(path)
    if pure.is_absolute() or ".." in pure.parts:
        raise ManifestError(f"Unsafe manifest path: {path!r}")


def file_paths(manifest: Dict[str, Any]) -> List[Path]:
    """Returns the manifest's logical file set as relative ``Path`` objects."""
    return [Path(entry["path"]) for entry in manifest["files"]]


def verify_directory(job_dir: Path, manifest: Dict[str, Any]) -> None:
    """Verifies that ``job_dir`` matches ``manifest`` exactly.

    Checks that the set of files on disk equals the manifest's file set and that
    every file's size and SHA-256 agree. Directories (including an empty ``output/``)
    are not compared — the manifest lists files only.

    Raises:
        ManifestError: On any missing file, extra file, or size/checksum mismatch.
    """
    expected = {entry["path"]: entry for entry in manifest["files"]}
    actual = {
        path.as_posix(): (job_dir / path) for path in _walk_files(job_dir)
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


def _walk_files(root: Path) -> List[Path]:
    return sorted(
        child.relative_to(root) for child in root.rglob("*") if child.is_file()
    )
