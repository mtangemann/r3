"""Backend-agnostic archive handling: build a files-only ``tar.zst`` and extract it
safely.

This is the generic half of the remote-storage layer: it knows about
tar/zstd and path safety, but nothing about S3. ``S3Remote`` uses it to create the
payload archive and to reconstruct a job directory on fetch.

The archive contains **file members only** — no directory entries and no ``r3.yaml``
/ ``metadata.yaml`` sidecars (those are stored as separate objects). Each member is
hashed as it is written, so the returned entries describe exactly the archive's
contents. Extraction validates every member before writing it,
which closes the F-01 path-traversal blocker.
"""

import hashlib
import os
import shutil
import stat
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, List, Mapping, Set

import r3.utils
from r3.manifest import SIDECAR_PATHS, FileEntry

DEFAULT_FRAME_SIZE = 16 * 1024 * 1024

# Absolute defense-in-depth backstops for extraction. Extraction is primarily
# bounded by the manifest (every member must match an expected size), but these
# generous caps reject an over-large or over-populated manifest before any bytes are
# written, regardless of whether the manifest is honest about its own sizes.
DEFAULT_MAX_TOTAL_BYTES = 1024**4  # 1 TiB total
DEFAULT_MAX_FILE_COUNT = 1_000_000
DEFAULT_MAX_FILE_BYTES = 1024**4  # 1 TiB per file

# The stdlib tar extraction filter (PEP 706) is the well-maintained reference guard
# for the path-traversal / link-escape / special-file CVE class. It is present on
# Python 3.10.12+ (our floor's patched releases); we fall back to a manual check on
# older patch levels. Domain checks (manifest membership, size, dedup, sidecar,
# files-only) are layered on top — no library provides those.
_TAR_DATA_FILTER = getattr(tarfile, "data_filter", None)


class ArchiveError(Exception):
    """Raised when an archive cannot be built or a member is unsafe/invalid."""


@dataclass(frozen=True)
class ArchiveResult:
    """The outcome of :func:`create_archive`."""

    path: Path
    sha256: str
    size: int
    entries: List[FileEntry]


def _import_pyzstd() -> Any:
    try:
        import pyzstd
    except ImportError as error:  # pragma: no cover - pyzstd is a required dep
        raise ImportError(
            "The tar.zst archive format requires pyzstd. Install it with: "
            "pip install pyzstd"
        ) from error
    return pyzstd


class _HashingReader:
    """Wraps a binary file object, updating a hash with every byte read.

    ``tarfile.addfile`` reads the member through this, so the recorded digest is
    exactly the bytes written into the archive.
    """

    def __init__(self, fileobj: BinaryIO, hasher: "hashlib._Hash") -> None:
        self._fileobj = fileobj
        self._hasher = hasher

    def read(self, size: int = -1) -> bytes:
        data = self._fileobj.read(size)
        self._hasher.update(data)
        return data


def create_archive(
    job_dir: Path,
    member_paths: List[Path],
    archive_path: Path,
    frame_size: int = DEFAULT_FRAME_SIZE,
) -> ArchiveResult:
    """Builds a seekable ``tar.zst`` archive of ``member_paths`` under ``job_dir``.

    Parameters:
        job_dir: The job directory the members are relative to.
        member_paths: Relative paths of the files to include (files only; the caller
            must exclude the sidecars).
        archive_path: Where to write the archive.
        frame_size: Uncompressed zstd frame size (seekability granularity).

    Returns:
        The archive path, its whole-file SHA-256 and size, and a per-member
        :class:`FileEntry` list (hashed as written).

    Raises:
        ArchiveError: If a member is a sidecar, a directory, or not a regular file.
    """
    pyzstd = _import_pyzstd()
    entries: List[FileEntry] = []

    with pyzstd.SeekableZstdFile(
        str(archive_path), "w", max_frame_content_size=frame_size
    ) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|") as tar:
            for rel in member_paths:
                arcname = rel.as_posix()
                if arcname in SIDECAR_PATHS:
                    raise ArchiveError(
                        f"Sidecar {arcname!r} must not be an archive member."
                    )
                source = job_dir / rel
                mode = os.lstat(source).st_mode
                if not stat.S_ISREG(mode):
                    raise ArchiveError(
                        f"Refusing to archive non-regular file: {arcname!r} "
                        "(symlinks, devices, and FIFOs are not supported)."
                    )
                info = tar.gettarinfo(name=str(source), arcname=arcname)
                hasher = hashlib.sha256()
                with open(source, "rb") as file:
                    tar.addfile(info, _HashingReader(file, hasher))  # type: ignore[arg-type]
                entries.append(FileEntry(arcname, info.size, hasher.hexdigest()))

    return ArchiveResult(
        path=archive_path,
        sha256=r3.utils.hash_file(archive_path),
        size=archive_path.stat().st_size,
        entries=entries,
    )


def safe_extract(
    archive_path: Path,
    staging_dir: Path,
    expected_sizes: Mapping[str, int],
    *,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_file_count: int = DEFAULT_MAX_FILE_COUNT,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
) -> None:
    """Extracts ``archive_path`` into a fresh ``staging_dir``, validating each member.

    Streams the archive (``r|``) and, before writing any member, rejects: unsafe
    paths (absolute, ``..``, escaping the staging root) via the stdlib tar data
    filter; non-regular (non-file) members; sidecar names; names not in
    ``expected_sizes``; a declared size that disagrees with the manifest; and
    duplicates. Every accepted member must appear once in ``expected_sizes`` with a
    matching size, so extraction is bounded by the manifest; in addition, absolute
    backstop caps are enforced from ``expected_sizes`` before any bytes are written,
    rejecting an over-large or over-populated manifest regardless of whether it is
    honest about its own sizes. Parent directories are created as needed and a
    conventional ``output/`` is ensured.

    Parameters:
        expected_sizes: The archive-resident manifest entries as ``{path: size}``
            (manifest files minus the two sidecars). Every member name must be a key,
            and every member's declared size must equal the mapped value.
        max_total_bytes: Backstop cap on the summed declared size of all members.
        max_file_count: Backstop cap on the number of members.
        max_file_bytes: Backstop cap on any single member's declared size.

    Raises:
        ArchiveError: On any unsafe, unexpected, or size-mismatched member, if a
            backstop cap is exceeded, or if a member cannot be written.
    """
    _enforce_extraction_caps(
        expected_sizes, max_total_bytes, max_file_count, max_file_bytes
    )

    pyzstd = _import_pyzstd()
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_root = staging_dir.resolve()

    seen: Set[str] = set()

    with pyzstd.SeekableZstdFile(str(archive_path), "r") as compressed:
        with tarfile.open(fileobj=compressed, mode="r|") as tar:
            for member in tar:
                _validate_member(member, expected_sizes, seen, staging_root)

                dest = staging_dir / member.name
                try:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    source = tar.extractfile(member)
                    assert source is not None  # guaranteed for regular files
                    with open(dest, "wb") as out:
                        shutil.copyfileobj(source, out)
                except OSError as error:
                    raise ArchiveError(
                        f"Failed to write archive member {member.name!r}: {error}"
                    ) from error

    (staging_dir / "output").mkdir(parents=True, exist_ok=True)


def _enforce_extraction_caps(
    expected_sizes: Mapping[str, int],
    max_total_bytes: int,
    max_file_count: int,
    max_file_bytes: int,
) -> None:
    count = len(expected_sizes)
    if count > max_file_count:
        raise ArchiveError(
            f"Manifest lists {count} files, exceeding the extraction cap of "
            f"{max_file_count}."
        )
    total = sum(expected_sizes.values())
    if total > max_total_bytes:
        raise ArchiveError(
            f"Manifest declares {total} bytes, exceeding the extraction cap of "
            f"{max_total_bytes}."
        )
    for name, size in expected_sizes.items():
        if size > max_file_bytes:
            raise ArchiveError(
                f"Manifest file {name!r} declares {size} bytes, exceeding the "
                f"per-file cap of {max_file_bytes}."
            )


def _validate_member(
    member: tarfile.TarInfo,
    expected_sizes: Mapping[str, int],
    seen: Set[str],
    staging_root: Path,
) -> None:
    name = member.name

    # Path-traversal / link-escape safety: delegate to the stdlib data filter where
    # available (PEP 706), plus a manual within-root check as defense in depth.
    if _TAR_DATA_FILTER is not None:
        try:
            _TAR_DATA_FILTER(member, str(staging_root))
        except (tarfile.FilterError, OSError) as error:
            # The filter's realpath lstats the target; a member like ``a/b`` after a
            # regular file ``a`` raises a bare OSError (NotADirectoryError) we must
            # also surface as ArchiveError rather than let it escape.
            raise ArchiveError(f"Unsafe archive member {name!r}: {error}") from error
    else:  # pragma: no cover - only on Python < 3.10.12
        pure = PurePosixPath(name)
        if pure.is_absolute() or name.startswith("/") or ".." in pure.parts:
            raise ArchiveError(f"Unsafe archive member path: {name!r}")
    if not _is_within(staging_root / name, staging_root):
        raise ArchiveError(f"Member escapes the staging directory: {name!r}")

    # Only plain regular files. ``isreg()`` also accepts CONTTYPE/GNUTYPE_SPARSE and
    # the stdlib filter tolerates dirs/safe symlinks; we accept none of those.
    if member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE):
        raise ArchiveError(
            f"Refusing non-regular archive member: {name!r} "
            f"(type {member.type!r} — only regular files are allowed)."
        )

    if name in SIDECAR_PATHS:
        raise ArchiveError(f"Sidecar {name!r} must not appear inside the archive.")

    if name not in expected_sizes:
        raise ArchiveError(f"Archive member not listed in the manifest: {name!r}")

    if member.size != expected_sizes[name]:
        raise ArchiveError(
            f"Archive member {name!r} declares size {member.size}, "
            f"manifest expects {expected_sizes[name]}."
        )

    if name in seen:
        raise ArchiveError(f"Duplicate archive member: {name!r}")
    seen.add(name)


def _is_within(path: Path, root: Path) -> bool:
    try:
        resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    except OSError:
        return False
    return resolved == root or root in resolved.parents
