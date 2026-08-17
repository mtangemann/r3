"""Remote-storage reconciliation (ALPHA).

Reconciles each configured remote's bucket against the index, read-only. It reports
bucket/index drift without mutating anything (no deletes, no writes) — the read-only
foundation a future mutating ``gc`` would build on.

This module and its result types are provisional and may change without notice; they
are not part of R3's stable public API. ``Repository.remote_check`` is the public
entry point that delegates here.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set

import r3.manifest
import r3.utils
from r3.index import MAX_MANIFEST_BYTES, MAX_SIDECAR_BYTES, Index
from r3.manifest import SIDECAR_PATHS
from r3.remote import (
    _ARCHIVE_NAME,
    _MANIFEST_NAME,
    _STAGING_MANIFEST_NAME,
    Remote,
    RemoteError,
)

# The per-job object names (imported from ``r3.remote``, the one source of truth for
# the key scheme) that reconciliation classifies enumerated keys against.
#: Objects whose presence (without a manifest) marks a manifestless job prefix.
_PAYLOAD_NAMES = frozenset({_ARCHIVE_NAME, *SIDECAR_PATHS})


@dataclass(frozen=True)
class _RemoteCheckFinding:
    """One reconciliation finding for a single job on a single remote."""

    remote: str
    job_id: str
    detail: str


@dataclass(frozen=True)
class _MultipartUploadFinding:
    """An incomplete multipart upload lingering under a remote's prefix."""

    remote: str
    key: str
    upload_id: str


@dataclass
class _RemoteCheckReport:
    """Structured, read-only result of ``Repository.remote_check``.

    Each list groups one class of bucket/index drift. The report is a pure
    observation — producing it mutates nothing.
    """

    #: Complete manifests that would resurrect a job on rebuild: no index row, an
    #: index row that says ``local``, or one that names a different remote.
    resurrection_risks: List[_RemoteCheckFinding] = field(default_factory=list)
    #: Job prefixes with archive/sidecar objects but no ``manifest.json``.
    manifestless_prefixes: List[_RemoteCheckFinding] = field(default_factory=list)
    #: Leftover ``manifest.json.staging`` objects (interrupted publishes).
    staging_manifests: List[_RemoteCheckFinding] = field(default_factory=list)
    #: Index rows on a remote whose manifest, archive, or a sidecar is missing or
    #: inconsistent — e.g. an oversized/malformed/mismatched manifest, a missing or
    #: wrong-sized archive, or a sidecar that is missing, over-cap, or whose
    #: size/hash disagrees with the manifest.
    broken_rows: List[_RemoteCheckFinding] = field(default_factory=list)
    #: ``manifest.json`` keys whose job segment is not a canonical UUID (traversal- or
    #: nested-shaped). Rebuild fails closed on these; they are surfaced here rather
    #: than silently omitted. ``job_id`` carries the offending raw segment.
    malformed_keys: List[_RemoteCheckFinding] = field(default_factory=list)
    #: Incomplete multipart uploads wasting quota under a remote's prefix.
    incomplete_multipart_uploads: List[_MultipartUploadFinding] = field(
        default_factory=list
    )

    @property
    def has_findings(self) -> bool:
        """True iff any category holds at least one finding."""
        return bool(
            self.resurrection_risks
            or self.manifestless_prefixes
            or self.staging_manifests
            or self.broken_rows
            or self.malformed_keys
            or self.incomplete_multipart_uploads
        )


def check(index: Index, remotes: Dict[str, Remote]) -> _RemoteCheckReport:
    """Reconciles each configured remote's bucket against the index (read-only).

    Reports five classes of drift without mutating anything (no deletes, no
    writes): resurrection-risk complete manifests (no index row, or a row that
    disagrees on location), manifestless job prefixes, leftover staging
    manifests, index rows on a remote whose manifest, archive, or a sidecar is
    missing or inconsistent, and incomplete multipart uploads.
    """
    report = _RemoteCheckReport()
    for remote_name, remote in remotes.items():
        _check_remote(index, remote_name, remote, report)
    return report


def _check_remote(
    index: Index, remote_name: str, remote: Remote, report: _RemoteCheckReport
) -> None:
    """Reconciles a single remote's bucket against the index into ``report``."""
    # Enumerate every object once and group it by job id, recording which of the
    # known per-job objects each prefix carries.
    manifest_suffix = "/" + _MANIFEST_NAME
    objects: Dict[str, Set[str]] = {}
    for key in remote.iter_object_keys():
        rel = key[len(remote.prefix) :]
        # A manifest key whose job segment is not a canonical UUID (traversal- or
        # nested-shaped) is the exact shape rebuild fails closed on and that a fetch
        # would try to escape with. Report it explicitly rather than group or ignore
        # it — grouping by the first path component would otherwise hide it.
        if rel.endswith(manifest_suffix):
            segment = rel[: -len(manifest_suffix)]
            if not r3.utils.is_valid_job_id(segment):
                report.malformed_keys.append(
                    _RemoteCheckFinding(
                        remote_name,
                        segment,
                        f"manifest key '{key}' has a non-canonical job segment "
                        f"{segment!r} (not a UUID, or nested); rebuild fails "
                        "closed on it",
                    )
                )
                continue
        job_id, separator, rest = rel.partition("/")
        if not job_id or not separator or not rest:
            # A stray object directly under the prefix (e.g. a key like
            # `{prefix}/manifest.json` partitions to an empty job id); not a
            # job object.
            continue
        objects.setdefault(job_id, set()).add(rest)

    for job_id, names in objects.items():
        if _MANIFEST_NAME in names:
            # Rule 1: a complete manifest the index does not (correctly) point at.
            _classify_complete_manifest(index, remote_name, job_id, report)
        elif names & _PAYLOAD_NAMES and not _indexed_on(index, job_id, remote_name):
            # Rule 2: orphan payload (archive/sidecar objects, no completion
            # marker) the index does NOT place on this remote. When a row DOES
            # place it here, Rule 4 (broken_rows) owns the missing manifest and
            # this must stay disjoint from it.
            report.manifestless_prefixes.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    "job prefix has archive/sidecar objects but no "
                    "manifest.json and no index row placing it here "
                    "(orphan payload from an interrupted move/fetch/remove)",
                )
            )
        if _STAGING_MANIFEST_NAME in names:
            # Rule 3: an interrupted publish, reported even beside a final manifest.
            report.staging_manifests.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    "leftover manifest.json.staging (interrupted publish)",
                )
            )

    # Rule 4: index rows that claim this remote but do not verify against it.
    for job in index.find({}, location=remote_name):
        assert job.id is not None
        # A pre-existing row whose id is not a canonical UUID (only reachable via
        # external corruption) cannot be probed — get_manifest would validate the id
        # and raise, aborting the whole read-only check. Report it as a broken row
        # and move on, so the diagnostic survives a corrupt store.
        if not r3.utils.is_valid_job_id(job.id):
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job.id,
                    "indexed on this remote under a non-canonical job id "
                    "(corrupt index row); it cannot be probed and should be removed",
                )
            )
            continue
        _probe_remote_row(remote_name, remote, job.id, report)

    # Rule 5: incomplete multipart uploads wasting quota under the prefix.
    for key, upload_id in remote.list_incomplete_multipart_uploads():
        report.incomplete_multipart_uploads.append(
            _MultipartUploadFinding(remote_name, key, upload_id)
        )


def _indexed_on(index: Index, job_id: str, remote_name: str) -> bool:
    """True iff the index has a row placing ``job_id`` on ``remote_name``."""
    try:
        return index.get_location(job_id) == remote_name
    except KeyError:
        return False


def _classify_complete_manifest(
    index: Index, remote_name: str, job_id: str, report: _RemoteCheckReport
) -> None:
    """Classifies a bucket job with a complete manifest against its index row."""
    try:
        location = index.get_location(job_id)
    except KeyError:
        report.resurrection_risks.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "complete manifest on the remote but no index row (orphan)",
            )
        )
        return

    if location == "local":
        report.resurrection_risks.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "complete manifest on the remote but the index marks it 'local'",
            )
        )
    elif location != remote_name:
        report.resurrection_risks.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "complete manifest on the remote but the index marks it as "
                f"remote '{location}'",
            )
        )
    # else: the index already points at this remote — consistent.


def _probe_remote_row(
    remote_name: str,
    remote: Remote,
    job_id: str,
    report: _RemoteCheckReport,
) -> None:
    """Verifies an index row that claims ``remote_name`` against the bucket.

    Read-only: reads the manifest (bounded) and HEADs the archive, then reads each
    sidecar (bounded) and checks its size and SHA-256 against the manifest; never
    writes or deletes. This is the same minimally-complete-job bar ``fetch`` and
    ``rebuild`` enforce — a manifest bound to this ``job_id``, an archive of the
    recorded size, and both sidecars present, within cap, and matching their manifest
    entries — so a row this probe calls healthy is one ``fetch`` can restore, rather
    than one it would fail on. Like ``rebuild``, the (possibly huge) archive is checked
    by size only; only the small sidecars are content-hashed.
    """
    try:
        manifest_bytes = remote.get_manifest(job_id, max_bytes=MAX_MANIFEST_BYTES)
    except FileNotFoundError:
        report.broken_rows.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "indexed on this remote but its manifest.json is missing",
            )
        )
        return
    except RemoteError:
        report.broken_rows.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "indexed on this remote but its manifest.json exceeds the size cap",
            )
        )
        return

    try:
        manifest = r3.manifest.loads(manifest_bytes, expected_job_id=job_id)
    except r3.manifest.ManifestError as error:
        report.broken_rows.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                f"indexed on this remote but its manifest is malformed: {error}",
            )
        )
        return

    actual_size = remote.archive_size(job_id)
    expected_size = manifest["archive_size"]
    if actual_size is None:
        report.broken_rows.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                "indexed on this remote but its archive data.tar.zst is missing",
            )
        )
    elif actual_size != expected_size:
        report.broken_rows.append(
            _RemoteCheckFinding(
                remote_name,
                job_id,
                f"archive size {actual_size} != manifest archive_size "
                f"{expected_size}",
            )
        )

    # Both sidecars must be retrievable, within cap, and match their manifest entry
    # exactly: ``fetch`` reads and hashes them (its verify_directory would reject a
    # tampered sidecar), so a probe that only proved presence would call a row healthy
    # that ``fetch`` cannot restore. Reading each (bounded) proves it exists and bounds
    # the read, and comparing size+hash against the manifest — the identical check
    # ``rebuild`` runs — makes "healthy" mean fetchable rather than merely present.
    # Report every defective sidecar rather than stopping at the first.
    entries = {entry["path"]: entry for entry in manifest["files"]}
    for name in SIDECAR_PATHS:
        try:
            data = remote.get_sidecar(job_id, name, max_bytes=MAX_SIDECAR_BYTES)
        except FileNotFoundError:
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    f"indexed on this remote but its {name} is missing",
                )
            )
            continue
        except RemoteError:
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    f"indexed on this remote but its {name} exceeds the size cap",
                )
            )
            continue

        entry = entries.get(name)
        if entry is None:
            # A move-built manifest always lists both sidecars; a manifest missing one
            # cannot describe a fetchable job, so treat it as a broken row.
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    f"manifest has no entry for sidecar {name}",
                )
            )
        elif len(data) != entry["size"]:
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    f"sidecar {name} size {len(data)} != manifest {entry['size']}",
                )
            )
        elif r3.utils.hash_bytes(data) != entry["sha256"]:
            report.broken_rows.append(
                _RemoteCheckFinding(
                    remote_name,
                    job_id,
                    f"sidecar {name} content does not match the manifest hash",
                )
            )
