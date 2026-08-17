"""Remote storage backends for R3 repositories (ALPHA).

The S3/remote backend as a whole is provisional and may change without notice; it is
not part of R3's stable public API and is not exported from the top-level ``r3``
package.

A remote stores each job as four objects under ``{prefix}{job_id}/``:
``data.tar.zst`` (the payload archive), ``r3.yaml`` and ``metadata.yaml`` (sidecars),
and ``manifest.json`` (the integrity/listing record, written last as the completion
marker). The ``Remote`` interface is object-transport for those logical objects; the
archive and manifest *content* logic lives in ``r3.archive`` / ``r3.manifest``.
``Repository`` orchestrates the crash-safe ``move``/``fetch`` state machines on top.
"""

import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

import r3.utils
from r3.manifest import SIDECAR_PATHS

# Explicit multipart transfer configuration rather than evolving SDK defaults
# (conservative concurrency for CEPH RGW; see the design's live-test guidance).
MULTIPART_THRESHOLD = 8 * 1024 * 1024
MULTIPART_CHUNKSIZE = 16 * 1024 * 1024
MAX_CONCURRENCY = 4

_MANIFEST_NAME = "manifest.json"
_STAGING_MANIFEST_NAME = "manifest.json.staging"
_ARCHIVE_NAME = "data.tar.zst"

# Chunk size for streaming an object's hash. Bounds peak extra memory during
# upload verification to one chunk, so `move` never needs a second archive-sized
# local copy just to re-hash the upload.
_HASH_CHUNK_SIZE = 1024 * 1024


class RemoteError(Exception):
    """Raised when a remote transport operation fails or cannot be verified."""


# The index uses the literal string "local" as the location sentinel for "stored in
# local storage" (see r3.index / Repository). Reserving it as an invalid remote name
# at every boundary is what stops a remote named "local" from colliding with that
# sentinel and stranding a moved job on the remote.
RESERVED_REMOTE_NAME = "local"


def validate_remote_name(name: object) -> None:
    """Raises ``ValueError`` unless ``name`` is a valid remote name.

    Rejects the reserved name ``"local"`` (the index's location sentinel — a remote of
    that name would collide with it) and an empty or whitespace-only name. Called at
    both boundaries that accept a remote name: ``r3 remote add`` before it rewrites
    ``r3.yaml``, and ``Repository`` open for every configured remote. ``ValueError``
    is deliberate: the CLI already funnels these into clean user-facing errors.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Remote name must be a non-empty string.")
    if name == RESERVED_REMOTE_NAME:
        raise ValueError(
            f"{name!r} is a reserved remote name (it is the index's sentinel for "
            "local storage) and cannot be used."
        )


class Remote(ABC):
    """Abstract base class for remote storage backends.

    Provides object transport for a job's remote representation — the payload
    archive, the ``r3.yaml``/``metadata.yaml`` sidecars, and the manifest completion
    marker. Concrete backends (currently ``S3Remote``) implement the transport.
    """

    cache_file_list: bool = False
    """Whether the remote's storage is immutable enough to cache the job's file list
    in the index. Immutable backends (S3) set this True; a live shared filesystem
    would leave it False."""

    prefix: str = ""
    """The key prefix under which every object lives, as ``{prefix}{job_id}/...``.
    Reconciliation (``Repository.remote_check``) strips this from an enumerated key to
    recover the job id. Concrete backends set it in ``__init__``."""

    @abstractmethod
    def put_archive(self, job_id: str, archive_path: Path) -> None:
        """Uploads the payload archive for a job."""

    @abstractmethod
    def put_sidecar(self, job_id: str, name: str, data: bytes) -> None:
        """Uploads a sidecar object (``r3.yaml`` or ``metadata.yaml``)."""

    @abstractmethod
    def publish_manifest(self, job_id: str, manifest_bytes: bytes) -> None:
        """Publishes the manifest as the completion marker, verified-atomically."""

    @abstractmethod
    def delete_manifest(self, job_id: str) -> None:
        """Deletes the manifest (invalidates the publication); idempotent."""

    @abstractmethod
    def get_manifest(self, job_id: str, max_bytes: Optional[int] = None) -> bytes:
        """Returns the raw manifest bytes.

        Parameters:
            max_bytes: If given, reject an object larger than this many bytes before
                its body is read into memory (a defense-in-depth bound for callers,
                like rebuild, that parse an untrusted manifest). None reads it in full.

        Raises:
            FileNotFoundError: If the manifest does not exist.
            RemoteError: If ``max_bytes`` is given and the object exceeds it.
        """

    @abstractmethod
    def download_archive(self, job_id: str, destination: Path) -> None:
        """Downloads the payload archive to a local path."""

    @abstractmethod
    def archive_sha256(self, job_id: str) -> str:
        """Streams the uploaded payload archive and returns its SHA-256 hex digest.

        The object is read in fixed-size chunks and hashed on the fly, so verifying
        an upload never materializes a second full local copy of the archive (peak
        extra footprint is one chunk). Used by ``move`` to content-verify the archive
        against its expected digest without duplicating it in scratch.
        """

    @abstractmethod
    def get_sidecar(
        self, job_id: str, name: str, max_bytes: Optional[int] = None
    ) -> bytes:
        """Returns a sidecar object's bytes.

        Parameters:
            max_bytes: If given, reject an object larger than this many bytes before
                its body is read into memory. None reads it in full.

        Raises:
            RemoteError: If ``max_bytes`` is given and the object exceeds it.
        """

    @abstractmethod
    def archive_size(self, job_id: str) -> Optional[int]:
        """Returns the archive's byte size (HEAD), or None if it is missing."""

    @abstractmethod
    def exists(self, job_id: str) -> bool:
        """Returns True iff the job is completely published (its manifest exists)."""

    @abstractmethod
    def has_objects(self, job_id: str) -> bool:
        """Returns True iff ANY object exists under the job's prefix.

        Unlike `exists` (manifest-only), this also detects a partially-deleted job
        whose manifest is already gone but whose archive/sidecars/staging remain, so a
        crash-interrupted removal is still recognized and can be retried to completion.
        """

    @abstractmethod
    def delete_job(self, job_id: str) -> None:
        """Deletes all of a job's objects (manifest first); idempotent."""

    @abstractmethod
    def list_job_ids(self) -> Iterator[str]:
        """Yields the raw job segment of every ``*/manifest.json`` key under the prefix.

        The yielded segment is the substring between the prefix and the trailing
        ``/manifest.json`` — verbatim, WITHOUT validation. A malformed key (a segment
        that is not a canonical UUID, or that carries extra path components) is yielded
        as-is rather than silently dropped, so the consumer sees it: ``rebuild`` fails
        closed on it and ``remote check`` reports it. Callers that turn the segment
        into a path or key MUST validate it first.
        """

    @abstractmethod
    def iter_object_keys(self) -> Iterator[str]:
        """Yields every object key under the remote's prefix (paginated).

        Unlike `list_job_ids` (manifest-only), this enumerates the raw objects —
        archives, sidecars, manifests, and leftover staging manifests alike — so a
        reconciliation can classify each job prefix and detect leftovers. Read-only.
        """

    @abstractmethod
    def list_incomplete_multipart_uploads(self) -> Iterator[Tuple[str, str]]:
        """Yields ``(key, upload_id)`` for each in-progress multipart upload.

        These are uploads under the remote's prefix that were never completed or
        aborted (wasted quota). Read-only: this lists, it never aborts them.
        """

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "Remote":
        """Creates a remote from a configuration dictionary.

        Raises:
            ValueError: If the remote type is unknown or the config is invalid.
        """
        remote_type = config.get("type")
        if remote_type == "s3":
            return S3Remote.from_config(config)
        raise ValueError(f"Unknown remote type: {remote_type!r}")


class S3Remote(Remote):
    """Remote storage backend using an S3-compatible object store (incl. CEPH RGW)."""

    cache_file_list: bool = True

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        archive_frame_size: int = 16 * 1024 * 1024,
        addressing_style: Optional[str] = None,
        request_checksum_calculation: Optional[str] = None,
        response_checksum_validation: Optional[str] = None,
    ) -> None:
        """Initializes an S3 remote.

        Parameters:
            bucket: The S3 bucket name.
            prefix: Key prefix for all objects. Defaults to "".
            profile: AWS credential profile. Defaults to None.
            endpoint_url: S3 endpoint URL (e.g. a CEPH RGW). Defaults to None.
            archive_frame_size: Uncompressed seekable-zstd frame size. Defaults 16MiB.
            addressing_style: "auto", "path", or "virtual". CEPH RGW usually needs
                "path". Defaults to None (boto3 default).
            request_checksum_calculation: "when_supported" or "when_required". CEPH
                RGW builds that reject the integrity headers "when_supported" adds
                (misleading InvalidAccessKeyId) need "when_required". Default None.
            response_checksum_validation: "when_supported" or "when_required".
                Exposed because GET is on the critical path (move/fetch verify by
                re-download). Default None.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.profile = profile
        self.endpoint_url = endpoint_url
        self.archive_frame_size = archive_frame_size
        self.addressing_style = addressing_style
        self.request_checksum_calculation = request_checksum_calculation
        self.response_checksum_validation = response_checksum_validation

        self._client_instance: Any = None
        self._transfer_config_instance: Any = None

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "S3Remote":
        """Creates an S3 remote from a configuration dictionary.

        Raises:
            ValueError: If required fields are missing or a field is invalid.
        """
        if "bucket" not in config or not config["bucket"]:
            raise ValueError("S3 remote config requires a non-empty 'bucket'.")

        frame_size = config.get("archive_frame_size", 16 * 1024 * 1024)
        if not isinstance(frame_size, int) or frame_size <= 0:
            raise ValueError(
                f"archive_frame_size must be a positive integer; got {frame_size!r}"
            )

        for field in ("addressing_style",):
            value = config.get(field)
            if value is not None and value not in ("auto", "path", "virtual"):
                raise ValueError(
                    f"{field} must be one of 'auto', 'path', 'virtual'; got {value!r}"
                )
        for field in ("request_checksum_calculation", "response_checksum_validation"):
            value = config.get(field)
            if value is not None and value not in ("when_supported", "when_required"):
                raise ValueError(
                    f"{field} must be 'when_supported' or 'when_required'; "
                    f"got {value!r}"
                )

        return S3Remote(
            bucket=config["bucket"],
            prefix=config.get("prefix", ""),
            profile=config.get("profile"),
            endpoint_url=config.get("endpoint_url"),
            archive_frame_size=frame_size,
            addressing_style=config.get("addressing_style"),
            request_checksum_calculation=config.get("request_checksum_calculation"),
            response_checksum_validation=config.get("response_checksum_validation"),
        )

    # -- key scheme --------------------------------------------------------------
    #
    # Every per-job object key is built here, so validating the id in each key
    # helper is the single choke point that stops a non-canonical id (traversal,
    # separators, glob metacharacters, ...) from ever becoming an S3 key — no matter
    # which public transport method is called. The id-free enumeration methods
    # (`list_job_ids`, `iter_object_keys`, `list_incomplete_multipart_uploads`)
    # deliberately do NOT go through here: they must still surface malformed keys to
    # discovery/reconciliation rather than reject them.

    def _job_prefix(self, job_id: str) -> str:
        r3.utils.validate_job_id(job_id)
        return f"{self.prefix}{job_id}/"

    def _archive_key(self, job_id: str) -> str:
        r3.utils.validate_job_id(job_id)
        return f"{self.prefix}{job_id}/{_ARCHIVE_NAME}"

    def _sidecar_key(self, job_id: str, name: str) -> str:
        r3.utils.validate_job_id(job_id)
        return f"{self.prefix}{job_id}/{name}"

    def _manifest_key(self, job_id: str) -> str:
        r3.utils.validate_job_id(job_id)
        return f"{self.prefix}{job_id}/{_MANIFEST_NAME}"

    def _staging_manifest_key(self, job_id: str) -> str:
        r3.utils.validate_job_id(job_id)
        return f"{self.prefix}{job_id}/{_STAGING_MANIFEST_NAME}"

    # -- client ------------------------------------------------------------------

    @property
    def _client(self) -> Any:
        if self._client_instance is None:
            import boto3
            from botocore.config import Config

            session = boto3.Session(profile_name=self.profile)
            config_kwargs: Dict[str, Any] = {}
            if self.addressing_style is not None:
                config_kwargs["s3"] = {"addressing_style": self.addressing_style}
            if self.request_checksum_calculation is not None:
                config_kwargs["request_checksum_calculation"] = (
                    self.request_checksum_calculation
                )
            if self.response_checksum_validation is not None:
                config_kwargs["response_checksum_validation"] = (
                    self.response_checksum_validation
                )
            client_config = Config(**config_kwargs) if config_kwargs else None
            self._client_instance = session.client(
                "s3", endpoint_url=self.endpoint_url, config=client_config
            )
        return self._client_instance

    @property
    def _transfer_config(self) -> Any:
        if self._transfer_config_instance is None:
            from boto3.s3.transfer import TransferConfig

            self._transfer_config_instance = TransferConfig(
                multipart_threshold=MULTIPART_THRESHOLD,
                multipart_chunksize=MULTIPART_CHUNKSIZE,
                max_concurrency=MAX_CONCURRENCY,
            )
        return self._transfer_config_instance

    # -- low-level object primitives ---------------------------------------------

    def _head(self, key: str) -> Optional[int]:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=key)
        except self._client.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return response["ContentLength"]

    def _get_bytes(self, key: str, max_bytes: Optional[int] = None) -> bytes:
        # When a cap is requested, HEAD the object first and reject an over-cap size
        # before its body is pulled into memory. A missing object (HEAD -> None) falls
        # through to the GET below, which raises the usual FileNotFoundError.
        if max_bytes is not None:
            size = self._head(key)
            if size is not None and size > max_bytes:
                raise RemoteError(
                    f"Object {key} is {size} bytes, exceeding the {max_bytes}-byte "
                    "cap for this object."
                )
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except self._client.exceptions.NoSuchKey as error:
            raise FileNotFoundError(f"Object not found: {key}") from error
        except self._client.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"Object not found: {key}") from error
            raise
        data: bytes = response["Body"].read()
        return data

    def _delete(self, keys: list) -> None:
        """Batch-deletes keys, inspecting the per-object Errors array.

        Missing keys are not errors. Raises RemoteError on any reported failure.
        """
        if not keys:
            return
        response = self._client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RemoteError(f"Failed to delete objects: {errors}")

    # -- Remote interface --------------------------------------------------------

    def put_archive(self, job_id: str, archive_path: Path) -> None:
        self._client.upload_file(
            str(archive_path),
            self.bucket,
            self._archive_key(job_id),
            Config=self._transfer_config,
        )

    def put_sidecar(self, job_id: str, name: str, data: bytes) -> None:
        self._client.put_object(
            Bucket=self.bucket, Key=self._sidecar_key(job_id, name), Body=data
        )

    def publish_manifest(self, job_id: str, manifest_bytes: bytes) -> None:
        """Publishes the manifest via a verified staging-copy.

        PUT to a staging key, GET it back and byte-compare, then server-side copy
        the verified staging object to the final manifest key (bound to its ETag).
        The atomic server-side copy is what makes the final key appear only with
        already-verified bytes, closing the visible-but-unverified window.

        As belt-and-suspenders against spec-divergent backends, the CopyObject
        result is checked for a completed copy and the final key is re-read and
        byte-compared before staging is deleted — the final manifest is the job's
        completion marker, so it must be correct before this returns. On any
        failure both the staging object and the (possibly copied) final manifest
        are cleaned up best-effort and RemoteError is raised; if the untrusted
        final key cannot be removed, the error says so and names it.
        """
        staging_key = self._staging_manifest_key(job_id)
        final_key = self._manifest_key(job_id)

        try:
            put_response = self._client.put_object(
                Bucket=self.bucket, Key=staging_key, Body=manifest_bytes
            )
            staging_etag = put_response["ETag"]

            if self._get_bytes(staging_key) != manifest_bytes:
                raise RemoteError(
                    f"Staging manifest verification failed for job {job_id}."
                )

            # Server-side copy from the GET-verified staging object, bound to its
            # ETag. This atomic copy is what closes the visible-but-unverified
            # window on the final key.
            copy_response = self._client.copy_object(
                Bucket=self.bucket,
                Key=final_key,
                CopySource={"Bucket": self.bucket, "Key": staging_key},
                CopySourceIfMatch=staging_etag,
            )
            # A spec-divergent S3 can return 200 with an empty/partial result while
            # the final key was not materialized; treat a missing result as failure.
            if not copy_response.get("CopyObjectResult", {}).get("ETag"):
                raise RemoteError(
                    f"CopyObject did not report a completed copy for job {job_id}."
                )
            if self._get_bytes(final_key) != manifest_bytes:
                raise RemoteError(
                    f"Final manifest verification failed for job {job_id}."
                )
        except Exception as error:
            # Best-effort cleanup on any failure. Delete BOTH the staging key and
            # the final manifest key: move() invalidates any stale manifest (its
            # delete_manifest-first step) before ever calling publish_manifest, so
            # there is no previously-good final manifest to protect here. Leaving an
            # untrusted final key visible would instead make exists()/list_job_ids()
            # classify the job as published on a marker verification just rejected.
            # Deleting a not-yet-created final key (a failure at or before the copy)
            # is a harmless no-op, so including it on every failure path is safe.
            # Swallow secondary delete errors so they never mask the original failure.
            try:
                self._delete([staging_key])
            except Exception:
                pass
            final_key_cleared = True
            try:
                self._delete([final_key])
            except Exception:
                final_key_cleared = False

            if final_key_cleared and isinstance(error, RemoteError):
                raise
            if isinstance(error, RemoteError):
                message = str(error)
            else:
                message = f"Failed to publish manifest for job {job_id}: {error}"
            if not final_key_cleared:
                message += (
                    " An untrusted completion marker may still be visible at "
                    f"{final_key}."
                )
            raise RemoteError(message) from error

        # Best-effort: the publish has already succeeded (final key verified), so a
        # trailing delete hiccup must not turn a good publish into a move() failure.
        # A leftover staging object is harmless (overwritten on retry, swept by
        # delete_job, never visible to exists()/list_job_ids()).
        try:
            self._delete([staging_key])
        except Exception:
            pass

    def delete_manifest(self, job_id: str) -> None:
        self._delete([self._manifest_key(job_id)])

    def get_manifest(self, job_id: str, max_bytes: Optional[int] = None) -> bytes:
        return self._get_bytes(self._manifest_key(job_id), max_bytes=max_bytes)

    def download_archive(self, job_id: str, destination: Path) -> None:
        self._client.download_file(
            self.bucket,
            self._archive_key(job_id),
            str(destination),
            Config=self._transfer_config,
        )

    def archive_sha256(self, job_id: str) -> str:
        # Stream the object body in fixed-size chunks, hashing as we go, so the digest
        # is computed without ever holding the whole archive in memory or writing a
        # second full copy to scratch (the previous verify path downloaded a temp copy).
        response = self._client.get_object(
            Bucket=self.bucket, Key=self._archive_key(job_id)
        )
        body = response["Body"]
        digest = hashlib.sha256()
        while True:
            chunk = body.read(_HASH_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
        return digest.hexdigest()

    def get_sidecar(
        self, job_id: str, name: str, max_bytes: Optional[int] = None
    ) -> bytes:
        return self._get_bytes(self._sidecar_key(job_id, name), max_bytes=max_bytes)

    def archive_size(self, job_id: str) -> Optional[int]:
        return self._head(self._archive_key(job_id))

    def exists(self, job_id: str) -> bool:
        return self._head(self._manifest_key(job_id)) is not None

    def has_objects(self, job_id: str) -> bool:
        response = self._client.list_objects_v2(
            Bucket=self.bucket, Prefix=self._job_prefix(job_id), MaxKeys=1
        )
        return bool(response.get("Contents"))

    def delete_job(self, job_id: str) -> None:
        # Manifest first (its own call) so an interrupted cleanup leaves the job
        # "incomplete" (invisible to exists/rebuild), never manifest-without-payload.
        self._delete([self._manifest_key(job_id)])
        self._delete(
            [self._archive_key(job_id), self._staging_manifest_key(job_id)]
            + [self._sidecar_key(job_id, name) for name in SIDECAR_PATHS]
        )

    def list_job_ids(self) -> Iterator[str]:
        # Yields the raw job segment for every manifest key, malformed ones included
        # (see the base-class contract); it must never validate or filter here, or
        # discovery/reconciliation would be blind to a traversal-shaped key.
        paginator = self._client.get_paginator("list_objects_v2")
        suffix = f"/{_MANIFEST_NAME}"
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(suffix):
                    yield key[len(self.prefix) : -len(suffix)]

    def iter_object_keys(self) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def list_incomplete_multipart_uploads(self) -> Iterator[Tuple[str, str]]:
        paginator = self._client.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for upload in page.get("Uploads", []):
                yield upload["Key"], upload["UploadId"]
