"""Remote storage backends for R3 repositories."""

import os
import tarfile
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional


class Remote(ABC):
    """Abstract base class for remote storage backends."""

    cache_file_list: bool = False
    """Whether the remote's storage is immutable enough to cache the file list
    in the index. Subclasses that store immutable copies (S3) override this
    to True; subclasses pointing at potentially-mutable storage (live shared
    filesystems) leave it False."""

    @abstractmethod
    def upload(self, job_id: str, job_path: Path) -> None:
        """Uploads a job directory to the remote.

        Parameters:
            job_id: The ID of the job to upload.
            job_path: The local path of the job directory.
        """

    @abstractmethod
    def download(self, job_id: str, destination: Path) -> None:
        """Downloads a job from the remote.

        Parameters:
            job_id: The ID of the job to download.
            destination: The local directory to download the job to.

        Raises:
            FileNotFoundError: If the job does not exist on the remote.
        """

    @abstractmethod
    def remove(self, job_id: str) -> None:
        """Removes a job from the remote.

        Parameters:
            job_id: The ID of the job to remove.
        """

    @abstractmethod
    def exists(self, job_id: str) -> bool:
        """Checks whether a job exists on the remote.

        Parameters:
            job_id: The ID of the job to check.

        Returns:
            True if the job exists, False otherwise.
        """

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "Remote":
        """Creates a remote from a configuration dictionary.

        Parameters:
            config: The configuration dictionary. Must contain a "type" key that
                specifies the remote type.

        Returns:
            The remote instance.

        Raises:
            ValueError: If the remote type is unknown.
        """
        remote_type = config.get("type")


        if remote_type == "s3":
            return S3Remote.from_config(config)

        raise ValueError(f"Unknown remote type: {remote_type}")


class S3Remote(Remote):
    """Remote storage backend using Amazon S3."""

    cache_file_list: bool = True

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        archive_format: Optional[str] = None,
        archive_frame_size: int = 16 * 1024 * 1024,
        addressing_style: Optional[str] = None,
        request_checksum_calculation: Optional[str] = None,
    ) -> None:
        """Initializes an S3 remote.

        Parameters:
            bucket: The S3 bucket name.
            prefix: The prefix for all S3 keys. Defaults to "".
            profile: The AWS profile name. Defaults to None.
            endpoint_url: The S3 endpoint URL. Defaults to None.
            archive_format: Optional archive format. If "tar.zst", jobs are
                stored as a single seekable .tar.zst object instead of
                individual files. Defaults to None (no archiving).
            archive_frame_size: Uncompressed frame size in bytes for the
                seekable zstd archive. Smaller frames give finer-grained
                random access at a small compression cost. Defaults to
                16 MiB.
            addressing_style: S3 addressing style. One of "auto" (boto3's
                default), "path", or "virtual". CEPH RGW typically requires
                "path". Defaults to None (boto3 default).
            request_checksum_calculation: One of "when_supported" (boto3's
                default since 1.36) or "when_required" (pre-1.36 behavior).
                Some non-AWS S3 implementations (older CEPH RGW builds)
                reject PutObject requests carrying the integrity headers
                that "when_supported" adds, returning a misleading
                InvalidAccessKeyId. Set to "when_required" to suppress
                those headers. Defaults to None (boto3 default).
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.profile = profile
        self.endpoint_url = endpoint_url
        self.archive_format = archive_format
        self.archive_frame_size = archive_frame_size
        self.addressing_style = addressing_style
        self.request_checksum_calculation = request_checksum_calculation

        self._client_instance: Any = None

    @property
    def _client(self) -> Any:
        """Returns the S3 client, creating it lazily on first access."""
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
            client_config = Config(**config_kwargs) if config_kwargs else None
            self._client_instance = session.client(
                "s3",
                endpoint_url=self.endpoint_url,
                config=client_config,
            )
        return self._client_instance

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "S3Remote":
        """Creates an S3 remote from a configuration dictionary.

        Parameters:
            config: The configuration dictionary with keys: bucket, prefix, and
                optionally profile, endpoint_url, archive_format, and
                archive_frame_size.

        Returns:
            The S3 remote instance.
        """
        archive_format = config.get("archive_format")
        if archive_format is not None and archive_format != "tar.zst":
            raise ValueError(
                f"Unsupported archive_format: {archive_format!r}. "
                f"Only 'tar.zst' is supported."
            )

        archive_frame_size = config.get("archive_frame_size", 16 * 1024 * 1024)
        if not isinstance(archive_frame_size, int) or archive_frame_size <= 0:
            raise ValueError(
                f"archive_frame_size must be a positive integer; "
                f"got {archive_frame_size!r}"
            )

        addressing_style = config.get("addressing_style")
        if addressing_style is not None and addressing_style not in (
            "auto", "path", "virtual",
        ):
            raise ValueError(
                f"addressing_style must be one of 'auto', 'path', 'virtual'; "
                f"got {addressing_style!r}"
            )

        request_checksum_calculation = config.get("request_checksum_calculation")
        if request_checksum_calculation is not None and (
            request_checksum_calculation not in ("when_supported", "when_required")
        ):
            raise ValueError(
                f"request_checksum_calculation must be one of 'when_supported', "
                f"'when_required'; got {request_checksum_calculation!r}"
            )

        return S3Remote(
            bucket=config["bucket"],
            prefix=config.get("prefix", ""),
            profile=config.get("profile"),
            endpoint_url=config.get("endpoint_url"),
            archive_format=archive_format,
            archive_frame_size=archive_frame_size,
            addressing_style=addressing_style,
            request_checksum_calculation=request_checksum_calculation,
        )

    def _job_prefix(self, job_id: str) -> str:
        """Returns the S3 key prefix for a job."""
        return f"{self.prefix}{job_id}/"

    def _import_pyzstd(self) -> Any:
        """Lazily imports pyzstd with a friendly error message."""
        try:
            import pyzstd
        except ImportError as e:
            raise ImportError(
                "archive_format='tar.zst' requires pyzstd. "
                "Install it with: pip install pyzstd"
            ) from e
        return pyzstd

    def _archive_key(self, job_id: str) -> str:
        """Returns the S3 key for a job's archive."""
        return f"{self.prefix}{job_id}.tar.zst"

    def upload(self, job_id: str, job_path: Path) -> None:
        """Uploads a job directory to S3.

        With archive_format='tar.zst', creates a single seekable .tar.zst
        object. Without archive_format, uploads individual files.

        Parameters:
            job_id: The ID of the job to upload.
            job_path: The local path of the job directory.
        """
        if self.archive_format == "tar.zst":
            pyzstd = self._import_pyzstd()
            tmp = tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=False)
            tmp_path = Path(tmp.name)
            tmp.close()
            try:
                with pyzstd.SeekableZstdFile(
                    str(tmp_path),
                    "w",
                    max_frame_content_size=self.archive_frame_size,
                ) as zfh:
                    with tarfile.open(fileobj=zfh, mode="w|") as tar:
                        tar.add(str(job_path), arcname=".")
                self._client.upload_file(
                    str(tmp_path), self.bucket, self._archive_key(job_id)
                )
            finally:
                tmp_path.unlink(missing_ok=True)
        else:
            for root, _dirs, files in os.walk(job_path):
                for filename in files:
                    local_path = Path(root) / filename
                    relative_path = local_path.relative_to(job_path)
                    s3_key = f"{self._job_prefix(job_id)}{relative_path}"
                    self._client.upload_file(str(local_path), self.bucket, s3_key)

    def download(self, job_id: str, destination: Path) -> None:
        """Downloads a job from S3.

        Parameters:
            job_id: The ID of the job to download.
            destination: The local directory to download the job to.

        Raises:
            FileNotFoundError: If the job does not exist on the remote.
        """
        if not self.exists(job_id):
            raise FileNotFoundError(
                f"Job not found on remote: {job_id}"
            )

        if self.archive_format == "tar.zst":
            pyzstd = self._import_pyzstd()
            tmp = tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=False)
            tmp_path = Path(tmp.name)
            tmp.close()
            try:
                self._client.download_file(
                    self.bucket, self._archive_key(job_id), str(tmp_path)
                )
                destination.mkdir(parents=True, exist_ok=True)
                with pyzstd.SeekableZstdFile(str(tmp_path), "r") as zfh:
                    with tarfile.open(fileobj=zfh, mode="r|") as tar:
                        # tarfile resolves leading "./" in member names to
                        # destination itself, so files land in destination/<rel>
                        # not destination/./<rel>. No post-processing needed.
                        tar.extractall(path=str(destination))
            finally:
                tmp_path.unlink(missing_ok=True)
        else:
            prefix = self._job_prefix(job_id)
            paginator = self._client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    relative_path = s3_key[len(prefix):]
                    local_path = destination / relative_path

                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    self._client.download_file(self.bucket, s3_key, str(local_path))

    def remove(self, job_id: str) -> None:
        """Removes a job from S3.

        Parameters:
            job_id: The ID of the job to remove.
        """
        if self.archive_format == "tar.zst":
            self._client.delete_object(
                Bucket=self.bucket, Key=self._archive_key(job_id)
            )
            return

        prefix = self._job_prefix(job_id)
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            if contents:
                delete_objects = [{"Key": obj["Key"]} for obj in contents]
                self._client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": delete_objects},
                )

    def exists(self, job_id: str) -> bool:
        """Checks whether a job exists on S3.

        Parameters:
            job_id: The ID of the job to check.

        Returns:
            True if the job exists, False otherwise.
        """
        if self.archive_format == "tar.zst":
            try:
                self._client.head_object(
                    Bucket=self.bucket, Key=self._archive_key(job_id)
                )
                return True
            except self._client.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                    return False
                raise
        prefix = self._job_prefix(job_id)
        response = self._client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=1
        )
        return response.get("KeyCount", 0) > 0
