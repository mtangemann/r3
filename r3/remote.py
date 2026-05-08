"""Remote storage backends for R3 repositories."""

import os
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
    ) -> None:
        """Initializes an S3 remote.

        Parameters:
            bucket: The S3 bucket name.
            prefix: The prefix for all S3 keys. Defaults to "".
            profile: The AWS profile name. Defaults to None.
            endpoint_url: The S3 endpoint URL. Defaults to None.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.profile = profile
        self.endpoint_url = endpoint_url

        self._client_instance: Any = None

    @property
    def _client(self) -> Any:
        """Returns the S3 client, creating it lazily on first access."""
        if self._client_instance is None:
            import boto3

            session = boto3.Session(profile_name=self.profile)
            self._client_instance = session.client(
                "s3", endpoint_url=self.endpoint_url
            )
        return self._client_instance

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "S3Remote":
        """Creates an S3 remote from a configuration dictionary.

        Parameters:
            config: The configuration dictionary with keys: bucket, prefix, and
                optionally profile and endpoint_url.

        Returns:
            The S3 remote instance.
        """
        return S3Remote(
            bucket=config["bucket"],
            prefix=config.get("prefix", ""),
            profile=config.get("profile"),
            endpoint_url=config.get("endpoint_url"),
        )

    def _job_prefix(self, job_id: str) -> str:
        """Returns the S3 key prefix for a job."""
        return f"{self.prefix}{job_id}/"

    def upload(self, job_id: str, job_path: Path) -> None:
        """Uploads a job directory to S3.

        Parameters:
            job_id: The ID of the job to upload.
            job_path: The local path of the job directory.
        """
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
        prefix = self._job_prefix(job_id)
        response = self._client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=1
        )
        return response.get("KeyCount", 0) > 0
