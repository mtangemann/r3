"""Unit tests for r3.remote (the S3 object-transport backend).

Uses moto to mock S3. Note that moto cannot reproduce CEPH RGW quirks (multipart
ETags, checksum-header handling); those are covered by the opt-in live_s3 suite.
"""

from pathlib import Path
from typing import Generator

import boto3
import pytest
from moto import mock_aws

from r3.remote import Remote, S3Remote

BUCKET = "test-remote-bucket"
PREFIX = "r3/jobs/"


@pytest.fixture
def s3_remote() -> Generator[S3Remote, None, None]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield S3Remote(bucket=BUCKET, prefix=PREFIX)


def _publish(remote: S3Remote, job_id: str, tmp_path: Path) -> None:
    """Uploads a minimal well-formed job (archive + sidecars + manifest)."""
    archive = tmp_path / f"{job_id}.tar.zst"
    archive.write_bytes(b"pretend-archive-bytes")
    remote.put_archive(job_id, archive)
    remote.put_sidecar(job_id, "r3.yaml", b"version: 1\n")
    remote.put_sidecar(job_id, "metadata.yaml", b"tags: []\n")
    remote.publish_manifest(job_id, b'{"manifest_version": 1}')


# ---------------------------------------------------------------- from_config


def test_from_config_dispatches_to_s3() -> None:
    remote = Remote.from_config({"type": "s3", "bucket": "b", "prefix": "p/"})
    assert isinstance(remote, S3Remote)
    assert remote.bucket == "b"
    assert remote.prefix == "p/"


def test_from_config_rejects_unknown_type() -> None:
    with pytest.raises(ValueError):
        Remote.from_config({"type": "ftp", "bucket": "b"})


def test_from_config_requires_bucket() -> None:
    with pytest.raises(ValueError):
        Remote.from_config({"type": "s3"})


def test_from_config_accepts_ceph_fields() -> None:
    remote = S3Remote.from_config(
        {
            "type": "s3",
            "bucket": "b",
            "endpoint_url": "https://ceph.example.com",
            "addressing_style": "path",
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        }
    )
    assert remote.addressing_style == "path"
    assert remote.request_checksum_calculation == "when_required"
    assert remote.response_checksum_validation == "when_required"


@pytest.mark.parametrize(
    "field",
    [
        "addressing_style",
        "request_checksum_calculation",
        "response_checksum_validation",
    ],
)
def test_from_config_rejects_bad_enum(field: str) -> None:
    with pytest.raises(ValueError):
        S3Remote.from_config({"type": "s3", "bucket": "b", field: "nonsense"})


def test_from_config_rejects_bad_frame_size() -> None:
    with pytest.raises(ValueError):
        S3Remote.from_config({"type": "s3", "bucket": "b", "archive_frame_size": 0})


def test_s3_remote_caches_file_list() -> None:
    assert S3Remote(bucket="b").cache_file_list is True


# ---------------------------------------------------------------- transport


def test_put_and_get_sidecar(s3_remote: S3Remote) -> None:
    s3_remote.put_sidecar("job1", "r3.yaml", b"hello")
    assert s3_remote.get_sidecar("job1", "r3.yaml") == b"hello"


def test_put_and_download_archive(s3_remote: S3Remote, tmp_path: Path) -> None:
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload-bytes")
    s3_remote.put_archive("job1", archive)
    assert s3_remote.archive_size("job1") == len(b"payload-bytes")

    out = tmp_path / "out.tar.zst"
    s3_remote.download_archive("job1", out)
    assert out.read_bytes() == b"payload-bytes"


def test_archive_size_none_when_missing(s3_remote: S3Remote) -> None:
    assert s3_remote.archive_size("nope") is None


def test_get_manifest_raises_when_missing(s3_remote: S3Remote) -> None:
    with pytest.raises(FileNotFoundError):
        s3_remote.get_manifest("nope")


def test_exists_only_after_manifest_published(
    s3_remote: S3Remote, tmp_path: Path
) -> None:
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"x")
    s3_remote.put_archive("job1", archive)
    s3_remote.put_sidecar("job1", "r3.yaml", b"y")
    # Payload present but no manifest yet -> not a complete job.
    assert s3_remote.exists("job1") is False

    s3_remote.publish_manifest("job1", b"{}")
    assert s3_remote.exists("job1") is True


def test_publish_manifest_is_verified_and_leaves_no_staging(
    s3_remote: S3Remote,
) -> None:
    s3_remote.publish_manifest("job1", b'{"ok": true}')
    assert s3_remote.get_manifest("job1") == b'{"ok": true}'
    # The staging key must not linger after a successful publish.
    client = boto3.client("s3", region_name="us-east-1")
    keys = {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get(
            "Contents", []
        )
    }
    assert f"{PREFIX}job1/manifest.json" in keys
    assert f"{PREFIX}job1/manifest.json.staging" not in keys


def test_delete_manifest_makes_job_incomplete(
    s3_remote: S3Remote, tmp_path: Path
) -> None:
    _publish(s3_remote, "job1", tmp_path)
    assert s3_remote.exists("job1")
    s3_remote.delete_manifest("job1")
    assert not s3_remote.exists("job1")
    s3_remote.delete_manifest("job1")  # idempotent


def test_delete_job_removes_all_objects(s3_remote: S3Remote, tmp_path: Path) -> None:
    _publish(s3_remote, "job1", tmp_path)
    s3_remote.delete_job("job1")
    client = boto3.client("s3", region_name="us-east-1")
    remaining = client.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}job1/").get(
        "Contents", []
    )
    assert remaining == []
    s3_remote.delete_job("job1")  # idempotent on an already-empty prefix


def test_list_job_ids(s3_remote: S3Remote, tmp_path: Path) -> None:
    _publish(s3_remote, "job-a", tmp_path)
    _publish(s3_remote, "job-b", tmp_path)
    # A payload-only prefix (no manifest) must not be listed.
    s3_remote.put_sidecar("job-c", "r3.yaml", b"z")
    assert set(s3_remote.list_job_ids()) == {"job-a", "job-b"}


def test_empty_prefix(tmp_path: Path) -> None:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        remote = S3Remote(bucket=BUCKET, prefix="")
        remote.publish_manifest("job1", b"{}")
        assert list(remote.list_job_ids()) == ["job1"]
        assert remote.exists("job1")
