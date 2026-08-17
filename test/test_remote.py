"""Unit tests for r3.remote (the S3 object-transport backend).

Uses moto to mock S3. Note that moto cannot reproduce CEPH RGW quirks (multipart
ETags, checksum-header handling); those are covered by the opt-in live_s3 suite.
"""

from pathlib import Path
from typing import Generator

import boto3
import pytest
from moto import mock_aws

from r3.remote import Remote, RemoteError, S3Remote

BUCKET = "test-remote-bucket"
PREFIX = "r3/jobs/"

# Canonical UUIDs standing in for the transport tests' former shorthand ids
# (JOB1, JOB_A, ...): S3Remote now validates the id before building any
# object key, so every id fed to a transport method must be a canonical UUID.
JOB1 = "11111111-1111-4111-8111-111111111111"
JOB2 = "22222222-2222-4222-8222-222222222222"
JOB_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
JOB_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
JOB_C = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
ABSENT = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"


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
    s3_remote.put_sidecar(JOB1, "r3.yaml", b"hello")
    assert s3_remote.get_sidecar(JOB1, "r3.yaml") == b"hello"


def test_put_and_download_archive(s3_remote: S3Remote, tmp_path: Path) -> None:
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload-bytes")
    s3_remote.put_archive(JOB1, archive)
    assert s3_remote.archive_size(JOB1) == len(b"payload-bytes")

    out = tmp_path / "out.tar.zst"
    s3_remote.download_archive(JOB1, out)
    assert out.read_bytes() == b"payload-bytes"


def test_archive_size_none_when_missing(s3_remote: S3Remote) -> None:
    assert s3_remote.archive_size(ABSENT) is None


def test_get_manifest_raises_when_missing(s3_remote: S3Remote) -> None:
    with pytest.raises(FileNotFoundError):
        s3_remote.get_manifest(ABSENT)


def test_exists_only_after_manifest_published(
    s3_remote: S3Remote, tmp_path: Path
) -> None:
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"x")
    s3_remote.put_archive(JOB1, archive)
    s3_remote.put_sidecar(JOB1, "r3.yaml", b"y")
    # Payload present but no manifest yet -> not a complete job.
    assert s3_remote.exists(JOB1) is False

    s3_remote.publish_manifest(JOB1, b"{}")
    assert s3_remote.exists(JOB1) is True


def test_publish_manifest_is_verified_and_leaves_no_staging(
    s3_remote: S3Remote,
) -> None:
    s3_remote.publish_manifest(JOB1, b'{"ok": true}')
    assert s3_remote.get_manifest(JOB1) == b'{"ok": true}'
    # The staging key must not linger after a successful publish.
    client = boto3.client("s3", region_name="us-east-1")
    keys = {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get(
            "Contents", []
        )
    }
    assert f"{PREFIX}{JOB1}/manifest.json" in keys
    assert f"{PREFIX}{JOB1}/manifest.json.staging" not in keys


def test_delete_manifest_makes_job_incomplete(
    s3_remote: S3Remote, tmp_path: Path
) -> None:
    _publish(s3_remote, JOB1, tmp_path)
    assert s3_remote.exists(JOB1)
    s3_remote.delete_manifest(JOB1)
    assert not s3_remote.exists(JOB1)
    s3_remote.delete_manifest(JOB1)  # idempotent


def test_delete_job_removes_all_objects(s3_remote: S3Remote, tmp_path: Path) -> None:
    _publish(s3_remote, JOB1, tmp_path)
    s3_remote.delete_job(JOB1)
    client = boto3.client("s3", region_name="us-east-1")
    remaining = client.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}{JOB1}/").get(
        "Contents", []
    )
    assert remaining == []
    s3_remote.delete_job(JOB1)  # idempotent on an already-empty prefix


def test_list_job_ids(s3_remote: S3Remote, tmp_path: Path) -> None:
    _publish(s3_remote, JOB_A, tmp_path)
    _publish(s3_remote, JOB_B, tmp_path)
    # A payload-only prefix (no manifest) must not be listed.
    s3_remote.put_sidecar(JOB_C, "r3.yaml", b"z")
    assert set(s3_remote.list_job_ids()) == {JOB_A, JOB_B}


def test_empty_prefix(tmp_path: Path) -> None:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        remote = S3Remote(bucket=BUCKET, prefix="")
        remote.publish_manifest(JOB1, b"{}")
        assert list(remote.list_job_ids()) == [JOB1]
        assert remote.exists(JOB1)


# ---------------------------------------------------------------- error paths


def _keys_under(prefix: str) -> set:
    client = boto3.client("s3", region_name="us-east-1")
    return {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get(
            "Contents", []
        )
    }


def test_delete_reports_per_object_errors_as_remote_error(
    s3_remote: S3Remote, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # moto never populates the per-object Errors array (a missing key is not an
    # error), so stub the client to guard the "failed delete silently treated as
    # success" hazard.
    _publish(s3_remote, JOB1, tmp_path)

    def _delete_objects_with_errors(**kwargs: object) -> dict:
        return {
            "Errors": [
                {
                    "Key": f"{PREFIX}{JOB1}/manifest.json",
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            ]
        }

    monkeypatch.setattr(
        s3_remote._client, "delete_objects", _delete_objects_with_errors
    )
    with pytest.raises(RemoteError):
        s3_remote.delete_job(JOB1)


def test_publish_manifest_staging_mismatch_cleans_up(
    s3_remote: S3Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Force the staging read-back to disagree with the bytes we PUT.
    monkeypatch.setattr(s3_remote, "_get_bytes", lambda key: b"corrupted")
    with pytest.raises(RemoteError):
        s3_remote.publish_manifest(JOB1, b'{"ok": true}')
    # Staging is cleaned up and the final manifest key was never created.
    assert _keys_under(f"{PREFIX}{JOB1}/") == set()


def test_publish_manifest_incomplete_copy_result_is_failure(
    s3_remote: S3Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A spec-divergent S3 can return 200 with no CopyObjectResult while the final
    # key was not materialized. That must be a RemoteError, not silent success,
    # or move() would delete the only local copy (data loss).
    monkeypatch.setattr(s3_remote._client, "copy_object", lambda **kwargs: {})
    with pytest.raises(RemoteError):
        s3_remote.publish_manifest(JOB1, b'{"ok": true}')
    # Staging is cleaned up and the final manifest key is absent.
    assert _keys_under(f"{PREFIX}{JOB1}/") == set()


def test_publish_manifest_final_verify_mismatch_cleans_up_staging(
    s3_remote: S3Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Staging verify PASSES but the copied final key reads back wrong bytes. This
    # exercises the final re-GET byte-compare (the H1 linchpin): a copy that
    # "succeeded" but did not materialize the right bytes must be a RemoteError.
    manifest_bytes = b'{"ok": true}'

    def _get_bytes_final_differs(key: str) -> bytes:
        return manifest_bytes if key.endswith(".staging") else b"wrong-bytes"

    monkeypatch.setattr(s3_remote, "_get_bytes", _get_bytes_final_differs)
    with pytest.raises(RemoteError):
        s3_remote.publish_manifest(JOB1, manifest_bytes)
    # Staging is cleaned up. (The final key is intentionally left in place; move()
    # keeps the local copy and clears it via delete_manifest-first on retry.)
    keys = _keys_under(f"{PREFIX}{JOB1}/")
    assert f"{PREFIX}{JOB1}/manifest.json.staging" not in keys


def test_list_job_ids_paginates_beyond_one_page(s3_remote: S3Remote) -> None:
    # moto caps a list page at 1000 keys; 1001 manifests forces the paginator
    # across multiple pages. Manifest keys are written directly (not via
    # publish_manifest) to keep this fast.
    expected = {f"job-{i:04d}" for i in range(1001)}
    for job_id in expected:
        s3_remote._client.put_object(
            Bucket=BUCKET, Key=f"{PREFIX}{job_id}/manifest.json", Body=b"{}"
        )
    assert set(s3_remote.list_job_ids()) == expected


# ------------------------------------------------ enumeration / reconciliation


def test_iter_object_keys_yields_every_key(s3_remote: S3Remote) -> None:
    # Every object under the prefix — manifest, archive, sidecars, staging — is
    # enumerated, regardless of its role (unlike list_job_ids, which is
    # manifest-only). This is the raw feed the reconciliation classifies.
    s3_remote._client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}{JOB1}/manifest.json", Body=b"{}"
    )
    s3_remote._client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}{JOB1}/data.tar.zst", Body=b"x"
    )
    s3_remote._client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}{JOB2}/manifest.json.staging", Body=b"{}"
    )
    assert set(s3_remote.iter_object_keys()) == {
        f"{PREFIX}{JOB1}/manifest.json",
        f"{PREFIX}{JOB1}/data.tar.zst",
        f"{PREFIX}{JOB2}/manifest.json.staging",
    }


def test_iter_object_keys_empty(s3_remote: S3Remote) -> None:
    assert list(s3_remote.iter_object_keys()) == []


def test_iter_object_keys_paginates_beyond_one_page(s3_remote: S3Remote) -> None:
    # moto caps a list page at 1000 keys; 1001 objects forces the paginator across
    # multiple pages, guarding a single-page enumeration bug.
    expected = {f"{PREFIX}job-{i:04d}/data.tar.zst" for i in range(1001)}
    for key in expected:
        s3_remote._client.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    assert set(s3_remote.iter_object_keys()) == expected


def test_list_incomplete_multipart_uploads(s3_remote: S3Remote) -> None:
    response = s3_remote._client.create_multipart_upload(
        Bucket=BUCKET, Key=f"{PREFIX}{JOB1}/data.tar.zst"
    )
    upload_id = response["UploadId"]
    assert (f"{PREFIX}{JOB1}/data.tar.zst", upload_id) in set(
        s3_remote.list_incomplete_multipart_uploads()
    )


def test_list_incomplete_multipart_uploads_empty(s3_remote: S3Remote) -> None:
    assert list(s3_remote.list_incomplete_multipart_uploads()) == []


# ------------------------------------------------ job-id validation (security)


_BAD_IDS = [
    "../../escaped",
    "/abs",
    "a/b",
    "job-*",
    "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    "not-a-uuid",
    "",
]


@pytest.mark.parametrize("bad", _BAD_IDS)
def test_transport_methods_reject_non_canonical_id(
    s3_remote: S3Remote, tmp_path: Path, bad: str
) -> None:
    """Every public transport method must refuse a non-canonical id before it can
    become an object key (defense in depth against a traversal-shaped id)."""
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"x")
    with pytest.raises(ValueError):
        s3_remote.put_archive(bad, archive)
    with pytest.raises(ValueError):
        s3_remote.put_sidecar(bad, "r3.yaml", b"x")
    with pytest.raises(ValueError):
        s3_remote.publish_manifest(bad, b"{}")
    with pytest.raises(ValueError):
        s3_remote.delete_manifest(bad)
    with pytest.raises(ValueError):
        s3_remote.get_manifest(bad)
    with pytest.raises(ValueError):
        s3_remote.download_archive(bad, tmp_path / "out.tar.zst")
    with pytest.raises(ValueError):
        s3_remote.get_sidecar(bad, "r3.yaml")
    with pytest.raises(ValueError):
        s3_remote.archive_size(bad)
    with pytest.raises(ValueError):
        s3_remote.exists(bad)
    with pytest.raises(ValueError):
        s3_remote.has_objects(bad)
    with pytest.raises(ValueError):
        s3_remote.delete_job(bad)


def test_list_job_ids_surfaces_malformed_segments_unvalidated(
    s3_remote: S3Remote,
) -> None:
    """Discovery must NOT validate: a traversal- or nested-shaped manifest key is
    yielded verbatim so rebuild/reconciliation can see and reject it."""
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}../../escaped/manifest.json", Body=b"{}"
    )
    client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}nested/uuid/manifest.json", Body=b"{}"
    )
    client.put_object(Bucket=BUCKET, Key=f"{PREFIX}{JOB1}/manifest.json", Body=b"{}")

    segments = set(s3_remote.list_job_ids())
    assert "../../escaped" in segments
    assert "nested/uuid" in segments
    assert JOB1 in segments
