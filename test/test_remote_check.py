"""Tests for the read-only ``Repository.remote_check`` reconciliation.

Uses moto to mock S3. ``remote_check`` reconciles each remote's bucket against the
index and reports drift; it must mutate nothing (no deletes, no writes).
"""

from pathlib import Path
from typing import Generator, Set

import boto3
import pytest
import yaml
from moto import mock_aws

from r3.remote import S3Remote
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"

BUCKET = "test-check-bucket"
PREFIX = "r3/jobs/"
OTHER_PREFIX = "r3/other/"


def get_dummy_job(name: str):
    from r3.job import Job

    return Job(DATA_PATH / "jobs" / name)


@pytest.fixture
def repo(tmp_path: Path) -> Generator[Repository, None, None]:
    """A repository with a single S3 remote named 'archive' (moto-backed)."""
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

        repository = Repository.init(tmp_path / "repository")
        config_path = repository.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        config["remotes"] = {
            "archive": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        yield Repository(repository.path)


@pytest.fixture
def repo_two_remotes(tmp_path: Path) -> Generator[Repository, None, None]:
    """A repository with two S3 remotes 'archive' and 'other' (moto-backed)."""
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

        repository = Repository.init(tmp_path / "repository")
        config_path = repository.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        config["remotes"] = {
            "archive": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX},
            "other": {"type": "s3", "bucket": BUCKET, "prefix": OTHER_PREFIX},
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        yield Repository(repository.path)


def _all_keys() -> Set[str]:
    client = boto3.client("s3", region_name="us-east-1")
    return {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    }


def _ids(findings) -> Set[str]:
    return {finding.job_id for finding in findings}


def _s3(repo: Repository, name: str) -> S3Remote:
    """Returns a remote narrowed to S3Remote (for its transport internals in tests)."""
    remote = repo.remotes[name]
    assert isinstance(remote, S3Remote)
    return remote


# ---------------------------------------------------------------- rule 1


def test_orphan_manifest_no_index_row(repo: Repository) -> None:
    remote = _s3(repo, "archive")
    remote.publish_manifest("orphan-job", b"{}")

    report = repo.remote_check()

    assert "orphan-job" in _ids(report.resurrection_risks)
    finding = next(f for f in report.resurrection_risks if f.job_id == "orphan-job")
    assert finding.remote == "archive"
    assert "no" in finding.detail.lower() and "row" in finding.detail.lower()


def test_disagreement_indexed_local(repo: Repository) -> None:
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    # A leftover complete manifest for a job the index still marks local.
    repo.remotes["archive"].publish_manifest(job.id, b"{}")

    report = repo.remote_check()

    assert job.id in _ids(report.resurrection_risks)
    finding = next(f for f in report.resurrection_risks if f.job_id == job.id)
    assert "local" in finding.detail.lower()


def test_disagreement_different_remote(repo_two_remotes: Repository) -> None:
    repo = repo_two_remotes
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    # A leftover complete manifest on 'other' for a job the index says is on 'archive'.
    repo.remotes["other"].publish_manifest(job.id, b"{}")

    report = repo.remote_check()

    disagreements = [
        f for f in report.resurrection_risks if f.remote == "other"
    ]
    assert job.id in {f.job_id for f in disagreements}
    finding = next(f for f in disagreements if f.job_id == job.id)
    assert "archive" in finding.detail
    # The correctly-placed 'archive' copy is not itself flagged.
    assert job.id not in {
        f.job_id for f in report.resurrection_risks if f.remote == "archive"
    }


# ---------------------------------------------------------------- rule 2


def test_manifestless_prefix(repo: Repository, tmp_path: Path) -> None:
    remote = _s3(repo, "archive")
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload")
    remote.put_archive("stray-job", archive)
    remote.put_sidecar("stray-job", "r3.yaml", b"version: 1\n")
    remote.put_sidecar("stray-job", "metadata.yaml", b"tags: []\n")
    # No manifest.json -> incomplete/interrupted.

    report = repo.remote_check()

    assert "stray-job" in _ids(report.manifestless_prefixes)
    # No complete manifest, so it is not a resurrection risk.
    assert "stray-job" not in _ids(report.resurrection_risks)


# ---------------------------------------------------------------- rule 3


def test_staging_manifest_with_final(repo: Repository) -> None:
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._staging_manifest_key(job.id), Body=b"{}"
    )

    report = repo.remote_check()

    assert job.id in _ids(report.staging_manifests)


def test_staging_manifest_without_final(repo: Repository) -> None:
    remote = _s3(repo, "archive")
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._staging_manifest_key("staging-only"), Body=b"{}"
    )

    report = repo.remote_check()

    assert "staging-only" in _ids(report.staging_manifests)
    # Staging alone (no archive/sidecar keys) is not a manifestless prefix.
    assert "staging-only" not in _ids(report.manifestless_prefixes)


# ---------------------------------------------------------------- rule 4


def test_broken_row_manifest_missing(repo: Repository) -> None:
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    # Delete only the manifest: the index still says 'archive'.
    repo.remotes["archive"].delete_manifest(job.id)

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)
    finding = next(f for f in report.broken_rows if f.job_id == job.id)
    assert "manifest" in finding.detail.lower()


def test_broken_row_archive_missing(repo: Repository) -> None:
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.delete_object(Bucket=BUCKET, Key=remote._archive_key(job.id))

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)
    finding = next(f for f in report.broken_rows if f.job_id == job.id)
    assert "archive" in finding.detail.lower()


def test_broken_row_archive_size_mismatch(repo: Repository) -> None:
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    # Overwrite the archive with a different byte length than the manifest records.
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._archive_key(job.id), Body=b"tiny"
    )

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)
    finding = next(f for f in report.broken_rows if f.job_id == job.id)
    assert "size" in finding.detail.lower()


def test_broken_row_malformed_manifest(repo: Repository) -> None:
    """A present-but-unparseable manifest for a row on this remote is a broken row."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    # Overwrite the manifest with bytes that parse as JSON but fail schema validation.
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._manifest_key(job.id), Body=b"{}"
    )

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)
    finding = next(f for f in report.broken_rows if f.job_id == job.id)
    assert "malformed" in finding.detail.lower()


# --------------------------------------------- rule 2 vs rule 4 are disjoint


def test_interrupted_remove_is_broken_row_not_manifestless(repo: Repository) -> None:
    """A row on THIS remote whose manifest was deleted (archive/sidecars remain) is a
    broken row (Rule 4), never also a manifestless prefix (Rule 2). ``delete_job``
    deletes the manifest first, so an interrupted remove lands exactly here."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    repo.remotes["archive"].delete_manifest(job.id)

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)
    assert job.id not in _ids(report.manifestless_prefixes)


def test_orphan_payload_is_manifestless_not_broken_row(
    repo: Repository, tmp_path: Path
) -> None:
    """Payload with no index row placing it here is a manifestless prefix (Rule 2)
    only — Rule 4 has no row to probe."""
    remote = _s3(repo, "archive")
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload")
    remote.put_archive("stray-job", archive)
    remote.put_sidecar("stray-job", "r3.yaml", b"version: 1\n")

    report = repo.remote_check()

    assert "stray-job" in _ids(report.manifestless_prefixes)
    assert "stray-job" not in _ids(report.broken_rows)


# ---------------------------------------------------------------- rule 5


def test_incomplete_multipart_upload(repo: Repository) -> None:
    remote = _s3(repo, "archive")
    response = remote._client.create_multipart_upload(
        Bucket=BUCKET, Key=f"{PREFIX}mp-job/data.tar.zst"
    )
    upload_id = response["UploadId"]

    report = repo.remote_check()

    keys = {f.key for f in report.incomplete_multipart_uploads}
    assert f"{PREFIX}mp-job/data.tar.zst" in keys
    finding = next(
        f
        for f in report.incomplete_multipart_uploads
        if f.key == f"{PREFIX}mp-job/data.tar.zst"
    )
    assert finding.upload_id == upload_id
    assert finding.remote == "archive"


# ---------------------------------------------------------------- clean repo


def test_consistent_repo_no_findings(repo: Repository) -> None:
    local_job = repo.commit(get_dummy_job("base"))
    moved_job = repo.commit(get_dummy_job("base"))
    assert moved_job.id is not None
    repo.move(moved_job.id, "archive")

    report = repo.remote_check()

    assert not report.has_findings
    assert report.resurrection_risks == []
    assert report.manifestless_prefixes == []
    assert report.staging_manifests == []
    assert report.broken_rows == []
    assert report.incomplete_multipart_uploads == []
    # sanity: the local job was never touched
    assert (repo.path / "jobs" / str(local_job.id)).exists()


# ---------------------------------------------------------------- read-only


def test_remote_check_mutates_nothing(repo: Repository, tmp_path: Path) -> None:
    remote = _s3(repo, "archive")

    # A mix of every finding category.
    remote.publish_manifest("orphan-job", b"{}")
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload")
    remote.put_archive("stray-job", archive)
    remote.put_sidecar("stray-job", "r3.yaml", b"version: 1\n")
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._staging_manifest_key("staging-only"), Body=b"{}"
    )
    moved = repo.commit(get_dummy_job("base"))
    assert moved.id is not None
    repo.move(moved.id, "archive")
    repo.remotes["archive"].delete_manifest(moved.id)  # break the row
    mp = remote._client.create_multipart_upload(
        Bucket=BUCKET, Key=f"{PREFIX}mp-job/data.tar.zst"
    )

    keys_before = _all_keys()
    uploads_before = set(remote.list_incomplete_multipart_uploads())
    location_before = repo._index.get_location(moved.id)

    report = repo.remote_check()
    assert report.has_findings

    assert _all_keys() == keys_before
    assert set(remote.list_incomplete_multipart_uploads()) == uploads_before
    assert repo._index.get_location(moved.id) == location_before
    # The multipart upload was neither aborted nor completed.
    assert mp["UploadId"] in {uid for _, uid in uploads_before}
