"""Tests for the read-only ``Repository.remote_check`` reconciliation.

Uses moto to mock S3. ``remote_check`` reconciles each remote's bucket against the
index and reports drift; it must mutate nothing (no deletes, no writes).
"""

import json
from pathlib import Path
from typing import Dict, Generator, Set

import boto3
import pytest
import yaml
from moto import mock_aws

from r3.index import MAX_MANIFEST_BYTES, MAX_SIDECAR_BYTES
from r3.remote import S3Remote
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"

BUCKET = "test-check-bucket"
PREFIX = "r3/jobs/"
OTHER_PREFIX = "r3/other/"

# Canonical UUIDs replacing the former shorthand ids (ORPHAN, ...):
# publishing/putting via S3Remote now validates the id, so setup ids that reach
# a transport helper must be canonical UUIDs.
ORPHAN = "0deada11-0000-4000-8000-00000000abcd"
STRAY = "57ada11a-0000-4000-8000-00000000beef"
STAGING_ONLY = "57a91201-0000-4000-8000-00000000cafe"
# A different canonical UUID, used to rewrite a manifest's own job_id.
MISMATCH_ID = "0deada11-0000-4000-8000-00000000d00d"


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


def _all_keys_and_etags() -> Dict[str, str]:
    """Every object key mapped to its ETag (a content fingerprint) in the bucket."""
    client = boto3.client("s3", region_name="us-east-1")
    return {
        obj["Key"]: obj["ETag"]
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
    remote.publish_manifest(ORPHAN, b"{}")

    report = repo.remote_check()

    assert ORPHAN in _ids(report.resurrection_risks)
    finding = next(f for f in report.resurrection_risks if f.job_id == ORPHAN)
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
    remote.put_archive(STRAY, archive)
    remote.put_sidecar(STRAY, "r3.yaml", b"version: 1\n")
    remote.put_sidecar(STRAY, "metadata.yaml", b"tags: []\n")
    # No manifest.json -> incomplete/interrupted.

    report = repo.remote_check()

    assert STRAY in _ids(report.manifestless_prefixes)
    # No complete manifest, so it is not a resurrection risk.
    assert STRAY not in _ids(report.resurrection_risks)


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
        Bucket=BUCKET, Key=remote._staging_manifest_key(STAGING_ONLY), Body=b"{}"
    )

    report = repo.remote_check()

    assert STAGING_ONLY in _ids(report.staging_manifests)
    # Staging alone (no archive/sidecar keys) is not a manifestless prefix.
    assert STAGING_ONLY not in _ids(report.manifestless_prefixes)


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


def test_broken_row_metadata_sidecar_missing(repo: Repository) -> None:
    """Deleting only ``metadata.yaml`` (manifest + archive intact) breaks the row.

    This is the reviewer's reproduction: the probe used to pass while ``fetch`` fails
    on the missing sidecar. The finding must name the missing sidecar.
    """
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.delete_object(
        Bucket=BUCKET, Key=remote._sidecar_key(job.id, "metadata.yaml")
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("metadata.yaml" in f.detail for f in findings)


def test_broken_row_r3_sidecar_missing(repo: Repository) -> None:
    """Deleting only ``r3.yaml`` (manifest + archive intact) breaks the row."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.delete_object(
        Bucket=BUCKET, Key=remote._sidecar_key(job.id, "r3.yaml")
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("r3.yaml" in f.detail for f in findings)


def test_broken_row_both_sidecars_missing(repo: Repository) -> None:
    """Both sidecars missing yields one finding per sidecar (not just the first)."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    for name in ("r3.yaml", "metadata.yaml"):
        remote._client.delete_object(
            Bucket=BUCKET, Key=remote._sidecar_key(job.id, name)
        )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("r3.yaml" in f.detail for f in findings)
    assert any("metadata.yaml" in f.detail for f in findings)


def test_broken_row_mismatched_manifest_id(repo: Repository) -> None:
    """A manifest whose own ``job_id`` names a different job breaks the row.

    The manifest stays structurally valid; only its ``job_id`` is rewritten to another
    canonical UUID, so the identity binding in ``loads(expected_job_id=...)`` is the
    sole reason it is rejected (never reported healthy).
    """
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")

    manifest = json.loads(remote.get_manifest(job.id))
    manifest["job_id"] = MISMATCH_ID
    remote._client.put_object(
        Bucket=BUCKET,
        Key=remote._manifest_key(job.id),
        Body=json.dumps(manifest).encode("utf-8"),
    )

    report = repo.remote_check()

    assert job.id in _ids(report.broken_rows)


def test_broken_row_oversized_manifest(repo: Repository) -> None:
    """An over-cap ``manifest.json`` breaks the row (rejected before its body read)."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.put_object(
        Bucket=BUCKET,
        Key=remote._manifest_key(job.id),
        Body=b"\0" * (MAX_MANIFEST_BYTES + 1),
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("manifest" in f.detail.lower() and "cap" in f.detail.lower()
               for f in findings)


def test_broken_row_oversized_sidecar(repo: Repository) -> None:
    """An over-cap sidecar breaks the row (probe bounds the read like fetch/rebuild)."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    remote._client.put_object(
        Bucket=BUCKET,
        Key=remote._sidecar_key(job.id, "metadata.yaml"),
        Body=b"\0" * (MAX_SIDECAR_BYTES + 1),
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("metadata.yaml" in f.detail and "cap" in f.detail.lower()
               for f in findings)


def test_broken_row_sidecar_content_mismatch_metadata(repo: Repository) -> None:
    """A present, same-size ``metadata.yaml`` with WRONG content (only the hash
    differs) breaks the row: presence and cap alone are not enough, because
    ``fetch``'s verify_directory would reject the mismatched bytes."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    original = remote.get_sidecar(job.id, "metadata.yaml")
    tampered = bytes(byte ^ 0xFF for byte in original)
    assert len(tampered) == len(original) and tampered != original
    remote._client.put_object(
        Bucket=BUCKET,
        Key=remote._sidecar_key(job.id, "metadata.yaml"),
        Body=tampered,
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("metadata.yaml" in f.detail for f in findings)


def test_broken_row_sidecar_content_mismatch_r3(repo: Repository) -> None:
    """Same-size, wrong-content ``r3.yaml`` (only the hash differs) breaks the row."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")
    remote = _s3(repo, "archive")
    original = remote.get_sidecar(job.id, "r3.yaml")
    tampered = bytes(byte ^ 0xFF for byte in original)
    assert len(tampered) == len(original) and tampered != original
    remote._client.put_object(
        Bucket=BUCKET,
        Key=remote._sidecar_key(job.id, "r3.yaml"),
        Body=tampered,
    )

    report = repo.remote_check()

    findings = [f for f in report.broken_rows if f.job_id == job.id]
    assert any("r3.yaml" in f.detail for f in findings)


def test_healthy_moved_row_clean_and_probe_read_only(repo: Repository) -> None:
    """A fully-present moved job yields NO broken finding, and probing it (including
    the sidecar reads) mutates no remote object."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    before = _all_keys_and_etags()
    report = repo.remote_check()

    assert job.id not in _ids(report.broken_rows)
    assert report.broken_rows == []
    assert _all_keys_and_etags() == before


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
    remote.put_archive(STRAY, archive)
    remote.put_sidecar(STRAY, "r3.yaml", b"version: 1\n")

    report = repo.remote_check()

    assert STRAY in _ids(report.manifestless_prefixes)
    assert STRAY not in _ids(report.broken_rows)


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
    remote.publish_manifest(ORPHAN, b"{}")
    archive = tmp_path / "a.tar.zst"
    archive.write_bytes(b"payload")
    remote.put_archive(STRAY, archive)
    remote.put_sidecar(STRAY, "r3.yaml", b"version: 1\n")
    remote._client.put_object(
        Bucket=BUCKET, Key=remote._staging_manifest_key(STAGING_ONLY), Body=b"{}"
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


# ---------------------------------------------------------------- malformed keys


def test_reports_traversal_manifest_key(repo: Repository) -> None:
    """A traversal-shaped manifest key is reported as a malformed key, never hidden."""
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}../../escaped/manifest.json", Body=b"{}"
    )

    report = repo.remote_check()

    assert report.has_findings
    assert "../../escaped" in _ids(report.malformed_keys)
    finding = next(f for f in report.malformed_keys if f.job_id == "../../escaped")
    assert finding.remote == "archive"


def test_reports_nested_manifest_key(repo: Repository) -> None:
    """A nested manifest key (extra path components) is reported as malformed."""
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}nested/uuid/manifest.json", Body=b"{}"
    )

    report = repo.remote_check()

    assert "nested/uuid" in _ids(report.malformed_keys)


def test_well_formed_manifest_key_is_not_malformed(repo: Repository) -> None:
    """A canonical-UUID manifest key must never be flagged malformed."""
    job = repo.commit(get_dummy_job("base"))
    assert job.id is not None
    repo.move(job.id, "archive")

    report = repo.remote_check()

    assert job.id not in _ids(report.malformed_keys)
    assert report.malformed_keys == []


def test_survives_corrupt_index_row_with_invalid_id(repo: Repository) -> None:
    """A pre-existing index row whose id is not a canonical UUID (only possible via
    external corruption) must be REPORTED, not crash the read-only check.

    Rule 4 probes each row on the remote via ``get_manifest``, which validates the id
    and would otherwise raise ``ValueError`` and abort the whole diagnostic — exactly
    the run you'd make on a corrupt store."""
    from datetime import datetime

    import r3.index

    with r3.index.Transaction(repo.path / "index.sqlite") as tx:
        tx.execute(
            "INSERT INTO jobs (id, timestamp, metadata, location)"
            " VALUES (?, ?, ?, ?)",
            ("../../escaped", datetime.now().isoformat(), "{}", "archive"),
        )

    report = repo.remote_check()  # must not raise

    assert report.has_findings
    assert "../../escaped" in _ids(report.broken_rows)
    finding = next(f for f in report.broken_rows if f.job_id == "../../escaped")
    assert finding.remote == "archive"
