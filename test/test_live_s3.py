"""Smoke tests against a live S3-compatible endpoint (archive-only transport).

Storage is archive-only: every job is stored as a ``data.tar.zst`` payload archive
plus ``r3.yaml``/``metadata.yaml`` sidecars and a ``manifest.json`` completion
marker under ``{prefix}{job_id}/``. There is a single transport path (no
"no-archive"/"with-archive" split, and no ``archive_format`` config field).

These tests are skipped by default. To run, set:
- R3_TEST_S3_ENDPOINT_URL: S3 endpoint URL (e.g. https://ceph.example.com)
- R3_TEST_S3_BUCKET: existing bucket the user has access to
- R3_TEST_S3_PREFIX: optional base prefix within the bucket
- R3_TEST_S3_PROFILE: optional AWS credential profile
- R3_TEST_S3_ADDRESSING_STYLE: optional "auto" | "path" | "virtual"
  (CEPH RGW typically requires "path")
- R3_TEST_S3_REQUEST_CHECKSUM_CALCULATION: optional "when_supported" |
  "when_required" (older CEPH RGW builds need "when_required" or PUTs fail with a
  misleading InvalidAccessKeyId)
- R3_TEST_S3_RESPONSE_CHECKSUM_VALIDATION: optional "when_supported" |
  "when_required" (GET is on the move/fetch critical path, so it is exposed too)

Then: pytest -m live_s3

Multipart note: the multipart round-trip test uploads a ~50 MiB incompressible
archive. That is intentional (it forces boto3 to split the upload into several
parts) and fine for a manual live test, but it is heavy — do not run it on a
metered or slow link expecting it to be quick.

Bucket lifecycle recommendation (incomplete multipart uploads): R3 *lists*
in-progress multipart uploads (``remote check`` surfaces them via
``list_incomplete_multipart_uploads``) but never auto-aborts them. Interrupted
uploads therefore linger and consume quota. Operators SHOULD configure a bucket
lifecycle rule that auto-aborts incomplete multipart uploads after N days so
crash-interrupted moves cannot accumulate wasted storage.
"""

import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, Generator, List

import boto3
import pytest
import yaml
from botocore.config import Config

import r3.manifest
from r3 import Job, Repository
from r3.remote import Remote

_LIVE_ENDPOINT = os.environ.get("R3_TEST_S3_ENDPOINT_URL")
_LIVE_BUCKET = os.environ.get("R3_TEST_S3_BUCKET")
_LIVE_PREFIX = os.environ.get("R3_TEST_S3_PREFIX", "").rstrip("/")
_LIVE_PROFILE = os.environ.get("R3_TEST_S3_PROFILE")
_LIVE_ADDRESSING_STYLE = os.environ.get("R3_TEST_S3_ADDRESSING_STYLE")
_LIVE_REQUEST_CHECKSUM = os.environ.get("R3_TEST_S3_REQUEST_CHECKSUM_CALCULATION")
_LIVE_RESPONSE_CHECKSUM = os.environ.get("R3_TEST_S3_RESPONSE_CHECKSUM_VALIDATION")

# Object names within a job prefix (mirrors r3.remote's private constants).
_ARCHIVE_NAME = "data.tar.zst"
_MANIFEST_NAME = "manifest.json"
_STAGING_MANIFEST_NAME = "manifest.json.staging"

# A multipart object's ETag is "<md5hex>-<partcount>"; a single PUT has no suffix.
_MULTIPART_ETAG_RE = re.compile(r"[0-9a-f]{32}-[0-9]+")

# ~50 MiB of incompressible payload. zstd cannot shrink random data, so the
# archive stays > 3x the 16 MiB multipart chunk size and boto3 uploads >= 3 parts.
_MULTIPART_PAYLOAD_MIB = 50


pytestmark = [
    pytest.mark.live_s3,
    pytest.mark.skipif(
        not (_LIVE_ENDPOINT and _LIVE_BUCKET),
        reason="R3_TEST_S3_ENDPOINT_URL and R3_TEST_S3_BUCKET must be set",
    ),
]


def _live_client() -> Any:
    session = boto3.Session(profile_name=_LIVE_PROFILE)
    config_kwargs: Dict[str, Any] = {}
    if _LIVE_ADDRESSING_STYLE:
        config_kwargs["s3"] = {"addressing_style": _LIVE_ADDRESSING_STYLE}
    if _LIVE_REQUEST_CHECKSUM:
        config_kwargs["request_checksum_calculation"] = _LIVE_REQUEST_CHECKSUM
    if _LIVE_RESPONSE_CHECKSUM:
        config_kwargs["response_checksum_validation"] = _LIVE_RESPONSE_CHECKSUM
    client_config = Config(**config_kwargs) if config_kwargs else None
    return session.client("s3", endpoint_url=_LIVE_ENDPOINT, config=client_config)


def _remote_config(run_prefix: str) -> Dict[str, Any]:
    """Builds a `Remote.from_config`-valid S3 remote config from the env.

    Threads only valid fields (no removed ``archive_format``). CEPH RGW typically
    needs ``addressing_style=path`` + ``request_checksum_calculation=when_required``
    (see the ``S3Remote`` docstring); those are supplied via env at run time.
    """
    config: Dict[str, Any] = {
        "type": "s3",
        "bucket": _LIVE_BUCKET,
        "prefix": run_prefix,
        "endpoint_url": _LIVE_ENDPOINT,
    }
    if _LIVE_PROFILE:
        config["profile"] = _LIVE_PROFILE
    if _LIVE_ADDRESSING_STYLE:
        config["addressing_style"] = _LIVE_ADDRESSING_STYLE
    if _LIVE_REQUEST_CHECKSUM:
        config["request_checksum_calculation"] = _LIVE_REQUEST_CHECKSUM
    if _LIVE_RESPONSE_CHECKSUM:
        config["response_checksum_validation"] = _LIVE_RESPONSE_CHECKSUM
    return config


def _make_repo(tmp_path: Path, run_prefix: str) -> Repository:
    repo = Repository.init(tmp_path / "repository")
    config_path = repo.path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["remotes"] = {"archive": _remote_config(run_prefix)}
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return Repository(repo.path)


def _make_remote(run_prefix: str) -> Remote:
    return Remote.from_config(_remote_config(run_prefix))


@pytest.fixture
def run_prefix() -> Generator[str, None, None]:
    """A unique prefix per test run; cleaned up at teardown.

    Asserts the prefix is empty before tests start to defend against accidental
    reuse, and at teardown deletes every object under the prefix (failing loudly
    on leftovers) and aborts any incomplete multipart uploads it left behind.
    """
    base = (_LIVE_PREFIX + "/") if _LIVE_PREFIX else ""
    run_id = uuid.uuid4().hex
    prefix = f"{base}{run_id}/"

    client = _live_client()
    response = client.list_objects_v2(Bucket=_LIVE_BUCKET, Prefix=prefix, MaxKeys=1)
    assert response.get("KeyCount", 0) == 0, (
        f"Prefix {prefix} unexpectedly non-empty before test run"
    )

    yield prefix

    # Teardown: delete every key under the run prefix. We deliberately raise if
    # cleanup fails — this surfaces orphaned keys clearly so the user can manually
    # clean up. The next run's "prefix is empty" assert is a second layer.
    paginator = client.get_paginator("list_objects_v2")
    failed: List[str] = []
    for page in paginator.paginate(Bucket=_LIVE_BUCKET, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        try:
            client.delete_objects(
                Bucket=_LIVE_BUCKET,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
            )
        except Exception as exc:
            failed.append(f"{exc!r}")

    # Best-effort: abort any incomplete multipart uploads left under the prefix.
    # These are not regular objects (the delete above does not touch them) and
    # would otherwise leak quota. A bucket lifecycle rule is the durable safety
    # net (see the module docstring); this is a convenience for the suite, so it
    # is swallowed rather than allowed to fail an otherwise-clean run.
    try:
        mpu_paginator = client.get_paginator("list_multipart_uploads")
        for page in mpu_paginator.paginate(Bucket=_LIVE_BUCKET, Prefix=prefix):
            for upload in page.get("Uploads", []):
                try:
                    client.abort_multipart_upload(
                        Bucket=_LIVE_BUCKET,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                except Exception:
                    pass
    except Exception:
        pass

    if failed:
        pytest.fail(
            "Live-S3 teardown could not delete some keys; manual cleanup may be "
            f"needed under {prefix}: {failed}"
        )


def _commit_dummy_job(repo: Repository, name: str = "live-test") -> Job:
    """Creates a small job in `repo` and commits it."""
    src = repo.path.parent / f"src-{name}"
    src.mkdir()
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text(f"tags: [{name}]\n")
    (src / "run.py").write_text("print('hello')\n")
    (src / "output").mkdir()
    (src / "output" / "result.txt").write_text("result data")
    return repo.commit(Job(src))


def _full_lifecycle(repo: Repository, tmp_path: Path) -> None:
    job = _commit_dummy_job(repo)
    assert job.id is not None
    original_hash = job.hash()
    expected_files = sorted(job.files.keys())

    repo.move(job.id, "archive")
    assert not (repo.path / "jobs" / job.id).exists()
    assert repo._index.get_location(job.id) == "archive"

    # find() projects the remote job from the index (files from the cached list).
    found = repo.find({"tags": "live-test"})
    assert len(found) == 1
    assert sorted(found[0].files.keys()) == expected_files

    repo.fetch(job.id)
    assert (repo.path / "jobs" / job.id).exists()

    fetched = repo.get_job_by_id(job.id)
    assert fetched.hash(recompute=True) == original_hash

    checkout_path = tmp_path / "checkout"
    repo.checkout(fetched, checkout_path)
    assert (checkout_path / "run.py").read_text() == "print('hello')\n"


def test_live_s3_full_lifecycle(tmp_path: Path, run_prefix: str) -> None:
    """Small job: commit -> move -> find-projects-it -> fetch -> checkout."""
    repo = _make_repo(tmp_path, run_prefix)
    _full_lifecycle(repo, tmp_path)


def test_live_s3_multipart_round_trip(tmp_path: Path, run_prefix: str) -> None:
    """Multipart upload round-trip, verified to have actually used multipart.

    NOTE: heavy (~50 MiB) upload. Incompressible payload forces the compressed
    archive above 3x the 16 MiB chunk size so boto3 splits it into >= 3 parts.
    """
    repo = _make_repo(tmp_path, run_prefix)
    src = repo.path.parent / "src-multipart"
    src.mkdir()
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text("tags: [multipart-test]\n")
    (src / "output").mkdir()
    with open(src / "output" / "blob.bin", "wb") as f:
        for _ in range(_MULTIPART_PAYLOAD_MIB):
            f.write(os.urandom(1024 * 1024))
    job = repo.commit(Job(src))
    assert job.id is not None
    original_hash = job.hash()

    repo.move(job.id, "archive")
    assert not (repo.path / "jobs" / job.id).exists()

    # Confirm the upload really used multipart, not a single PUT: HEAD the archive
    # and require a multipart ETag shape ("<md5hex>-<partcount>"). boto3 chunks the
    # upload client-side, so the reported part count reflects our TransferConfig
    # (16 MiB chunks) regardless of the backend.
    archive_key = f"{run_prefix}{job.id}/{_ARCHIVE_NAME}"
    etag = _live_client().head_object(
        Bucket=_LIVE_BUCKET, Key=archive_key
    )["ETag"].strip('"')
    assert _MULTIPART_ETAG_RE.fullmatch(etag), (
        f"Expected a multipart ETag for {archive_key}, got {etag!r}: the archive "
        "was stored as a single PUT, so multipart was not exercised."
    )
    part_count = int(etag.rsplit("-", 1)[1])
    assert part_count >= 3, (
        f"Expected >= 3 multipart parts for a ~{_MULTIPART_PAYLOAD_MIB} MiB "
        f"archive with 16 MiB chunks, got {part_count}."
    )

    repo.fetch(job.id)
    fetched = repo.get_job_by_id(job.id)
    assert fetched.hash(recompute=True) == original_hash


def test_live_s3_incomplete_multipart_uploads(run_prefix: str) -> None:
    """list/abort of in-progress multipart uploads work on the real endpoint.

    Starts a multipart upload under the prefix, asserts the remote surfaces it,
    then aborts it (verifying both LIST and ABORT permissions). R3 lists but never
    auto-aborts these — operators rely on a bucket lifecycle rule (module docstring).
    """
    client = _live_client()
    key = f"{run_prefix}incomplete-mpu-probe"
    upload_id = client.create_multipart_upload(Bucket=_LIVE_BUCKET, Key=key)[
        "UploadId"
    ]

    remote = _make_remote(run_prefix)

    # LIST: the remote must report the in-progress upload.
    listed = list(remote.list_incomplete_multipart_uploads())
    assert (key, upload_id) in listed, (
        f"Started multipart upload {key!r} not reported by "
        f"list_incomplete_multipart_uploads: {listed}"
    )

    # ABORT: the bucket must permit aborting it. A raise here signals a missing
    # permission on the endpoint (the thing this test exists to catch).
    client.abort_multipart_upload(Bucket=_LIVE_BUCKET, Key=key, UploadId=upload_id)

    # After abort it should no longer be listed. CEPH RGW reflects the abort
    # immediately; on an eventually-consistent backend this could briefly lag.
    remaining = [
        pair
        for pair in remote.list_incomplete_multipart_uploads()
        if pair[0] == key
    ]
    assert remaining == [], f"Upload {key!r} still listed after abort: {remaining}"


def test_live_s3_publish_manifest_copyobject_round_trip(
    tmp_path: Path, run_prefix: str
) -> None:
    """move() exercises publish_manifest's staging-copy against the endpoint.

    publish_manifest PUTs a staging manifest, GET-verifies it, server-side
    ``copy_object``s it to the final key (bound via ``CopySourceIfMatch``), then
    re-GET-verifies. Assert the final manifest exists and re-GETs as a valid
    manifest, and that no ``manifest.json.staging`` object is left behind.
    """
    repo = _make_repo(tmp_path, run_prefix)
    job = _commit_dummy_job(repo, name="publish-test")
    assert job.id is not None

    repo.move(job.id, "archive")

    manifest_key = f"{run_prefix}{job.id}/{_MANIFEST_NAME}"
    staging_key = f"{run_prefix}{job.id}/{_STAGING_MANIFEST_NAME}"

    # Final manifest re-GETs and parses as a valid manifest for this job.
    remote = repo.remotes["archive"]
    manifest = r3.manifest.loads(remote.get_manifest(job.id))
    assert manifest["job_id"] == job.id

    client = _live_client()
    # HEAD confirms the final key exists (raises if missing).
    client.head_object(Bucket=_LIVE_BUCKET, Key=manifest_key)
    # The staging copy must be gone.
    keys = [
        obj["Key"]
        for obj in client.list_objects_v2(
            Bucket=_LIVE_BUCKET, Prefix=f"{run_prefix}{job.id}/"
        ).get("Contents", [])
    ]
    assert staging_key not in keys, f"Leftover staging manifest: {staging_key}"


@pytest.mark.skip(
    reason="Mid-publish fault injection (pre-copy failure / post-copy "
    "interruption) cannot be forced cleanly against a live endpoint; the "
    "deterministic coverage is the moto unit tests. See the docstring for the "
    "manual live procedure."
)
def test_live_s3_publish_manifest_fault_injection() -> None:
    """Placeholder for publish_manifest's deep crash-safety paths.

    These need a fault injected mid-operation, which cannot be done cleanly
    against a live endpoint. To reproduce by hand against CEPH, pause/kill the
    process at the marked points:

      * pre-copy failure: interrupt after the staging PUT + GET-verify but before
        ``copy_object``. A re-run of ``move`` must overwrite staging, publish
        cleanly, and leave no staging object.
      * post-copy interruption: interrupt after ``copy_object`` materializes the
        final key but before staging is deleted. ``exists()`` must report the job
        published, ``get_manifest`` must re-GET the correct bytes, and a later
        ``delete_job``/``remove`` must sweep the leftover staging key.

    Both are covered deterministically by the moto unit tests in test_remote.py.
    """


def test_live_s3_remove_clears_prefix(tmp_path: Path, run_prefix: str) -> None:
    """remove() deletes every object under the job prefix on the real endpoint.

    Forcing a real per-object delete error is not cleanly possible live, so
    delete_job's per-object ``Errors`` handling is unit-tested with moto; here we
    assert a normal remove leaves the job prefix empty.
    """
    repo = _make_repo(tmp_path, run_prefix)
    job = _commit_dummy_job(repo, name="remove-test")
    assert job.id is not None

    repo.move(job.id, "archive")
    job_prefix = f"{run_prefix}{job.id}/"
    client = _live_client()
    before = client.list_objects_v2(
        Bucket=_LIVE_BUCKET, Prefix=job_prefix, MaxKeys=1
    )
    assert before.get("KeyCount", 0) > 0, "move() left no objects to remove."

    repo.remove(job.id)

    after = client.list_objects_v2(
        Bucket=_LIVE_BUCKET, Prefix=job_prefix, MaxKeys=1
    )
    assert after.get("KeyCount", 0) == 0, (
        "remove() left objects under the job prefix."
    )
