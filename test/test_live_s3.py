"""Smoke tests against a live S3-compatible endpoint.

These tests are skipped by default. To run, set:
- R3_TEST_S3_ENDPOINT_URL: S3 endpoint URL (e.g. https://ceph.example.com)
- R3_TEST_S3_BUCKET: existing bucket the user has access to
- R3_TEST_S3_PREFIX: optional base prefix within the bucket
- R3_TEST_S3_PROFILE: optional AWS credential profile

Then: pytest -m live_s3
"""

import os
import uuid
from pathlib import Path
from typing import Generator, List

import boto3
import pytest
import yaml

from r3 import Repository
from r3.remote import S3Remote  # noqa: F401  (imported for visibility/type assertion)

_LIVE_ENDPOINT = os.environ.get("R3_TEST_S3_ENDPOINT_URL")
_LIVE_BUCKET = os.environ.get("R3_TEST_S3_BUCKET")
_LIVE_PREFIX = os.environ.get("R3_TEST_S3_PREFIX", "").rstrip("/")
_LIVE_PROFILE = os.environ.get("R3_TEST_S3_PROFILE")


pytestmark = [
    pytest.mark.live_s3,
    pytest.mark.skipif(
        not (_LIVE_ENDPOINT and _LIVE_BUCKET),
        reason="R3_TEST_S3_ENDPOINT_URL and R3_TEST_S3_BUCKET must be set",
    ),
]


def _live_client():
    session = boto3.Session(profile_name=_LIVE_PROFILE)
    return session.client("s3", endpoint_url=_LIVE_ENDPOINT)


@pytest.fixture
def run_prefix() -> Generator[str, None, None]:
    """A unique prefix per test run; cleaned up at teardown.

    Asserts the prefix is empty before tests start to defend against
    accidental reuse.
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

    # Teardown: delete every key under the run prefix. We deliberately raise
    # if cleanup fails — this surfaces orphaned keys clearly so the user can
    # manually clean up. The next run's "prefix is empty" assert provides a
    # second layer of defense.
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
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{exc!r}")
    if failed:
        pytest.fail(
            "Live-S3 teardown could not delete some keys; manual cleanup may be "
            f"needed under {prefix}: {failed}"
        )


def _make_repo(tmp_path: Path, run_prefix: str, archive: bool) -> Repository:
    repo = Repository.init(tmp_path / "repository")
    config_path = repo.path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    remote_config = {
        "type": "s3",
        "bucket": _LIVE_BUCKET,
        "prefix": run_prefix,
        "endpoint_url": _LIVE_ENDPOINT,
    }
    if _LIVE_PROFILE:
        remote_config["profile"] = _LIVE_PROFILE
    if archive:
        remote_config["archive_format"] = "tar.zst"
    config["remotes"] = {"archive": remote_config}
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return Repository(repo.path)
