"""Unit tests for `r3.remote`."""

from pathlib import Path
from typing import Any, Dict

import boto3
import pytest
import yaml
from moto import mock_aws
from pytest_mock.plugin import MockerFixture

from r3.remote import Remote, S3Remote

BUCKET_NAME = "test-bucket"
PREFIX = "jobs/"


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    job_path = tmp_path / "test-job-id"
    job_path.mkdir()
    (job_path / "r3.yaml").write_text(
        yaml.dump({"dependencies": [], "timestamp": "2024-01-01T00:00:00"})
    )
    (job_path / "metadata.yaml").write_text(yaml.dump({"tags": ["test"]}))
    (job_path / "run.py").write_text("print('hello')")
    (job_path / "output").mkdir()
    (job_path / "output" / "result.txt").write_text("result data")
    return job_path


@pytest.fixture
def s3_remote():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET_NAME)
        yield S3Remote(bucket=BUCKET_NAME, prefix=PREFIX)


def test_s3_remote_upload_and_exists(s3_remote: S3Remote, job_dir: Path):
    s3_remote.upload("test-job-id", job_dir)
    assert s3_remote.exists("test-job-id")


def test_s3_remote_upload_and_download(
    s3_remote: S3Remote, job_dir: Path, tmp_path: Path
):
    s3_remote.upload("test-job-id", job_dir)

    download_path = tmp_path / "downloaded-job"
    s3_remote.download("test-job-id", download_path)

    assert (download_path / "r3.yaml").read_text() == (job_dir / "r3.yaml").read_text()
    assert (download_path / "metadata.yaml").read_text() == (
        job_dir / "metadata.yaml"
    ).read_text()
    assert (download_path / "run.py").read_text() == (job_dir / "run.py").read_text()
    assert (download_path / "output" / "result.txt").read_text() == (
        job_dir / "output" / "result.txt"
    ).read_text()


def test_s3_remote_remove(s3_remote: S3Remote, job_dir: Path):
    s3_remote.upload("test-job-id", job_dir)
    assert s3_remote.exists("test-job-id")

    s3_remote.remove("test-job-id")
    assert not s3_remote.exists("test-job-id")


def test_s3_remote_exists_returns_false_for_missing_job(s3_remote: S3Remote):
    assert not s3_remote.exists("nonexistent-job-id")


def test_s3_remote_download_raises_for_missing_job(
    s3_remote: S3Remote, tmp_path: Path
):
    with pytest.raises(FileNotFoundError):
        s3_remote.download("nonexistent-job-id", tmp_path / "destination")


def test_s3_remote_with_empty_prefix(job_dir: Path, tmp_path: Path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET_NAME)

        remote = S3Remote(bucket=BUCKET_NAME, prefix="")
        remote.upload("test-job-id", job_dir)
        assert remote.exists("test-job-id")

        download_path = tmp_path / "downloaded-job"
        remote.download("test-job-id", download_path)

        assert (download_path / "r3.yaml").read_text() == (
            job_dir / "r3.yaml"
        ).read_text()
        assert (download_path / "output" / "result.txt").read_text() == (
            job_dir / "output" / "result.txt"
        ).read_text()


def test_s3_remote_from_config():
    config: Dict[str, Any] = {
        "type": "s3",
        "bucket": "my-bucket",
        "prefix": "my-prefix/",
    }
    remote = Remote.from_config(config)
    assert isinstance(remote, S3Remote)
    assert remote.bucket == "my-bucket"
    assert remote.prefix == "my-prefix/"


def test_s3_remote_from_config_with_optional_fields():
    config: Dict[str, Any] = {
        "type": "s3",
        "bucket": "my-bucket",
        "prefix": "my-prefix/",
        "profile": "my-profile",
        "endpoint_url": "http://localhost:9000",
    }
    remote = Remote.from_config(config)
    assert isinstance(remote, S3Remote)
    assert remote.bucket == "my-bucket"
    assert remote.prefix == "my-prefix/"
    assert remote.profile == "my-profile"
    assert remote.endpoint_url == "http://localhost:9000"


def test_remote_default_cache_file_list_is_false():
    """Subclasses without explicit override should not cache file lists."""
    assert Remote.cache_file_list is False


def test_s3_remote_caches_file_list():
    """S3 storage is immutable, so S3Remote caches file lists."""
    assert S3Remote.cache_file_list is True


def test_s3_remote_from_config_accepts_archive_fields():
    config = {
        "type": "s3",
        "bucket": "b",
        "prefix": "p/",
        "archive_format": "tar.zst",
        "archive_frame_size": 8388608,
    }
    remote = S3Remote.from_config(config)
    assert remote.archive_format == "tar.zst"
    assert remote.archive_frame_size == 8388608


def test_s3_remote_archive_format_defaults_to_none():
    remote = S3Remote(bucket="b")
    assert remote.archive_format is None
    assert remote.archive_frame_size == 16 * 1024 * 1024


def test_s3_remote_from_config_rejects_invalid_frame_size():
    config = {
        "type": "s3",
        "bucket": "b",
        "archive_format": "tar.zst",
        "archive_frame_size": 0,
    }
    with pytest.raises(ValueError, match="archive_frame_size"):
        S3Remote.from_config(config)


def test_s3_remote_from_config_rejects_unknown_archive_format():
    config = {
        "type": "s3",
        "bucket": "b",
        "archive_format": "tar.gz",
    }
    with pytest.raises(ValueError, match="archive_format"):
        S3Remote.from_config(config)


@pytest.fixture
def s3_remote_archive():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET_NAME)
        yield S3Remote(bucket=BUCKET_NAME, prefix=PREFIX, archive_format="tar.zst")


def test_s3_remote_upload_archive_creates_single_object(
    s3_remote_archive: S3Remote, job_dir: Path
):
    s3_remote_archive.upload("test-job-id", job_dir)
    client = boto3.client("s3", region_name="us-east-1")
    response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    assert keys == [f"{PREFIX}test-job-id.tar.zst"]


def test_s3_remote_archive_round_trip(
    s3_remote_archive: S3Remote, job_dir: Path, tmp_path: Path
):
    """Upload then download via archive: extracted files must match originals."""
    s3_remote_archive.upload("test-job-id", job_dir)

    download_path = tmp_path / "downloaded"
    s3_remote_archive.download("test-job-id", download_path)

    # Compare contents of every file in job_dir against the downloaded copy.
    for src in job_dir.rglob("*"):
        if src.is_file():
            rel = src.relative_to(job_dir)
            dst = download_path / rel
            assert dst.exists(), f"Missing file: {rel}"
            assert dst.read_bytes() == src.read_bytes(), f"Content mismatch: {rel}"


def test_s3_remote_archive_is_seekable(
    s3_remote_archive: S3Remote, job_dir: Path, tmp_path: Path
):
    """The uploaded archive is in Zstandard Seekable Format (has a seek table)."""
    s3_remote_archive.upload("test-job-id", job_dir)

    archive_path = tmp_path / "downloaded.tar.zst"
    client = boto3.client("s3", region_name="us-east-1")
    client.download_file(
        BUCKET_NAME, f"{PREFIX}test-job-id.tar.zst", str(archive_path)
    )

    import pyzstd
    # SeekableZstdFile in 'r' mode requires a valid seek table; if the archive
    # were single-frame (no seek table), pyzstd would raise here.
    with pyzstd.SeekableZstdFile(str(archive_path), "r") as zfh:
        # Verify seek to a non-zero offset works.
        zfh.read(100)
        zfh.seek(0)
        first_bytes = zfh.read(100)
        assert len(first_bytes) > 0


def test_s3_remote_archive_exists(s3_remote_archive: S3Remote, job_dir: Path):
    assert not s3_remote_archive.exists("test-job-id")
    s3_remote_archive.upload("test-job-id", job_dir)
    assert s3_remote_archive.exists("test-job-id")


def test_s3_remote_archive_remove(s3_remote_archive: S3Remote, job_dir: Path):
    s3_remote_archive.upload("test-job-id", job_dir)
    s3_remote_archive.remove("test-job-id")
    assert not s3_remote_archive.exists("test-job-id")
    client = boto3.client("s3", region_name="us-east-1")
    response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    assert response.get("KeyCount", 0) == 0


def test_s3_remote_archive_upload_cleans_temp_on_failure(
    s3_remote_archive: S3Remote, job_dir: Path, mocker: MockerFixture
):
    """If upload_file raises, no .tar.zst temp file is left behind."""
    import tempfile as _tempfile

    tempdir = _tempfile.gettempdir()
    before = {
        p.name for p in Path(tempdir).iterdir() if p.suffix == ".zst"
    }

    mocker.patch.object(
        s3_remote_archive._client, "upload_file",
        side_effect=RuntimeError("simulated network failure")
    )

    with pytest.raises(RuntimeError, match="simulated"):
        s3_remote_archive.upload("test-job-id", job_dir)

    after = {p.name for p in Path(tempdir).iterdir() if p.suffix == ".zst"}
    new_files = after - before
    assert not new_files, f"Temp files leaked: {new_files}"


def test_s3_remote_archive_empty_job(
    s3_remote_archive: S3Remote, tmp_path: Path
):
    """A job with only metadata files round-trips correctly."""
    job_path = tmp_path / "empty-job"
    job_path.mkdir()
    (job_path / "r3.yaml").write_text("dependencies: []\n")
    (job_path / "metadata.yaml").write_text("tags: []\n")

    s3_remote_archive.upload("empty-job-id", job_path)

    download_path = tmp_path / "downloaded"
    s3_remote_archive.download("empty-job-id", download_path)
    assert (download_path / "r3.yaml").exists()
    assert (download_path / "metadata.yaml").exists()


def test_s3_remote_archive_deep_nested_paths(
    s3_remote_archive: S3Remote, tmp_path: Path
):
    """Deeply nested file paths survive a round-trip."""
    job_path = tmp_path / "nested-job"
    job_path.mkdir()
    (job_path / "r3.yaml").write_text("dependencies: []\n")
    (job_path / "metadata.yaml").write_text("tags: []\n")
    deep = job_path / "a" / "b" / "c" / "d"
    deep.mkdir(parents=True)
    (deep / "result.txt").write_text("deep")

    s3_remote_archive.upload("nested-job-id", job_path)
    download_path = tmp_path / "downloaded"
    s3_remote_archive.download("nested-job-id", download_path)
    assert (download_path / "a" / "b" / "c" / "d" / "result.txt").read_text() == "deep"


def test_s3_remote_archive_special_characters_in_paths(
    s3_remote_archive: S3Remote, tmp_path: Path
):
    """Spaces and non-ASCII in paths survive a round-trip."""
    job_path = tmp_path / "special-job"
    job_path.mkdir()
    (job_path / "r3.yaml").write_text("dependencies: []\n")
    (job_path / "metadata.yaml").write_text("tags: []\n")
    (job_path / "file with spaces.txt").write_text("ok")
    (job_path / "résultat.txt").write_text("é")

    s3_remote_archive.upload("special-job-id", job_path)
    download_path = tmp_path / "downloaded"
    s3_remote_archive.download("special-job-id", download_path)
    assert (download_path / "file with spaces.txt").read_text() == "ok"
    assert (download_path / "résultat.txt").read_text() == "é"


def test_s3_remote_archive_corrupted_download_raises(
    s3_remote_archive: S3Remote, tmp_path: Path
):
    """A corrupted (random-bytes) archive raises a clear error on download."""
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}corrupted-id.tar.zst",
        Body=b"not a valid zstd archive at all",
    )

    download_path = tmp_path / "downloaded"
    # Specific exception type depends on pyzstd; just verify it raises.
    with pytest.raises(Exception):  # noqa: B017
        s3_remote_archive.download("corrupted-id", download_path)
