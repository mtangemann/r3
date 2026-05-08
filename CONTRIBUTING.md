# Contributing

Standard development setup and contribution guidelines for R3.

## Live S3 smoke tests

Some tests exercise a real S3-compatible endpoint (CEPH, MinIO) to catch
behaviours that `moto` does not faithfully simulate. They are skipped by
default. To run:

```bash
export R3_TEST_S3_ENDPOINT_URL=https://your-ceph.example.com
export R3_TEST_S3_BUCKET=your-existing-bucket
export R3_TEST_S3_PREFIX=r3-smoke-tests/   # optional sub-prefix
# AWS credentials via env vars or AWS profile (R3_TEST_S3_PROFILE)
pytest -m live_s3
```

Each test run uses a UUID-scoped sub-prefix and cleans up its own keys at
teardown. If teardown fails, the test surfaces a clear error so you can
manually delete the affected sub-prefix.
