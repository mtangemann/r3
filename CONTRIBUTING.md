# Contributing

Standard development setup and contribution guidelines for R3.

## Live S3 smoke tests

Some tests exercise a real S3-compatible endpoint (CEPH, MinIO) to catch
behaviours that `moto` does not faithfully simulate. They are skipped by
default. To run:

```bash
export R3_TEST_S3_ENDPOINT_URL=https://your-ceph.example.com
export R3_TEST_S3_BUCKET=your-existing-bucket
export R3_TEST_S3_PREFIX=r3-smoke-tests/        # optional sub-prefix
export R3_TEST_S3_ADDRESSING_STYLE=path         # required for CEPH RGW
export R3_TEST_S3_REQUEST_CHECKSUM_CALCULATION=when_required  # for older CEPH
# AWS credentials: either direct env vars
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
# OR a configured profile in ~/.aws/credentials
# export R3_TEST_S3_PROFILE=ceph-prod

python -m pytest -m live_s3
```

`R3_TEST_S3_ADDRESSING_STYLE` should usually be `path` for CEPH RGW (and
some MinIO setups). Boto3 defaults to virtual-host-style, which CEPH
typically does not support — leaving this unset against such a backend
will yield cryptic `InvalidAccessKeyId` errors. If your s3cmd config has
`host_base == host_bucket` (no `%(bucket)s` placeholder), use `path`.

`R3_TEST_S3_REQUEST_CHECKSUM_CALCULATION=when_required` may also be
needed on older CEPH RGW builds. Boto3 1.36+ defaults to `when_supported`
and adds CRC32 integrity headers (`x-amz-sdk-checksum-algorithm`,
`x-amz-checksum-crc32`) to `PutObject`. Older CEPH RGW versions reject
those headers under SigV4 and return the same misleading
`InvalidAccessKeyId` — `LIST` works but `PUT` fails. Setting
`when_required` restores the pre-1.36 behavior and keeps GETs/LISTs
unaffected. Same field is exposed in `r3.yaml` remote config as
`request_checksum_calculation`.

Each test run uses a UUID-scoped sub-prefix and cleans up its own keys at
teardown. If teardown fails, the test surfaces a clear error so you can
manually delete the affected sub-prefix.
