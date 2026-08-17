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
export R3_TEST_S3_RESPONSE_CHECKSUM_VALIDATION=when_required  # optional, if needed
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

`R3_TEST_S3_RESPONSE_CHECKSUM_VALIDATION` (`when_supported` or
`when_required`) is the analogous knob for response-side checksum
validation and is exposed in `r3.yaml` remote config as
`response_checksum_validation`. Leave it unset unless a backend needs it.

Each test run uses a UUID-scoped sub-prefix and cleans up its own keys at
teardown. If teardown fails, the test surfaces a clear error so you can
manually delete the affected sub-prefix.

## Running the moto-backed tests

The rest of the S3 tests run against `moto` and need no live endpoint, but
two environment gotchas can bite:

- **`AWS_CONFIG_FILE=/dev/null`.** If a global `~/.aws/config` sets an
  `endpoint_url`, boto3 will pick it up and the moto-backed tests will try
  to reach that real endpoint. Run them with `AWS_CONFIG_FILE=/dev/null` to
  isolate them from your local AWS config.
- **Use `python -m pytest`, not plain `pytest`.** `python -m pytest` puts
  the current directory on `sys.path`, so the tests import the working-tree
  `r3`; a plain `pytest` may not, and can fail to import or pick up an
  installed copy instead.
