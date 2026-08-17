# Remote storage

Remote storage lets you archive committed jobs to an S3-compatible bucket (AWS S3,
CEPH RGW, MinIO, …) to free local disk space while keeping the jobs findable. You
`move` a job to a remote to upload and archive it, and `fetch` it back on demand when
you need its files again.

!!! warning "Alpha feature"
    Remote storage is an **alpha** feature. The bucket layout, on-disk layout, CLI,
    and Python API may still change in backward-incompatible ways between releases.
    Back up anything you cannot easily reproduce, and treat the durable metadata in
    `r3.yaml` as something you must keep (see [Good to know](#good-to-know)).

## Overview

A committed job normally lives entirely on local disk. When you `move` it to a
configured remote, R3 archives the job's files, uploads them (and its metadata) to the
bucket, verifies the upload, and only then deletes the local copy. The job stays in
the index and remains findable with `r3 find`: it becomes a **metadata-only
projection** whose metadata you can still query, but whose files, content hash, and
resolved dependencies are unavailable until you `fetch` it back. Trying to use those
(for example, `r3 checkout`) tells you to fetch first.

The remote bucket is the source of truth for a moved job; the local SQLite index is a
cache that can be rebuilt from the bucket with `r3 rebuild-index`.

## Configuring a remote

Add a remote with `r3 remote add`:

```bash
r3 remote add my-remote --type s3 --bucket my-bucket --prefix r3-jobs/
```

Remotes are stored per repository in `r3.yaml`. The repository is taken from
`--repository` or the `R3_REPOSITORY` environment variable.

Flags for `r3 remote add`:

| Flag | Purpose |
| --- | --- |
| `--type` (required) | Remote type. Use `s3` for S3/CEPH/MinIO. |
| `--bucket` | Bucket name. |
| `--prefix` | Key prefix within the bucket under which this repository's jobs live. |
| `--endpoint-url` | Endpoint URL for non-AWS backends (CEPH RGW, MinIO). Omit for AWS S3. |
| `--profile` | Named AWS credential profile (from `~/.aws/credentials`). Otherwise the usual AWS credential sources (env vars, instance role, …) apply. |
| `--addressing-style` | `auto`, `path`, or `virtual`. CEPH RGW usually needs `path`. |
| `--request-checksum-calculation` | `when_supported` or `when_required`. Older CEPH RGW builds need `when_required`. |
| `--response-checksum-validation` | `when_supported` or `when_required`. |

### CEPH / MinIO note

Against CEPH RGW (and some MinIO setups) you usually need two extra flags:

```bash
r3 remote add ceph --type s3 \
  --bucket my-bucket --prefix r3-jobs/ \
  --endpoint-url https://ceph.example.com \
  --addressing-style path \
  --request-checksum-calculation when_required
```

- `--addressing-style path`: boto3 defaults to virtual-host-style addressing, which
  CEPH typically does not support. Leaving this unset against such a backend fails
  with a **misleading** `InvalidAccessKeyId` error even when your credentials are
  correct.
- `--request-checksum-calculation when_required`: boto3 1.36+ adds integrity headers
  to uploads by default (`when_supported`). Older CEPH RGW builds reject those headers
  under SigV4 and return the same misleading `InvalidAccessKeyId` — `LIST` works but
  `PUT` fails. `when_required` restores the pre-1.36 behavior.

(The exact endpoint and bucket above are placeholders; substitute your own.)

### Listing and removing remotes

List configured remotes:

```bash
r3 remote list
```

Remove a remote's configuration:

```bash
r3 remote remove my-remote
```

`remote remove` only drops the configuration entry; it never deletes bucket objects.
To avoid orphaning archived jobs, it probes the bucket directly and **refuses** while
complete jobs still live under the prefix (fetch or remove those first). If only
residual, manifestless debris remains, it also refuses unless you pass `--force`, in
which case those leftover objects become unmanaged (no longer reachable through R3).

## Moving and fetching jobs

Move a job to a remote:

```bash
r3 move <job_id> <remote>
```

This archives the job (files only), uploads the archive and the job's metadata,
content-verifies every uploaded object, publishes a completion marker, flips the
index entry to the remote, and finally deletes the local copy. Jobs that depend on the
moved job are reported (informational — the move still proceeds). Use `--dry-run` to
see what would move, including dependents, without doing it.

`move` only moves jobs that have finished. It re-checks the job's files just before
deleting the local copy and aborts if they changed, so a still-running job is detected
rather than silently truncated.

`move` builds a temporary compressed archive of the job under the system temp
directory (Python's `tempfile`, usually `/tmp`) before uploading it. When moving large
jobs, make sure that location has enough free space for one archive-sized file. On an
HPC node where `/tmp` is small, point `tempfile` at a roomier scratch filesystem by
setting the `TMPDIR` environment variable (for example
`TMPDIR=/scratch/$USER r3 move <job_id> <remote>`).

Fetch a moved job back to local storage:

```bash
r3 fetch <job_id>
```

After a fetch the job is fully local again: its files, hash, and dependencies are
available and `r3 checkout` works. Checking out a job that is still on a remote fails
with a message telling you to `r3 fetch` it first.

## Removing a job

```bash
r3 remove <job_id>
```

`remove` deletes a job **everywhere** it is stored — the local copy and every
configured remote — and removes it from the index. It refuses if another job still
depends on it. The protocol is idempotent and retryable, so re-running it after an
interruption completes whatever a partial run left behind.

## Checking remotes

```bash
r3 remote check
```

`remote check` is a **read-only** reconciliation of the index against the configured
remote bucket(s). It changes nothing and does **not** verify local job integrity (so
it is not a full-repository check). It reports drift such as:

- resurrection-risk orphans / location disagreements,
- manifestless job prefixes,
- leftover staging manifests,
- broken/unfetchable remote index rows,
- incomplete multipart uploads.

It exits non-zero when it finds any issue, so it is convenient in scripts. To recover
index drift under correct single-writer use, rebuild from the buckets with
`r3 rebuild-index`.

## Good to know

- **Single writer per repository.** R3 assumes one mutating process per repository at
  a time. Running mutating commands concurrently (for example a batch `r3 move`
  alongside `r3 commit`, or `r3 rebuild-index` overlapping a `move`/`fetch`) is
  unsupported and carries no data-safety guarantee.
- **One owner per remote.** Each remote prefix is assumed to belong to exactly one
  repository. Pointing two repositories at the same prefix is unsupported and can let
  one repository's cleanup affect another's jobs. Give each repository its own bucket
  or prefix.
- **Back up your `r3.yaml` remote config.** The remote configuration in `r3.yaml` is
  durable metadata, not a disposable cache: it records the endpoint, bucket, and
  prefix by which R3 reaches your archived jobs. Losing it loses the pointer to those
  jobs even though the bucket objects still exist. Back it up together with anything
  you archive.
- **`r3.yaml` is R3-managed.** `r3 remote add`/`remove` rewrite `r3.yaml`
  programmatically and do not preserve comments, blank lines, or key order. Don't
  hand-format it; use the CLI to change remotes.

For the current sharp edges and scope limits, see
[Known Limitations](https://github.com/mtangemann/r3/blob/main/LIMITATIONS.md). For
non-binding future directions, see the
[Roadmap](https://github.com/mtangemann/r3/blob/main/ROADMAP.md).
