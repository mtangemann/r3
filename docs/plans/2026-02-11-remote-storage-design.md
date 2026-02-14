# Remote Storage for R3

## Problem

R3 uses a single monolithic local repository. Large or old jobs consume disk
space that could be freed by moving them to cold storage (S3-compatible systems
like CEPH). The index and metadata should remain locally queryable.

## Design: Named Remotes

Repositories can have named remote storage locations. Jobs can be moved to a
remote to free local disk space. The SQLite index retains full metadata for all
jobs regardless of location, so queries always work.

## Configuration

Remotes are defined in the repository's `r3.yaml`:

```yaml
version: "1.0.0-beta.8"
remotes:
  archive:
    type: s3
    bucket: my-r3-archive
    prefix: r3/jobs/
    profile: ceph                              # optional boto3/AWS profile
    endpoint_url: https://ceph.example.com     # optional, for CEPH/MinIO
```

A `FilesystemRemote` type (with a `path` field) will be added later.

Management via CLI: `r3 remote add`, `r3 remote list`, `r3 remote remove`.

## On-Remote Storage Format

Each job is stored under `{prefix}{job_id}/` mirroring the local directory
structure: `r3.yaml`, `metadata.yaml`, and all job files. The format is
self-describing -- browsing the bucket directly shows recognizable job
directories.

Git bare clones (for `GitDependency`) are **not** uploaded. They stay local
since they are not a significant contributor to repository size.

## Index Changes

The `jobs` table gains a `location` column:

```sql
ALTER TABLE jobs ADD COLUMN location TEXT NOT NULL DEFAULT 'local';
```

Values: `"local"` or a remote name (e.g., `"archive"`). All metadata, timestamps,
and dependency graph entries are preserved regardless of location. `r3 find`
works identically for local and remote jobs.

## Remote Abstraction

A `Remote` base class defines the interface:

```python
class Remote(ABC):
    @abstractmethod
    def upload(self, job_id: str, job_path: Path) -> None: ...

    @abstractmethod
    def download(self, job_id: str, destination: Path) -> None: ...

    @abstractmethod
    def remove(self, job_id: str) -> None: ...

    @abstractmethod
    def exists(self, job_id: str) -> bool: ...
```

Implementations:

- **`S3Remote`**: Uses `boto3`. Supports `profile` and `endpoint_url` config
  for CEPH/MinIO compatibility. Uploads/downloads the full job directory as
  individual S3 objects under the job's prefix.
- **`FilesystemRemote`** (future): Uses `shutil.copytree`/`shutil.rmtree`.

The existing `Storage` class is **unchanged** -- it continues to handle local
jobs only. `Remote` is a parallel abstraction managed by `Repository`.

## CLI Commands

### `r3 move <job_id> <remote>`

Moves a single job to a remote:

1. Upload job directory to the remote
2. Verify upload (`remote.exists(job_id)`)
3. Remove local job files
4. Update index location to the remote name

Prints a warning if other local jobs depend on the moved job (informational,
not a blocker). Supports `--dry-run`.

Query-based batch moves are deferred -- a shell script can iterate over
`r3 find` results in the meantime.

### `r3 fetch <job_id>`

Downloads a job from its remote back to local storage:

1. Download job files from the remote
2. Place them in `jobs/{job_id}/`
3. Update index location to `"local"`

### `r3 find` changes

A `--location` filter option: `r3 find '{}' --location archive` lists jobs
on a specific remote.

## Checkout Behavior

### First iteration

When checking out a job or resolving a dependency that is archived, the
operation **fails with a clear error message**:

> Dependency {job_id} is archived on remote "archive". Run `r3 fetch {job_id}`
> to retrieve it first.

This keeps the implementation simple and behavior explicit.

### Future improvement

For archived dependencies, download files directly into the checkout directory
instead of back into the repository. This way:

- The archived job stays archived (location unchanged)
- The checkout directory is self-contained
- Cleanup is just deleting the checkout directory

## Move Safety

- Moving a job preserves its index entry (metadata, dependency graph). Queries
  and `find_dependents` continue to work.
- If other local jobs depend on the moved job, `r3 move` prints which jobs are
  affected as a warning but proceeds.
- Local files are only deleted after upload verification.

## Migration

Format version bumps from `1.0.0-beta.7` to `1.0.0-beta.8`.

A migration script at `migration/1_0_0_beta_8.py` follows the existing
pattern (click command with confirmation prompts and backup):

1. Confirm with user, verify backup exists
2. Update version in `r3.yaml`
3. Rebuild index (new schema includes `location` column, all jobs default
   to `"local"`)

No job file changes required.

## Scope for First Iteration

1. `Remote` ABC + `S3Remote` implementation
2. `location` column in index
3. `r3 move <job_id> <remote>` and `r3 fetch <job_id>`
4. `r3 remote add/list/remove`
5. Checkout fails with message for archived jobs/dependencies
6. Migration script
7. `--location` filter on `r3 find`

## Testing Strategy

Tests use `moto` to mock S3 and the existing `pyfakefs`/`pytest-mock` stack
for everything else.

### S3Remote unit tests

Test `upload`, `download`, `remove`, `exists` against a `moto`-mocked S3
bucket. Verify that the on-remote directory structure mirrors the local job
layout. Test error cases: missing jobs, upload failures, config with custom
`endpoint_url` and `profile`.

### Index tests

Extend existing index tests to cover the `location` column: default value
is `"local"` for new jobs, `set_location` updates correctly, `find` with
location filtering returns the right subset.

### Repository orchestration tests

End-to-end `move` and `fetch` using `moto` + `pyfakefs`:

- Move: job uploaded, local files removed, index location updated, job still
  findable via queries.
- Fetch: job downloaded, local files restored, index location reset to
  `"local"`.
- Move with dependents: warning printed, operation succeeds.
- Move then find: metadata queries still work for archived jobs.

### Checkout failure tests

Verify that checking out an archived job (or a job with an archived
dependency) raises an error with a message naming the job ID and remote.

### CLI tests

Test `r3 move`, `r3 fetch`, `r3 remote add/list/remove` via Click's
`CliRunner`, consistent with existing CLI test patterns.

### Migration test

Verify the migration script upgrades the version in `r3.yaml` and that the
rebuilt index includes the `location` column with `"local"` for all existing
jobs.

## Known Limitations

- Remote jobs returned by `find()` have cached metadata and timestamp but
  **no local files**. Accessing `job.files`, `job.dependencies`, or
  `job._config` on a remote job will fail or return incorrect results.
  Metadata queries are the intended use case for remote jobs. A future
  `RemoteJob` subclass with lazy file loading should address this properly.

## Future Extensions

- `RemoteJob` subclass with lazy file/config loading from remote storage
- `FilesystemRemote` backend
- `r3 copy` (upload without removing local files)
- Direct-to-checkout download for archived dependencies
- Query-based batch `r3 move`
- `r3 move` / `r3 copy` as a foundation for collaboration (shared remotes)
