# Remote Storage Extensions: Archive Format and File List Caching

## Problem

The first iteration of remote storage (feature/remote-storage) moves job directories
to S3-compatible backends as individual files. Two limitations motivate this extension:

1. **Object count**: CEPH and some S3 setups impose limits or performance penalties on
   repositories with many small objects. A job with many output files becomes many S3
   objects.

2. **Remote job file list**: Jobs in remote storage cannot populate `job.files` because
   the files are not present locally. This breaks dependency validation and any
   application that needs to know what files a job contains.

## Design

### 1. Archive support in `S3Remote`

Archiving is an `S3Remote`-level concern. The `Remote` ABC is unchanged. Each remote
owns its storage format entirely.

#### Configuration

Two new optional fields in the remote config block in `r3.yaml`:

```yaml
remotes:
  archive:
    type: s3
    bucket: my-r3-archive
    prefix: r3/jobs/
    archive_format: tar.zst       # optional, default: null (no archiving)
    archive_frame_size: 16777216  # optional, bytes; default: 16 MiB; must be > 0
```

`archive_format` accepts `"tar.zst"` only for now. Additional formats can be added
later without interface changes. `S3Remote.from_config()` reads both new fields.
`archive_frame_size` must be a positive integer; `from_config()` raises `ValueError`
for invalid values. If `archive_format` is set but `pyzstd` is not installed, the
lazy import raises `ImportError` with the message:
`"archive_format='tar.zst' requires pyzstd. Install it with: pip install pyzstd"`.

#### Why tar.zst with seekable format

`tar.zst` with the Zstandard Seekable Format is chosen over `tar.gz` for two reasons:

- **Object count** (current goal): a single archive object per job regardless of how
  many files the job contains.
- **Future ratarmount mounting**: the Zstandard Seekable Format embeds a seek table as
  a skippable frame at the end of the archive. ratarmount can read this table in O(1)
  and jump directly to any frame without a full linear scan. By contrast, `tar.gz`
  requires ratarmount to decompress the entire archive once to build an external
  `.index.sqlite` checkpoint file (10+ hours for multi-TB archives).

A single-frame `.tar.zst` (produced by standard `zstd` or `tar -I zstd`) provides
**no** seeking benefit — ratarmount cannot seek within a single frame at all. The
seekable format with multiple frames is essential.

`archive_frame_size` controls the uncompressed size per frame (default 16 MiB). Smaller
frames give finer-grained seeking at a minor compression cost; larger frames compress
slightly better. The compression ratio difference between 4 MiB and 64 MiB frames is
modest (~1–3%) at typical zstd compression levels. 16 MiB is a good default for
research jobs where output files are often in the tens-of-MB range.

#### Dependency: `pyzstd`

`pyzstd` is used for both writing and reading archives. It provides `SeekableZstdFile`,
which implements the Zstandard Seekable Format natively. `pyzstd` is an optional
dependency — lazily imported inside the archive code paths, only required when
`archive_format` is set.

Read compatibility: the Zstandard Seekable Format uses skippable frames, which are
part of the zstd spec. Any standard zstd tool or library (`zstd`, `zstandard`, `tar
-I zstd`) can decompress a `pyzstd`-produced archive transparently — they simply ignore
the seek table. This means archived jobs are always recoverable even if `pyzstd` is
unavailable in the future. If `pyzstd` becomes unmaintained, the write side can be
switched to `zstandard` with manual frame flushing (losing the embedded seek table but
retaining multi-frame structure and read compatibility).

#### Storage format on S3

Without `archive_format`: unchanged — individual files under `{prefix}{job_id}/`.

With `archive_format = "tar.zst"`: single object at `{prefix}{job_id}.tar.zst`.

#### Behaviour changes in `S3Remote`

All four methods branch on `self.archive_format`:

- **`upload(job_id, job_path)`**: creates a temporary `.tar.zst` file using
  `tempfile.NamedTemporaryFile` (system temp directory), streams the job directory
  into it via `tarfile` + `pyzstd.SeekableZstdFile(path, 'w',
  max_frame_content_size=self.archive_frame_size)`, uploads the single file to S3,
  deletes the temp file in a `finally` block (guaranteed cleanup even on upload
  failure). Disk space required: up to 1× compressed archive size in the temp
  directory.
- **`download(job_id, destination)`**: downloads the single archive key to a temp file
  in the system temp directory via `tempfile.NamedTemporaryFile`, extracts with
  `tarfile` reading through `pyzstd.SeekableZstdFile`, deletes the temp file in a
  `finally` block. Disk space required: up to 1× compressed archive size in the temp
  directory plus the extracted job files at `destination`.
- **`exists(job_id)`**: checks for key `{prefix}{job_id}.tar.zst`.
- **`remove(job_id)`**: deletes the single key.

#### Future: streaming directly to/from S3

The current implementation uses a local temp file as an intermediate step. For very
large jobs (100 GB+), streaming directly to S3 via multipart upload (write side) and
streaming decompression from S3 (read side) would eliminate the need for local temp
space. The archive code paths are self-contained within `S3Remote`, so this
optimisation can be added later without touching the rest of the system.

---

### 2. File list caching for remote jobs

#### `cache_file_list` on `Remote`

The `Remote` base class gains a concrete attribute:

```python
class Remote(ABC):
    cache_file_list: bool = False
```

`S3Remote` overrides it to `True` as a class attribute. `FilesystemRemote` (planned)
gets `False` by default — a live shared filesystem may not be immutable, so caching
its file list could produce stale results. When `FilesystemRemote` is implemented,
per-instance override via config can be added if needed.

#### SQLite schema

The `jobs` table gains a new nullable column:

```sql
ALTER TABLE jobs ADD COLUMN files JSON;
```

The value is a JSON array of relative POSIX path strings with no leading `./`,
consistent with what `r3.utils.find_files()` returns and with the keys of `job.files`
(which includes `r3.yaml` and `metadata.yaml`), e.g.:

```json
["r3.yaml", "metadata.yaml", "output/result.pt", "output/metrics.json"]
```

`NULL` means no cached file list. This applies to all local jobs (not needed) and to
jobs on remotes where `cache_file_list = False`.

`Index.rebuild()` drops and recreates the `jobs` table, so its `CREATE TABLE`
statement must include the `files JSON` column. Rebuilt jobs are re-inserted with
`files = NULL` (they are all local, so no cache is needed).

**Known limitation**: `rebuild_index()` loses cached file lists for remote jobs, since
it only reads from local storage and cannot repopulate the `files` column for remote
jobs. This is consistent with the pre-existing limitation that `rebuild_index()` also
loses location data for remote jobs (all jobs revert to `"local"`). Repositories with
remote jobs should treat `rebuild_index()` as unsupported. This limitation is
pre-existing and out of scope here.

#### `Repository.move()` — step ordering

The updated sequence, showing where file list capture fits:

```
1. get job (local files exist)
2. capture file list: file_list = list(job.files.keys())  # while files still exist
3. remote.upload(job_id, job.path)
4. remote.exists(job_id)  # verify
5. find_dependents(job)
6. _storage.remove(job)   # local files deleted here
7. _index.set_location(job_id, remote_name)
8. if remote.cache_file_list: _index.set_file_list(job_id, file_list)
```

Step 2 must happen before step 6. Steps 7 and 8 may be combined into a single
transaction if desired.

#### `Index` changes

Two new methods:
- `set_file_list(job_id: str, paths: List[Path]) -> None` — converts each `Path` to
  a POSIX string via `.as_posix()` before JSON serialization (not `str()`, which is
  platform-dependent).
- `get_file_list(job_id: str) -> Optional[List[Path]]` — deserializes the JSON array
  of POSIX strings back to `Path` objects via `Path(s)`.

**`Index.find()` and `Index.get()`**: both must select `location` and `files` in
addition to `timestamp` and `metadata`. Updated SQL for `get()`:

```sql
SELECT timestamp, metadata, location, files FROM jobs WHERE id = ?
```

When constructing a `Job` for a remote job (the `FileNotFoundError` branch), pass
`cached_file_paths` from `json.loads(files_json)` mapped through `Path`, or `None` if
the `files` column is `NULL`. `Index.get()` gains the same `FileNotFoundError` fallback
as `find()`. When `files IS NULL` for a remote job, both `find()` and `get()` return a
`Job` with `cached_file_paths=None` (not raise) — the same as a job on a remote that
does not support file list caching. This keeps `Storage` unaware of file list caching
— the list is attached to the `Job` by `Index`, which is the component with access to
both the location and the file list.

**`Repository.fetch()`**: does not null out the `files` column after restoring a job
locally. After fetch, `location = "local"` and `storage.get()` succeeds, so `find()`
takes the non-fallback path and constructs the Job from disk — the cached `files`
column is not consulted. If the job is later moved again, step 2 of `move()` captures
a fresh file list from disk.

#### `Job.files` type change

`job.files` changes from `Mapping[Path, Path]` to `Mapping[Path, Optional[Path]]`.

| State | Keys | Values |
|---|---|---|
| Local job | relative paths (from disk) | absolute local paths |
| Remote job, file list cached | relative paths (from SQLite) | `None` |
| Remote job, no cache | — | raises (same as current) |
| Future: mounted via ratarmount | relative paths | absolute mount paths |

The `Optional[Path]` value slot is the natural place for ratarmount to plug in later:
once a job is mounted, the values can be filled with mount paths, restoring full
file access without any API change.

`job.hash()` checks `if self._cached_file_paths is not None` at the start, before
accessing `self.files`, and raises
`ValueError("Cannot compute hash of a remote job: files are not available locally")`.
Checking `_cached_file_paths` directly (rather than inspecting the values returned by
`self.files`) avoids constructing the dict unnecessarily and gives a clear error
immediately rather than an obscure `TypeError` mid-iteration.

The `Job` constructor gains `cached_file_paths: Optional[List[Path]] = None`. When
set, `job.files` returns `{path: None for path in cached_file_paths}` without
touching the filesystem.

#### `Repository.__contains__` for remote dependencies

Currently checks `target.exists()` for a `JobDependency`. With file list caching, if
the local path does not exist, fall back to the cached file list:

```python
if isinstance(resolved_item, JobDependency):
    target = self.path / "jobs" / resolved_item.job / resolved_item.source
    if target.exists():
        return True
    file_list = self._index.get_file_list(resolved_item.job)
    if file_list is not None:
        if resolved_item.source == Path("."):
            # source=Path(".") means the whole job directory; present if non-empty
            return len(file_list) > 0
        return resolved_item.source in file_list
    return False
```

`resolved_item.source` is already a `Path` object. `get_file_list()` returns
`List[Path]` reconstructed from POSIX strings, so `in` comparison between `Path`
objects is consistent. `source = Path(".")` (the default, meaning the whole job
directory) is handled separately: the job is considered present if the file list is
non-empty.

Steps 7 and 8 of `Repository.move()` are kept as separate index operations (not
combined into a single transaction) because the existing `Transaction` context manager
always commits and does not support rollback on exceptions. If step 8 (`set_file_list`)
raises after step 7 (`set_location`) has already committed, the job is correctly marked
as remote with no cached file list — the same state as `cache_file_list = False`. No
recovery logic is needed; the missing file list is a tolerable degradation.

---

## Scope

1. `S3Remote`: `archive_format` and `archive_frame_size` config fields; updated
   `from_config()`; archive upload/download/exists/remove code paths; `pyzstd`
   optional dependency
2. `Remote` base class: `cache_file_list = False` attribute
3. `S3Remote`: `cache_file_list = True` class attribute
4. `Job.files`: type change to `Mapping[Path, Optional[Path]]`; `cached_file_paths`
   constructor parameter; `job.hash()` guard
5. `Index`: `files` JSON column in `CREATE TABLE`; `set_file_list` / `get_file_list`
   methods; pass file list when constructing remote jobs in `find()` and `get()`
6. `Repository.move()`: capture and store file list (step ordering as specified above)
7. `Repository.__contains__`: fallback to cached file list for remote dependencies,
   including `source=Path(".")` case
8. `Repository.get_job_by_id()`: route through `Index.get()` instead of
   `Storage.get()` directly, so remote jobs return a `Job` with cached file paths
   rather than raising `KeyError`
9. Migration: bump format version; use `ALTER TABLE jobs ADD COLUMN files JSON` (not
   `rebuild_index()`, which would wipe remote job location data)
10. Update design doc (`docs/plans/2026-02-11-remote-storage-design.md`) to reflect
    resolved limitations

## Out of scope

- `FilesystemRemote` implementation
- ratarmount mounting integration
- Streaming upload/download to/from S3 (temp file approach is sufficient for now)
- Per-job or per-move-time archive format override
- Archive formats other than `tar.zst`
- `rebuild_index()` support for repositories with remote jobs (pre-existing limitation;
  to be fixed in the base branch by preserving remote job rows before the table drop,
  at which point the `files` column is preserved automatically)
- On-demand file list recovery from S3 for jobs without a cached list (future spec;
  promising approach: mount the archive via ratarmount and list the mountpoint, avoiding
  a full archive download)

## Testing strategy

### Archive tests (`S3Remote`)
- Upload with `archive_format = "tar.zst"`: verify single S3 object at correct key
  with `.tar.zst` extension; verify no individual file objects exist
- Download: verify extracted files match originals
- Round-trip (upload then download): file contents identical
- `exists()` and `remove()` with archive format
- Temp file is cleaned up even when upload raises
- `from_config()` raises `ValueError` for invalid `archive_frame_size`
- Without `archive_format`: existing tests unaffected

### File list caching tests
- `Repository.move()` with a `cache_file_list = True` remote: verify SQLite `files`
  column populated with correct relative paths (including `r3.yaml`, `metadata.yaml`)
- `Repository.move()` with a `cache_file_list = False` remote: verify `files` is `NULL`
- `job.files` for remote job with cached list: correct keys, all values `None`
- `job.files` for remote job without cache: raises
- `job.hash()` on a remote job with a cached file list: raises `ValueError` with clear
  message
- `Repository.__contains__` for a `JobDependency` on a remote job: `True` when source
  path is in cached list, `False` when not
- `Repository.__contains__` for a `JobDependency` with `source="."` on a remote job:
  `True` when file list is non-empty
- Migration: existing jobs get `files = NULL`, format version bumped
- `Index.rebuild()`: recreated table includes `files` column; remote jobs (if any) have
  `NULL` after rebuild

### `Index.get()` on a remote job
- `Index.get()` for a job with a remote location and a cached file list: returns a
  Job with `cached_file_paths` populated correctly
- `Index.get()` for a job with a remote location and no cached file list: returns a
  Job with `cached_file_paths = None`
- `Index.get()` for an unknown `job_id`: raises `KeyError` (existing behaviour
  preserved)

### `Repository.get_job_by_id()` for a remote job
- Returns a `Job` with `cached_file_paths` populated (via `Index.get()`)
- Does not raise `KeyError` for a remote job that exists in the index

### Regression
- All existing remote storage tests continue to pass (no archive format configured)
- All existing local job tests continue to pass (`job.files` values non-`None` for
  local jobs; `job.hash()` unaffected)
