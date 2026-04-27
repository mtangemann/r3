# Remote Storage Extensions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add archive format support (`tar.zst` with Zstandard Seekable Format) and file-list caching to R3's remote storage feature, addressing CEPH object-count concerns and enabling dependency validation against remote jobs.

**Architecture:** Two extensions to the existing `feature/remote-storage` branch:
1. `S3Remote` gains `archive_format` and `archive_frame_size` config; when set, uploads/downloads via a single seekable `.tar.zst` object instead of per-file objects.
2. `Job.files` becomes `Mapping[Path, Optional[Path]]` with a SQLite-cached file list for remote jobs, populated by `Repository.move()` when the remote is immutable. `__contains__` and `get_job_by_id` use the cache for remote dependency checks.

**Tech Stack:** Python 3.9–3.12, SQLite, boto3, `pyzstd` (optional, for archive support), pytest with moto for mocked S3, pyfakefs.

**Spec:** `docs/superpowers/specs/2026-03-21-remote-storage-extensions-design.md`

---

## File Structure

**Modified:**
- `pyproject.toml` — add `pyzstd` to optional dependencies
- `r3/repository.py` — bump `R3_FORMAT_VERSION`; update `move()`, `__contains__`, `get_job_by_id()`
- `r3/index.py` — add `files` column to schema; add `set_file_list`/`get_file_list`; update `find()` and `get()` to load files; update `rebuild()` to preserve `files` for remote jobs
- `r3/job.py` — add `cached_file_paths` constructor param; change `files` type; add `hash()` guard
- `r3/remote.py` — `cache_file_list` attribute; `S3Remote` archive support
- `test/test_remote.py` — archive tests, edge cases, failure modes
- `test/test_index.py` — file-list column tests
- `test/test_repository.py` — move/__contains__/get_job_by_id tests
- `test/test_job.py` — `Job.files` type, `hash()` guard, `cached_file_paths`
- `docs/plans/2026-02-11-remote-storage-design.md` — note resolved limitations

**Created:**
- `migration/1_0_0_beta_9.py` — `ALTER TABLE` to add `files` column
- `test/test_live_s3.py` — live S3-compatible smoke tests (opt-in)
- `CONTRIBUTING.md` (or appended to README) — instructions for live S3 tests

---

## Task 1: Add `pyzstd` as optional dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `pyzstd` to optional dependencies**

In `pyproject.toml`, add after the `[dependency-groups]` section:

```toml
[project.optional-dependencies]
archive = ["pyzstd~=0.16"]
```

- [ ] **Step 2: Install in dev environment**

Run: `uv sync --extra archive`
Expected: `pyzstd` installed without errors.

- [ ] **Step 3: Verify import works**

Run: `python -c "import pyzstd; print(pyzstd.__version__)"`
Expected: prints version, no ImportError.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m ":heavy_plus_sign: Add pyzstd as optional dependency for archive support"
```

---

## Task 2: Add `cache_file_list` attribute to `Remote` ABC

**Files:**
- Modify: `r3/remote.py`
- Test: `test/test_remote.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_remote.py`:

```python
def test_remote_default_cache_file_list_is_false():
    """Subclasses without explicit override should not cache file lists."""
    assert Remote.cache_file_list is False


def test_s3_remote_caches_file_list():
    """S3 storage is immutable, so S3Remote caches file lists."""
    assert S3Remote.cache_file_list is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_remote.py::test_remote_default_cache_file_list_is_false test/test_remote.py::test_s3_remote_caches_file_list -v`
Expected: FAIL with `AttributeError: type object 'Remote'/'S3Remote' has no attribute 'cache_file_list'`.

- [ ] **Step 3: Add `cache_file_list` attributes**

In `r3/remote.py`, in the `Remote` class body (right after the docstring):

```python
class Remote(ABC):
    """Abstract base class for remote storage backends."""

    cache_file_list: bool = False
    """Whether the remote's storage is immutable enough to cache the file list
    in the index. Subclasses that store immutable copies (S3) override this
    to True; subclasses pointing at potentially-mutable storage (live shared
    filesystems) leave it False."""
```

In the `S3Remote` class body (right after the docstring):

```python
class S3Remote(Remote):
    """Remote storage backend using Amazon S3."""

    cache_file_list: bool = True
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_remote.py::test_remote_default_cache_file_list_is_false test/test_remote.py::test_s3_remote_caches_file_list -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Add cache_file_list attribute to Remote/S3Remote"
```

---

## Task 3: Add `files` column to Index schema

**Files:**
- Modify: `r3/index.py:30-44` (CREATE TABLE), `r3/index.py:107-123` (rebuild loop), `r3/index.py:74-76` (rebuild INSERT)
- Test: `test/test_index.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_index.py`:

```python
def test_index_rebuild_creates_files_column(storage: Storage):
    """The rebuilt schema must include the files column."""
    index = Index(storage)
    index.rebuild()
    import sqlite3
    conn = sqlite3.connect(str(storage.root / "index.sqlite"))
    cursor = conn.execute("PRAGMA table_info(jobs)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()
    assert "files" in columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test/test_index.py::test_index_rebuild_creates_files_column -v`
Expected: FAIL — column missing.

- [ ] **Step 3: Add `files` column to CREATE TABLE**

In `r3/index.py`, change the `CREATE TABLE jobs` block in `rebuild()`:

```python
transaction.execute(
    """
    CREATE TABLE jobs (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        metadata JSON NOT NULL,
        location TEXT NOT NULL DEFAULT 'local',
        files JSON
    )
    """
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test/test_index.py::test_index_rebuild_creates_files_column -v`
Expected: PASS.

- [ ] **Step 5: Run full test suite for regressions**

Run: `python -m pytest`
Expected: all 195+ tests pass (the new column is nullable; existing inserts unaffected).

- [ ] **Step 6: Commit**

```bash
git add r3/index.py test/test_index.py
git commit -m ":sparkles: Add files column to job index schema"
```

---

## Task 4: Update `Index.rebuild()` to preserve `files` for remote jobs

**Files:**
- Modify: `r3/index.py:30-90` (rebuild method)
- Test: `test/test_repository.py` (extend existing test)

- [ ] **Step 1: Write failing test**

Append to `test/test_repository.py`:

```python
def test_rebuild_index_preserves_remote_job_file_list(
    repository_with_remote: Repository,
) -> None:
    """The cached file list for remote jobs must survive rebuild_index."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    file_list_before = repository_with_remote._index.get_file_list(job.id)
    assert file_list_before is not None
    assert len(file_list_before) > 0

    repository_with_remote.rebuild_index()

    file_list_after = repository_with_remote._index.get_file_list(job.id)
    assert file_list_after == file_list_before
```

(Note: this test will only fully exercise once Task 7 wires `move()` to populate the file list. It will fail correctly until then for the right reason. Run it after Task 7.)

- [ ] **Step 2: Update `rebuild()` to preserve `files`**

In `r3/index.py`, in the `rebuild()` method, change the SELECT for remote jobs:

```python
transaction.execute(
    "SELECT id, timestamp, metadata, location, files FROM jobs"
    " WHERE location != 'local'"
)
remote_jobs = transaction.fetchall()
```

And the INSERT for re-inserting remote jobs:

```python
transaction.executemany(
    "INSERT INTO jobs (id, timestamp, metadata, location, files)"
    " VALUES (?, ?, ?, ?, ?)",
    remote_jobs,
)
```

- [ ] **Step 3: Run full test suite for regressions**

Run: `python -m pytest test/test_repository.py::test_rebuild_index_preserves_remote_jobs -v`
Expected: PASS (existing test still works; the new column comes through as `NULL`).

- [ ] **Step 4: Commit**

```bash
git add r3/index.py test/test_repository.py
git commit -m ":sparkles: Preserve files column for remote jobs in rebuild_index"
```

---

## Task 5: Add `Index.set_file_list` / `get_file_list`

**Files:**
- Modify: `r3/index.py` (add methods near `set_location`/`get_location`)
- Test: `test/test_index.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_index.py`:

```python
def test_index_set_and_get_file_list(storage: Storage):
    """File list round-trips through SQLite as a JSON array of POSIX strings."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("output/result.pt")]
    index.set_file_list(job.id, paths)
    result = index.get_file_list(job.id)
    assert result == paths


def test_index_get_file_list_returns_none_when_unset(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    assert index.get_file_list(job.id) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_index.py::test_index_set_and_get_file_list test/test_index.py::test_index_get_file_list_returns_none_when_unset -v`
Expected: FAIL — methods don't exist.

- [ ] **Step 3: Add the methods**

In `r3/index.py`, after `get_location()`:

```python
def set_file_list(self, job_id: str, paths: List[Path]) -> None:
    """Sets the cached file list for a job.

    Paths are stored as POSIX strings in a JSON array, regardless of
    platform, so the cached list is portable.
    """
    files_json = json.dumps([p.as_posix() for p in paths])
    with Transaction(self._path) as transaction:
        transaction.execute(
            "UPDATE jobs SET files = ? WHERE id = ?",
            (files_json, job_id),
        )

def get_file_list(self, job_id: str) -> Optional[List[Path]]:
    """Returns the cached file list for a job, or None if unset."""
    with Transaction(self._path) as transaction:
        transaction.execute(
            "SELECT files FROM jobs WHERE id = ?", (job_id,)
        )
        result = transaction.fetchone()
    if result is None or result[0] is None:
        return None
    return [Path(s) for s in json.loads(result[0])]
```

Make sure `from pathlib import Path` is already imported (it is at line 5).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_index.py::test_index_set_and_get_file_list test/test_index.py::test_index_get_file_list_returns_none_when_unset -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add r3/index.py test/test_index.py
git commit -m ":sparkles: Add Index.set_file_list and get_file_list"
```

---

## Task 6: Update `Job` to accept `cached_file_paths`

**Files:**
- Modify: `r3/job.py:14-40` (constructor), `r3/job.py:115-130` (`files` property), `r3/job.py:164-190` (`hash` method)
- Test: `test/test_job.py`

- [ ] **Step 1: Write failing tests**

Append to `test/test_job.py`:

```python
def test_job_with_cached_file_paths_returns_none_values():
    """A job constructed with cached_file_paths returns those paths with None values."""
    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("output/result.pt")]
    job = Job("/nonexistent/path", id="abc", cached_file_paths=paths)
    files = job.files
    assert set(files.keys()) == set(paths)
    assert all(v is None for v in files.values())


def test_job_hash_raises_for_remote_job():
    """Computing hash on a job with cached_file_paths raises ValueError."""
    paths = [Path("r3.yaml"), Path("output/result.pt")]
    job = Job("/nonexistent/path", id="abc", cached_file_paths=paths)
    with pytest.raises(ValueError, match="remote job"):
        job.hash()
```

(Make sure `from pathlib import Path` and `import pytest` are at the top of the file.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_job.py::test_job_with_cached_file_paths_returns_none_values test/test_job.py::test_job_hash_raises_for_remote_job -v`
Expected: FAIL — constructor doesn't accept `cached_file_paths`.

- [ ] **Step 3: Update `Job.__init__`**

In `r3/job.py`, replace the constructor signature and body:

```python
def __init__(
    self,
    path: Union[str, os.PathLike],
    id: Optional[str] = None,
    cached_timestamp: Optional[datetime] = None,
    cached_metadata: Optional[Dict[str, Any]] = None,
    cached_file_paths: Optional[Sequence[Path]] = None,
) -> None:
    """Initializes a job instance.

    Parameters:
        path: Path to the job's root directory.
        id: Job id for committed jobs. This is set automatically for jobs
            retrieved from a repository.
        cached_timestamp: Pre-loaded timestamp from the index.
        cached_metadata: Pre-loaded metadata from the index.
        cached_file_paths: Pre-loaded file path list from the index. Used for
            remote jobs whose files are not available locally; when set,
            `job.files` returns these paths with None values.
    """
    self._path = Path(path).absolute()
    self.id = id

    self._metadata: Optional[Dict[str, Any]] = cached_metadata
    self._metadata_from_cache = cached_metadata is not None
    self._timestamp = cached_timestamp
    self._cached_file_paths: Optional[Sequence[Path]] = cached_file_paths
    self._files: Optional[Dict[Path, Optional[Path]]] = None
    self.__config: Optional[Dict[str, Any]] = None
    self._dependencies: Optional[Sequence["Dependency"]] = None
    self._hash: Optional[str] = None
```

- [ ] **Step 4: Update `Job.files` property**

Replace the existing `files` property:

```python
@property
def files(self) -> Mapping[Path, Optional[Path]]:
    """Files belonging to this job.

    For local jobs, values are absolute paths to the file on disk. For
    remote jobs constructed with `cached_file_paths`, the keys are the
    relative paths and the values are `None` (files not available
    locally).
    """
    if self._files is None:
        if self._cached_file_paths is not None:
            self._files = {p: None for p in self._cached_file_paths}
        else:
            ignore = self._config.get("ignore", [])
            for dependency in self.dependencies:
                ignore.append(f"/{dependency.destination}")
            self._files = {
                file: (self.path / file).absolute()
                for file in r3.utils.find_files(self.path, ignore)
            }
    return self._files
```

- [ ] **Step 5: Update `Job.hash()`**

In the `hash()` method, add a guard at the top (before the `if self._hash is None or recompute:` block):

```python
def hash(self, recompute: bool = False) -> str:
    """Returns the hash of this job."""
    if self._cached_file_paths is not None:
        raise ValueError(
            "Cannot compute hash of a remote job: files are not available locally"
        )
    if self._hash is None or recompute:
        # ... existing body unchanged ...
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest test/test_job.py -v`
Expected: PASS for new tests; existing tests pass (cached_file_paths defaults to None).

- [ ] **Step 7: Run full test suite for regressions**

Run: `python -m pytest`
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add r3/job.py test/test_job.py
git commit -m ":sparkles: Add cached_file_paths support to Job; update files type"
```

---

## Task 7: Update `Repository.move()` to capture file list

**Files:**
- Modify: `r3/repository.py:290-323` (move method)
- Test: `test/test_repository.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_repository.py`:

```python
def test_move_populates_file_list_when_remote_caches(
    repository_with_remote: Repository,
) -> None:
    """When the remote sets cache_file_list=True, move stores the file list."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    expected_files = sorted(job.files.keys())

    repository_with_remote.move(job.id, "archive")

    cached = repository_with_remote._index.get_file_list(job.id)
    assert cached is not None
    assert sorted(cached) == expected_files


def test_move_skips_file_list_when_remote_does_not_cache(
    repository_with_remote: Repository,
) -> None:
    """When the remote sets cache_file_list=False, move does not store a file list."""
    repository_with_remote.remotes["archive"].cache_file_list = False
    try:
        job = get_dummy_job("base")
        job = repository_with_remote.commit(job)
        assert job.id is not None

        repository_with_remote.move(job.id, "archive")
        assert repository_with_remote._index.get_file_list(job.id) is None
    finally:
        repository_with_remote.remotes["archive"].cache_file_list = True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_repository.py::test_move_populates_file_list_when_remote_caches test/test_repository.py::test_move_skips_file_list_when_remote_does_not_cache -v`
Expected: FAIL — `move()` does not yet populate `files`.

- [ ] **Step 3: Update `Repository.move()`**

In `r3/repository.py`, replace the `move()` method body. Capture the file list **before** local removal, and call `set_file_list` after `set_location`:

```python
def move(self, job_id: str, remote_name: str) -> Set[Job]:
    """Moves a job to a remote storage backend. ... (docstring unchanged)"""
    if remote_name not in self._remotes:
        raise ValueError(f"Unknown remote: {remote_name}")

    remote = self._remotes[remote_name]
    job = self.get_job_by_id(job_id)

    file_list: Optional[List[Path]] = None
    if remote.cache_file_list:
        file_list = list(job.files.keys())

    remote.upload(job_id, job.path)

    if not remote.exists(job_id):
        raise RuntimeError(f"Upload verification failed for job {job_id}")

    dependents = self._index.find_dependents(job)
    self._storage.remove(job)
    self._index.set_location(job_id, remote_name)

    if file_list is not None:
        self._index.set_file_list(job_id, file_list)

    return dependents
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_repository.py::test_move_populates_file_list_when_remote_caches test/test_repository.py::test_move_skips_file_list_when_remote_does_not_cache -v`
Expected: PASS.

- [ ] **Step 5: Run rebuild test (Task 4) to confirm now end-to-end**

Run: `python -m pytest test/test_repository.py::test_rebuild_index_preserves_remote_job_file_list -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add r3/repository.py test/test_repository.py
git commit -m ":sparkles: Capture file list during Repository.move when remote caches"
```

---

## Task 8: Update `Index.find()` and `Index.get()` to load file list

**Files:**
- Modify: `r3/index.py:134-156` (`get` method), `r3/index.py:229-277` (`find` method)
- Test: `test/test_index.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_index.py`:

```python
def test_index_find_returns_remote_job_with_cached_file_paths(
    storage: Storage, mocker: MockerFixture,
):
    """When find() returns a remote job, its cached_file_paths come from the index."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    # Simulate the move: set location to remote and store a file list
    index.set_location(job.id, "archive")
    paths = [Path("r3.yaml"), Path("metadata.yaml"), Path("run.py")]
    index.set_file_list(job.id, paths)

    # Force the FileNotFoundError fallback by removing the local files
    import shutil
    shutil.rmtree(storage.root / "jobs" / job.id)

    results = index.find({"tags": "test"})
    assert len(results) == 1
    found_job = results[0]
    assert set(found_job.files.keys()) == set(paths)
    assert all(v is None for v in found_job.files.values())


def test_index_get_returns_remote_job_with_cached_file_paths(
    storage: Storage,
):
    """Index.get() also applies the FileNotFoundError fallback."""
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.set_location(job.id, "archive")
    paths = [Path("r3.yaml"), Path("run.py")]
    index.set_file_list(job.id, paths)

    import shutil
    shutil.rmtree(storage.root / "jobs" / job.id)

    found = index.get(job.id)
    assert set(found.files.keys()) == set(paths)


def test_index_get_unknown_id_raises_keyerror(storage: Storage):
    index = Index(storage)
    with pytest.raises(KeyError):
        index.get("nonexistent-id")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_index.py::test_index_find_returns_remote_job_with_cached_file_paths test/test_index.py::test_index_get_returns_remote_job_with_cached_file_paths test/test_index.py::test_index_get_unknown_id_raises_keyerror -v`
Expected: FAIL — neither find nor get pass cached_file_paths; get has no FileNotFoundError fallback.

- [ ] **Step 3: Update `Index.find()`**

In `r3/index.py`, update the `find()` method's SELECT and the FileNotFoundError fallback:

```python
def find(
    self,
    query: Dict[str, Any],
    latest: bool = False,
    location: Optional[str] = None,
) -> List[Job]:
    """Finds jobs by tags. ... (docstring unchanged)"""
    sql_query = (
        f"SELECT id, timestamp, metadata, files FROM jobs WHERE "
        f"{mongo_to_sql(query)}"
    )
    if location is not None:
        sql_query += f" AND location = '{location}'"
    if latest:
        sql_query += " ORDER BY timestamp DESC LIMIT 1"

    with Transaction(self._path) as transaction:
        transaction.execute(sql_query)
        results = transaction.fetchall()

    jobs = []
    for result in results:
        job_id = result[0]
        cached_timestamp = datetime.fromisoformat(result[1])
        cached_metadata = json.loads(result[2])
        cached_file_paths: Optional[List[Path]] = None
        if result[3] is not None:
            cached_file_paths = [Path(s) for s in json.loads(result[3])]
        try:
            jobs.append(
                self.storage.get(job_id, cached_timestamp, cached_metadata)
            )
        except FileNotFoundError:
            # Job is on a remote; construct from cached data including file list.
            job = Job(
                self.storage.root / "jobs" / job_id,
                job_id,
                cached_timestamp=cached_timestamp,
                cached_metadata=cached_metadata,
                cached_file_paths=cached_file_paths,
            )
            jobs.append(job)
    return jobs
```

- [ ] **Step 4: Update `Index.get()`**

Replace the body of `get()`:

```python
def get(self, job_id: str) -> Job:
    """Gets a job by ID. ... (docstring unchanged)"""
    with Transaction(self._path) as transaction:
        transaction.execute(
            "SELECT timestamp, metadata, files FROM jobs WHERE id = ?",
            (job_id,),
        )
        result = transaction.fetchone()

    if result is None:
        raise KeyError(f"Job not found: {job_id}")

    cached_timestamp = datetime.fromisoformat(result[0])
    cached_metadata = json.loads(result[1])
    cached_file_paths: Optional[List[Path]] = None
    if result[2] is not None:
        cached_file_paths = [Path(s) for s in json.loads(result[2])]

    try:
        return self.storage.get(job_id, cached_timestamp, cached_metadata)
    except FileNotFoundError:
        return Job(
            self.storage.root / "jobs" / job_id,
            job_id,
            cached_timestamp=cached_timestamp,
            cached_metadata=cached_metadata,
            cached_file_paths=cached_file_paths,
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest test/test_index.py -v`
Expected: PASS for new and existing tests.

- [ ] **Step 6: Commit**

```bash
git add r3/index.py test/test_index.py
git commit -m ":sparkles: Index.find/get pass cached_file_paths for remote jobs"
```

---

## Task 9: Route `Repository.get_job_by_id()` through `Index.get()`

**Files:**
- Modify: `r3/repository.py:242-257` (get_job_by_id method)
- Test: `test/test_repository.py`

- [ ] **Step 1: Audit existing callers**

Run: `grep -rn "get_job_by_id\|repository\[" r3/ test/ migration/ | grep -v test_repository`
Review whether any caller relies on `KeyError` for remote jobs. (Spec note: this is a behaviour change — remote jobs were previously not retrievable.)

- [ ] **Step 2: Write failing test**

Append to `test/test_repository.py`:

```python
def test_get_job_by_id_returns_remote_job(
    repository_with_remote: Repository,
) -> None:
    """A remote job is retrievable by ID with its file list populated."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None
    expected_files = sorted(job.files.keys())

    repository_with_remote.move(job.id, "archive")

    found = repository_with_remote.get_job_by_id(job.id)
    assert sorted(found.files.keys()) == expected_files


def test_get_job_by_id_unknown_raises_keyerror(repository: Repository) -> None:
    with pytest.raises(KeyError):
        repository.get_job_by_id("nonexistent-id")
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest test/test_repository.py::test_get_job_by_id_returns_remote_job test/test_repository.py::test_get_job_by_id_unknown_raises_keyerror -v`
Expected: FAIL — get_job_by_id raises KeyError for remote jobs.

- [ ] **Step 4: Update `get_job_by_id` to use `Index.get`**

In `r3/repository.py`:

```python
def get_job_by_id(self, job_id: str):
    """Returns the job with the given ID.

    For remote jobs, returns a Job with cached_file_paths populated from the
    index (no local files). For unknown IDs, raises KeyError.
    """
    return self._index.get(job_id)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest test/test_repository.py -v`
Expected: PASS.

- [ ] **Step 6: Run full suite for regressions**

Run: `python -m pytest`
Expected: all pass. If `move()` calls `self.get_job_by_id(job_id)` on a local job, this still works because `Index.get()` falls back to `Storage.get()` only on `FileNotFoundError`.

- [ ] **Step 7: Commit**

```bash
git add r3/repository.py test/test_repository.py
git commit -m ":sparkles: Route Repository.get_job_by_id through Index.get"
```

---

## Task 10: Add `__contains__` fallback for remote dependencies

**Files:**
- Modify: `r3/repository.py:106-148` (__contains__ method)
- Test: `test/test_repository.py`

- [ ] **Step 1: Write failing tests**

Append to `test/test_repository.py`:

```python
def test_contains_remote_job_dependency_with_path_in_file_list(
    repository_with_remote: Repository,
) -> None:
    """A JobDependency on a remote job is contained iff source is in cached files."""
    base_job = get_dummy_job("base")
    base_job = repository_with_remote.commit(base_job)
    assert base_job.id is not None
    repository_with_remote.move(base_job.id, "archive")

    dep_present = JobDependency(
        destination="dest", job=base_job.id, source=Path("run.py")
    )
    dep_absent = JobDependency(
        destination="dest", job=base_job.id, source=Path("does-not-exist.txt")
    )

    assert dep_present in repository_with_remote
    assert dep_absent not in repository_with_remote


def test_contains_remote_job_dependency_with_default_source(
    repository_with_remote: Repository,
) -> None:
    """A JobDependency with source=Path('.') on a remote job is contained when file list non-empty."""
    base_job = get_dummy_job("base")
    base_job = repository_with_remote.commit(base_job)
    assert base_job.id is not None
    repository_with_remote.move(base_job.id, "archive")

    dep = JobDependency(destination="dest", job=base_job.id)
    assert dep in repository_with_remote
```

(Make sure `from r3.job import JobDependency` is imported.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_repository.py::test_contains_remote_job_dependency_with_path_in_file_list test/test_repository.py::test_contains_remote_job_dependency_with_default_source -v`
Expected: FAIL — currently `__contains__` only checks the local filesystem.

- [ ] **Step 3: Update `__contains__`**

In `r3/repository.py`, in the `__contains__` method, replace the `if isinstance(resolved_item, JobDependency):` block:

```python
if isinstance(resolved_item, JobDependency):
    target = self.path / "jobs" / resolved_item.job / resolved_item.source
    if target.exists():
        return True
    file_list = self._index.get_file_list(resolved_item.job)
    if file_list is not None:
        if resolved_item.source == Path("."):
            return len(file_list) > 0
        return resolved_item.source in file_list
    return False
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_repository.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add r3/repository.py test/test_repository.py
git commit -m ":sparkles: Repository.__contains__ falls back to cached file list"
```

---

## Task 11: Add `archive_format` / `archive_frame_size` config to `S3Remote`

**Files:**
- Modify: `r3/remote.py:75-128` (`S3Remote.__init__` and `from_config`)
- Test: `test/test_remote.py`

- [ ] **Step 1: Write failing tests**

Append to `test/test_remote.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_remote.py -v -k archive`
Expected: FAIL — fields don't exist.

- [ ] **Step 3: Update `S3Remote.__init__`**

In `r3/remote.py`:

```python
class S3Remote(Remote):
    """Remote storage backend using Amazon S3."""

    cache_file_list: bool = True

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        archive_format: Optional[str] = None,
        archive_frame_size: int = 16 * 1024 * 1024,
    ) -> None:
        """Initializes an S3 remote.

        Parameters:
            bucket: The S3 bucket name.
            prefix: The prefix for all S3 keys. Defaults to "".
            profile: The AWS profile name. Defaults to None.
            endpoint_url: The S3 endpoint URL. Defaults to None.
            archive_format: Optional archive format. If "tar.zst", jobs are
                stored as a single seekable .tar.zst object instead of
                individual files. Defaults to None (no archiving).
            archive_frame_size: Uncompressed frame size in bytes for the
                seekable zstd archive. Smaller frames give finer-grained
                random access at a small compression cost. Defaults to
                16 MiB.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.profile = profile
        self.endpoint_url = endpoint_url
        self.archive_format = archive_format
        self.archive_frame_size = archive_frame_size

        self._client_instance: Any = None
```

- [ ] **Step 4: Update `S3Remote.from_config`**

```python
@staticmethod
def from_config(config: Dict[str, Any]) -> "S3Remote":
    """Creates an S3 remote from a configuration dictionary."""
    archive_format = config.get("archive_format")
    if archive_format is not None and archive_format != "tar.zst":
        raise ValueError(
            f"Unsupported archive_format: {archive_format!r}. "
            f"Only 'tar.zst' is supported."
        )

    archive_frame_size = config.get("archive_frame_size", 16 * 1024 * 1024)
    if not isinstance(archive_frame_size, int) or archive_frame_size <= 0:
        raise ValueError(
            f"archive_frame_size must be a positive integer; "
            f"got {archive_frame_size!r}"
        )

    return S3Remote(
        bucket=config["bucket"],
        prefix=config.get("prefix", ""),
        profile=config.get("profile"),
        endpoint_url=config.get("endpoint_url"),
        archive_format=archive_format,
        archive_frame_size=archive_frame_size,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest test/test_remote.py -v`
Expected: PASS for new tests; existing tests still pass (defaults preserved).

- [ ] **Step 6: Commit**

```bash
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Add archive_format/archive_frame_size config to S3Remote"
```

---

## Task 12: Implement `S3Remote.upload` archive branch

**Files:**
- Modify: `r3/remote.py:134-146` (upload method)
- Test: `test/test_remote.py`

- [ ] **Step 1: Write failing tests**

Append to `test/test_remote.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test/test_remote.py::test_s3_remote_upload_archive_creates_single_object -v`
Expected: FAIL — currently uploads individual files.

- [ ] **Step 3: Add archive helpers and update `upload`**

At the top of `r3/remote.py`, add:

```python
import tarfile
import tempfile
```

Add a helper method to `S3Remote`:

```python
def _import_pyzstd(self):
    """Lazily imports pyzstd with a friendly error message."""
    try:
        import pyzstd
    except ImportError as e:
        raise ImportError(
            "archive_format='tar.zst' requires pyzstd. "
            "Install it with: pip install pyzstd"
        ) from e
    return pyzstd

def _archive_key(self, job_id: str) -> str:
    """Returns the S3 key for a job's archive."""
    return f"{self.prefix}{job_id}.tar.zst"
```

Replace `upload()`:

```python
def upload(self, job_id: str, job_path: Path) -> None:
    """Uploads a job directory to S3.

    With archive_format='tar.zst', creates a single seekable .tar.zst
    object. Without archive_format, uploads individual files.
    """
    if self.archive_format == "tar.zst":
        pyzstd = self._import_pyzstd()
        tmp = tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            with pyzstd.SeekableZstdFile(
                str(tmp_path),
                "w",
                max_frame_content_size=self.archive_frame_size,
            ) as zfh:
                with tarfile.open(fileobj=zfh, mode="w|") as tar:
                    tar.add(str(job_path), arcname=".")
            self._client.upload_file(
                str(tmp_path), self.bucket, self._archive_key(job_id)
            )
        finally:
            tmp_path.unlink(missing_ok=True)
    else:
        for root, _dirs, files in os.walk(job_path):
            for filename in files:
                local_path = Path(root) / filename
                relative_path = local_path.relative_to(job_path)
                s3_key = f"{self._job_prefix(job_id)}{relative_path}"
                self._client.upload_file(str(local_path), self.bucket, s3_key)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test/test_remote.py::test_s3_remote_upload_archive_creates_single_object -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Implement archive upload in S3Remote"
```

---

## Task 13: Implement `S3Remote.download` archive branch

**Files:**
- Modify: `r3/remote.py:148-173` (download method)
- Test: `test/test_remote.py`

- [ ] **Step 1: Write failing test**

Append to `test/test_remote.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test/test_remote.py::test_s3_remote_archive_round_trip -v`
Expected: FAIL — download still uses per-file path.

- [ ] **Step 3: Update `download()`**

```python
def download(self, job_id: str, destination: Path) -> None:
    """Downloads a job from S3."""
    if not self.exists(job_id):
        raise FileNotFoundError(f"Job not found on remote: {job_id}")

    if self.archive_format == "tar.zst":
        pyzstd = self._import_pyzstd()
        tmp = tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            self._client.download_file(
                self.bucket, self._archive_key(job_id), str(tmp_path)
            )
            destination.mkdir(parents=True, exist_ok=True)
            with pyzstd.SeekableZstdFile(str(tmp_path), "r") as zfh:
                with tarfile.open(fileobj=zfh, mode="r|") as tar:
                    tar.extractall(path=str(destination))
            # tar.add(arcname=".") creates a "./" prefix; flatten if present
            inner = destination / "."
            if inner.exists() and inner.is_dir():
                for item in inner.iterdir():
                    item.rename(destination / item.name)
                inner.rmdir()
        finally:
            tmp_path.unlink(missing_ok=True)
    else:
        prefix = self._job_prefix(job_id)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]
                relative_path = s3_key[len(prefix):]
                local_path = destination / relative_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                self._client.download_file(self.bucket, s3_key, str(local_path))
```

(Note: `tar.add(arcname=".")` on a directory inserts entries as `./file`. Verify behaviour empirically — adjust the flattening logic if entries are stored without the `./` prefix on your platform.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test/test_remote.py::test_s3_remote_archive_round_trip -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Implement archive download in S3Remote"
```

---

## Task 14: Implement `S3Remote.exists` and `remove` archive branches

**Files:**
- Modify: `r3/remote.py:175-206` (exists, remove)
- Test: `test/test_remote.py`

- [ ] **Step 1: Write failing tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_remote.py -v -k "archive_exists or archive_remove"`
Expected: FAIL.

- [ ] **Step 3: Update `exists()`**

```python
def exists(self, job_id: str) -> bool:
    """Checks whether a job exists on S3."""
    if self.archive_format == "tar.zst":
        try:
            self._client.head_object(
                Bucket=self.bucket, Key=self._archive_key(job_id)
            )
            return True
        except self._client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
    prefix = self._job_prefix(job_id)
    response = self._client.list_objects_v2(
        Bucket=self.bucket, Prefix=prefix, MaxKeys=1
    )
    return response.get("KeyCount", 0) > 0
```

- [ ] **Step 4: Update `remove()`**

```python
def remove(self, job_id: str) -> None:
    """Removes a job from S3."""
    if self.archive_format == "tar.zst":
        self._client.delete_object(
            Bucket=self.bucket, Key=self._archive_key(job_id)
        )
        return
    prefix = self._job_prefix(job_id)
    paginator = self._client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if contents:
            delete_objects = [{"Key": obj["Key"]} for obj in contents]
            self._client.delete_objects(
                Bucket=self.bucket, Delete={"Objects": delete_objects},
            )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest test/test_remote.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Implement archive exists/remove in S3Remote"
```

---

## Task 15: Verify temp file cleanup on upload failure

**Files:**
- Test: `test/test_remote.py`

- [ ] **Step 1: Write test**

```python
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
```

- [ ] **Step 2: Run test to verify it passes**

Run: `python -m pytest test/test_remote.py::test_s3_remote_archive_upload_cleans_temp_on_failure -v`
Expected: PASS (the `finally` block in Task 12 already handles this).

- [ ] **Step 3: Commit**

```bash
git add test/test_remote.py
git commit -m ":white_check_mark: Verify archive temp file cleanup on upload failure"
```

---

## Task 16: Add edge-case tests for archive

**Files:**
- Test: `test/test_remote.py`

- [ ] **Step 1: Write tests**

```python
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
```

- [ ] **Step 2: Run tests**

Run: `python -m pytest test/test_remote.py -v -k "empty_job or deep_nested or special_characters"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/test_remote.py
git commit -m ":white_check_mark: Edge-case tests for archive (empty, deep, special chars)"
```

---

## Task 17: Add re-move idempotency test

**Files:**
- Test: `test/test_repository.py`

- [ ] **Step 1: Write test**

```python
def test_repository_re_move_after_fetch_preserves_file_list(
    repository_with_remote: Repository,
) -> None:
    """move → fetch → move: file list is captured fresh each time."""
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None
    expected = sorted(job.files.keys())

    repository_with_remote.move(job.id, "archive")
    first = sorted(repository_with_remote._index.get_file_list(job.id))
    assert first == expected

    repository_with_remote.fetch(job.id)
    repository_with_remote.move(job.id, "archive")
    second = sorted(repository_with_remote._index.get_file_list(job.id))
    assert second == expected
```

- [ ] **Step 2: Run test to verify it passes**

Run: `python -m pytest test/test_repository.py::test_repository_re_move_after_fetch_preserves_file_list -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/test_repository.py
git commit -m ":white_check_mark: Test re-move idempotency for file list capture"
```

---

## Task 18: Add corrupted archive test

**Files:**
- Test: `test/test_remote.py`

- [ ] **Step 1: Write test**

```python
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
    with pytest.raises(Exception):
        # Specific exception type depends on pyzstd; just verify it raises.
        s3_remote_archive.download("corrupted-id", download_path)
```

- [ ] **Step 2: Run test to verify it passes**

Run: `python -m pytest test/test_remote.py::test_s3_remote_archive_corrupted_download_raises -v`
Expected: PASS — pyzstd will raise on the bad magic.

- [ ] **Step 3: Commit**

```bash
git add test/test_remote.py
git commit -m ":white_check_mark: Test corrupted archive raises on download"
```

---

## Task 19: Migration script (1.0.0-beta.8 → 1.0.0-beta.9)

**Files:**
- Create: `migration/1_0_0_beta_9.py`
- Modify: `r3/repository.py:30` (R3_FORMAT_VERSION constant)
- Test: `test/test_repository.py` (or new `test/test_migration_beta_9.py`)

- [ ] **Step 1: Write failing test**

Append to `test/test_repository.py`:

```python
def test_format_version_is_beta_9() -> None:
    from r3.repository import R3_FORMAT_VERSION
    assert R3_FORMAT_VERSION == "1.0.0-beta.9"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test/test_repository.py::test_format_version_is_beta_9 -v`
Expected: FAIL — version still beta.8.

- [ ] **Step 3: Bump version**

In `r3/repository.py:30`:

```python
R3_FORMAT_VERSION = "1.0.0-beta.9"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test/test_repository.py::test_format_version_is_beta_9 -v`
Expected: PASS.

- [ ] **Step 5: Create migration script**

Create `migration/1_0_0_beta_9.py`:

```python
#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.8 to 1.0.0-beta.9.

Adds the 'files' column to the index for caching remote-job file lists.
Existing rows get NULL (no cached file list, which behaves the same as
before this version).
"""

import sqlite3
from pathlib import Path

import click
import yaml

OLD_VERSION = "1.0.0-beta.8"
NEW_VERSION = "1.0.0-beta.9"


@click.command()
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def migrate(repository_path: Path) -> None:
    if not (repository_path / "r3.yaml").exists():
        click.echo("This is not a valid R3 repository.")
        return

    with open(repository_path / "r3.yaml") as file:
        config = yaml.safe_load(file)
    if config["version"] != OLD_VERSION:
        click.echo(f"This repository is not at version {OLD_VERSION}.")
        return

    click.echo("This script is migrating the following R3 repository:")
    click.echo(f"  {repository_path} ({OLD_VERSION} -> {NEW_VERSION})")
    click.echo()
    click.echo("Changes:")
    click.echo("  - Update repository format version")
    click.echo("  - Add 'files' column to index (existing rows get NULL)")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)

    click.echo("Adding 'files' column to index...")
    index_path = repository_path / "index.sqlite"
    if index_path.exists():
        conn = sqlite3.connect(str(index_path))
        # SQLite ALTER TABLE ADD COLUMN is idempotent-ish: it raises
        # OperationalError if the column already exists. Catch and continue.
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN files JSON")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                click.echo("  (column already exists, skipping)")
            else:
                raise
        finally:
            conn.close()
    else:
        click.echo("  (no index file; will be created on next access)")

    click.echo("Done.")
    click.echo()
    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
```

- [ ] **Step 6: Add migration test**

Append to `test/test_repository.py`:

```python
def test_migration_beta_9_adds_files_column(tmp_path: Path) -> None:
    """The migration script bumps version and adds the files column via ALTER TABLE."""
    import sqlite3
    import subprocess
    import sys

    # Set up a fake beta.8 repository: r3.yaml + a beta.8-shaped index.
    repo_path = tmp_path / "old-repo"
    repo_path.mkdir()
    (repo_path / "r3.yaml").write_text("version: 1.0.0-beta.8\n")

    # Create a beta.8 index with the four-column schema.
    conn = sqlite3.connect(str(repo_path / "index.sqlite"))
    conn.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            metadata JSON NOT NULL,
            location TEXT NOT NULL DEFAULT 'local'
        )
        """
    )
    conn.execute(
        "INSERT INTO jobs (id, timestamp, metadata) VALUES (?, ?, ?)",
        ("test-id", "2026-01-01T00:00:00", '{"tags": ["test"]}'),
    )
    conn.commit()
    conn.close()

    # Run the migration with confirm-via-input.
    result = subprocess.run(
        [sys.executable, "migration/1_0_0_beta_9.py", "--repository", str(repo_path)],
        input="y\ny\n",
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    # Verify the version bump.
    with open(repo_path / "r3.yaml") as f:
        new_config = yaml.safe_load(f)
    assert new_config["version"] == "1.0.0-beta.9"

    # Verify the column was added and existing row preserved with NULL files.
    conn = sqlite3.connect(str(repo_path / "index.sqlite"))
    columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
    assert "files" in columns
    row = conn.execute(
        "SELECT id, files FROM jobs WHERE id = 'test-id'"
    ).fetchone()
    assert row[0] == "test-id"
    assert row[1] is None
    conn.close()
```

- [ ] **Step 7: Run migration test**

Run: `python -m pytest test/test_repository.py::test_migration_beta_9_adds_files_column -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add r3/repository.py migration/1_0_0_beta_9.py test/test_repository.py
git commit -m ":label: Bump format version to 1.0.0-beta.9 with files column migration"
```

---

## Task 20: Add `@pytest.mark.live_s3` smoke-test infrastructure

**Files:**
- Modify: `pyproject.toml` (add custom marker)
- Create: `test/test_live_s3.py`

- [ ] **Step 1: Register the marker**

In `pyproject.toml`, add (or extend) the pytest config:

```toml
[tool.pytest.ini_options]
markers = [
    "live_s3: tests against a live S3-compatible endpoint (requires env vars; opt-in)",
]
```

- [ ] **Step 2: Create `test/test_live_s3.py`**

```python
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
from typing import Generator

import boto3
import pytest
import yaml

from r3 import Repository
from r3.remote import S3Remote

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

    # Teardown: delete every key under the run prefix.
    paginator = client.get_paginator("list_objects_v2")
    failed: list[str] = []
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
```

- [ ] **Step 3: Verify the suite is skipped without env vars**

Run: `python -m pytest test/test_live_s3.py -v`
Expected: SKIPPED (no test functions yet, but suite collects).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml test/test_live_s3.py
git commit -m ":hammer: Add live_s3 pytest marker and isolation fixture"
```

---

## Task 21: Live S3 lifecycle tests (no archive, with archive, pagination)

**Files:**
- Modify: `test/test_live_s3.py`
- Test fixture: requires real CEPH/MinIO

- [ ] **Step 1: Add lifecycle tests**

Append to `test/test_live_s3.py`:

```python
def _commit_dummy_job(repo: Repository, name: str = "live-test"):
    """Creates a small job in `repo` and commits it."""
    from r3 import Job
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


def test_live_s3_full_lifecycle_no_archive(tmp_path: Path, run_prefix: str):
    repo = _make_repo(tmp_path, run_prefix, archive=False)
    _full_lifecycle(repo, tmp_path)


def test_live_s3_full_lifecycle_with_archive(tmp_path: Path, run_prefix: str):
    repo = _make_repo(tmp_path, run_prefix, archive=True)
    _full_lifecycle(repo, tmp_path)


def test_live_s3_pagination_no_archive(tmp_path: Path, run_prefix: str):
    """Without archiving, a job with > 1000 files exercises list_objects_v2 paging."""
    repo = _make_repo(tmp_path, run_prefix, archive=False)
    from r3 import Job
    src = repo.path.parent / "big"
    src.mkdir()
    (src / "r3.yaml").write_text("dependencies: []\n")
    (src / "metadata.yaml").write_text("tags: [pagination-test]\n")
    (src / "data").mkdir()
    for i in range(1100):
        (src / "data" / f"file_{i:04d}.txt").write_text(str(i))
    job = repo.commit(Job(src))
    assert job.id is not None
    expected_files = sorted(job.files.keys())

    repo.move(job.id, "archive")
    repo.fetch(job.id)

    fetched = repo.get_job_by_id(job.id)
    actual_files = sorted(fetched.files.keys())
    assert actual_files == expected_files
    # Verify content for one file from each "page"
    for i in (5, 1050):
        assert (repo.path / "jobs" / job.id / "data" / f"file_{i:04d}.txt").read_text() == str(i)
```

- [ ] **Step 2: Document how to run**

Append to `CONTRIBUTING.md` (create if missing — short notes only, do not invent extensive docs):

```markdown
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
```

- [ ] **Step 3: Verify suite still skipped without env vars**

Run: `python -m pytest test/test_live_s3.py -v`
Expected: all tests SKIPPED.

- [ ] **Step 4: (Manual) Run against MinIO or CEPH if available**

Run with env vars set: `pytest -m live_s3 -v`
Expected: all tests pass against the real backend.

- [ ] **Step 5: Commit**

```bash
git add test/test_live_s3.py CONTRIBUTING.md
git commit -m ":white_check_mark: Live S3 lifecycle and pagination smoke tests"
```

---

## Task 22: Update existing design doc with resolved limitations

**Files:**
- Modify: `docs/plans/2026-02-11-remote-storage-design.md`

- [ ] **Step 1: Update the "Known Limitations" section**

Locate the section reading "Remote jobs returned by `find()` have cached metadata and timestamp but no local files..." and replace with:

```markdown
## Known Limitations

- Recomputing `job.hash()` on a remote job raises `ValueError`. The hash
  is already stored at commit time and does not need recomputation; this
  guard provides a clear error rather than a confusing TypeError.
```

The file-list limitation noted in beta.8 is now resolved by the file-list
caching introduced in beta.9.

- [ ] **Step 2: Commit**

```bash
git add docs/plans/2026-02-11-remote-storage-design.md
git commit -m ":memo: Update remote storage design doc with resolved limitations"
```

---

## Task 23: Final regression sweep + lint + type check

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `make test`
Expected: all tests pass, coverage doesn't regress.

- [ ] **Step 2: Run linting**

Run: `make lint`
Expected: no ruff or mypy errors.

- [ ] **Step 3: If anything fails, fix and commit**

Address any failures with focused fixes. Commit each separately.

- [ ] **Step 4: Update memory if any non-obvious lessons emerged**

If you discovered surprising constraints during implementation (e.g.,
particular `tarfile`/`pyzstd` quirks), capture them as a brief note in
project memory.

---

## Task 24: Final review and merge prep

- [ ] **Step 1: Review the full diff**

```bash
git diff main...HEAD --stat
git log main..HEAD --oneline
```

- [ ] **Step 2: Run the manual smoke test against real CEPH**

Set the env vars and run `pytest -m live_s3 -v`. Confirm all tests pass.

- [ ] **Step 3: Push the branch**

```bash
git push origin feature/remote-storage
```

- [ ] **Step 4: Open the PR (or merge directly per workflow)**

Use the existing repository conventions. The PR description should
reference the spec at `docs/superpowers/specs/2026-03-21-remote-storage-extensions-design.md` and call out the format version bump (beta.8 → beta.9) and the migration script.

---

## Done

The two extensions are implemented, tested at multiple levels (unit, integration, live S3), documented, and migration-ready.
