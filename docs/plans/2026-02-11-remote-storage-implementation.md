# Remote Storage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add named remote storage (S3/CEPH) so jobs can be moved off local disk while keeping their metadata queryable.

**Architecture:** A new `Remote` ABC with `S3Remote` implementation sits alongside the existing `Storage` class (unchanged). The SQLite index gains a `location` column to track where each job's files live. `Repository` orchestrates move/fetch between local storage and remotes.

**Tech Stack:** boto3 (S3 client), moto (S3 mocking for tests), existing click/pytest/pyfakefs stack.

**Design doc:** `docs/plans/2026-02-11-remote-storage-design.md`

---

### Task 1: Add dependencies

**Files:**
- Modify: `pyproject.toml:15-19` (add boto3 to dependencies)
- Modify: `pyproject.toml:25-39` (add moto to dev dependencies)

**Step 1: Add boto3 to project dependencies**

In `pyproject.toml`, add `boto3` to the `dependencies` list:

```toml
dependencies = [
    "boto3~=1.35",
    "click~=8.1",
    "executor~=23.2",
    "pyyaml~=6.0",
    "tqdm~=4.66"
]
```

**Step 2: Add moto and boto3-stubs to dev dependencies**

In `pyproject.toml`, add to the `dev` dependency group:

```toml
dev = [
    "boto3-stubs[s3]~=1.35",
    # ... existing entries ...
    "moto[s3]~=5.0",
    # ... existing entries ...
]
```

Also add mypy override for moto:

```toml
[[tool.mypy.overrides]]
module = [
    "executor",
    "moto",
    "moto.s3",
    "pyfakefs.fake_filesystem",
]
ignore_missing_imports = true
```

**Step 3: Install dependencies**

Run: `uv sync`
Expected: Dependencies install successfully.

**Step 4: Commit**

```
git add pyproject.toml uv.lock
git commit -m ":heavy_plus_sign: Add boto3 and moto dependencies"
```

---

### Task 2: Remote ABC and S3Remote

**Files:**
- Create: `r3/remote.py`
- Create: `test/test_remote.py`

**Step 1: Write failing tests for Remote/S3Remote**

Create `test/test_remote.py`:

```python
"""Unit tests for r3.remote."""

import os
from pathlib import Path

import boto3
import pytest
import yaml
from moto import mock_aws

from r3.remote import S3Remote

BUCKET = "test-bucket"
PREFIX = "r3/jobs/"


@pytest.fixture
def s3_remote():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield S3Remote(bucket=BUCKET, prefix=PREFIX)


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    """Create a minimal job directory for testing."""
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


def test_s3_remote_upload_and_exists(s3_remote: S3Remote, job_dir: Path):
    job_id = job_dir.name
    assert not s3_remote.exists(job_id)
    s3_remote.upload(job_id, job_dir)
    assert s3_remote.exists(job_id)


def test_s3_remote_upload_and_download(
    s3_remote: S3Remote, job_dir: Path, tmp_path: Path
):
    job_id = job_dir.name
    s3_remote.upload(job_id, job_dir)

    download_path = tmp_path / "downloaded" / job_id
    s3_remote.download(job_id, download_path)

    assert (download_path / "r3.yaml").exists()
    assert (download_path / "metadata.yaml").exists()
    assert (download_path / "run.py").exists()
    assert (download_path / "run.py").read_text() == "print('hello')"
    assert (download_path / "output" / "result.txt").exists()
    assert (download_path / "output" / "result.txt").read_text() == "result data"


def test_s3_remote_remove(s3_remote: S3Remote, job_dir: Path):
    job_id = job_dir.name
    s3_remote.upload(job_id, job_dir)
    assert s3_remote.exists(job_id)

    s3_remote.remove(job_id)
    assert not s3_remote.exists(job_id)


def test_s3_remote_exists_returns_false_for_missing_job(s3_remote: S3Remote):
    assert not s3_remote.exists("nonexistent-job-id")


def test_s3_remote_download_raises_for_missing_job(
    s3_remote: S3Remote, tmp_path: Path
):
    with pytest.raises(FileNotFoundError):
        s3_remote.download("nonexistent", tmp_path / "dest")


def test_s3_remote_with_empty_prefix():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        remote = S3Remote(bucket=BUCKET, prefix="")
        assert not remote.exists("some-job")


def test_s3_remote_from_config():
    config = {
        "bucket": "my-bucket",
        "prefix": "jobs/",
    }
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="my-bucket")
        remote = S3Remote.from_config(config)
        assert remote._bucket == "my-bucket"
        assert remote._prefix == "jobs/"


def test_s3_remote_from_config_with_optional_fields():
    config = {
        "bucket": "my-bucket",
        "prefix": "jobs/",
        "profile": "ceph",
        "endpoint_url": "https://ceph.example.com",
    }
    # Just test that from_config accepts these fields without error.
    # Actual S3 connection with custom endpoint is not tested here.
    remote = S3Remote.from_config(config)
    assert remote._bucket == "my-bucket"
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_remote.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'r3.remote'`

**Step 3: Implement Remote ABC and S3Remote**

Create `r3/remote.py`:

```python
"""Remote storage backends for R3 repositories."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

import boto3


class Remote(ABC):
    """Abstract base class for remote storage backends."""

    @abstractmethod
    def upload(self, job_id: str, job_path: Path) -> None:
        """Upload a job directory to the remote.

        Parameters:
            job_id: The job's UUID.
            job_path: Local path to the job directory.
        """

    @abstractmethod
    def download(self, job_id: str, destination: Path) -> None:
        """Download a job from the remote to a local directory.

        Parameters:
            job_id: The job's UUID.
            destination: Local path to download the job to.

        Raises:
            FileNotFoundError: If the job does not exist on the remote.
        """

    @abstractmethod
    def remove(self, job_id: str) -> None:
        """Remove a job from the remote.

        Parameters:
            job_id: The job's UUID.
        """

    @abstractmethod
    def exists(self, job_id: str) -> bool:
        """Check whether a job exists on the remote.

        Parameters:
            job_id: The job's UUID.

        Returns:
            True if the job exists on the remote.
        """

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "Remote":
        """Create a Remote instance from a config dictionary.

        Parameters:
            config: Remote configuration. Must include a "type" key.

        Returns:
            A Remote instance.
        """
        remote_type = config.get("type")
        if remote_type == "s3":
            return S3Remote.from_config(config)
        raise ValueError(f"Unknown remote type: {remote_type}")


class S3Remote(Remote):
    """S3-compatible remote storage backend."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._profile = profile
        self._endpoint_url = endpoint_url

    @staticmethod
    def from_config(config: Dict[str, Any]) -> "S3Remote":
        """Create an S3Remote from a config dictionary.

        Parameters:
            config: Must include "bucket". Optional: "prefix", "profile",
                "endpoint_url".
        """
        return S3Remote(
            bucket=config["bucket"],
            prefix=config.get("prefix", ""),
            profile=config.get("profile"),
            endpoint_url=config.get("endpoint_url"),
        )

    def _client(self):
        session = boto3.Session(profile_name=self._profile)
        return session.client("s3", endpoint_url=self._endpoint_url)

    def _job_prefix(self, job_id: str) -> str:
        return f"{self._prefix}{job_id}/"

    def upload(self, job_id: str, job_path: Path) -> None:
        client = self._client()
        for root, dirs, files in os.walk(job_path):
            for filename in files:
                local_file = Path(root) / filename
                relative = local_file.relative_to(job_path)
                key = f"{self._job_prefix(job_id)}{relative}"
                client.upload_file(str(local_file), self._bucket, key)

    def download(self, job_id: str, destination: Path) -> None:
        if not self.exists(job_id):
            raise FileNotFoundError(
                f"Job {job_id} not found on remote s3://{self._bucket}/{self._prefix}"
            )

        client = self._client()
        prefix = self._job_prefix(job_id)
        paginator = client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                relative = key[len(prefix):]
                local_path = destination / relative
                local_path.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(self._bucket, key, str(local_path))

    def remove(self, job_id: str) -> None:
        client = self._client()
        prefix = self._job_prefix(job_id)
        paginator = client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                client.delete_objects(
                    Bucket=self._bucket, Delete={"Objects": objects}
                )

    def exists(self, job_id: str) -> bool:
        client = self._client()
        prefix = self._job_prefix(job_id)
        response = client.list_objects_v2(
            Bucket=self._bucket, Prefix=prefix, MaxKeys=1
        )
        return response.get("KeyCount", 0) > 0
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_remote.py -v`
Expected: All PASS.

**Step 5: Run linting and type checking**

Run: `make lint`
Expected: PASS (fix any issues).

**Step 6: Commit**

```
git add r3/remote.py test/test_remote.py
git commit -m ":sparkles: Add Remote ABC and S3Remote implementation"
```

---

### Task 3: Index location tracking

**Files:**
- Modify: `r3/index.py:29-53` (rebuild — add location column)
- Modify: `r3/index.py:106-131` (add — include location)
- Modify: `r3/index.py:194-219` (find — support location filter)
- Add methods: `set_location`, `get_location`
- Test: `test/test_index.py`

**Step 1: Write failing tests for location tracking**

Add to `test/test_index.py`:

```python
def test_index_add_defaults_location_to_local(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None
    assert index.get_location(job.id) == "local"


def test_index_set_location(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.set_location(job.id, "archive")
    assert index.get_location(job.id) == "archive"

    index.set_location(job.id, "local")
    assert index.get_location(job.id) == "local"


def test_index_rebuild_defaults_location_to_local(storage: Storage):
    index = Index(storage)
    job = get_dummy_job("base")
    job = storage.add(job)
    index.add(job)
    assert job.id is not None

    index.rebuild()
    assert index.get_location(job.id) == "local"


def test_index_find_with_location_filter(storage_with_jobs: Storage):
    index = Index(storage_with_jobs)

    all_jobs = index.find({})
    assert len(all_jobs) == 3

    # Mark one job as archived
    job = all_jobs[0]
    assert job.id is not None
    index.set_location(job.id, "archive")

    local_jobs = index.find({}, location="local")
    assert len(local_jobs) == 2

    archived_jobs = index.find({}, location="archive")
    assert len(archived_jobs) == 1
    assert archived_jobs[0].id == job.id

    # Without location filter, all jobs are still returned
    all_jobs_again = index.find({})
    assert len(all_jobs_again) == 3
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_index.py::test_index_add_defaults_location_to_local -v`
Expected: FAIL — `Index.get_location` does not exist.

**Step 3: Implement location tracking in Index**

Modify `r3/index.py`:

In `rebuild()`, update the CREATE TABLE statement to include `location`:

```sql
CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    metadata JSON NOT NULL,
    location TEXT NOT NULL DEFAULT 'local'
)
```

In `add()`, update the INSERT to include `location`:

```python
transaction.execute(
    "INSERT INTO jobs (id, timestamp, metadata, location) VALUES (?, ?, ?, ?)",
    (job.id, job.timestamp.isoformat(), json.dumps(job.metadata), "local")
)
```

Add `set_location` method:

```python
def set_location(self, job_id: str, location: str) -> None:
    """Sets the location of a job.

    Parameters:
        job_id: The ID of the job.
        location: The location name ("local" or a remote name).
    """
    with Transaction(self._path) as transaction:
        transaction.execute(
            "UPDATE jobs SET location = ? WHERE id = ?",
            (location, job_id)
        )
```

Add `get_location` method:

```python
def get_location(self, job_id: str) -> str:
    """Gets the location of a job.

    Parameters:
        job_id: The ID of the job.

    Returns:
        The location name ("local" or a remote name).
    """
    with Transaction(self._path) as transaction:
        transaction.execute(
            "SELECT location FROM jobs WHERE id = ?",
            (job_id,)
        )
        result = transaction.fetchone()

    if result is None:
        raise KeyError(f"Job not found: {job_id}")

    return result[0]
```

Update `find()` signature and implementation to accept optional `location` parameter:

```python
def find(
    self, query: Dict[str, Any], latest: bool = False, location: Optional[str] = None
) -> List[Job]:
```

In the `find` method body, if `location` is not None, append a WHERE clause:

```python
sql_query = f"SELECT id, timestamp, metadata FROM jobs WHERE {mongo_to_sql(query)}"
if location is not None:
    sql_query += f" AND location = '{location}'"
if latest:
    sql_query += " ORDER BY timestamp DESC LIMIT 1"
```

Note: The location value comes from our own code (not user input), so this is safe. But if preferred, use parameterized queries by restructuring the SQL building.

Add `Optional` to the imports from typing.

**Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_index.py -v`
Expected: All PASS (both old and new tests).

**Step 5: Run linting and type checking**

Run: `make lint`
Expected: PASS.

**Step 6: Commit**

```
git add r3/index.py test/test_index.py
git commit -m ":sparkles: Add location tracking to job index"
```

---

### Task 4: Repository remote loading, move, and fetch

**Files:**
- Modify: `r3/repository.py:32-65` (init — load remotes from config)
- Modify: `r3/repository.py` (add move/fetch methods)
- Test: `test/test_repository.py`

**Step 1: Write failing tests for remote loading and move/fetch**

Add to `test/test_repository.py`:

```python
import boto3
from moto import mock_aws
from r3.remote import S3Remote

BUCKET = "test-bucket"
PREFIX = "r3/jobs/"


@pytest.fixture
def repository_with_remote(tmp_path: Path):
    """Create a repository with a mocked S3 remote configured."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        repo = Repository.init(tmp_path / "repository")

        # Add remote to config
        config_path = repo.path / "r3.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        config["remotes"] = {
            "archive": {
                "type": "s3",
                "bucket": BUCKET,
                "prefix": PREFIX,
            }
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        # Re-open repository to load remotes
        repo = Repository(repo.path)
        yield repo


def test_repository_loads_remotes_from_config(repository_with_remote: Repository):
    assert "archive" in repository_with_remote.remotes


def test_repository_without_remotes(repository: Repository):
    assert len(repository.remotes) == 0


def test_repository_move_uploads_and_removes_local(
    repository_with_remote: Repository,
):
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None
    assert job.path.exists()

    repository_with_remote.move(job.id, "archive")

    # Local files should be gone
    assert not (repository_with_remote.path / "jobs" / job.id).exists()

    # Job should still be findable in index
    results = repository_with_remote.find({})
    assert len(results) == 1
    assert results[0].id == job.id

    # Location should be updated
    assert repository_with_remote._index.get_location(job.id) == "archive"


def test_repository_move_raises_for_unknown_remote(repository: Repository):
    job = get_dummy_job("base")
    job = repository.commit(job)
    assert job.id is not None

    with pytest.raises(ValueError, match="Unknown remote"):
        repository.move(job.id, "nonexistent")


def test_repository_move_raises_for_unknown_job(repository_with_remote: Repository):
    with pytest.raises(KeyError):
        repository_with_remote.move("nonexistent-id", "archive")


def test_repository_fetch_downloads_and_restores_local(
    repository_with_remote: Repository,
):
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")
    assert not (repository_with_remote.path / "jobs" / job.id).exists()

    repository_with_remote.fetch(job.id)

    # Local files should be restored
    assert (repository_with_remote.path / "jobs" / job.id).exists()
    assert (repository_with_remote.path / "jobs" / job.id / "r3.yaml").exists()

    # Location should be back to local
    assert repository_with_remote._index.get_location(job.id) == "local"


def test_repository_fetch_raises_for_local_job(repository_with_remote: Repository):
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    with pytest.raises(ValueError, match="already local"):
        repository_with_remote.fetch(job.id)


def test_repository_move_warns_about_dependents(
    repository_with_remote: Repository,
):
    job1 = get_dummy_job("base")
    job1 = repository_with_remote.commit(job1)
    assert job1.id is not None

    job2 = get_dummy_job("base")
    dep = JobDependency("destination", job1.id)
    job2._dependencies = [dep]
    job2._config["dependencies"] = [dep.to_config()]
    job2 = repository_with_remote.commit(job2)

    # Move should succeed but return dependent info
    dependents = repository_with_remote.move(job1.id, "archive")
    assert len(dependents) == 1
    assert job2.id in {d.id for d in dependents}


def test_repository_find_still_works_after_move(
    repository_with_remote: Repository,
):
    job = get_dummy_job("base")
    job.metadata["tags"] = ["experiment"]
    job = repository_with_remote.commit(job)

    repository_with_remote.move(job.id, "archive")

    results = repository_with_remote.find({"tags": "experiment"})
    assert len(results) == 1
    assert results[0].id == job.id
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_repository.py::test_repository_loads_remotes_from_config -v`
Expected: FAIL — `Repository` has no `remotes` attribute.

**Step 3: Implement remote loading, move, and fetch in Repository**

Modify `r3/repository.py`:

Add imports at top:

```python
from r3.remote import Remote
```

In `__init__`, after loading config, load remotes:

```python
self._remotes: Dict[str, Remote] = {}
for name, remote_config in config.get("remotes", {}).items():
    self._remotes[name] = Remote.from_config(remote_config)
```

Add property:

```python
@property
def remotes(self) -> Dict[str, "Remote"]:
    """Returns the configured remotes."""
    return self._remotes
```

Add `move` method:

```python
def move(self, job_id: str, remote_name: str) -> Set[Job]:
    """Moves a job to a remote.

    Parameters:
        job_id: The ID of the job to move.
        remote_name: The name of the remote to move the job to.

    Returns:
        Set of jobs that depend on the moved job (informational warning).

    Raises:
        ValueError: If the remote name is not configured.
        KeyError: If the job ID does not exist.
    """
    if remote_name not in self._remotes:
        raise ValueError(f"Unknown remote: {remote_name}")

    job = self.get_job_by_id(job_id)
    remote = self._remotes[remote_name]

    # Upload to remote
    remote.upload(job_id, job.path)

    # Verify upload
    if not remote.exists(job_id):
        raise RuntimeError(f"Upload verification failed for job {job_id}")

    # Find dependents before removing local files
    dependents = self._index.find_dependents(job)

    # Remove local files
    self._storage.remove(job)

    # Update index location
    self._index.set_location(job_id, remote_name)

    return dependents
```

Add `fetch` method:

```python
def fetch(self, job_id: str) -> None:
    """Fetches a job from its remote back to local storage.

    Parameters:
        job_id: The ID of the job to fetch.

    Raises:
        KeyError: If the job ID does not exist in the index.
        ValueError: If the job is already local.
    """
    location = self._index.get_location(job_id)

    if location == "local":
        raise ValueError(f"Job {job_id} is already local.")

    remote = self._remotes[location]
    destination = self._storage.root / "jobs" / job_id

    remote.download(job_id, destination)

    self._index.set_location(job_id, "local")
```

Also need to add `Set` to imports from typing if not already there.

**Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_repository.py -v`
Expected: All PASS (old and new tests).

**Step 5: Run full test suite + linting**

Run: `make test && make lint`
Expected: PASS.

**Step 6: Commit**

```
git add r3/repository.py test/test_repository.py
git commit -m ":sparkles: Add Repository.move() and Repository.fetch()"
```

---

### Task 5: Checkout error for archived jobs

**Files:**
- Modify: `r3/storage.py:49-62` (__contains__ — check location)
- Modify: `r3/repository.py:163-178` (checkout — check location)
- Test: `test/test_repository.py`

**Step 1: Write failing tests**

Add to `test/test_repository.py`:

```python
def test_checkout_raises_for_archived_job(
    repository_with_remote: Repository, tmp_path: Path
):
    job = get_dummy_job("base")
    job = repository_with_remote.commit(job)
    assert job.id is not None

    repository_with_remote.move(job.id, "archive")

    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch"):
        repository_with_remote.checkout(job, tmp_path / "checkout")


def test_checkout_raises_for_archived_dependency(
    repository_with_remote: Repository, tmp_path: Path
):
    dep_job = get_dummy_job("base")
    dep_job.metadata["tags"] = ["dep"]
    dep_job = repository_with_remote.commit(dep_job)
    assert dep_job.id is not None

    main_job = get_dummy_job("base")
    dependency = JobDependency("data", dep_job.id, "run.py")
    main_job._dependencies = [dependency]
    main_job._config["dependencies"] = [dependency.to_config()]
    main_job = repository_with_remote.commit(main_job)

    repository_with_remote.move(dep_job.id, "archive")

    with pytest.raises(ValueError, match="archived.*archive.*r3 fetch"):
        repository_with_remote.checkout(main_job, tmp_path / "checkout")
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_repository.py::test_checkout_raises_for_archived_job -v`
Expected: FAIL — checkout does not check location.

**Step 3: Implement checkout location check**

The check should happen in `Repository.checkout`. Before delegating to storage, verify that the job and all its dependencies are local.

Add a helper method to `Repository`:

```python
def _check_job_is_local(self, job_id: str) -> None:
    """Raises ValueError if a job is not stored locally."""
    location = self._index.get_location(job_id)
    if location != "local":
        raise ValueError(
            f"Job {job_id} is archived on remote \"{location}\". "
            f"Run `r3 fetch {job_id}` to retrieve it first."
        )
```

In `Repository.checkout`, before delegating to `self._storage.checkout`:

```python
def checkout(self, item, path):
    resolved_item = self.resolve(item)

    if isinstance(resolved_item, list):
        for dependency in resolved_item:
            if isinstance(dependency, JobDependency):
                self._check_job_is_local(dependency.job)
            self._storage.checkout(dependency, path)
    else:
        if isinstance(resolved_item, Job) and resolved_item.id is not None:
            self._check_job_is_local(resolved_item.id)
            # Also check dependencies
            for dep in resolved_item.dependencies:
                if isinstance(dep, JobDependency):
                    self._check_job_is_local(dep.job)
        elif isinstance(resolved_item, JobDependency):
            self._check_job_is_local(resolved_item.job)
        self._storage.checkout(resolved_item, path)
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_repository.py -v`
Expected: All PASS.

**Step 5: Run full test suite + linting**

Run: `make test && make lint`
Expected: PASS.

**Step 6: Commit**

```
git add r3/repository.py test/test_repository.py
git commit -m ":boom: Checkout fails with clear message for archived jobs"
```

---

### Task 6: CLI commands

**Files:**
- Modify: `r3/cli.py` (add `move`, `fetch`, `remote` commands)
- Test: CLI tests via `CliRunner`

This task adds three CLI command groups. Since the existing CLI tests are in
`test/test_repository.py` (there is no separate `test/test_cli.py`), and the
CLI is thin, test the CLI commands with the `CliRunner` in a new test file.

**Step 1: Write failing tests for CLI commands**

Create `test/test_cli.py`:

```python
"""Tests for r3 CLI commands related to remote storage."""

from pathlib import Path

import boto3
import yaml
from click.testing import CliRunner
from moto import mock_aws

from r3.cli import cli
from r3.repository import Repository

BUCKET = "test-bucket"
PREFIX = "r3/jobs/"
DATA_PATH = Path(__file__).parent / "data"


def get_dummy_job_path(name: str) -> Path:
    return DATA_PATH / "jobs" / name


def setup_repo_with_remote(tmp_path: Path):
    """Helper: create repo, configure S3 remote, commit a job."""
    repo = Repository.init(tmp_path / "repository")

    config_path = repo.path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["remotes"] = {
        "archive": {"type": "s3", "bucket": BUCKET, "prefix": PREFIX}
    }
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    repo = Repository(repo.path)
    from r3.job import Job
    job = Job(get_dummy_job_path("base"))
    job = repo.commit(job)
    return repo, job


def test_cli_move(tmp_path: Path):
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        repo, job = setup_repo_with_remote(tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "move", job.id, "archive",
            "--repository", str(repo.path),
        ])
        assert result.exit_code == 0
        assert not (repo.path / "jobs" / job.id).exists()


def test_cli_fetch(tmp_path: Path):
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        repo, job = setup_repo_with_remote(tmp_path)

        runner = CliRunner()
        runner.invoke(cli, [
            "move", job.id, "archive",
            "--repository", str(repo.path),
        ])

        result = runner.invoke(cli, [
            "fetch", job.id,
            "--repository", str(repo.path),
        ])
        assert result.exit_code == 0
        assert (repo.path / "jobs" / job.id).exists()


def test_cli_remote_add_and_list(tmp_path: Path):
    repo = Repository.init(tmp_path / "repository")
    runner = CliRunner()

    result = runner.invoke(cli, [
        "remote", "add", "archive",
        "--type", "s3",
        "--bucket", "my-bucket",
        "--prefix", "jobs/",
        "--repository", str(repo.path),
    ])
    assert result.exit_code == 0

    result = runner.invoke(cli, [
        "remote", "list",
        "--repository", str(repo.path),
    ])
    assert result.exit_code == 0
    assert "archive" in result.output
    assert "s3" in result.output


def test_cli_remote_remove(tmp_path: Path):
    repo = Repository.init(tmp_path / "repository")
    runner = CliRunner()

    runner.invoke(cli, [
        "remote", "add", "archive",
        "--type", "s3",
        "--bucket", "my-bucket",
        "--repository", str(repo.path),
    ])

    result = runner.invoke(cli, [
        "remote", "remove", "archive",
        "--repository", str(repo.path),
    ])
    assert result.exit_code == 0

    result = runner.invoke(cli, [
        "remote", "list",
        "--repository", str(repo.path),
    ])
    assert "archive" not in result.output


def test_cli_find_location_filter(tmp_path: Path):
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        repo, job = setup_repo_with_remote(tmp_path)

        runner = CliRunner()
        runner.invoke(cli, [
            "move", job.id, "archive",
            "--repository", str(repo.path),
        ])

        result = runner.invoke(cli, [
            "find", "--location", "archive",
            "--repository", str(repo.path),
        ])
        assert result.exit_code == 0
        assert job.id in result.output

        result = runner.invoke(cli, [
            "find", "--location", "local",
            "--repository", str(repo.path),
        ])
        assert result.exit_code == 0
        assert job.id not in result.output
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest test/test_cli.py -v`
Expected: FAIL — commands don't exist yet.

**Step 3: Implement CLI commands**

Add to `r3/cli.py`:

```python
@cli.command()
@click.argument("job_id", type=str)
@click.argument("remote_name", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
@click.option("--dry-run", is_flag=True, help="Show what would be moved without doing it.")
def move(job_id: str, remote_name: str, repository_path: Path, dry_run: bool) -> None:
    """Move a job to a remote storage location."""
    repository = r3.Repository(repository_path)

    try:
        if dry_run:
            job = repository.get_job_by_id(job_id)
            dependents = repository.find_dependents(job)
            print(f"Would move job {job_id} to remote '{remote_name}'.")
            if dependents:
                print("The following jobs depend on this job:")
                for dep in dependents:
                    print(f"  - {dep.id}")
            return

        dependents = repository.move(job_id, remote_name)
        print(f"Moved job {job_id} to remote '{remote_name}'.")
        if dependents:
            print("Warning: the following jobs depend on this job:")
            for dep in dependents:
                print(f"  - {dep.id}")
    except (ValueError, KeyError) as error:
        print(f"Error: {error}")
        sys.exit(1)


@cli.command()
@click.argument("job_id", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def fetch(job_id: str, repository_path: Path) -> None:
    """Fetch a job from remote storage back to local."""
    repository = r3.Repository(repository_path)

    try:
        repository.fetch(job_id)
        print(f"Fetched job {job_id} to local storage.")
    except (ValueError, KeyError) as error:
        print(f"Error: {error}")
        sys.exit(1)


@cli.group()
def remote() -> None:
    """Manage remote storage locations."""
    pass


@remote.command("add")
@click.argument("name", type=str)
@click.option("--type", "remote_type", type=str, required=True, help="Remote type (e.g., s3).")
@click.option("--bucket", type=str, help="S3 bucket name.")
@click.option("--prefix", type=str, default="", help="S3 key prefix.")
@click.option("--profile", type=str, default=None, help="AWS/boto3 profile name.")
@click.option("--endpoint-url", type=str, default=None, help="S3 endpoint URL (for CEPH/MinIO).")
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_add(
    name: str,
    remote_type: str,
    bucket: str,
    prefix: str,
    profile: str,
    endpoint_url: str,
    repository_path: Path,
) -> None:
    """Add a remote storage location."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.setdefault("remotes", {})
    if name in remotes:
        print(f"Error: Remote '{name}' already exists.")
        sys.exit(1)

    remote_config: dict = {"type": remote_type}
    if bucket:
        remote_config["bucket"] = bucket
    if prefix:
        remote_config["prefix"] = prefix
    if profile:
        remote_config["profile"] = profile
    if endpoint_url:
        remote_config["endpoint_url"] = endpoint_url

    remotes[name] = remote_config

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    print(f"Added remote '{name}'.")


@remote.command("list")
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_list(repository_path: Path) -> None:
    """List configured remote storage locations."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.get("remotes", {})
    if not remotes:
        print("No remotes configured.")
        return

    for name, remote_config in remotes.items():
        remote_type = remote_config.get("type", "unknown")
        print(f"{name} ({remote_type})")


@remote.command("remove")
@click.argument("name", type=str)
@click.option(
    "--repository",
    "repository_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    envvar="R3_REPOSITORY",
)
def remote_remove(name: str, repository_path: Path) -> None:
    """Remove a remote storage location."""
    config_path = repository_path / "r3.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    remotes = config.get("remotes", {})
    if name not in remotes:
        print(f"Error: Remote '{name}' not found.")
        sys.exit(1)

    del remotes[name]

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    print(f"Removed remote '{name}'.")
```

Add `import yaml` to the imports in `r3/cli.py`.

Update the existing `find` command to add `--location`:

```python
@click.option("--location", type=str, default=None,
    help="Filter by job location (e.g., 'local', 'archive').")
```

And pass it through:

```python
def find(tags, latest, long, repository_path, location):
    repository = r3.Repository(repository_path)
    query = {"tags": {"$all": tags}}
    for job in repository.find(query, latest, location=location):
```

This requires updating `Repository.find` to forward `location` to `Index.find`:

In `r3/repository.py`, update `find`:

```python
def find(self, query, latest=False, location=None):
    return self._index.find(query, latest, location=location)
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest test/test_cli.py -v`
Expected: All PASS.

**Step 5: Run full test suite + linting**

Run: `make test && make lint`
Expected: PASS.

**Step 6: Commit**

```
git add r3/cli.py r3/repository.py test/test_cli.py
git commit -m ":sparkles: Add CLI commands for move, fetch, and remote management"
```

---

### Task 7: Migration script and format version bump

**Files:**
- Create: `migration/1_0_0_beta_8.py`
- Modify: `r3/repository.py:29` (bump R3_FORMAT_VERSION)

**Step 1: Write the migration script**

Create `migration/1_0_0_beta_8.py` following the pattern in
`migration/1_0_0_beta_7.py`:

```python
#!/usr/bin/env python
"""Migrates a repository from 1.0.0-beta.7 to 1.0.0-beta.8."""

from pathlib import Path

import click
import yaml

OLD_VERSION = "1.0.0-beta.7"
NEW_VERSION = "1.0.0-beta.8"


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
    click.echo("  - Rebuild index with location tracking column")
    click.echo()

    click.confirm("Do you want to continue?", abort=True)
    click.confirm("Do you have a backup of your data?", abort=True)
    click.echo()

    click.echo("Updating repository version...")
    config["version"] = NEW_VERSION
    with open(repository_path / "r3.yaml", "w") as file:
        yaml.safe_dump(config, file)

    click.echo("Rebuilding index...")
    # Import here to avoid version check failure during import
    from r3 import Repository
    repository = Repository(repository_path)
    repository.rebuild_index()

    click.echo("Done.")
    click.echo()
    click.echo("Migration complete.")


if __name__ == "__main__":
    migrate()
```

**Step 2: Bump the format version**

In `r3/repository.py:29`, change:

```python
R3_FORMAT_VERSION = "1.0.0-beta.8"
```

**Step 3: Run the full test suite**

Run: `make test`
Expected: All PASS. The tests use `Repository.init()` which writes the current
`R3_FORMAT_VERSION`, so existing tests automatically work with the new version.

**Step 4: Run linting**

Run: `make lint`
Expected: PASS.

**Step 5: Commit**

```
git add r3/repository.py migration/1_0_0_beta_8.py
git commit -m ":label: Bump format version to 1.0.0-beta.8 and add migration script"
```

---

### Task 8: Final integration verification

**Step 1: Run full test suite with coverage**

Run: `make test`
Expected: All tests pass, coverage includes new `r3/remote.py`.

**Step 2: Run full linting**

Run: `make lint`
Expected: Clean.

**Step 3: Review the diff**

Run: `git log --oneline feature/remote-storage --not main`
Expected: Clean sequence of commits matching the tasks above.
