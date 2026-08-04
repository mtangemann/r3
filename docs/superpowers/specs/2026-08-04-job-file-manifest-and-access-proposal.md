# Proposal: Separate Job Manifest from File Access

**Status:** Exploratory suggestion; not final or approved for implementation.

**Timing:** Revisit only after the current remote-storage correctness, security,
migration, and recovery issues have been fixed. This proposal should not expand the
scope of that repair work.

## Motivation

A job has two related but distinct properties:

1. Its logical file manifest: which files belong to the job.
2. Its current materialization: whether those files are accessible locally and where.

Keeping these concepts separate may provide clearer semantics than representing
inaccessible files as `None` values in `job.files`.

## Proposed API

```python
job.file_paths  # Collection[Path]
job.files       # Mapping[Path, Path]
```

`job.file_paths` represents the logical manifest and is available for both local and
remote jobs.

`job.files` contains usable local paths. For an unmounted remote job, accessing it
raises a specific exception:

```python
class FilesUnavailableError(RuntimeError):
    pass
```

Example:

```python
for relative_path in job.file_paths:
    print(relative_path)

# Raises FilesUnavailableError if the remote job is not mounted or fetched.
local_path = job.files[Path("output/result.pt")]
```

## Job states

| State | `file_paths` | `files` |
|---|---|---|
| Local | Manifest | Paths inside the repository |
| Remote, unmounted | Manifest | Raises `FilesUnavailableError` |
| Remote, mounted | Manifest | Paths inside the mountpoint |
| Fetched | Manifest | Paths inside the repository |

Internally, this could be modeled with a manifest and an optional materialization root:

```python
class Job:
    def __init__(
        self,
        ...,
        file_paths: Collection[Path],
        file_root: Path | None,
    ) -> None:
        self._file_paths = frozenset(file_paths)
        self._file_root = file_root

    @property
    def file_paths(self) -> Collection[Path]:
        return self._file_paths

    @property
    def files(self) -> Mapping[Path, Path]:
        if self._file_root is None:
            raise FilesUnavailableError(
                "Job files are not locally accessible. Fetch or mount the job first."
            )

        return {
            relative_path: self._file_root / relative_path
            for relative_path in self._file_paths
        }
```

The exact collection and path types remain open questions. A persisted manifest may
ultimately use POSIX path strings or `PurePosixPath`, while materialized paths should
use the local platform's `Path`.

## Future mounting API

Mounting should be explicit because it creates an external resource with a limited
lifetime:

```python
with repository.mount(job) as mounted_job:
    result = mounted_job.files[Path("output/result.pt")]
    process(result)
```

The original remote `Job` remains unmaterialized. `mounted_job` is a temporary view
whose file root points to the ratarmount mountpoint.

When the context exits:

- ratarmount is stopped;
- the mountpoint is cleaned up;
- paths obtained from `mounted_job.files` are no longer valid.

Mounting belongs on `Repository`, rather than `Job`, because the repository knows the
job's remote, storage representation, credentials, endpoint, and cleanup requirements.

A later, more general API could hide whether access is provided by a local job, a mount,
or a temporary download:

```python
with repository.access(job) as accessible_job:
    use(accessible_job.files)
```

For local jobs, this would be a no-op. For remote jobs, it could mount or temporarily
download depending on backend capabilities.

## Manifest semantics requiring definition

Before implementing this proposal, the manifest contract must specify:

- whether it contains files only or also directories;
- how directory dependencies such as `source="output"` are validated;
- whether empty directories are meaningful;
- whether mutable output files are included;
- path normalization and traversal restrictions;
- whether entries include size, checksum, or other integrity information.

A richer manifest entry may eventually be useful:

```python
@dataclass(frozen=True)
class FileManifestEntry:
    path: Path
    size: int
    checksum: str
```

That decision should be coordinated with the remote-storage integrity and recovery
design.

## Alternatives

The existing proposed API remains a valid alternative:

```python
job.files: Mapping[Path, Path | None]
```

It is compact and presents one unified collection. Its tradeoff is that every consumer
must distinguish membership from accessibility and handle `None`, including code that
only expects local jobs.

Another alternative is a richer value object representing each file's manifest data
and current availability. That may be useful later but appears unnecessarily elaborate
for the present API.

## Open questions

- Should the public names be `file_paths` and `files`, or `manifest` and `files`?
- Should `job.files` raise immediately for remote jobs or return an empty/unavailable
  view?
- Should mounting return another `Job`, a `MountedJob`, or a dedicated file-access
  object?
- Should `repository.access()` be the primary API, with `mount()` remaining
  backend-specific?
- Should manifests be stored in SQLite, alongside the remote object, or both?
- How should manifest versions evolve independently of the repository format?

## Recommendation

Do not implement this proposal as part of the immediate bug-fixing work. First establish
safe remote object identity, integrity verification, atomic state transitions, secure
extraction, and reliable migrations. Then compare this model against
`Mapping[Path, Optional[Path]]` using concrete checkout, dependency-validation, and
ratarmount use cases.
