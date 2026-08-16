# Independent Review: Remote Storage Feature Branch

> **Superseded** — this predates and is superseded by the current durable design
> ([`../superpowers/specs/2026-08-04-remote-storage-durable-design.md`](../superpowers/specs/2026-08-04-remote-storage-durable-design.md)),
> which was written to address this review; kept for historical context.

**Review date:** 2026-08-04

**Feature branch:** `feature/remote-storage` at `6b5b81a`

**Comparison base:** `9136fbb`

**Review status:** Findings and non-binding suggestions for further work

> **Important:** This document is a review, not an approved implementation plan.
> Sections titled **Suggested direction** contain possibilities for the maintainer to
> evaluate. They are deliberately not design decisions or authorization to implement a
> particular solution.

## Executive summary

The feature addresses a real need and has a reasonable high-level decomposition:

- `Remote` is kept separate from local `Storage`;
- repository metadata remains queryable after files move away;
- S3-compatible storage is tested with both mocked and opt-in live backends;
- archive support addresses the many-small-objects problem;
- the extension work recognizes that remote jobs still require a file manifest.

The branch is not ready to merge in its current form. The most serious issues are:

1. remote downloads can write outside their destination;
2. the migration path from beta.7 is broken at the branch head;
3. changing or removing remote configuration can strand jobs;
4. move and fetch are not crash-safe and do not verify complete content;
5. several dependency operations fail once jobs or their dependents are remote;
6. the branch is behind `main`, with conflicts that affect file-manifest semantics;
7. the SQLite index has become authoritative for remote jobs without being treated as
   durable authoritative state.

The existing automated suite passes, but it mainly demonstrates happy paths. Several
failure guarantees written in the specifications are not enforced by the tests.

## Scope and method

Both `main` and `upstream/stable` have the same merge-base with the feature branch:
`9136fbb5893d840450f98507aa0b337b9f056f6e`. The review therefore covers all 46 feature
commits after that point. At review time, `main` also contained 21 commits not present
on the feature branch.

The review included:

- the complete feature diff;
- both remote-storage design documents;
- both step-by-step implementation plans;
- implementation and unit/integration tests;
- comparison with relevant changes on current `main`;
- focused reproductions using temporary repositories and moto-backed S3.

Checks run on the feature branch:

- `239 passed, 3 skipped` in the normal test suite;
- the skipped tests were the opt-in live-S3 tests;
- Ruff passed;
- mypy passed;
- `git diff --check` passed.

No real CEPH/MinIO endpoint was available for this independent review.

## Severity definitions

- **Blocker:** Should be resolved before the feature is used with valuable data or
  merged for general use.
- **High:** A core workflow is unsafe, incorrect, or can leave repository state
  inconsistent.
- **Medium:** Material correctness, API, scalability, or maintainability problem that
  may not affect the simplest happy path.
- **Low:** Smaller robustness, usability, or cleanup issue.

## Findings

### F-01 — Blocker: downloads allow writes outside the destination

Archive downloads call `tar.extractall()` without validating member paths in
[`r3/remote.py`](../../r3/remote.py#L292). A tar member can contain `..`, an absolute
path, a symlink, or a hard link. With the supported Python versions, extraction cannot
assume that unsafe members are filtered automatically.

The individual-object code has the analogous problem in
[`r3/remote.py`](../../r3/remote.py#L302): the suffix of an S3 key is joined directly
onto `destination`. A key such as `<job-prefix>../../outside` can therefore escape the
job directory.

Reproduction result:

```text
archive_wrote_outside_destination: True
```

An actor or process able to modify the remote bucket could overwrite any file writable
by the R3 process. Accidental malformed remote data can also damage unrelated paths.

#### Suggested direction — not a decision

Possible safeguards to evaluate:

- For individual objects, parse the suffix as a relative POSIX path and reject absolute
  paths, `..`, and any resolved destination outside the staging root.
- For tar archives, validate every member before extraction. Consider accepting only
  regular files and directories and rejecting links, devices, and FIFOs unless a real
  use case requires them.
- Extract into a fresh staging directory rather than the final job directory.
- Add tests for `../`, absolute paths, symlink escape, hard-link escape, and malicious
  individual-object keys.

### F-02 — Blocker: the beta.7 to beta.8 migration fails at branch HEAD

[`migration/1_0_0_beta_8.py`](../../migration/1_0_0_beta_8.py#L43) first changes the
repository version to beta.8 and then constructs the current `Repository`. The current
code accepts only beta.9, so the constructor fails before rebuilding the index.

Reproduction result:

```text
migration_beta8_exit: 1
migration_beta8_exception: ValueError(
    "Invalid repository version: 1.0.0-beta.8. Please migrate to 1.0.0-beta.9."
)
migration_beta8_version_after: 1.0.0-beta.8
```

This leaves the repository marked beta.8 while its index can still have the beta.7
schema. Running the beta.9 migration after that can produce a beta.9 repository whose
index still lacks the `location` column.

The beta.9 migration has a related ordering risk: it writes the new version before its
schema alteration in
[`migration/1_0_0_beta_9.py`](../../migration/1_0_0_beta_9.py#L49). If the alteration
fails for an unexpected reason, the format marker can again get ahead of the data.

#### Suggested direction — not a decision

Possible approaches:

- Keep migrations independent of the current version-strict `Repository` constructor;
  perform the specific SQLite transformation directly or use migration-specific
  helpers.
- Make the schema/data change first and write the new format version only after all
  changes succeed.
- Preserve a recoverable copy of the old index until the migration completes.
- Add an end-to-end test that starts from a genuine beta.7-shaped repository and runs
  beta.8 followed by beta.9 using the current checkout.
- Add failure-injection tests proving that the old version and old index remain usable
  when any migration step fails.

### F-03 — High: remote configuration is mutable while jobs depend on it

The index stores only a location string such as `archive`. The exact bucket, prefix,
object key, and storage format are interpreted from the current `r3.yaml` configuration
when the job is fetched.

This means that changing any of the following can make existing jobs inaccessible:

- `archive_format`;
- bucket;
- prefix;
- endpoint;
- backend type;
- credentials/profile assumptions.

For example, a job uploaded as individual objects becomes invisible if the same remote
is later configured with `archive_format: tar.zst`, because
[`S3Remote.download()`](../../r3/remote.py#L278) then looks only for the archive key.

Reproduction result:

```text
fetch_after_format_change: FileNotFoundError: Job not found on remote: <job-id>
```

The CLI also removes an in-use remote unconditionally in
[`r3/cli.py`](../../r3/cli.py#L348). The archived job remains in the index, but a later
fetch fails with an unhelpful `KeyError`:

```text
remove_in_use_remote_exit: 0
fetch_after_remote_removal: KeyError: 'archive'
```

Remote lifecycle semantics are also incomplete:

- `fetch()` keeps the remote object but changes the only recorded location to local;
- the retained object is then an untracked replica;
- removing the fetched local job cannot reliably clean up that remote replica;
- `S3Remote.remove()` has no repository-level lifecycle that calls it;
- moving a job that is already remote has no explicit precondition or cross-remote
  behavior.

#### Suggested direction — not a decision

Two broad models are worth evaluating:

1. **Immutable named remotes while referenced.** Refuse edits/removal when any indexed
   job uses a remote. This is likely the smaller short-term change.
2. **Persisted per-job remote descriptors.** Record the backend/config version, object
   key, representation format, and integrity metadata with each remote copy. This is
   more flexible and can eventually support replicas.

Whichever model is selected, explicitly decide whether `fetch` means:

- move the only copy back to local and delete the remote object; or
- create/retain a second tracked replica.

The current single `location` field cannot accurately represent the second meaning.
Remote removal should at minimum refuse while referenced and report the affected job
IDs. Configuration added through the CLI should be validated before writing `r3.yaml`.

### F-04 — High: move and fetch are not crash-safe

The current move order in
[`Repository.move()`](../../r3/repository.py#L288) is effectively:

1. upload;
2. check that something exists remotely;
3. find dependents;
4. delete local files;
5. update SQLite location;
6. write the cached file list.

A process crash or SQLite error after step 4 but before step 5 leaves the index claiming
that the job is local after its local files have been deleted. Conversely, errors after
upload can leave untracked remote objects.

Fetch writes directly into `jobs/<id>` in
[`Repository.fetch()`](../../r3/repository.py#L330). A corrupt archive already leaves
that destination behind:

```text
corrupt_download_exception: SeekableFormatError
destination_left_behind: True
```

A retry may mix stale partial content with newly downloaded content. Checkout has a
similar issue: a transitive remote dependency can be discovered only after part of the
checkout has already been created.

#### Suggested direction — not a decision

A possible state transition to evaluate for move is:

1. create a canonical manifest;
2. upload to a staging key/prefix;
3. verify the staged representation against the manifest;
4. finalize the immutable remote representation;
5. atomically commit the remote descriptor/location in SQLite;
6. remove local data.

If the process stops after step 5, both copies remain, which wastes space but does not
lose data. The repository must use the catalog location explicitly rather than infer it
from whichever files happen to exist.

A possible fetch flow is:

1. download/extract into a new temporary sibling directory;
2. verify all manifest entries and integrity information;
3. atomically rename it to `jobs/<id>`;
4. atomically commit the local state in SQLite;
5. apply the chosen remote-retention policy.

An explicit transitional state may be useful, but it is not necessarily required if
the ordering and recovery rules are sufficient. The design should state how every
interruption point is recovered.

### F-05 — High: `exists()` does not verify completeness or integrity

For individual-object jobs, [`S3Remote.exists()`](../../r3/remote.py#L338) returns true
when it sees any one object under the job prefix. It cannot distinguish a complete job
from a prefix containing one object after partial deletion or corruption.

For archives, `HEAD` proves that an object exists, not that it is the expected complete
archive. Multipart ETags cannot generally be treated as content hashes, and CEPH/S3
behavior may differ.

Consequently, `fetch()` can restore a subset of an individual-object job without any
download call failing and then mark the job local.

#### Suggested direction — not a decision

Consider making a canonical manifest part of every remote representation, independent
of whether the payload uses individual objects or an archive. Candidate fields include:

- relative path and entry type;
- byte size;
- cryptographic content checksum;
- archive/object representation version;
- whole-representation checksum when practical.

Verification should compare downloaded or staged content to the manifest rather than
use `exists()` as a proxy. If individual-object mode remains supported, a final manifest
or completion marker written only after all payload objects can distinguish finalized
uploads from partial prefixes.

### F-06 — High: remote dependency handling is incomplete

#### Directory dependencies become false negatives

[`Repository.__contains__()`](../../r3/repository.py#L126) checks an exact path against
the cached file list. Manifests contain files, not directory entries, so a dependency
such as `source="output"` becomes missing after the source job is moved even when
`output/result.txt` is present.

Reproduction result:

```text
directory_dependency_before_move: True
directory_dependency_after_move: False
```

#### Remote dependents break `find_dependents()` and `move()`

[`Index.find_dependents()`](../../r3/index.py#L362) materializes every dependent through
local `Storage`. If a dependent is remote, the call raises `FileNotFoundError`.

This can make moving a parent fail after the parent has already uploaded:

```text
move_parent_with_remote_dependent: FileNotFoundError: Job not found: <child-id>
parent_still_local: True
parent_already_uploaded: True
```

#### Checkout validates only immediate dependencies

[`Repository.checkout()`](../../r3/repository.py#L180) checks the requested job and its
immediate job dependencies. Recursive checkout then proceeds inside `Storage`, which
does not know remote locations. An archived transitive dependency therefore raises an
unclear `FileNotFoundError` and leaves a partial checkout:

```text
transitive_checkout_error: FileNotFoundError: Job not found: <leaf-id>
partial_checkout_left: True
```

#### Suggested direction — not a decision

- Define directory membership as a manifest operation. A directory may be considered
  present if it is explicitly recorded or if any manifest entry is below it. Decide
  how empty directories should behave.
- Materialize dependents through an index-aware path rather than directly through
  local `Storage`.
- Preflight the full recursive dependency graph before creating a checkout, or build
  the checkout in a staging directory and rename only after success.
- Add cases for remote parent, remote child, both remote, recursive dependents, file
  sources, directory sources, `source="."`, and empty directories.

### F-07 — High: integration with current `main` is unresolved

At review time the feature had 46 commits after the common base and `main` had 21
commits not present on the feature branch. A read-only merge analysis showed conflict
regions in CLI, job, repository, and test code.

The most important semantic conflict concerns `Job.files`:

- current `main` deliberately excludes `/output` from `Job.files` because that property
  participates in input copying and hashing;
- the feature captures its remote manifest from `job.files.keys()` in
  [`Repository.move()`](../../r3/repository.py#L312);
- resolving the conflict by preserving both behaviors would omit all output files from
  the cached remote manifest even though `S3Remote.upload()` uploads them.

Other `main` changes that must not be lost include package discovery/version metadata,
CLI job-ID behavior and error handling, safer `Storage.__contains__`, default output
exclusion, and ignore-list recursion fixes.

Both branches also independently add `test/test_cli.py`, so the test suites must be
combined rather than choosing one side.

#### Suggested direction — not a decision

Before extensive repair work, consider rebasing or merging the intended target branch
and resolving conflicts deliberately. If `stable` rather than `main` is the intended
target, identify which mainline correctness fixes still need to be incorporated.

Create the remote storage manifest by enumerating the stored job representation with
explicit remote-manifest semantics. Do not reuse `Job.files` if that property is defined
as the set of hashable/copyable inputs.

### F-08 — High: the index is authoritative but still treated like a rebuildable cache

Before remote storage, most index data could be reconstructed from local job
directories. Once a job is remote, SQLite may hold the only local copies of:

- metadata;
- timestamp;
- location;
- cached file manifest;
- dependency edges needed to understand the remote job.

[`Index.rebuild()`](../../r3/index.py#L29) now reads remote rows, deletes the index file,
and constructs a new index in place. A crash or insertion error after deletion can lose
the authoritative remote catalog. This is especially risky because
[`Transaction.__exit__()`](../../r3/index.py#L403) commits even when the context exits
with an exception.

A partial fetch directory can also collide with a preserved remote row during rebuild,
causing a primary-key failure after the old index has already been removed.

The extension specification still says remote-aware rebuilding is unsupported, while
the implementation attempts to support it. The documentation and implementation have
therefore diverged.

#### Suggested direction — not a decision

- Fix transaction semantics so exceptions roll back.
- Build a replacement index in a new file, validate it, and atomically replace the old
  index only after success.
- Preserve a recoverable old index until replacement completes.
- Treat the remote catalog as durable repository data in documentation and backup
  guidance, not merely as a cache.
- Add failure injection during every rebuild stage, including duplicate IDs, malformed
  metadata, missing schema columns, and remote/local collisions.

### F-09 — Medium: missing local data is silently represented as a remote-like job

The extension specification says `Index.get()` and `Index.find()` should select
`location`. The implementation does not. Instead,
[`Index.get()`](../../r3/index.py#L175) catches any `FileNotFoundError` from local
storage and returns a `Job` built from cached fields.

Therefore an indexed job whose location is `local` but whose directory was accidentally
deleted is not reported as local corruption. It is silently returned as a partially
populated remote-like job.

The returned object also lacks cached `r3.yaml` content. Accessing `job.dependencies`
can therefore produce an empty dependency list rather than the job's real dependencies.
That makes `Job` capabilities depend on hidden construction details.

#### Suggested direction — not a decision

- Select and inspect `location` explicitly.
- Treat a missing local directory for a locally indexed job as corruption.
- Decide which operations a remote job object guarantees. Possibilities include caching
  immutable job configuration, returning a distinct indexed/remote projection, or
  raising specific availability errors for unsupported properties.
- Avoid inferring remote state solely from filesystem absence.

### F-10 — Medium: file manifests are eagerly loaded into every query result

[`Index.find()`](../../r3/index.py#L307) selects and deserializes the entire `files` JSON
value for every matching job. The feature is motivated partly by jobs with very many
files, so these JSON values can be large. Metadata-only queries can therefore incur
substantial database I/O, JSON parsing, and memory use for data the caller never needs.

The manifest is also stored as a single JSON value without integrity metadata or a
clear schema/version of its own.

#### Suggested direction — not a decision

Evaluate lazy manifest loading, a normalized manifest table, or a versioned compressed
manifest blob loaded only when requested. The choice should be informed by realistic
file counts and query benchmarks rather than selected prematurely.

### F-11 — Medium: the location filter is interpolated into SQL

[`Index.find()`](../../r3/index.py#L325) appends `location` to SQL through an f-string.
The implementation plan claims the value comes only from internal code, but it is
exposed directly through the CLI and public API.

Reproduction result:

```text
location_injection_count: 2 of 2
```

This was produced with a location resembling:

```text
missing' OR 1=1 --
```

Although users with CLI access can already query their repository, interpolation is
still an avoidable correctness and security defect.

#### Suggested direction — not a decision

Use a bound SQLite parameter for `location` and add a regression test containing quotes
and SQL operators. This review does not expand the finding to the existing general
query translator, which should be assessed separately if its trust model changes.

### F-12 — Medium: promised failure-mode coverage is incomplete

The extension specification explicitly calls for:

- interrupted-upload behavior;
- interrupted-download cleanup;
- cleanup after corrupted archives;
- a clear corruption error;
- content integrity verification.

The existing corrupted-archive test in
[`test/test_remote.py`](../../test/test_remote.py#L395) accepts any exception and does
not assert that the destination was removed. The upload-failure test checks only local
temporary-file cleanup, not whether a partial remote representation remains visible.

The live-S3 cleanup fixture also catches exceptions from `delete_objects`, but S3 can
return HTTP success with per-object failures in an `Errors` response. Those failures
are not currently inspected in
[`test/test_live_s3.py`](../../test/test_live_s3.py#L80).

#### Suggested direction — not a decision

Turn each written failure guarantee into a focused assertion:

- the final destination does not exist after failed download/extraction;
- no finalized remote representation is visible after failed upload;
- retries start from a clean state;
- corruption raises a documented R3 exception;
- individual-object and archive downloads validate identical manifest semantics;
- live teardown inspects and reports per-object `Errors`.

Use fault injection at several points rather than only at the outermost boto call.

### F-13 — Medium: remote-management CLI bypasses repository validation

The `remote add`, `list`, and `remove` commands edit YAML directly in
[`r3/cli.py`](../../r3/cli.py#L266). `remote add` can persist an unknown type or an S3
remote without a bucket. The command reports success, but later constructing any
`Repository` may fail while eagerly loading remotes.

The CLI also does not expose newer archive/addressing/checksum fields, requiring users
to edit YAML manually for documented CEPH configurations.

#### Suggested direction — not a decision

Move configuration validation and lifecycle checks behind repository-level methods,
then let the CLI translate expected failures into `click.ClickException`. Decide whether
the CLI should expose all backend options or whether direct YAML editing is the intended
advanced interface; document one coherent path.

### F-14 — Low: Python 3.9 and the new boto dependency are misaligned

The project declares Python `>=3.9`, while the test environment emits a boto warning
that Python 3.9 support ended on 2026-04-29. Remote storage makes boto a required runtime
dependency, so this mismatch now affects a core installation rather than only an
optional integration.

#### Suggested direction — not a decision

Evaluate raising the minimum Python version to 3.10 or later. Avoid pinning indefinitely
to an unsupported boto release merely to retain Python 3.9 unless the project explicitly
accepts the maintenance and security tradeoff.

## Specification assessment

### What is strong

The specifications are unusually explicit about user workflows, backend compatibility,
test categories, and deferred scope. The live-S3 isolation design is especially useful
for CEPH/MinIO behavior that moto cannot reproduce. The `Remote`/`Storage` separation is
also a sensible architectural boundary.

### What should be strengthened

The specifications spend considerable detail on method signatures and seekable-frame
configuration while leaving the repository's fundamental persistence invariants
underspecified. Before finalizing the design, it should answer:

- What record is authoritative once local files are deleted?
- What exact remote representation belongs to each job?
- How is completeness verified?
- What happens at every interruption point?
- Can a referenced remote be edited or removed?
- Does fetch move a copy or create a replica?
- How are directories, empty directories, outputs, and links represented?
- How can a remote job expose dependencies and immutable job configuration?
- What repository data must be backed up?

The initial design describes `exists()` as upload verification, but existence is not
integrity. The extension design calls SQLite an index while relying on it for remote
state that cannot be reconstructed locally. Those concepts should be revised before
the repair implementation is considered complete.

### Spec and implementation drift

Examples of drift include:

- the extension spec says remote-aware index rebuilding is unsupported, while the
  implementation adds preservation logic;
- the spec says index lookup should select location, while the implementation infers
  remote state from missing files;
- the spec promises interrupted-download cleanup, while the implementation leaves a
  partial destination;
- the implementation plan says `location` is trusted internal input, while the CLI
  accepts it from users.

The two committed implementation plans total roughly 3,700 lines and reproduce large
amounts of literal code. They are already becoming stale. Consider retaining concise
design records and marking implementation transcripts as historical, or removing them
from the long-lived documentation set after the feature stabilizes.

## Complexity and API observations

These observations are lower priority than the correctness findings above.

### `Remote` is a useful abstraction

Keeping remote transport separate from local `Storage` is a good choice. The problem is
not the existence of the abstraction but that repository state transitions and durable
remote identity are not yet modeled around it.

### Dual S3 representations increase lifecycle complexity

Supporting both individual objects and archives doubles several paths: upload,
download, existence, removal, validation, migration, and compatibility after config
changes. Because broad backward compatibility is not yet a strong project constraint,
it may be worth evaluating one canonical representation for newly archived jobs.

That is only a tradeoff to consider. Individual objects retain the original goal of a
browsable, self-describing bucket, while archives solve object-count problems.

### Seekability is motivated by a future feature

Seekable zstd is technically well motivated for future ratarmount integration, but the
current code always downloads the complete archive before extraction. The public
`archive_frame_size` knob therefore has no current user-visible access benefit.

Consider whether it should remain an internal default until mounting is implemented.
Keeping it public may still be reasonable if ratarmount is an imminent, validated next
step.

### File identity and file access may deserve separate APIs

The existing extension changes `job.files` to `Mapping[Path, Optional[Path]]`. That is a
compact and defensible design, but it combines logical membership with local
accessibility and requires all callers to handle `None`.

An exploratory alternative is documented in
[`2026-08-04-job-file-manifest-and-access-proposal.md`](../superpowers/specs/2026-08-04-job-file-manifest-and-access-proposal.md).
It proposes a persistent `job.file_paths` manifest and a materialized `job.files`
mapping, with explicit repository-managed mounting in the future.

That document is intentionally non-final. This API question should be revisited only
after the current correctness and data-safety problems are fixed.

## Suggested repair sequence — not a decision

The following ordering is offered to reduce rework; it is not an approved plan.

1. **Choose and integrate the intended base branch.** Resolve current `main` changes,
   especially output/manifest semantics, packaging, CLI behavior, and tests.
2. **Fix the immediate security issue.** Secure both archive extraction and
   individual-object destination handling, with adversarial tests.
3. **Repair and test migrations.** Establish a reliable beta.7 to beta.9 path before
   changing the schema further.
4. **Revise the persistence invariants in the design.** Decide remote identity,
   manifest/integrity semantics, remote configuration lifecycle, fetch retention, and
   interruption recovery.
5. **Implement crash-safe move/fetch around those invariants.** Use staging, validation,
   atomic local publication, and safe catalog ordering.
6. **Make the remote catalog durable.** Fix rollback, atomic index rebuild, backup, and
   explicit location handling.
7. **Repair dependency and checkout behavior.** Include directory and transitive cases,
   and ensure failure leaves no partial checkout.
8. **Strengthen failure tests and run live S3 tests.** Exercise both representations
   against the actual target CEPH/MinIO deployment.
9. **Revisit optional API improvements.** Only after correctness is established, decide
   file-manifest APIs, ratarmount integration, and whether both S3 representations are
   worth retaining.

## Suggested acceptance criteria — not a decision

The maintainer may wish to adapt the following as merge criteria:

- No remote-controlled path can escape its staging/destination root.
- A complete beta.7 repository migrates to the current format using scripts at branch
  HEAD, and injected failures leave a recoverable old repository.
- Referenced remote configuration cannot be silently changed or removed in a way that
  strands jobs.
- Every remote job has a stable, persisted representation descriptor.
- Interrupted move/fetch operations cannot produce data loss and have documented
  recovery behavior.
- Fetch verifies a manifest/checksum before publishing local state.
- Missing local data for a `local` row is reported as corruption.
- Directory, file, root, and transitive remote dependencies behave consistently.
- Index rebuild failure preserves the previous authoritative catalog.
- Location filtering uses bound SQL parameters.
- All written failure-mode guarantees have explicit tests.
- Current mainline correctness fixes and tests remain present after integration.
- Live tests pass against the intended S3-compatible deployment, with cleanup errors
  reliably reported.

## Final assessment

The feature has a useful core and substantial test effort, but the present design is
still shaped like a local cache extension rather than a durable remote-storage system.
Because R3 stores research artifacts and removes the local copy during `move`, the
standard should be closer to a data-migration tool: immutable identity, verified
content, explicit lifecycle, atomic publication, and recoverable interruption.

Addressing those invariants first is likely to simplify several downstream issues. It
will clarify what belongs in SQLite, what a `Remote` must guarantee, what `Job` can expose
while remote, and how future mounting should fit into the API.
