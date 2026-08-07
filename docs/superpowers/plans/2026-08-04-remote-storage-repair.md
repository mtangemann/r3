# Remote Storage Repair — Implementation Plan

> **For agentic workers:** implement task-by-task with TDD (failing test → watch it
> fail → implement → watch it pass → commit). Invariants, layout, state machines, and
> the manifest contract are in the **frozen** design:
> [`docs/superpowers/specs/2026-08-04-remote-storage-durable-design.md`](../specs/2026-08-04-remote-storage-durable-design.md)
> (four external review rounds + internal panel; converged). This plan is the ordered
> task list; the design is the reference. Section refs (§N) point at the design.

**Goal:** repair `feature/remote-storage` to the durable-remote-storage standard —
secure extraction, verified content, atomic/retryable state transitions, a durable
bucket-authoritative catalog, correct dependency behavior — and integrate current
`main`.

**Approach:** integrate `main` first; then build outward from the manifest primitive
through crash-safe transitions, index durability, dependency behavior, and
CLI/lifecycle. TDD; one commit per task; a regression test for every bug fixed.

**Constraints:** preserve untracked `templates/`. No live-S3 tests in this env
(`-m "not live_s3"`; set `AWS_CONFIG_FILE=/dev/null` per the CEPH memo). Don't push.
Final gate: full pytest (minus `live_s3`), ruff, mypy (`r3 test migration`),
`git diff --check`.

**Remote layout (§4), four objects per job:** `data.tar.zst` (file members only —
manifest files minus the two sidecars), `r3.yaml` (sidecar), `metadata.yaml`
(sidecar), `manifest.json` (integrity/listing; published last via verified
staging-copy).

---

## Phase A — Integrate `main` (F-07)

Prerequisite for everything else. Merge (don't rebase — published branch).

### A0. Commit the design docs, tag a backup
- [ ] Commit the new docs (design, this plan, `LIMITATIONS.md`, the three review docs)
  as one docs commit. Do **not** stage `templates/`.
- [ ] `git tag pre-merge-main-backup`.
- [ ] `git merge main --no-commit` (expect conflicts: `pyproject.toml`, `r3/cli.py`,
  `r3/job.py`, `r3/repository.py`, `r3/storage.py`, `test/test_cli.py`,
  `test/test_repository.py`).

### A1. Resolve conflicts
- [ ] **`r3/job.py`** → take `main`'s version wholesale (`Job.files: Mapping[Path,Path]`
  with the copied ignore list + `/output` exclusion). This *is* the §8 revert: drop
  `cached_file_paths`, the `Optional` widening, the `{p: None}` branch, and the
  `assert source is not None` lines.
- [ ] **`r3/storage.py`** → take `main`'s safer `__contains__` + return annotation +
  docstring; drop the feature's `assert source is not None`.
- [ ] **`pyproject.toml`** → keep both sides; set `requires-python = ">=3.10,<3.13"`
  (§13); move `pyzstd` from the optional `archive` extra into required `dependencies`
  (§3); keep `boto3`, `boto3-stubs`, the `live_s3` marker, main's packaging/version
  metadata.
- [ ] **`r3/repository.py`** → union: main's constructor error handling + return
  annotations, plus the feature's remote/move/fetch/remotes members. (Bodies are
  rewritten in later phases; here just merge-clean + importable.)
- [ ] **`r3/cli.py`** → union: main's job-ID commands + error handling
  (`edit`/`checkout`/`remove`) plus the feature's `move`/`fetch`/`remote` commands.
- [ ] **`test/test_cli.py`**, **`test/test_repository.py`** → combine both suites; keep
  every test from both sides. Remove only feature tests asserting the reverted
  `Optional`/`{p: None}` behavior; convert any asserting a remote job's `.files` to
  expect `FilesUnavailableError` (mark `xfail` with a `# Phase B` note if B isn't in yet).

### A2. Verify + commit
- [ ] `AWS_CONFIG_FILE=/dev/null python -m pytest -m "not live_s3" -q` → green (adjust
  reverted-API tests). Then `make lint`.
- [ ] Confirm `git status` still shows `templates/` untracked and intact.
- [ ] `git commit` (merge): `:twisted_rightwards_arrows: Merge main; revert interim
  Job.files API`. **Pause for review.**

---

## Phase B — Manifest primitive, archive-only S3Remote, safe extraction (F-01, F-05)

### B1. Manifest module (`r3/manifest.py`)
- [ ] Tests: `build_manifest` from a temp job dir produces `files[]` = full logical set
  (incl. `r3.yaml`, `metadata.yaml`, all `output/…`) as relative POSIX strings with
  per-file `size`+`sha256`, plus `archive_sha256`, `archive_size`, `representation`,
  `manifest_version`; **no** `dependencies`/`timestamp`. Round-trip `dumps`/`loads`;
  reject unknown `manifest_version`. `verify_directory` passes on a faithful dir and
  fails on any mismatch/extra/missing.
- [ ] Implement per §4.1: `build_manifest(...)`, `dumps`/`loads`,
  `verify_directory(job_dir, manifest)`. Per-file hashes are computed by the caller
  in the same pass that writes the archive (§5 step 1) — the module accepts precomputed
  file entries; it does not do its own independent walk.

### B2. Archive-only `S3Remote` primitives
- [ ] Remove all individual-object branches. `pyzstd` imported unconditionally.
- [ ] Archive builder: given the job dir + the resolved file set, add each file as an
  **individual member** (no directory entries; exclude `r3.yaml`/`metadata.yaml`),
  hashing each as written; return `archive_sha256`, `archive_size`, and per-file hashes.
  Reject special members (symlink/hardlink/device/FIFO) before producing the archive
  (§11).
- [ ] Multipart config explicit (§3, review Q3): set `multipart_threshold`, part size,
  max concurrency on the `TransferConfig` rather than SDK defaults.
- [ ] Object primitives (moto tests each): `upload_archive`, `upload_sidecar`,
  `download_object`/`download_to`, `head` (size), `delete_keys` (inspect per-object
  `Errors`), `list_prefix` (paginated), `exists(job_id)` = HEAD on `manifest.json`.
- [ ] `publish_manifest(job_id, manifest_bytes)` via **verified staging-copy** (§5
  step 5): PUT `…/manifest.json.staging` → GET+byte-compare → low-level
  `copy_object` (with `CopySourceIfMatch` = staging ETag; process full response, treat
  embedded errors as failure) → delete staging.
- [ ] Tests (moto): after a full publish, exactly the 4 keys exist, `metadata.yaml`
  and `r3.yaml` are **not** inside the archive; `exists` false when only
  archive/sidecars present (no manifest); a truncated/omitted archive is detectable by
  `head` size.

### B3. Safe extraction (F-01) — adversarial
- [ ] Tests (from the confirmed repro): tar member with `..`, absolute path, symlink,
  hardlink, duplicate name, a member named `r3.yaml`/`metadata.yaml`, and
  oversized/too-many members → each raises a clear R3 error and writes **nothing**
  outside the staging root.
- [ ] Implement `safe_extract(tar_stream, staging_root, manifest)` per §11: streaming
  (`r|`), per-member validation before write (no abs/`..`/outside-root; files only;
  no dup; name agrees with a non-sidecar manifest path; reject sidecar names; count/size
  limits); create parent dirs; ensure `output/` exists.

### B4. `Job` remote projection + `FilesUnavailableError`
- [ ] Test: a remote-projection `Job` returns cached `id`/`timestamp`/`metadata`;
  `.files`, `.hash()`, `.dependencies` each raise `FilesUnavailableError`.
- [ ] Implement (§8): add `FilesUnavailableError(RuntimeError)`; a private remote marker
  on `Job` (set only by `Index`); guard the three accessors. Public `Job.files` type
  unchanged.

---

## Phase C — Migrations (F-02)

### C1/C2. Repair beta.8 + beta.9
- [ ] Tests: from a genuine beta.7-shaped fixture, beta.8 then beta.9 at HEAD →
  version marker advances, index reaches HEAD **schema** (assert columns `location`,
  `files` present), no version-strict-`Repository` error.
- [ ] Fix `migration/1_0_0_beta_8.py` and `_9.py` (§12): migration-local SQLite (never
  construct `Repository` or reuse the live `Index`); write the version marker **last**
  via temp+`os.replace`; back up the index and **refuse to overwrite an existing
  `.bak`** (offer to restore).
### C3. Failure injection
- [ ] Test: a simulated *process* interruption between the schema change and the
  version write leaves the old version + a usable index; a re-run does not clobber the
  good `.bak`.

---

## Phase D — Crash-safe move/fetch (F-04, F-05)

### D1. `Repository.move` (§5, 8-step)
- [ ] Tests (moto): capture builds a files-only archive + manifest by a single pass
  (not `job.files`); stale manifest is **invalidated (delete confirmed) before** any
  payload overwrite; every payload object **and** the manifest are content-verified
  before the index commit; manifest published via staging-copy; quiescence re-check
  aborts + discards the published manifest if the dir changed since capture; index
  flips to `remote` then local deleted atomically (`os.replace`→`.trash`→`rmtree`).
  Ordering test: inject a sha mismatch → aborts before the manifest is published and
  before local delete; local job survives. Precondition `location=='local'`.
- [ ] Implement per §5.

### D2. `Repository.fetch` (§6, with receipt)
- [ ] Tests (moto): step-0 idempotent finalize (pre-existing valid `jobs/<id>` →
  verify against remote-manifest **or** receipt, agree-if-both, short-circuit to
  cleanup+flip); receipt written via temp+`os.replace`; archive sha verified; safe
  extraction; sidecars written from sidecar objects (rejected if in the archive);
  atomic rename; cleanup idempotent/unconditional (manifest first, inspect `Errors`);
  index flips to local **last**; receipt deleted (failure non-fatal). Corrupt archive →
  clear error, **no `jobs/<id>`**, index still `remote`.
- [ ] Implement per §6.

---

## Phase E — Index durability (F-08, F-09, F-10, F-11)

### E1. Transaction rollback
- [ ] Test: an exception inside a `Transaction` leaves the DB unchanged.
- [ ] Fix `Transaction.__exit__`: rollback on exception, commit only on clean exit.
### E2. Atomic, bucket-backed, fail-closed rebuild (§7.2)
- [ ] Tests (moto): rebuild restores a remote row from the bucket (deps/timestamp from
  the `r3.yaml` sidecar, metadata from `metadata.yaml`, files from the manifest);
  local-wins on a job present both places; **fails closed** (old index intact, reported)
  on a corrupt/missing sidecar, a schema violation, a job-id/key mismatch, or a
  missing/size-mismatched archive (HEAD); **aborts** on any LIST/read error;
  **rejects duplicate job-id across remotes**; a `jobs/<id>` missing `r3.yaml` aborts.
  Builds into `index.sqlite.new`, commit+close, then `os.replace`; a stale `.new` is
  discarded.
- [ ] Implement per §7.2.
### E3. Explicit location; absence = corruption (§7.3)
- [ ] Test: `get`/`find` select `location`; a `local` row with a missing dir (or missing
  `r3.yaml`) raises corruption; a `remote` row → projection.
- [ ] Implement; remove the `except FileNotFoundError` inference.
### E4. Lazy file-list (F-10) + bound location param (F-11)
- [ ] Tests: metadata-only `find` doesn't load `files`; `find(location="missing' OR
  1=1 --")` returns nothing (bound param).
- [ ] Implement.

---

## Phase F — Dependencies & checkout (F-06)

### F1. Directory dependencies
- [ ] Test: `source="output"` still contained after move (any manifest entry beneath);
  `source="."` iff file list non-empty; conventional empty `output/` treated present
  for a complete remote job (§10).
- [ ] Implement `__contains__` via `get_file_list`.
### F2. Remote dependents
- [ ] Test: `find_dependents` returns a remote child without raising; moving a parent
  with a remote dependent succeeds.
- [ ] Implement via the location-aware projection (not `Storage.get`).
### F3. Transitive-checkout preflight
- [ ] Test: checkout of a job with an archived *transitive* dep raises "fetch first"
  and leaves **no** partial checkout; a non-recursive edge to a remote grand-dep does
  not over-refuse; a top-level remote job reports "fetch first" (not
  `FilesUnavailableError`).
- [ ] Implement per §10: a shared helper mirroring `Storage.checkout_job`'s actual
  traversal; the locality preflight runs **before** `resolve()` on the top-level item.

---

## Phase G — CLI + lifecycle (F-03, F-13)

### G1. `remove` protocol (§9)
- [ ] Tests: remove a remote job deletes its 4 keys + staging; remove a local job sweeps
  **every** configured remote + local receipt/`.fetch/`/`.trash/` artifacts; retry from
  the raw row tolerates a missing local dir; referenced-by guard; per-object `Errors`
  inspected.
- [ ] Implement.
### G2. `r3 remote check` (read-only, §9)
- [ ] Tests (moto): reports orphan/location-disagreeing manifests, manifestless
  prefixes, staging manifests (even beside a final), missing/size-mismatched archives,
  incomplete multipart; mutates nothing.
- [ ] Implement.
### G3. `remote add`/`remove` validation (F-03, F-13)
- [ ] Tests (CliRunner): `remote add` unknown-type / S3-without-bucket → `ClickException`,
  `r3.yaml` unchanged; valid add persists + re-opens; CEPH flags round-trip
  (`--addressing-style`, `--request-checksum-calculation`,
  `--response-checksum-validation`); `r3.yaml` written via temp+`os.replace`.
  `remote remove` refuses while complete manifests exist under the prefix (bucket check,
  not index); refuses residual debris unless `--force` (which reports it).
- [ ] Implement; `archive_format` is not a CLI flag.
### G4. `r3 edit` refuses remote jobs
- [ ] Test: `edit` on a remote job → clear "fetch first", **no stray file**; also the
  `except KeyError` returns (no unbound-`job` deref).
- [ ] Implement.

---

## Phase H — Failure tests, docs, live-S3, final gate (F-12, F-14)

### H1. Failure-mode assertions (F-12)
- [ ] failed download/extraction leaves no `jobs/<id>`; failed upload leaves no
  finalized manifest (`exists` false); retries start clean; corruption raises a
  documented R3 error; `delete_objects` per-object `Errors` asserted in `fetch`/`remove`
  and the live-S3 teardown.
### H2. Live-S3 (opt-in, skipped here) — CEPH/multipart (review Q3)
- [ ] A `@pytest.mark.live_s3` `move`→`fetch` over an archive **> multipart_threshold**
  (≥3 unequal parts); confirm multipart was used, full GET+sha; failure-after-parts +
  abort/list perms; `CopyObject` success + pre-copy failure + post-copy interruption;
  per-object delete errors; conservative concurrency. Document a bucket lifecycle rule
  for incomplete multipart. (Run manually against CEPH before deployment; not in CI.)
### H3. Docs
- [ ] "Superseded by durable-design" banners on the two old design docs; update
  `CONTRIBUTING.md` live-S3 instructions + the new CEPH flags; keep `LIMITATIONS.md`
  current.
### H4. Final verification
- [ ] `AWS_CONFIG_FILE=/dev/null python -m pytest -m "not live_s3"` all pass;
  `make lint` clean; `git diff --check` clean.
- [ ] Produce the F-01…F-14 + C/T disposition summary.

---

## Deferred (see design §14 / `LIMITATIONS.md`)
`job.file_paths`/mounting; ratarmount; `FilesystemRemote`; `r3 copy`/replicas; the
mutating `r3 gc`; shared/collaborator remotes; an enforced repository lock; power-loss
`fsync`; query-based batch `move`; manifest v2 per-file offsets.
