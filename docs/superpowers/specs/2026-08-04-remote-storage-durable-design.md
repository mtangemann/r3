# Remote Storage: Durable Design (revised)

**Date:** 2026-08-04
**Status:** Approved design for the `feature/remote-storage` repair work.
**Supersedes:** the changed parts of
[`2026-02-11-remote-storage-design.md`](../../plans/2026-02-11-remote-storage-design.md) and
[`2026-03-21-remote-storage-extensions-design.md`](2026-03-21-remote-storage-extensions-design.md).
Where this document and those disagree, this document wins. Those two are retained
as historical records of the original design.
**Explicitly out of scope:** the `job.file_paths` / mounting API redesign in
[`2026-08-04-job-file-manifest-and-access-proposal.md`](2026-08-04-job-file-manifest-and-access-proposal.md).
That remains exploratory and is a separate later PR.

**Revision note (2026-08-04):** revised after a 4-lens internal spec review
(readiness, data-safety, S3/CEPH realism, findings-faithfulness). Material changes
from the first draft: `r3.yaml` moved out of the archive to a sidecar and the
manifest reduced to a pure integrity/listing record (single source of truth for the
dependency graph); **every** uploaded object is content-verified before publish, not
just the archive; local deletion and index rebuild are made atomic; `remove` handles
remote jobs and sweeps orphans; a read-only `r3 remote check` is added.

**Revision note (2026-08-05):** revised after an independent external review
([`2026-08-05-remote-storage-durable-design-review.md`](../../reviews/2026-08-05-remote-storage-durable-design-review.md)).
Material changes: `move` invalidates a stale manifest **before** overwriting payload
keys and content-verifies the manifest itself (making it a reliable publication
marker across crash-and-retry); `rebuild` **fails closed** with a validation
boundary instead of silently omitting a job whose remote artifacts don't reconstruct;
`fetch` cleanup is idempotent/unconditional and keeps the index at `remote` until
cleanup completes; `remove` gets an explicit retryable interruption protocol and
checks the **bucket** (not the index) before dropping a referenced remote; a
quiescence precondition + pre-delete stability check for `move`; one symmetric
regular-files-only filesystem model; the crash model is scoped to
**process-interruption, not power-loss** (fsync follow-up); and user-facing
limitations are collected in a top-level [`LIMITATIONS.md`](../../../LIMITATIONS.md).

**Revision note (2026-08-06):** revised after the external *confirmation* review
([`2026-08-05-remote-storage-durable-design-confirmation-review.md`](../../reviews/2026-08-05-remote-storage-durable-design-confirmation-review.md)),
which confirmed D2-02 resolved and D2-01's overwrite defect resolved but found
residual protocol/contract issues (C-01…C-06). Material changes: the manifest is now
published via a **verified staging-copy** (PUT staging → GET-verify → server-side copy
to the final key), closing the "manifest visible before verified" window; `fetch`
persists a local **manifest receipt** so a retry can verify after the remote manifest
is deleted; `remove` retries from the **raw index row** and sweeps **every** configured
remote; the archive holds **file members only** (no directory members); `rebuild` and
`remote check` add an **archive existence/size** check; `remote remove` guards residual
manifestless debris; and the concurrency limitation is corrected to promise **no**
data-safety/recovery guarantee. A subsequent third-pass confirmation
([`2026-08-06-remote-storage-durable-design-third-pass-review.md`](../../reviews/2026-08-06-remote-storage-durable-design-third-pass-review.md))
found no remaining data-loss blocker; its tidy-ups are folded in: the `CopyObject`
success contract is made explicit (low-level single-op copy, `CopySourceIfMatch` on the
staging ETag, process the full response) and I8 distinguishes direct vs derived
verification; `remove` and `remote check` also sweep/report the transient recovery
artifacts (remote `manifest.json.staging`, the fetch receipt, `.fetch/` staging dirs,
`.trash/` entries); and the archive-member set is corrected to **(manifest files − the
two sidecars)**, with `fetch` rejecting a sidecar found inside the archive.

---

## 1. Why this revision exists

The independent review
([`2026-08-04-remote-storage-feature-review.md`](../../reviews/2026-08-04-remote-storage-feature-review.md))
found that the feature was "shaped like a local cache extension rather than a
durable remote-storage system." Because R3 **deletes the local copy** during
`move`, the correct standard is that of a data-migration tool: immutable identity,
verified content, explicit lifecycle, atomic publication, and recoverable
interruption.

The fixes we adopt change the feature's *persistence invariants*, not just its
code. This document states those invariants first, then derives the layout,
state machines, and recovery behavior from them.

---

## 2. Core invariants

These hold at all times and are the acceptance standard for the repair.

**I1 — The remote objects are the source of truth; SQLite is a pure cache.**
For any remote job, the authoritative record lives in the remote bucket. The
SQLite index caches metadata, timestamp, location, dependency edges, and the file
list for fast local queries, but every cached value is reconstructible by reading
the bucket. `rebuild-index` restores a remote job's row from the bucket alone, and
reads each authoritative artifact **directly** (dependencies/timestamp from
`r3.yaml`, metadata from `metadata.yaml`) rather than from a derived copy — so a
rebuilt index cannot silently disagree with the job it describes. (Resolves F-08;
answers the proposal's "SQLite, remote, or both?" — both, remote authoritative.)

**I2 — A remote job is immutable in this release.** Every mutation path (`r3 edit`,
etc.) refuses on a remote job with a clear "fetch first" error, so today a remote
job never changes until it is fetched, and editing requires `fetch → edit → move`.
The *layout* (metadata as a separate object, §4) is deliberately built so a future
release can support editing an archived job's metadata in place — but that is **not
enabled now**. (See §9 for that forward path.)

**I3 — "An object exists" is never proof of a complete, valid job.** Completeness
is established only by the manifest object, which is written last and lists every
logical file with its checksum. `exists()`/`HEAD` are liveness hints, not integrity
proofs. (Resolves F-05.)

**I4 — No remote-controlled path may escape its destination.** Every path derived
from remote data (tar member names) is validated against its staging root before any
write. (Resolves F-01.)

**I5 — Interruption never loses data.** At every interruption point in `move`,
`fetch`, and `rebuild`, either the old state or the new state is fully intact; a
partial state is never published as authoritative. Wasting space (both copies
present) is acceptable; losing or corrupting a job is not. (Resolves F-04.)

**I6 — Referenced remote configuration cannot be silently invalidated.** Removing
a remote that jobs already live on is refused with the affected job IDs named, and a
job's storage representation is pinned by its own `manifest.json` (not by current
config). Re-pointing a remote's prefix/bucket/endpoint without moving the objects
fails *loudly* at fetch (missing manifest), never silently. (Resolves F-03.)

**I7 — Location is explicit, absence is corruption.** A job's location is read from
the index, never inferred from whether local files happen to exist. A job marked
`local` whose directory is missing (or present but missing its `r3.yaml`) is
reported as corruption, not silently treated as remote. (Resolves F-09.)

**I8 — Verify content before you publish or delete.** No authoritative state
transition is committed until the objects it depends on have been verified — by one of
two means: **direct** verification (download-and-rehash / byte-compare against the
manifest, not existence/HEAD) for the payload objects and the *staging* manifest; and
**derived** verification for the final `manifest.json` marker, whose bytes are created
by an atomic server-side copy from the already-direct-verified staging object (§5
step 5), so no client-upload interval ever exposes an unverified final marker. (This
treats a small single-operation `CopyObject` as atomic + byte-faithful — a backend
capability to demonstrate on the exact RGW; a separate completion marker is the
fallback if it cannot be trusted.) Correspondingly, publication is *invalidated before
it is overwritten*: a stale manifest is deleted (delete confirmed) before any payload
key is rewritten, so "manifest present" can never describe a half-overwritten payload
after an interrupted retry. The losing copy is deleted only after the winning copy is
verified present. This is what makes I3/I5 real on a backend (CEPH RGW under
`when_required`) that does not validate uploads server-side.

**Assumption — crash model is process-interruption, not power loss.** "Interruption"
throughout this document means process termination (crash, signal, `KeyboardInterrupt`)
with the filesystem intact. R3 uses temp files, SQLite transactions, and `os.replace`
for process-interruption safety, but does **not** yet `fsync` new files and their
parent directories, so a host power loss mid-operation is not covered. In practice
the authoritative stores already cover the important cases (SQLite commits are
`fsync`-durable by default; S3/CEPH is service-durable), leaving only the transient
local rename/`.trash`/staging steps, which are recoverable by re-running. Explicit
power-loss `fsync` is a documented follow-up.

**Known limitation — single writer, NOT enforced.** These transitions assume one
writer per repository. Concurrent `move`/`fetch`/`remove`/`rebuild` on the same
repository is **unsupported and carries no data-safety or automatic-recovery
guarantee.** The single-writer protocols are what this design verifies; run
concurrently they can corrupt the index and, in some interleavings, cause loss R3
cannot auto-recover — e.g. `r3 remote remove` seeing an empty prefix while a
concurrent `move` is still uploading can drop the remote config, after which the move
publishes and deletes local, leaving surviving bucket objects with no recorded
endpoint/bucket/prefix to reach them. `rebuild` in particular is a whole-index writer
and must not overlap the others. R3 does **not** enforce this (no
lock); a repository-lock design is deferred (§14) — most relevant once concurrent
writers become normal (shared-remote collaboration) and best designed with it. This
is recorded prominently in [`LIMITATIONS.md`](../../../LIMITATIONS.md) so users do not
infer that concurrent mutation is safe.

---

## 3. Representation: archive-only

`S3Remote` stores each job as a **single seekable `tar.zst` archive plus three
sidecar objects** (`r3.yaml`, `metadata.yaml`, `manifest.json`). The pre-existing
"individual objects" representation is **removed** — no repository has been migrated,
and dropping it halves the surface that needs security hardening, crash-safety, and
adversarial testing. The browsable-bucket property that individual objects provided
is recovered by the sidecars (§4). `FilesystemRemote` (future) is a different
*backend*, not a different representation, and is unaffected by this decision.

Consequence: `pyzstd` becomes a **required** runtime dependency of `S3Remote`
(moved out of the optional `archive` extra), because there is no non-archive path
left. The read-compatibility argument from the extensions spec still holds: a
`pyzstd`-produced seekable `.tar.zst` is readable by any standard zstd tool.

Because the archive is now the primary (and only) upload path, and `boto3`'s
`upload_file` switches to **multipart** above its 8 MB threshold, the multipart path
is production-critical and **must be exercised on the real CEPH endpoint** — the
current live tests only ever upload sub-threshold objects (see §12 / repair plan).

---

## 4. Remote layout

Per job, under the remote's configured `{prefix}`, four objects:

| Object key | Mutable? | Purpose |
|---|---|---|
| `{prefix}{job_id}/data.tar.zst` | no | The immutable bulk payload: **input files + `output/` only.** Excludes `r3.yaml` and `metadata.yaml` (both sidecars below). |
| `{prefix}{job_id}/r3.yaml` | no | The immutable provenance record: resolved dependencies, timestamp, config. The **authoritative** source of the dependency graph; read directly by `rebuild-index`. |
| `{prefix}{job_id}/metadata.yaml` | not yet (see §9) | The job's metadata — the **single** copy. A future release can edit it in place; **not editable in this release** (I2). |
| `{prefix}{job_id}/manifest.json` | no | Pure integrity + listing record. **Written last** — its presence marks the job complete. |

Notes:

- Object count is **O(1) per job** (four), so the CEPH object-count concern that
  motivated archiving is fully addressed. A multipart-stored archive still counts as
  one object in the bucket index.
- The bucket is **self-describing and browsable**: `r3.yaml`, `metadata.yaml`, and
  `manifest.json` are plain readable objects per job prefix; you can read a job's
  dependencies and metadata without decompressing a multi-GB archive.
- **`r3.yaml` and `metadata.yaml` are sidecars, excluded from the archive.** This
  gives a single source of truth for each: the dependency graph and timestamp live
  only in `r3.yaml`; metadata lives only in `metadata.yaml`. Nothing is duplicated
  between a sidecar and the archive, so no sidecar can go stale against an archived
  copy. `metadata.yaml` is additionally the one part a future release may edit in
  place (§9); `r3.yaml` is immutable but is a sidecar too so that `rebuild-index`
  reads the *authoritative* dependency graph directly (I1) instead of a derived copy.
- **`fetch` reconstructs the full job directory** by extracting the archive (inputs
  + output) and writing `r3.yaml` and `metadata.yaml` from their sidecars (§6). Each
  is content-verified against the manifest.
- **Git dependencies are unaffected.** Bare git clones live at repository level
  (`{repo}/git/`), never inside a job directory, so they are neither archived nor
  moved. The `r3/{job_id}` tag created at commit persists through `move`/`fetch`
  (neither touches `git/`), so a moved-and-fetched job's `GitDependency` still
  resolves. Uploading git clones remains out of scope (per the original design).

### 4.1 Manifest contract

`manifest.json` is a UTF-8 JSON object — a **pure integrity + listing** record. It
deliberately carries **no** timestamp and **no** dependency list (those are read
from the authoritative `r3.yaml`, I1) and **no** absolute object keys:

```json
{
  "manifest_version": 1,
  "job_id": "<uuid>",
  "representation": "tar.zst",
  "archive_sha256": "<hex>",
  "archive_size": 123456,
  "files": [
    {"path": "r3.yaml", "size": 812, "sha256": "<hex>"},
    {"path": "metadata.yaml", "size": 96, "sha256": "<hex>"},
    {"path": "output/result.pt", "size": 10485760, "sha256": "<hex>"}
  ]
}
```

- `files` is the whole *logical* file set — a full walk of `jobs/<id>/` including
  every input, all of `output/`, and both sidecars (`r3.yaml`, `metadata.yaml`) — as
  relative POSIX strings with no leading `./`. Note this is a **superset** of
  `Job.files`, which excludes `output/` (§8), so **do not** build the manifest from
  `Job.files`. `move` sets the index `files` column to exactly this list (§5 step 7).
  Physically, `r3.yaml` and `metadata.yaml` are sidecars and the rest live in
  `data.tar.zst`; `fetch` verifies each entry regardless of where it physically
  resides.
- **Per-file hashes are computed from the same bytes that are written to the
  archive / uploaded as sidecars**, in a single pass — *not* from an independent
  directory walk. This closes the gap where a separate walk and a separate tar pass
  could disagree (empty dirs, exclusion mismatches, a mid-build bit-flip) and produce
  a manifest the archive can never satisfy. `archive_sha256` is the SHA-256 of the
  whole `data.tar.zst`; verifying it end-to-end (§5 step 4) therefore proves the
  stored archive matches exactly the **archive-resident** files the manifest lists.
  The two sidecar entries (`r3.yaml`, `metadata.yaml`) are not in the archive and are
  proven by the separate per-object content-verify in that same step.
- Keys are derived at read time from the remote's current `{prefix}` + `{job_id}` +
  the fixed layout, so changing a remote's prefix (and moving the objects) needs no
  manifest rewrite. `representation` pins the storage *format* per job.
- `manifest_version` allows additive evolution (e.g. a future v2 adding per-file
  archive offsets for ratarmount-free extraction). Readers reject versions they do
  not understand.
- The manifest is the F-05 completion marker: **uploaded strictly last**, so its
  presence means the archive and both sidecars are already fully uploaded **and
  content-verified** (§5).

### 4.1.1 The manifest is not a universal `job.file_paths`

The manifest is the **S3 / immutable-representation's** integrity+listing record.
Its per-file `sha256` values are meaningful *only because an archived job is
immutable* (I2). It is therefore **not** the general `job.file_paths` abstraction
(the deferred proposal), and must not be promoted to one:

- A future `job.file_paths` (logical file membership, available for local and remote
  jobs) is a **backend-specific projection**: a local job walks its directory; an S3
  remote job derives `file_paths` from *this manifest's* path list; a
  `FilesystemRemote` job lists the live remote directory.
- A `FilesystemRemote` points at a **mutable** shared filesystem (its `output/` is
  not frozen), so binding per-file hashes there would be wrong — which is exactly why
  the original design gives `FilesystemRemote` `cache_file_list = False`. Such a
  backend needs a **hash-free listing**, not this manifest.

So the manifest stays the S3 backend's record (in `r3.manifest`, used by
`S3Remote`); `file_paths`, when it lands, is a thin logical view sourced per backend.
This is documented so a future change does not assume "manifest = file_paths
everywhere."

### 4.2 Why a manifest rather than reading the archive

Reading the file list out of the archive requires walking every tar header; headers
are interleaved with file data, so for a many-small-files job (the case archiving
exists for) that degenerates into decompressing the whole archive. tar also
checksums only headers, never file contents, so integrity verification needs
checksums we compute ourselves regardless. The manifest makes listing and integrity
O(1)-per-job and independent of ratarmount. ratarmount remains a future, optional
enhancement for random *access* (mounting / targeted extraction); it is never on a
core path (`rebuild-index`, dependency validation, integrity).

---

## 5. `move` state machine

`Repository.move(job_id, remote_name)` — publishes a local job to a remote and
deletes the local copy. Ordered so that I5/I8 hold at every step.

```
Preconditions:
  - remote_name is configured                          else ValueError
  - job_id exists and location == "local"              else KeyError / ValueError

 1. Capture, single pass over jobs/<id>/: create the archive as INDIVIDUAL file
    members — the archive member set is exactly (manifest files − {r3.yaml,
    metadata.yaml}); the two sidecars are NOT archive members, and NO directory
    members are written (§11) — hashing each member as it is written; also hash the
    r3.yaml and metadata.yaml sidecar bytes. Assemble the manifest (files[] = the full
    logical set incl. both sidecars, with per-file size+sha256, archive_sha256,
    archive_size). No deps/timestamp in it. Record the captured file set + sizes/mtimes
    (for step 6).
 2. Invalidate any stale publication: if a manifest already exists at the job's
    manifest key (a prior interrupted move), DELETE it and CONFIRM the deletion
    succeeded before touching any payload key (abort if it is not confirmed). This
    makes "manifest absent" the in-progress state, so an interrupted retry can never
    leave an old manifest describing freshly-overwritten payload keys.
 3. Upload data.tar.zst, then r3.yaml, then metadata.yaml (overwriting the fixed keys).
 4. Verify EVERY payload object by content (I8): stream each back and confirm its
    sha256 (archive_sha256 for the archive; the manifest files[] entry for each
    sidecar). Streaming-hash the archive (no second temp file); the step-1 archive
    temp may be released before this step.
 5. Publish the manifest via a VERIFIED staging-copy, so the final manifest key only
    ever appears post-verification (closes the visible-but-unverified window):
    (a) PUT the manifest to a staging key ({job}/manifest.json.staging);
    (b) GET it back and byte-compare to the locally built manifest;
    (c) server-side copy staging → the final {job}/manifest.json key using the
        low-level single-operation `copy_object` (NOT a transfer-manager multipart
        copy), ideally with `CopySourceIfMatch` = the staging object's ETag so the copy
        is bound to the exact bytes just verified; **process the complete CopyObject
        response and treat any exception OR an error embedded in an HTTP-200 body as
        failure**; (d) delete the staging key. A crash before (c) leaves no final
    manifest (job incomplete/invisible), so the final key's presence implies its bytes
    were verified. (CopyObject is a server-side byte-faithful op, not a client PUT, so
    it is outside the when_required upload-checksum gap; if a deployment ever distrusts
    it, fall back to a separate content-insensitive completion marker written only
    after the GET-verify. An optional GET of the final key before step 7 is a harmless
    extra deployment check, but the atomic copy — not that GET — is what closes the
    window.)
 6. Quiescence re-check: re-scan jobs/<id>; if its file set or sizes/mtimes changed
    since step 1, ABORT — delete the final manifest just published (discard the stale
    snapshot, leaving no valid remote job) and leave the job local. If that delete
    fails, REPORT it (do not treat invalidation as successful — a stale published
    manifest must not be left behind silently). (Guards the "job still running" case;
    a lock against a concurrent external writer is out of scope — see the quiescence
    precondition below and LIMITATIONS.md.)
 7. Commit the index transition (single transaction):
       set location = remote_name
       set files    = manifest file list
    (dependency edges are unchanged — they were set at commit time.)
 8. Delete local ATOMICALLY: os.replace(jobs/<id>, {repo}/.trash/<id>-<rand>),
    then rmtree the trash entry. The rename is instantaneous, so a complete
    jobs/<id> never lingers visibly after the index says remote.

Return: the set of dependent jobs (informational warning; not a blocker).
```

**Quiescence precondition.** A job must not be running or otherwise mutating during
`move`. The step-6 re-check detects the common "I thought it was done, but slurm was
still writing `output/`" case and aborts before publishing a stale snapshot as
authoritative; it cannot fully prevent a concurrent external writer racing the tiny
window between the re-check and the local delete. Documented in `LIMITATIONS.md`.

**Why content-verify every object, not HEAD (step 4).** On CEPH RGW with
`request_checksum_calculation: when_required` (the lab config, see `CONTRIBUTING.md`)
boto3 suppresses the integrity headers that would let the server validate an upload,
and a multipart ETag is not a content hash — so `HEAD` proves neither integrity nor
completeness. Because `move` then **deletes the only local copy**, and because the
sidecars are the *sole* copies of the dependency graph and metadata, every object —
the archive, both sidecars, *and* the manifest marker — must be confirmed by content
before the index is committed. The extra download is the price of not risking silent
data loss on a rare, deliberate operation; a future optimization can skip it on
backends that expose a verifiable content checksum. (`response_checksum_validation`
is also exposed, §9, since GET is now on the critical path.)

Interruption recovery (I5). The invalidate-first step and the staging-copy publish
mean the **final** manifest key is present iff its bytes were verified — the "1–5"
window is split at the invalidation and publication boundaries:

| Crash between | Bucket state | Index state | Local state | Recovery |
|---|---|---|---|---|
| 1–2 (before invalidation) | old publication may still be complete (not yet invalidated) | `local` | intact | Local job and the old remote snapshot are both intact/valid. Re-run `move`: step 2 invalidates the old manifest, then republishes. |
| 2–5c (invalidated; final manifest not yet copied) | **no final manifest** (old deleted; a `.staging` object may exist) | `local` | intact | Job is fully local and authoritative; manifest-less payload/staging objects are invisible to `exists()`/`fetch`/`rebuild`. Re-run `move`. |
| 5c–7 (final manifest published) | complete (final manifest present ⇒ verified via staging-copy) | `local` | intact | Both copies valid; served locally. Re-run `move` (idempotent: re-invalidate + re-upload + re-verify), then proceeds. |
| 7–8 | complete | `remote` | complete (or a `.trash/` entry) | Both copies valid; served from remote. Leftover local dir/`.trash` is dead weight. If `rebuild` runs first it adopts the complete local dir (local-wins) and leaves a **detectable** remote orphan — surfaced by `r3 remote check`, reclaimed on `remove` (§9). **No data loss.** |

Key ordering guarantees: a stale manifest is invalidated **before** any payload key
is overwritten (step 2, delete confirmed); the final manifest key appears **only via
a server-side copy from a GET-verified staging object** (step 5), so "final manifest
present ⟹ job stored and verified complete" (I3/I8) with no visible-but-unverified
window; the index flips to `remote` **only after** that; local files are deleted
**only after** the index says `remote`, **via an atomic rename** (never a half-deleted
`jobs/<id>` that `rebuild` would mistake for a valid local job). Re-running `move` on
an already-`remote` job fails its precondition with a clear error (not a silent no-op;
leftover cleanup is `remove`/`gc`'s job, §9).

---

## 6. `fetch` state machine

`Repository.fetch(job_id)` — restores a remote job locally and (per the approved
decision) **deletes the remote copy**, making `fetch` the exact inverse of `move`.
A single `location` field stays truthful; no untracked replicas.

```
Preconditions:
  - job_id exists and location != "local"              else ValueError

 0. Idempotent finalize: if jobs/<id> already EXISTS (a previous fetch/move crashed
    after the staging rename), verify it against the manifest — the still-present
    remote manifest, or, if remote cleanup already deleted it, the local recovery
    RECEIPT (step 1). If BOTH are present they must AGREE (byte-identical); disagreement
    is an error, not a silent pick. If valid, skip to steps 7–8 (cleanup + index flip).
    If it exists but no manifest/receipt is available to verify against, raise
    corruption rather than overwrite. This branch keys off the valid LOCAL directory,
    so it works even when the remote is already partly deleted.
 1. Read manifest.json from the remote, and write it to a local recovery RECEIPT
    outside the staging dir (e.g. {repo}/.fetch/<id>.receipt.json), via a temp file +
    os.replace, which persists until step 8. This lets step 0 re-verify a restored
    jobs/<id> even after step 7 has deleted the remote manifest (the fetch-retry gap).
 2. Download data.tar.zst to a local temp file; verify sha256 == archive_sha256. (I3/I8)
 3. Download the r3.yaml and metadata.yaml sidecars.
 4. Extract the archive into a fresh staging dir {repo}/.fetch/<id>-<rand>/ (same
    filesystem as jobs/ so the step-6 rename is atomic; outside jobs/ so
    Storage.jobs()/rebuild never see it), validating every tar member per §11 (paths
    against the staging root; file members only — no directory members; no duplicate
    members). Then place the r3.yaml and metadata.yaml sidecars into staging and
    create any parent directories + the conventional output/ (§11).
 5. Verify staging against the manifest: every entry present with matching size+sha256
    and its name agreeing with a manifest path; reject on any mismatch, extra, or
    missing file, or on exceeding sane count/size limits. (I3/I8)
 6. Atomically rename staging → jobs/<id> (os.replace).
 7. Delete the remote objects, idempotently and UNCONDITIONALLY for this job_id: the
    manifest FIRST as its own call (confirm success, inspecting per-object Errors),
    then archive + both sidecars — do NOT require the manifest to still exist before
    deleting the other three keys (a prior interrupted cleanup may have removed it).
 8. Commit the index transition: set location = "local"; then delete the local receipt.
    (Receipt deletion is cleanup only: if it fails, do NOT roll back or misreport the
    committed local transition — a leftover receipt is harmless debris, swept by
    `remove`/`remote check`.)
```

The index flips to `local` **last** (step 8), after remote cleanup, so there is never
a window where the index says `local` while a complete remote copy still exists
undiscovered; step 0 keys recovery off the verified local directory (against the
remote manifest **or** the local receipt), so every interruption is retryable even
after the remote manifest has been deleted.

Interruption recovery (I5):

| Crash between | Local state | Index state | Bucket state | Recovery |
|---|---|---|---|---|
| 1–6 | only a staging dir under `.fetch/` | `remote` | complete | `jobs/<id>` does not exist yet; index still says `remote`. Stale `.fetch/` dirs are safe to delete. Re-run `fetch`. **No partial job at `jobs/<id>`** (fixes F-04's "destination left behind"). |
| 6–7 | `jobs/<id>` complete | `remote` | complete or partly deleted | Both copies valid (or local + a manifestless remote remnant). Re-run `fetch`: step 0 verifies the local dir and short-circuits to idempotent cleanup + index flip. Any remnant is surfaced by `r3 remote check`. |
| 7–8 | `jobs/<id>` complete | `remote` | deleted | Job is fully local; index still says `remote`. Re-run `fetch`: step 0 verifies the local dir **against the local receipt** (the remote manifest is gone), cleanup is a no-op, index flips to `local`. Or `rebuild` adopts the local dir (local-wins). **No data loss.** |

**Bucket-versioning caveat.** "Delete the remote copy" is literal only on a
non-versioned bucket. On a **versioned** bucket an ordinary delete writes a delete
marker and retains prior versions, so the objects are not truly gone (and storage is
still consumed). The deployment assumption for this design is a **non-versioned**
bucket (or one whose lifecycle expires noncurrent versions); this is documented so a
versioned deployment is a conscious choice, not a silent surprise. The same caveat
applies to `remove` (§9).

---

## 7. Index as durable-but-rebuildable cache

### 7.1 Transaction rollback (F-08)

`Transaction.__exit__` currently commits even on exception. Fix: **roll back on
exception, commit only on clean exit.** Prerequisite for the atomic index
transitions in `move`/`fetch` (§5–6), independent of rebuild.

### 7.2 Atomic, bucket-backed rebuild (F-08)

Two problems: the current branch reads remote rows from the *old index* then deletes
it (a crash loses the remote catalog — local storage has no remote jobs), and the
rebuild is in-place (a crash mid-rebuild leaves a silently-incomplete index). Fix
both:

1. **Build into a new file** `index.sqlite.new`; `os.replace` it over `index.sqlite`
   only after the rebuild completes and validates. The old index stays intact until
   the swap — so a crashed rebuild never destroys the previous catalog (satisfies the
   review's "rebuild failure preserves the previous authoritative catalog").
2. **Enumerate local jobs** from `Storage`, inserting `location='local'`. A
   `jobs/<id>` **missing its `r3.yaml`** is treated as corruption (I7), not adopted
   as a valid local job. (R3's own write paths cannot produce such a directory —
   `move` deletes via an atomic rename and `fetch` writes via an atomic rename, §5–6
   — so this guards against external/manual corruption, per I7.) (`.fetch/` staging
   dirs live outside `jobs/` and are never enumerated.)
3. **For each configured remote**, paginate the *full* object listing under its
   prefix (`list_objects_v2` is prefix-only; there is no server-side suffix filter,
   and a single unpaginated call truncates at 1000 objects) and select keys ending
   `/manifest.json` — a job is complete iff its manifest exists. For each such
   `job_id` **not already present locally** (local wins — handles leftover remote
   objects after an interrupted fetch), restore the row **from the authoritative
   artifacts**: dependencies + timestamp from the `r3.yaml` sidecar (parsed exactly
   as a local job's, so remote and local rebuild share one code path), metadata from
   `metadata.yaml`, file list from the manifest, `location=<remote>`.

**Fail closed, with a validation boundary.** Before accepting a remote job, validate
(without a full archive re-download): manifest schema + supported `manifest_version`;
that the manifest's `job_id` matches the object-key `job_id`; a recognized
`representation`; normalized, unique, in-bounds logical paths; the sidecar hashes and
sizes are present and match the fetched sidecars; the manifest key has the exact
expected shape under the prefix; and **`HEAD data.tar.zst` succeeds with
`ContentLength == archive_size`** (C-04 — catches an externally deleted or truncated
archive that a manifest+sidecars would otherwise vouch for; still no rehash). This is
*structural* validation ("is this a well-formed published job"), distinct from
re-hashing every archive on every rebuild (expensive; reserved for `move`/`fetch` or a
future deep-check mode). If **any** authoritative remote job fails to reconstruct
(corrupt/missing sidecar, missing/short archive, transient read error, schema
violation), **refuse the index swap** — keep the old index in place and report each
affected remote and key. Silently omitting a job (the earlier "skip with a warning")
is wrong: it turns remote corruption or a transient error into a job that vanishes
from a cache the design calls authoritative-by-the-bucket. (A future explicit
`--allow-incomplete` / quarantine mode could deliberately build a partial index and
report the omissions; that is a specified state, not the default fallback.)

**Implementation contract (from the confirmation review).** An error on **any** LIST
page or object read aborts the whole rebuild (no partial swap). Manifests from **all**
configured remotes are collected **before** duplicate detection and the local-wins
rule are applied. Local corruption (a `jobs/<id>` missing its `r3.yaml`, §7.2 point 2)
also aborts rather than being skipped. A stale `index.sqlite.new` left by an
interrupted prior attempt is discarded and recreated fresh. The new SQLite database is
committed and closed **before** `os.replace` installs it over `index.sqlite`.

**Duplicate job IDs across remotes.** A complete manifest for the same `job_id` under
two configured remotes (possible after a fetch-cleanup interruption followed by a move
to a different remote, §6) has no meaning under the one-`location` model. Rebuild must
**fail with a diagnostic** naming both remotes/keys rather than silently picking one by
iteration order.

Because both sources (local storage, remote bucket) are durable, a rebuild
interrupted by a crash is simply **re-run**. `rebuild-index` is documented as
re-runnable and as the recovery path for a damaged index; the remote bucket is the
durable remote catalog (backup guidance).

**Consistency assumption.** Rebuild's completeness test is a bucket LIST; a
single-zone CEPH RGW is strongly consistent (RADOS-backed bucket index), so a
just-moved manifest is listable. This is documented as a **precondition**:
multi-site/eventually-consistent RGW replication could omit a recent manifest from a
rebuild until it propagates. **Locking:** because rebuild is a whole-index writer, a
concurrent `move`/`fetch` can lose a row against the freshly rebuilt index; a
repository lock preventing overlap is recommended (single-writer assumption, §2).

### 7.3 Explicit location, absence = corruption (F-09)

`Index.get()` and `Index.find()` **select `location`** and branch:

- `location == "local"`: construct from `Storage.get()`; a missing directory (or a
  directory missing its `r3.yaml`) raises a corruption error (I7) — never a silent
  fallback to a cached projection.
- `location != "local"`: construct a **remote projection** Job (§8).

The `try/except FileNotFoundError` inference is removed.

### 7.4 Lazy file-list loading (F-10)

`find()` stops eagerly selecting/deserializing the `files` JSON for every row; it is
loaded only when needed (remote-dependency validation via `get_file_list()`).
Metadata-only queries pay no manifest I/O.

### 7.5 Bound location parameter (F-11)

The `location` filter is passed as a bound SQLite parameter, not f-string
interpolated. Regression test includes `missing' OR 1=1 --`. (Out of scope, but
noted: `query.py` still interpolates tag/values via f-string — the original review
explicitly scoped the general translator out; it remains an open door to assess
separately if its trust model changes.)

---

## 8. `Job` API stance — revert the interim extension

`Job.files` **reverts to `Mapping[Path, Path]`** (matching `main`, including main's
`/output` exclusion). The interim `Mapping[Path, Optional[Path]]` and the
`cached_file_paths` parameter are removed. Rationale: the planned follow-up — the
`job.file_paths` / access split in
[`2026-08-04-job-file-manifest-and-access-proposal.md`](2026-08-04-job-file-manifest-and-access-proposal.md)
— returns `.files` to exactly `Mapping[Path, Path]` and adds a separate `.file_paths`
for membership. So the `Optional` widening is a transient state on the way to an
end-state that matches `main` (A→B→A); reverting makes the arc a single deliberate
A→A' change, and this branch makes **zero** change to the public type of
`Job.files`.

A **remote job is a metadata projection**: `Index.get()`/`find()` construct it with
cached `id`, `timestamp`, `metadata`, and a private remote marker. Accessing
`.files`, `.hash()`, or `.dependencies` on it raises:

```python
class FilesUnavailableError(RuntimeError):
    """Raised when a job's files are not locally available (job is on a remote)."""
```

This fixes F-09's "silently returns an empty dependency list" and pre-stages the
exact exception the future `file_paths` PR keeps (that PR is then purely additive).
The remote file list is not exposed through `Job`; it lives in the index
(`get_file_list`) and the manifest, consumed internally.

Confirmed no other path calls `.files`/`.dependencies`/`.hash()` on a projection
unexpectedly: `find_dependents` uses only `.id`; `commit`/`checkout`/`resolve`
operate on local jobs (checkout gated by `_check_job_is_local`); `__contains__` uses
`index.get_file_list` (SQL); `move` builds the manifest by walking `jobs/<id>/` (so
it never calls `job.files` — which is also why the revert is nearly free). The one
edge: `Repository.remove` iterates `job.files` in `Storage.remove`; §9 gives it a
clear guard rather than letting `FilesUnavailableError` surface from deep in Storage.

---

## 9. Remote configuration & job lifecycle (F-03, F-13)

- **Immutability of referenced remotes (I6).** `r3 remote remove` on a referenced
  remote is **refused**, naming the affected job IDs. The referenced check inspects
  the **bucket prefix directly** (list manifests under `{prefix}`), *not* the SQLite
  index — the index is a disposable cache and may be stale or damaged, so trusting it
  here could let removal discard the only metadata (endpoint/bucket/prefix) that
  locates otherwise-orphaned bucket objects. Remote configuration is therefore
  **durable repository metadata that must be backed up** (unlike the rebuildable
  index); this is stated in `LIMITATIONS.md`. Format pinning in the manifest does not
  recover a lost endpoint/bucket/prefix. A remote job's storage *format* is pinned by
  its manifest's `representation`, so a future format change cannot strand it; keys
  derive from `{prefix}{job_id}` (not stored, §4.1), so re-pointing without moving the
  objects fails *loudly* at fetch ("manifest not found"). (Archive-only means F-03's
  original "individual→archive switch strands jobs" scenario cannot occur.) Beyond
  live jobs, if manifestless payload objects or incomplete multipart uploads remain
  under the prefix (leaked debris, not live jobs), removal is **refused unless
  `--force`**; `--force` reports exactly what will become unmanaged (C-05). This keeps
  `remote remove` from silently orphaning the only route by which `r3 remote check` /
  a future `gc` could reclaim that debris.
- **Validated CLI management (F-13).** `remote add` validates config through
  `Remote.from_config()` **before** writing `r3.yaml`; an unknown type or an S3
  remote without a bucket is rejected with a `ClickException`, not persisted. The CLI
  exposes the documented CEPH fields (`addressing_style`,
  `request_checksum_calculation`, `response_checksum_validation`, `endpoint_url`,
  `profile`, `prefix`). `archive_format` is **not** a user knob (archive-only; the
  only value is `tar.zst`) and is dropped from the CLI. All `r3.yaml` writes
  (migrations and `remote add`) use temp-file + `os.replace` so a crash mid-write
  cannot truncate the file and drop the `remotes` map.
- **`r3 edit` refuses remote jobs (I2).** Editing a remote job currently creates a
  stray file at the deleted job path; it is changed to refuse with "job is on remote
  X; run `r3 fetch` first". (Also fix the existing `edit` bug where `except KeyError`
  prints but does not `return`, then dereferences an unbound `job`.)
- **`remove` handles remote jobs and sweeps orphans (with its own interruption
  protocol).** `Repository.remove` today only handles local jobs and only touches
  local storage + the index. It is extended so that removing a job guarantees it is
  gone *everywhere*, via an ordered, idempotent, retryable protocol:

  ```
  Preconditions: job exists; no other job depends on it (find_dependents) else refuse.
   Operate from the RAW index row + direct probes — NOT via Index.get()/Storage.get().
   1. Remote sweep — ALWAYS across EVERY configured remote (not only the indexed one):
      delete the manifest FIRST (own call, inspect per-object Errors), then the archive,
      both sidecars, AND the staging manifest ({job}/manifest.json.staging) — all
      UNCONDITIONALLY (do not gate on the manifest existing). Deleting the manifest
      first makes each remote copy immediately invisible to `rebuild`. Every batch must
      complete without reported per-object errors before advancing.
   2. Delete the local job directory if present (atomic os.replace → .trash → rmtree),
      AND remove this job's local recovery artifacts (so "gone everywhere" holds):
      the fetch receipt {repo}/.fetch/<id>.receipt.json, any stale
      {repo}/.fetch/<id>-*/ staging dirs, and any {repo}/.trash/<id>-* entries —
      including a trash entry left by an interrupted rmtree of THIS remove, so a retry
      that finds no live jobs/<id> still finishes the cleanup.
   3. Delete the index row.
  Every step tolerates its target already being absent (idempotent); a crash leaves a
  subset done and re-running `remove` (or `remote check` + `rebuild`) completes it.
  ```

  **Retry from the raw row (not the normal lookup).** A crash after step 2 but before
  step 3 leaves an index row saying `local` with the directory already gone — which
  normal `Index.get()`→`Storage.get()` reports as I7 corruption. `remove`'s retry must
  therefore work from the raw index row plus direct local/remote probes, tolerating an
  intentionally-missing local directory, rather than materializing the job through the
  corruption-raising path.

  **Sweep every remote, always.** Sweeping only the indexed remote would leak objects
  via a supported path: fetch from remote A interrupted after deleting A's manifest →
  `rebuild` adopts the restored local dir → the (now local) job is moved to remote B →
  removing it sweeps only B, leaving manifestless archive/sidecars on A after the index
  row is gone. Sweeping *all* configured remotes on every removal closes that (safe
  under the single-owner assumption below).

  Ordering rationale: the remote manifest (rebuild's remote-adoption trigger) goes
  first and the local directory (rebuild's local-adoption trigger) next, so a crash
  cannot leave a *complete* resurrectable copy on either side for long; a `rebuild`
  run in the brief window between steps could transiently re-adopt a half-removed job,
  which a `remove` re-run then clears (single-writer assumption, §2). Per-object S3
  delete errors are inspected, never assumed successful from an aggregate 200. A
  remote projection is guarded so `remove` gives a clear error path rather than
  raising `FilesUnavailableError` from inside `Storage`.

  **Ownership assumption.** Sweeping a `job_id`'s keys across *every* configured
  remote is safe **only because remotes are single-owner** in this release (no two
  repositories share a remote prefix — see `LIMITATIONS.md`). If shared remotes are
  ever supported, this sweep must be revisited so it cannot delete another
  repository's live job.
- **`r3 remote check` (read-only reconciliation).** A new command that reconciles the
  index against the bucket and **reports** (mutating nothing):
  - a **complete** manifest whose `job_id` has no matching index row, or whose row
    says `local`, or whose row says a *different* remote (resurrection-risk orphans /
    location disagreements);
  - a **manifestless** job prefix — archive/sidecar keys present with no manifest (a
    leftover from an interrupted move/fetch/remove; ordinary keys, not just multipart);
  - a leftover **staging manifest** (`manifest.json.staging`) — reported even when a
    complete final `manifest.json` also exists (an interrupted publish, T-03);
  - index rows marked remote whose manifest is missing, **or whose `data.tar.zst` is
    missing or has a `ContentLength` ≠ the manifest's `archive_size`** (broken /
    unfetchable — C-04);
  - incomplete multipart uploads under the prefix (wasted quota).

  This makes index↔bucket drift *visible* — the standing safety net for interrupted
  operations and, later, for shared/collaborator remotes mutated outside this
  repository. It is the read-only foundation on which a future mutating `gc` (delete
  orphans, abort incomplete multipart) will build; reclamation is deferred so it can
  be designed against the shared-remote model (where one index's "orphan" may be
  another's live job).

**Forward path (not implemented now):** because `metadata.yaml` is the sole,
separate metadata object excluded from the archive (§4), a future release can edit an
archived job's metadata by overwriting that one object and updating the
index+manifest entry — no stale copy anywhere, no touching the immutable archive. I2
holds today (we refuse); the layout leaves the door open. Retrofitting this later
would be a format migration over a cold-storage bucket, which is why the layout is
built for it now.

---

## 10. Dependency and checkout behavior (F-06)

- **Directory dependencies.** `Repository.__contains__` for a `JobDependency` whose
  `source` is a directory (e.g. `output`) is present if the cached file list contains
  **any entry beneath it**, not only an exact match. `source == Path(".")` ⇒ present
  iff the file list is non-empty. Empty directories are not represented in a
  manifest; note the resulting asymmetry: a *local* job's always-created empty
  `output/` makes `source="output"` present, while a *moved* job with empty output
  has no entry beneath it. Resolution: the conventional `output/` is treated as
  present for any complete remote job (so it never flips local→remote). *Other*
  empty-directory dependencies are unsupported (§11) — files only. Depending on an
  empty directory is degenerate (jobs are immutable), and the one meaningful case
  (`output/`) is handled.
- **Remote dependents.** `Index.find_dependents()` must not materialize dependents
  through local `Storage` (which raises for remote jobs); it constructs each via the
  same location-aware projection as `get()`/`find()` (§7.3), so `move()` of a parent
  with a remote child no longer fails after upload.
- **Transitive checkout.** `checkout` preflights locality **using the same traversal
  `Storage.checkout_job` actually performs**, via a shared helper — not a naive
  full-graph walk. `checkout_job` descends into a dependency's own dependencies only
  for `source=="." && recursive_checkout` edges; every other `JobDependency` is a
  bare `os.symlink(jobs/<dep.job>/<source>, …)` that neither descends nor errors if
  `dep.job` is remote (it would silently create a dangling symlink). So the preflight
  must (a) **not** over-refuse a checkout that would succeed because a remote
  grand-dependency sits behind a non-recursive edge Storage never dereferences, and
  (b) **not** miss a non-recursive edge whose own `dep.job` is remote (dangling
  symlink). It checks the locality of exactly the jobs `checkout_job` will dereference
  or symlink, honoring `recursive_checkout`/`source`, and fails up front with the
  clear "archived; fetch first" error, leaving **no** partial checkout. This preflight
  must run **before** `Repository.checkout` calls `resolve()` on the top-level item:
  `resolve()` touches `Job.dependencies`, which on a remote projection raises
  `FilesUnavailableError` (§8), so a remote top-level job must be caught by the
  locality check first and reported as "archived; fetch first" rather than surfacing
  the raw `FilesUnavailableError`.

---

## 11. Filesystem model & safe extraction (F-01)

One filesystem model, enforced **symmetrically** on both sides, so a locally-accepted
job never produces an archive its inverse operation refuses or reconstructs
differently:

- **Regular files only — no directory members, no sidecars, in the archive.** `move`
  REJECTS a job containing symlinks, hardlinks, device nodes, FIFOs, or other special
  members **before publication** (during the step-1 capture), with a clear error —
  rather than producing an archive `fetch` would later refuse. It adds each file as an
  individual member and writes **no directory entries**, so the archive's member set is
  exactly **(manifest `files` − {`r3.yaml`, `metadata.yaml`})** — the two sidecars are
  stored only as sidecar objects, never inside the archive. Every archive member
  therefore has a corresponding non-sidecar manifest path.
- **Fetch rejects sidecar members.** On extraction, `r3.yaml` or `metadata.yaml`
  appearing *inside* the archive is an error (reject) — accepting them would create a
  second physical copy and break the sidecars' single-source guarantee. The sidecars
  come only from their downloaded sidecar objects (§6 steps 3–4).
- **Individual keys:** removed with the individual-object representation; the per-key
  traversal path is gone entirely.
- **Safe extraction (`fetch`, §6 steps 4–5):** streaming (`r|`) with per-member
  validation *before* each write (you cannot validate after a bulk `extractall`): for
  each member reject absolute paths, any `..` component, any resolved path outside the
  staging root, and any non-regular (non-file) member; reject duplicate member names;
  require each member name to agree with a manifest path; enforce sane per-file and
  total count/size limits. Only then extract, creating parent directories as needed;
  after extraction, ensure the conventional `output/` exists. Extraction targets a
  fresh staging dir, never the live `jobs/<id>`.
- **Empty directories are not represented** in the logical manifest (files only),
  except the conventional `output/`, which is always recreated (§10). Empty-directory
  dependencies are therefore unsupported and documented as such (`LIMITATIONS.md`).
- Adversarial regression tests: `../` traversal, absolute-path member, symlink escape,
  hardlink escape, duplicate member, oversized / too-many members (see repair plan).

---

## 12. Migration (F-02)

No real repository has been migrated on this branch, so the path is repaired cleanly
rather than stacked. **The durable design needs no new index schema:** `location`
(beta.8) and `files` (beta.9) suffice, because the manifest and representation live
on the remote. So there is no beta.10 — the work is to make the *existing* beta.7 →
beta.8 → beta.9 path correct.

- **Migrations never construct the version-strict `Repository`**, nor reuse the live
  `Index` (whose `rebuild()` unconditionally CREATEs the *current* HEAD schema —
  which is why a "beta.8 has `location` only" intermediate never really exists at
  HEAD). Each migration performs its SQLite change via a migration-*local* helper.
  The end-to-end test asserts the resulting **schema** (columns present), not just
  the version string.
- **Version marker written last**, via temp-file + `os.replace`, only after the
  schema/data change succeeds — so a failure (including a hard crash) leaves the old
  version and old, valid index in place.
- **Old index recoverable.** The index is backed up before any destructive step; the
  migration **refuses to overwrite an existing `.bak`** (a naive re-run would
  otherwise clobber the good backup with a now-partial index) and offers to restore
  it first.
- End-to-end test: a genuine beta.7-shaped repository migrates beta.7 → beta.8 →
  beta.9 using the scripts at HEAD; plus failure-injection tests (a simulated
  *process* interruption between the schema change and the version write) proving the
  old version/index remain usable.
- Per the §2 crash model, migration recovery is guaranteed against **process
  interruption**, not host power loss; adding `fsync` of the new index + `r3.yaml` and
  their parent directories for power-loss durability is a documented follow-up
  (applies equally to rebuild, the staging rename, and the `.trash` transition).

---

## 13. Python floor (F-14)

Raise the minimum Python to **3.10** (`requires-python = ">=3.10,<3.13"`). boto3 is
now a required runtime dependency and has ended 3.9 support; pinning to an
unsupported boto to retain 3.9 is not worth the security/maintenance tradeoff.

---

## 14. Finding → resolution map

| Finding | Resolution | Invariant |
|---|---|---|
| F-01 Blocker: path traversal | §11 streaming per-member validation into a staging dir; individual-key path removed | I4 |
| F-02 Blocker: broken migration | §12 migration-local SQLite, version-last via `os.replace`, `.bak`-preserving, schema-asserting test | — |
| F-03 High: mutable remote config strands jobs | §9 immutable-while-referenced + representation pinned per-job manifest; keys derived not stored | I6 |
| F-04 High: move/fetch not crash-safe | §5/§6: invalidate-stale-manifest-first, content-verify-all-incl-manifest before publish, atomic local delete, quiescence re-check, idempotent-finalize fetch with index-flip-last + unconditional cleanup, per-step recovery | I5, I8 |
| F-05 High: `exists()` ≠ integrity | §4.1 manifest completion marker + §5 content-verification of **every** object — archive, both sidecars, **and the manifest marker itself** — before publish | I3, I8 |
| F-06 High: remote dependency handling | §10 directory membership, location-aware dependents, traversal-matched transitive preflight | — |
| F-07 High: integration with `main` | merge `main`; manifest from single-pass hashing, not `Job.files`; take main's output-exclusion + safer `Storage.__contains__` + packaging + CLI | — |
| F-08 High: index authoritative but cache-like | §7.1 txn rollback + §7.2 atomic, bucket-backed, re-runnable rebuild that reads authoritative artifacts, **fails closed** with a validation boundary (no silent omission), and rejects duplicate job IDs across remotes | I1 |
| F-09 Med: missing local = remote-like | §7.3 explicit location (missing dir / missing `r3.yaml` = corruption); §8 projection raises | I7 |
| F-10 Med: eager manifest loading | §7.4 lazy file-list | — |
| F-11 Med: location SQL injection | §7.5 bound parameter (broader `query.py` noted, out of scope) | — |
| F-12 Med: failure-mode coverage | §5/§6 assertions (no finalized upload on failed verify; no `jobs/<id>` after failed fetch; corruption raises); `delete_objects` `Errors` inspected in both `fetch`/`remove` and the live-S3 teardown; a live multipart (>threshold) `move`/`fetch` test | I3/I5/I8 |
| F-13 Med: CLI bypasses validation | §9 validated `remote add`, exposed CEPH fields, `archive_format` dropped, atomic `r3.yaml` writes | — |
| F-14 Low: Python/boto mismatch | §13 raise floor to 3.10 | — |

New behaviors added beyond the findings (across three review rounds): manifest
published via verified staging-copy (no visible-but-unverified window) + invalidate-
before-overwrite; fail-closed rebuild with a validation boundary (incl. an archive
existence/size HEAD), duplicate-ID detection, and an explicit read-error/abort
contract; idempotent/unconditional fetch cleanup with index-flip-last + a local
manifest receipt for retry; a `remove` interruption protocol that retries from the raw
index row and sweeps every configured remote + single-owner ownership assumption;
`remote remove` checks the bucket not the index and guards residual debris; a `move`
quiescence precondition + pre-delete stability check; one symmetric **files-only**
filesystem model; atomic local delete and atomic rebuild; read-only `r3 remote check`
(reports orphans, manifestless prefixes, archive size mismatches, incomplete
multiparts); `response_checksum_validation` exposed; and explicit deployment/scope
assumptions — single-writer (not enforced, **no** concurrency safety guarantee),
single-owner remotes, non-versioned bucket, process-interruption (not power-loss)
crash model. **User-facing limitations are collected in the top-level
[`LIMITATIONS.md`](../../../LIMITATIONS.md).**

**Deferred (documented, not implemented here):** `job.file_paths`/mounting API
(proposal doc); ratarmount integration; `FilesystemRemote`; `r3 copy`/replicas; the
**mutating** `r3 gc` (reclaim orphaned objects + abort incomplete multipart uploads —
designed later against the shared-remote model; the read-only `remote check` is its
foundation and *is* in scope); shared/collaborator remotes; a repository lock to
*enforce* (not just document) the single-writer assumption — revisit with the
shared-remote work; power-loss (`fsync`) durability; query-based batch `move`; per-file
archive offsets in manifest v2.
