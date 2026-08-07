# Independent Confirmation Review: Revised Remote Storage Durable Design

**Review date:** 2026-08-05

**Feature branch:** `feature/remote-storage` at `aeb4f2e`

**Reviewed document:**
[`docs/superpowers/specs/2026-08-04-remote-storage-durable-design.md`](../superpowers/specs/2026-08-04-remote-storage-durable-design.md),
including its 2026-08-05 revision

**Companion document reviewed:** [`LIMITATIONS.md`](../../LIMITATIONS.md)

**Review status:** Focused confirmation pass; no implementation reviewed

> **Important:** This document is a review, not an approved implementation plan.
> Sections titled **Suggested direction** contain possibilities for the maintainer to
> evaluate. They are deliberately not design decisions or authorization to implement a
> particular solution.

## Scope

This review checks whether the 2026-08-05 revision resolves findings D2-01 through
D2-09 from the preceding
[`2026-08-05-remote-storage-durable-design-review.md`](2026-08-05-remote-storage-durable-design-review.md),
with particular attention to:

1. whether manifest publication is safe across every move crash-and-retry state;
2. whether rebuild can install an index that silently omits a complete remote job;
3. whether the move, fetch, rebuild, and remove interruption protocols hold as written;
4. whether the revision introduces or leaves contradictions.

The following maintainer-accepted scope decisions are not reopened here:

- move quiescence is a precondition plus a pre-delete stability re-check, without
  run-lifecycle integration;
- durability covers process interruption, not host power loss; explicit `fsync` is
  deferred;
- single-writer operation is documented but not enforced; repository locking is
  deferred to separate shared-remote/concurrency work.

These limitations are mentioned only where the design makes a stronger safety claim
than the limitation permits.

## Executive conclusion

The revision resolves most of the prior review cleanly:

- stale publication is invalidated before fixed payload keys are overwritten;
- the manifest object is now included in move's content-verification sequence;
- rebuild fails closed and preserves the old index on any reconstruction error;
- duplicate job IDs across remotes are rejected;
- fetch keeps the index remote until cleanup completes;
- remote cleanup targets are unconditional and idempotent;
- remove has an ordered protocol and inspects deletion errors;
- remote removal consults the bucket rather than trusting SQLite;
- move and fetch now share a regular-files-only filesystem model;
- accepted operational limitations are visible in `LIMITATIONS.md`.

**D2-02 is confirmed resolved** under the documented single-writer and strongly
consistent LIST assumptions. **D2-01's stale-manifest overwrite defect is resolved, but
the stronger "manifest presence means content-verified" invariant is not yet true at
one interruption point:** the final manifest PUT becomes visible before its verification
GET completes.

The fetch and remove protocols also retain concrete command-retry gaps. The design
therefore needs one more focused revision before it becomes the final implementation
basis.

## Blocker confirmation

### C-01 — Blocker: final manifest publication still has an unverified-visible window

The new move step 2 correctly deletes a stale manifest before overwriting any fixed
payload key. Once that deletion succeeds, a retry cannot leave an old manifest
describing a mixture of old and new payload objects. This resolves the principal
overwrite defect identified as D2-01.

Move step 5 nevertheless performs the final publication in this order:

1. PUT `manifest.json` at its final key;
2. GET it and compare it with the locally constructed bytes.

The final object is visible between those operations. A process interruption after the
successful PUT but before completion of the GET can therefore leave an object that has
not passed R3's content verification. Under the design's own threat model — uploads are
rehash-verified because this CEPH configuration does not provide a trusted
server-validated checksum — that object may be corrupt yet parseable and may describe
payload that does not match it. No persisted state distinguishes this candidate from a
manifest whose verification GET completed.

Consequently, these statements are not strictly true at every point:

- manifest presence implies that the manifest itself was content-verified;
- the move table's combined 1–5 row has no visible valid-looking manifest;
- rebuild can infer that every visible marker previously completed move verification.

This is a limitation in the earlier review's suggested upload-then-GET sequence as
well: the GET protects the later index transition and local deletion, but it cannot
retroactively make the preceding visibility window verified.

#### Suggested direction — not a decision

Because the manifest is small and uses a single-part upload, consider supplying an
explicit `Content-MD5` and defining a successful server-validated PUT as the publication
event. Keep the verification GET before changing SQLite and deleting locally as a
second check. Amazon S3 documents that a `Content-MD5` PUT succeeds only when its
server-computed digest agrees; Ceph documents the request header, but enforcement must
be demonstrated against the exact lab RGW release and configuration:

- [AWS: Checking object integrity for data uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)
- [Ceph: S3 Put Object](https://docs.ceph.com/en/reef/radosgw/s3/objectops/)

If the deployed RGW does not enforce that check reliably, consider separating the
integrity manifest from a content-insensitive completion object: upload and GET-verify
the manifest first, then create a marker whose content has no semantic role and whose
presence alone commits the generation. A third option is to weaken manifest presence to
mean "candidate publication," but rebuild and all readers would then need enough
validation to establish completeness independently.

Move must also abort before touching payload if stale-manifest deletion is not confirmed.
Likewise, if the quiescence check fails after publication, failure to remove the new
manifest must be reported rather than treating invalidation as successful.

### C-02 — Confirmed resolved: rebuild fails closed instead of omitting a remote job

The revised rebuild validation boundary resolves D2-02:

- the old index remains in place until reconstruction and validation finish;
- corrupt, unavailable, or malformed authoritative artifacts prevent the swap;
- errors are reported rather than converted into skipped rows;
- duplicate job IDs across remotes cause a diagnostic failure rather than an
  iteration-order choice.

Under the documented single-writer and strong-LIST preconditions, no process-crash
sequence was found that installs an index omitting a complete remote job.

The implementation contract should make these details explicit:

- an error on any LIST page or object read aborts the rebuild;
- manifests from all configured remotes are collected for duplicate detection before
  applying local-wins behavior;
- local corruption, such as a job directory missing `r3.yaml`, also aborts rather than
  being skipped;
- a stale `index.sqlite.new` from an interrupted prior attempt is discarded and
  recreated;
- the new SQLite database is committed and closed before `os.replace` installs it.

The structural boundary should additionally HEAD `data.tar.zst` and require its
existence and `ContentLength == archive_size`. This deliberately does not rehash the
archive, but it prevents rebuild from accepting an obviously missing or truncated
payload as structurally published.

## Interruption-protocol audit

### Move — not fully correct as written

Invalidate-before-overwrite makes payload replacement safe after successful
invalidation. The current recovery table nevertheless combines states that differ:

- before step 2, an old complete remote publication may still exist;
- after successful invalidation and before the final manifest PUT, the manifest is
  absent;
- after the final PUT but before its verification completes, a manifest may be visible
  but unverified;
- after verification and before the index transition, both copies are valid;
- after the index transition, the remote copy is authoritative and a local copy may
  remain until its atomic rename.

The first state is safe — local and the old remote snapshot are both intact — but the
table's statement that the old manifest was already deleted is inaccurate for a crash
between steps 1 and 2. The third state is C-01.

#### Suggested direction — not a decision

Split the 1–5 row at the invalidation and publication boundaries. Once C-01 has a
publication mechanism, include its precise success point in the table.

### Fetch — data-safe, but not command-retryable as written

Fetch step 0 says that an already-restored `jobs/<id>` is verified against the manifest
before cleanup resumes. Step 7 deletes the remote manifest first. A crash after that
delete leaves:

- a complete, previously verified local directory;
- an index row still marked remote;
- no remote manifest against which step 0 can perform its promised verification.

This directly contradicts the 6–7 and 7–8 recovery rows. `rebuild-index` can recover by
adopting the local directory, so the data remains safe, but rerunning `fetch` cannot
follow the stated protocol.

#### Suggested direction — not a decision

Persist the downloaded manifest as a local recovery receipt outside `jobs/` before the
staging rename. Keep it until unconditional remote cleanup and the index transition
finish. Step 0 can then verify against either the still-present remote manifest or the
local receipt. Remove the receipt after SQLite says local; a crash after the transition
may leave only harmless recovery-state debris.

### Rebuild — holds with small lifecycle clarifications

The old/new index construction gives the desired process-interruption states:

- before `os.replace`, the old complete index remains active;
- after `os.replace`, the new fully validated index is active;
- an interrupted build affects only its temporary file and is safely restarted.

This conclusion depends on recreating a fresh temporary database on every attempt and
closing it before replacement, as noted under C-02. A short explicit table would make
the contract clearer but is not required for correctness.

### Remove — not fully retryable as written

Two gaps remain.

First, a process may stop after `jobs/<id>` is atomically renamed away but before the
index row is deleted. The row says local while the directory is absent. Under I7,
ordinary location-aware job lookup must report that as corruption. A retried `remove`
must therefore avoid materializing the job through the normal `Index.get()` →
`Storage.get()` path and instead operate from the raw index row plus direct local and
remote probes.

Second, a remote job sweeps only its indexed remote, whereas a local job sweeps every
configured remote. This can leave objects behind through supported recovery paths:

1. fetch from remote A is interrupted after deleting A's manifest;
2. rebuild adopts the already-restored local directory;
3. the local job is moved to remote B;
4. removing the now-remote job sweeps B only;
5. manifestless archive/sidecars remain on A after the index row is gone.

That contradicts the stated guarantee that remove makes the job gone everywhere.

#### Suggested direction — not a decision

- Specify that remove's retry path works from the raw index entry and tolerates an
  intentionally missing local directory.
- Given the accepted single-owner assumption, sweep all configured remotes for every
  removal, irrespective of current indexed location.
- Require every deletion batch to complete without reported per-object errors before
  advancing to local deletion or index removal.

## Other contradictions and remaining gaps

### C-03 — Medium: directory members and the file-only manifest disagree

Safe extraction allows regular files and directories and requires every tar member name
to agree with a manifest path. The manifest deliberately contains files only, so
ordinary directory members have no manifest path with which to agree. An archive made
by recursively adding directories can therefore be rejected by its own inverse.

#### Suggested direction — not a decision

Either omit directory members entirely and create parent directories plus conventional
`output/` during extraction, or define a directory member as valid only when it is
`output/` or a normalized prefix of at least one manifest file path. Empty directories
other than `output/` remain unsupported as already documented.

### C-04 — Medium: structural checks do not cover an archive missing beneath a manifest

Rebuild verifies manifest shape and the two sidecars but does not state that it checks
whether `data.tar.zst` exists and has the declared size. External deletion or corruption
could therefore leave a manifest and sidecars that rebuild accepts even though fetch is
guaranteed to fail.

#### Suggested direction — not a decision

HEAD the archive and compare `ContentLength` with `archive_size` during rebuild. Report
the same missing/size-mismatch condition from `r3 remote check`. Continue reserving full
archive GET-and-rehash for move/fetch or a future deep-check mode.

### C-05 — Medium: remote removal can strand manifestless storage debris

`r3 remote remove` now checks the prefix for complete manifests, which protects live
remote jobs independently of SQLite and resolves the data-safety part of D2-06. It may
still drop the only configured route to manifestless payload objects or incomplete
multipart uploads. This is leaked storage rather than live-job loss, but the read-only
check and later garbage collection can no longer reach it through R3.

#### Suggested direction — not a decision

Consider refusing removal while *any* objects or multipart uploads remain under the
owned prefix, or require an explicit force option that reports what will become
unmanaged.

### C-06 — High documentation correction: unsupported concurrency has no data-safety guarantee

`LIMITATIONS.md` currently says concurrent mutation can corrupt the index but "is not
data loss" and is recoverable through rebuild plus remote check. That is stronger than
the unsupported single-writer model can guarantee.

For example, `remote remove` can list an empty prefix while a concurrent move is still
uploading, remove the remote configuration, and then allow move to publish and delete
locally using the already-loaded remote object. The physical bucket objects survive,
but R3 has lost their endpoint/bucket/prefix locator; rebuild and remote check cannot
reach them without manually recovering that configuration.

This does not require reopening repository locking in this pull request. It requires
the documented limitation to avoid promising safety for behavior explicitly declared
unsupported.

#### Suggested direction — not a decision

State that overlapping mutating commands are unsupported and carry no data-safety or
automatic-recovery guarantee. The normal single-writer protocols remain the guarantee
being reviewed here.

## Direct answers

### 1. Do D2-01 and D2-02 now hold?

- **D2-01:** stale-manifest invalidation now holds, but the complete publication claim
  does not yet hold across a crash between final manifest PUT and verification GET.
- **D2-02:** yes, under the documented single-writer, complete-pagination, and
  strong-LIST assumptions. Errors on any source must abort the swap.

### 2. Do the interruption tables hold as written?

- **Move:** no; split the pre-invalidation and PUT-before-verification states.
- **Fetch:** no; retry lacks a manifest after cleanup has deleted it.
- **Rebuild:** essentially yes, after specifying fresh-temp and close-before-replace
  handling.
- **Remove:** no; retry must bypass normal missing-local corruption lookup, and remote
  jobs must sweep every configured remote to satisfy "gone everywhere."

### 3. Did the revision introduce or leave anything?

The material remaining issues are C-01 through C-06 above. None requires reopening the
accepted quiescence, power-loss, or enforced-locking scope. C-01, fetch recovery, and
remove recovery should be resolved before the implementation plan is finalized; the
other items are smaller contract and diagnostics corrections.

## Recommended disposition

Make one more focused design revision before regenerating the implementation plan:

1. define a manifest publication event with no visible-but-unverified state;
2. persist fetch recovery evidence until remote deletion and index commit complete;
3. make remove retry from a raw index row and sweep every owned remote;
4. correct the move table and directory-member contract;
5. add archive existence/size checks to rebuild and remote check;
6. narrow the unsupported-concurrency claim in `LIMITATIONS.md`.

After those changes, the revised design should be ready to drive implementation and
live CEPH validation.
