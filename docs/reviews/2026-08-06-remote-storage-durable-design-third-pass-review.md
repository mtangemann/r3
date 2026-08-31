# Third-Pass Confirmation Review: Remote Storage Durable Design

**Review date:** 2026-08-06

**Feature branch:** `feature/remote-storage` at `aeb4f2e`

**Reviewed document:**
[`docs/superpowers/specs/2026-08-04-remote-storage-durable-design.md`](../superpowers/specs/2026-08-04-remote-storage-durable-design.md),
including its 2026-08-06 revision

**Companion document reviewed:** [`LIMITATIONS.md`](../../LIMITATIONS.md)

**Review status:** Third focused design-confirmation pass; no implementation reviewed

> **Important:** This document is a review, not an approved implementation plan.
> Sections titled **Suggested direction** contain possibilities for the maintainer to
> evaluate. They are deliberately not design decisions or authorization to implement a
> particular solution.

## Scope

This pass checks the changes made in response to findings C-01 through C-06 and the
fetch/remove retry gaps from
[`2026-08-05-remote-storage-durable-design-confirmation-review.md`](2026-08-05-remote-storage-durable-design-confirmation-review.md).
It focuses on three questions:

1. whether verified staging plus `CopyObject` closes the final manifest's
   visible-but-unverified window;
2. whether the fetch receipt and raw-row/all-remotes remove protocols recover at every
   process-interruption boundary;
3. whether the revision introduces or retains any contradiction.

The maintainer-accepted scope remains unchanged and is not reopened:

- move quiescence is a precondition plus a pre-delete stability re-check;
- durability covers process interruption, not power loss;
- repository locking is deferred and single-writer operation is an unenforced hard
  operating rule.

## Executive conclusion

The 2026-08-06 revision closes the remaining data-loss blockers under its stated
assumptions:

- **C-01 is resolved** if the deployed CEPH RGW provides the normal S3 contract that a
  small, single-operation `CopyObject` is atomic and byte-faithful. A final manifest is
  now either absent or copied atomically from GET-verified staging bytes.
- **Fetch recovery is resolved.** The local receipt preserves the integrity evidence
  needed after remote cleanup deletes the original manifest.
- **Remove's authoritative-state ordering is resolved.** Raw-row access survives the
  intentionally missing local-directory state, and sweeping every configured remote
  closes the cross-remote debris path.
- **Rebuild remains resolved.** Its explicit error boundary, all-remote enumeration,
  duplicate detection, archive HEAD/size check, and replace-last lifecycle are
  coherent.
- The files-only archive, guarded remote removal, and corrected concurrency limitation
  address C-03 through C-06 in principle.

No unresolved data-loss blocker was found. Three targeted contract/cleanup corrections
remain before the implementation plan should be finalized:

1. define the exact `CopyObject` success contract and describe final-marker verification
   accurately;
2. include transient remote and local recovery artifacts in remove/reconciliation;
3. correct the archive-member set to exclude the two sidecars.

## Findings

### T-01 — Confirmed resolved: verified staging-copy closes C-01

The revised publication protocol has the necessary states:

1. the stale final manifest is deleted and its absence is confirmed;
2. payload objects are uploaded and content-verified;
3. the candidate manifest is PUT under `manifest.json.staging`;
4. the staging object is GET and byte-compared with the local manifest;
5. a server-side `CopyObject` atomically creates the final `manifest.json` from those
   verified bytes;
6. the staging object is deleted;
7. only afterward may the index transition and local deletion occur.

Under the standard S3 `CopyObject` contract, a crash before the copy leaves the final
key absent, while a crash after the copy leaves the complete byte-faithful copy. There
is no client-upload interval in which the final key is visible before its source bytes
have been verified. Amazon S3 documents a copy below 5 GB as a single atomic operation;
the manifest is necessarily far below that limit:

- [AWS: Copying, moving, and renaming objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html)
- [Ceph: S3 Object Operations](https://docs.ceph.com/en/reef/radosgw/s3/objectops/)

This conclusion treats atomic, byte-faithful Copy Object as a backend capability that
must be demonstrated on the exact lab RGW release. The design already supplies a
separate completion-marker fallback if that capability cannot be trusted.

#### Suggested direction — not a decision

Make the implementation contract explicit:

- use the low-level, single-operation `copy_object`, not a transfer-manager multipart
  copy;
- wait for and process the complete CopyObject response;
- treat exceptions and embedded response errors as failure;
- exercise successful copy, pre-copy failure, and post-copy interruption against the
  live CEPH endpoint;
- consider `CopySourceIfMatch` using the staging ETag, binding the copy to the exact
  staging object that was verified.

AWS notes that CopyObject may return HTTP 200 with an error embedded in the response;
the complete response must be processed. AWS SDKs detect and handle this condition:

- [AWS: CopyObject API response and errors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)

I8's wording should distinguish direct from derived verification. The payload objects
and staging manifest are downloaded and rehashed/byte-compared; the final marker is
derived atomically from those verified staging bytes through the trusted copy operation.
An optional GET of the final key before changing SQLite would provide an additional
deployment check, but the atomic staging-copy — not that later GET — is what closes the
publication window.

### T-02 — Confirmed resolved: fetch receipt provides complete retry evidence

The receipt repairs the previous contradiction between deleting the remote manifest
first and needing that manifest during retry:

- before local rename, an interrupted fetch leaves the complete remote copy and can
  recreate receipt/staging state;
- after local rename but before remote cleanup, either the remote manifest or receipt
  verifies the restored directory;
- after manifest deletion or partial payload cleanup, the receipt remains available;
- after complete remote deletion but before the index transition, the receipt proves
  the local job and permits idempotent cleanup plus commit;
- after the index commit, the job is authoritatively local and a leftover receipt is
  only cleanup debris.

No state now requires integrity evidence that has already been deleted.

#### Suggested direction — not a decision

- Write the receipt through a temporary file plus `os.replace`.
- When both the remote manifest and receipt exist, require them to agree rather than
  silently preferring one.
- Treat receipt deletion after the committed SQLite transition as cleanup. Failure to
  delete it must not roll back or misreport the location transition that already
  succeeded.

### T-03 — High: remove still omits protocol recovery artifacts

Raw-index-row operation and the all-remotes sweep repair the prior authoritative-state
gaps. The stated stronger result — a removed job is gone everywhere — still omits
artifacts created by move/fetch recovery:

- `{prefix}{job_id}/manifest.json.staging` on every configured remote;
- `{repo}/.fetch/<job_id>.receipt.json`;
- stale `{repo}/.fetch/<job_id>-<rand>/` extraction staging directories;
- `{repo}/.trash/<job_id>-<rand>` created by an interrupted move or remove.

For example, a process can stop after the final manifest copy but before deletion of
the staging object. A later `remove` deletes the four canonical remote keys and index
row but leaves `manifest.json.staging`. Similarly, a process can stop after
`os.replace(jobs/<id>, .trash/<id>-<rand>)` but before `rmtree`; a retry sees no live
job directory and currently has no specified step that removes the trash copy.

These are not authoritative-state or data-loss failures, but they contradict remove's
explicit "gone everywhere" contract and may retain full local job data.

#### Suggested direction — not a decision

Make all recovery artifacts first-class, idempotent remove targets before deleting the
index row:

- delete the remote staging-manifest key on every remote;
- delete the local fetch receipt and matching stale fetch directories;
- delete matching trash entries left by prior operations.

`r3 remote check` should report a staging manifest even when a complete final manifest
also exists. Its current manifestless-prefix rule naturally catches staging when the
final key is absent, but not necessarily staging debris beside a successful publication.

### T-04 — Medium: the files-only archive contract still includes sidecars incorrectly

The revision correctly removes directory members, but §11 says the archive member set
is exactly the manifest's `files` set. That is false under the selected layout:
`r3.yaml` and `metadata.yaml` are listed in the logical manifest but are deliberately
excluded from the archive and stored only as sidecars.

The exact contract should be:

```text
archive member paths
    == manifest file paths - {"r3.yaml", "metadata.yaml"}
```

Fetch should reject `r3.yaml` or `metadata.yaml` as archive members. Accepting and
extracting them before overwriting them with downloaded sidecars would create two
physical representations and weaken the layout's single-source guarantee.

#### Suggested direction — not a decision

- Define the archive member set as every non-sidecar manifest entry, exactly once.
- Reject both sidecar names if encountered inside the archive.
- Continue recreating parent directories and conventional `output/` locally; neither
  needs a tar directory member.

### T-05 — Confirmed resolved: rebuild, remote removal, and limitations

The remaining revised areas are internally consistent:

- **Rebuild:** any LIST/read/validation error aborts; all remote manifests are collected
  before duplicate/local-wins handling; local corruption aborts; stale temporary index
  files are replaced; the new database is committed and closed before installation;
  archive existence and declared size are structurally checked.
- **Remote check:** missing or size-mismatched archives are reported for indexed remote
  jobs. It should additionally report the staging debris described in T-03.
- **Remote removal:** complete jobs, manifestless objects, and incomplete multipart
  uploads prevent removal unless the user explicitly supplies `--force`, which reports
  what becomes unmanaged.
- **Filesystem model:** files-only tar entries plus locally recreated directories remove
  the directory/manifest ambiguity, subject only to T-04's sidecar-set correction.
- **Concurrency:** the design and `LIMITATIONS.md` now correctly promise no data-safety
  or automatic-recovery guarantee for unsupported concurrent mutation. This resolves
  C-06 without requiring a repository lock in this pull request.

The accepted quiescence and process-interruption-only durability boundaries introduce
no new contradiction in this revision.

## Direct answers

### 1. Does staging-copy close the visible-but-unverified window?

**Yes**, provided the exact RGW deployment satisfies the standard small-object
`CopyObject` contract and the implementation fully processes the response. The final
key is atomically copied from already GET-verified staging bytes; it is not independently
verified by a client GET, which should be reflected accurately in I8's wording.

### 2. Do fetch and remove hold at every interruption point?

- **Fetch:** yes for authoritative state and command retry. The receipt exists for every
  state after the local rename in which the remote manifest may be absent.
- **Remove:** yes for the four canonical remote objects, live local directory, and raw
  index row. It does **not yet** satisfy "gone everywhere" because remote staging,
  receipt, fetch-staging, and trash artifacts are omitted.

### 3. Did the edits introduce a contradiction or gap?

Two material cleanup/contract issues remain:

1. temporary recovery artifacts are not consistently enumerated by remove and remote
   check;
2. the archive-member equality incorrectly includes the two sidecars.

The CopyObject response and capability assumptions also need to be explicit and covered
by the live CEPH test, but the staging-copy architecture itself is sound.

## Recommended disposition

No further architectural redesign is needed. Before regenerating the final
implementation plan:

1. specify CopyObject response processing and describe indirect final-marker
   verification accurately;
2. add remote staging, local receipt/fetch-staging, and trash entries to remove and
   reconciliation;
3. define the archive member set as manifest entries minus the two sidecars.

After those targeted corrections, the durable design is ready to drive implementation
and live CEPH validation.
