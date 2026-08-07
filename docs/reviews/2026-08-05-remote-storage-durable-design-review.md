# Independent Review: Remote Storage Durable Design

**Review date:** 2026-08-05

**Feature branch:** `feature/remote-storage` at `aeb4f2e`

**Reviewed document:**
[`docs/superpowers/specs/2026-08-04-remote-storage-durable-design.md`](../superpowers/specs/2026-08-04-remote-storage-durable-design.md)

**Review status:** Second-pass design review; no implementation reviewed

> **Important:** This document is a review, not an approved implementation plan.
> Sections titled **Suggested direction** contain possibilities for the maintainer to
> evaluate. They are deliberately not design decisions or authorization to implement a
> particular solution.

The companion repair plan was intentionally excluded from this review because it is a
draft that will be regenerated after the design review.

## Executive summary

The revised design is substantially stronger than the implementation reviewed on
2026-08-04. In particular, these are good foundations:

- the remote bucket, rather than SQLite, is the authoritative source for remote jobs;
- the archive-only representation avoids two storage models with different behavior;
- the archive and human-readable sidecars have clear responsibilities;
- a manifest acts as a publication marker and integrity/listing record;
- location is represented explicitly rather than inferred from nullable fields;
- remote jobs fail explicitly when file access is unavailable;
- fetch uses safe staged extraction and complete content verification;
- rebuild constructs a new index before atomically replacing the old one;
- remote garbage collection is separated from the initial read-only consistency check.

The design nevertheless needs another focused revision before implementation starts.
The principal problem is not the overall architecture. It is that the current
publication and retry protocol does not yet make the promised invariant true: manifest
presence can temporarily or permanently coexist with incomplete or mismatched payload
objects. Rebuild can also silently install an index that omits authoritative remote
jobs, and fetch cleanup can leave remote objects that neither `remove` nor
`r3 remote check` discovers.

The repository-lock recommendation from the initial pass is deliberately **not a
blocker for this pull request**. Enforced multi-process serialization would broaden an
already large change and deserves its own design. For this pull request, the
single-writer limitation should be explicit and prominent. A lock design can follow in
a separate change, with the maintainer's current preference being to do that after this
pull request.

## Severity definitions

- **Blocker:** The design can lose or silently omit authoritative data, or a core
  invariant does not hold. Resolve before implementation.
- **High:** A normal interruption or lifecycle path can leave ambiguous or undiscoverable
  state. Resolve in the design unless the limitation is explicitly accepted.
- **Medium:** Material robustness, consistency, or maintainability issue that need not
  block the first implementation.
- **Follow-up:** Worth designing separately, but intentionally outside this pull
  request's scope.

## Findings

### D2-01 — Blocker: the manifest is not yet a reliable publication marker

The move protocol uploads and content-verifies the archive and two sidecars, then
uploads the manifest last. It does not download and content-verify the manifest before
changing SQLite and deleting the local job.

This leaves a direct gap in invariant I8. If the manifest upload is truncated,
corrupted, or otherwise not the bytes that were constructed locally, manifest presence
can still cause the only local copy to be deleted. The server behavior that motivates
GET-and-rehash verification for the other objects applies to the manifest as well.

There is a second and more serious retry problem. A complete old manifest may already
exist after an interruption between move steps 4 and 5, while the index and local job
still say that the local copy is authoritative. Rerunning move overwrites the fixed
archive and sidecar keys while that old manifest remains visible. During the upload it
can therefore describe a mixture of old and new objects. If this second attempt is
interrupted, the old manifest remains present even though its payload no longer
matches.

The documented single-writer assumption does not prevent this failure because it is a
sequential crash-and-retry path.

#### Suggested direction — not a decision

Within the selected fixed-key layout, consider making publication invalidation an
explicit first step:

1. If an old manifest exists and the local job is authoritative, delete or otherwise
   invalidate it before overwriting any payload key.
2. Upload the archive and sidecars.
3. Download and verify every payload object.
4. Upload the manifest last.
5. Download the manifest and verify its exact bytes too.
6. Only then change the index and remove the local job.

The interruption table should cover a retry beginning from an existing complete remote
copy, not only a first move into an empty prefix. Generation-specific payload keys would
provide an even cleaner publication model, but they are not necessary if the fixed-key
invalidation protocol is made complete.

The statement "manifest presence means complete" should also be scoped to objects
written through this protocol and not subsequently modified by an external writer.
Fetch must continue to verify content rather than trusting marker presence alone.

### D2-02 — Blocker: rebuild can silently omit authoritative remote jobs

The design says that a job whose sidecar is missing or corrupt is skipped with a
warning, after which the newly built index is installed. That turns remote corruption,
a transient read error, or an incomplete protocol state into silent removal of a row
from the cache.

This contradicts the model in which SQLite is disposable and the remote objects are
authoritative. Atomic replacement of the index file protects against a partial SQLite
write, but it does not make an incomplete logical reconstruction safe.

Rebuild also needs an explicit validation boundary. Before accepting a remote job, it
should validate at least:

- the manifest schema and supported version;
- that the manifest job ID matches the object-key job ID;
- the selected representation;
- normalized, unique, safe logical paths;
- declared hashes and sizes of both sidecars;
- the expected exact manifest-key shape below the configured prefix.

#### Suggested direction — not a decision

Collect reconstruction errors and refuse the final index swap if any authoritative job
cannot be reconstructed. Preserve the old index and report each affected remote and
key. If exposing corrupt jobs in the index is desirable, that should be a separately
specified state; silently omitting them should not be the fallback.

Deep verification of every archive during every rebuild would be expensive and is not
required by this suggestion. The design should distinguish a structurally published
remote job from one whose full archive content has just been reverified.

### D2-03 — High: fetch cleanup is not fully discoverable or retryable

Fetch deletes the manifest first and then deletes the archive and sidecars. This is a
reasonable way to remove the publication marker, but an interruption after the first
delete leaves ordinary objects without a manifest.

The current lifecycle operations do not fully account for that state:

- local `remove` probes for a leftover manifest and can therefore miss the remaining
  fixed payload keys;
- `r3 remote check` reports incomplete multipart uploads but not ordinary archive or
  sidecar keys whose manifest is absent;
- SQLite is changed to local before remote cleanup completes, so a complete leftover
  remote copy is no longer represented as the indexed location;
- a later move to another remote can create the same job ID on two remotes.

The last case makes rebuild ambiguous because its one-location model has no rule for
choosing between duplicate complete remote jobs. The current remote-check definition
would not necessarily flag the original copy when a matching local job still exists.

#### Suggested direction — not a decision

- Make cleanup of the four known keys idempotent and unconditional for a known job ID;
  do not require the manifest to exist before deleting the other three keys.
- Have `r3 remote check` report manifestless job prefixes and manifests on a remote that
  disagrees with the indexed location.
- Detect duplicate job IDs across configured remotes and fail rebuild with a useful
  diagnostic rather than selecting one based on iteration order.
- Consider keeping the indexed location remote until remote cleanup has succeeded. The
  complete restored local directory can make a retry safe even while the index still
  describes the pre-fetch location.

Bucket versioning and object retention need an explicit deployment assumption. In a
versioned S3 bucket, an ordinary delete normally creates a delete marker while retaining
previous versions, so "deletes the remote copy" is not literally true. See
[AWS: Deleting Amazon S3 objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjects.html).

### D2-04 — High: `remove` needs its own interruption protocol

`remove` now has repository-wide effects: it may delete the local directory, change
SQLite, and sweep copies or partial copies from multiple remotes. The design states the
intended result but does not give it the same interruption analysis as move and fetch.

Potential intermediate states include a deleted local directory with a stale local
index row, a removed manifest with remaining payload objects, and successful deletion
on only some configured remotes. Per-object S3 deletion errors also need to be examined
rather than treating an aggregate request as automatically successful.

#### Suggested direction — not a decision

Add a remove interruption table and require every step to be safely retryable. Specify
which copy remains authoritative until the operation commits and how partial remote
cleanup is reported. If shared remotes are permitted, also state why sweeping the same
job ID across every configured remote cannot delete another repository's live job; if
they are not permitted, document that ownership assumption.

### D2-05 — High: mutable output needs a quiescence boundary

Constructing the archive and hashes from the same bytes guarantees internal consistency
of the captured representation. It does not guarantee that the job directory was a
stable snapshot. A running command may create or modify output after the archive scan
and before local deletion, causing valid new output to be deleted without ever reaching
the remote.

The repository-wide single-writer assumption does not cover external processes writing
inside a job's output directory.

#### Suggested direction — not a decision

State a precondition that a job must not be running or otherwise mutable during move,
and decide how R3 can enforce or at least detect that condition. Possibilities include
a job-active marker, integration with the command lifecycle, or a final file-set and
metadata stability check before deletion. A complete filesystem snapshot would be
stronger but may be unnecessary for the initial design.

### D2-06 — High: remote removal still relies too heavily on the cache

`r3 remote remove` refuses removal when indexed jobs reference the remote. SQLite is,
however, explicitly a disposable cache and may be stale or damaged. In that situation,
removing the configuration can discard the only repository metadata that locates the
authoritative bucket objects.

Format pinning in the manifest does not recover a lost endpoint, bucket, or prefix.

#### Suggested direction — not a decision

Before removing a configured remote, inspect its prefix and refuse removal while
complete manifests exist, independently of SQLite. Make clear that remote configuration
is durable repository metadata that must be backed up even though the job index can be
rebuilt.

### D2-07 — Medium: move and fetch need one symmetric filesystem model

Fetch rejects path traversal and most non-regular archive members, but the move-side
policy for symlinks, hardlinks, device nodes, FIFOs, and arbitrary empty directories is
not fully specified. A locally accepted job must not produce an archive that its inverse
operation refuses or reconstructs differently.

The special treatment of an empty conventional output directory also does not address
an arbitrary dependency on an empty directory.

#### Suggested direction — not a decision

The simplest initial model is regular files and directories only, with move rejecting
everything else before publication. Include directory entries in the logical manifest
if empty directory dependencies are supported; otherwise document that they are not.

During fetch, additionally validate duplicate archive members, agreement between member
names and manifest paths, declared and extracted sizes, and reasonable count/size limits
before committing the staging directory.

### D2-08 — Medium: the crash model should distinguish process exit from power loss

The design uses temporary files, SQLite transactions, and `os.replace()`, which are good
tools for process-interruption safety. Atomic namespace replacement alone does not
guarantee persistence across power loss. Newly written files and affected parent
directories may require `fsync()` before an operation can claim durable completion.

#### Suggested direction — not a decision

Either define the initial guarantee as process-interruption recovery, or specify the
file and directory synchronization points needed for power-loss durability. Apply the
same distinction to index rebuild, migration, staging rename, and the local `.trash`
transition.

### D2-09 — Follow-up: repository locking should have a separate design

Overlapping repository mutations can still produce lost index updates or interfere with
move/fetch recovery. In particular, rebuild replaces a whole index constructed from a
snapshot while another process may be adding or relocating a job.

Enforced locking would nevertheless widen this pull request materially. Correct locking
also requires decisions about lock scope, stale-owner recovery, command nesting, and
the semantics of the filesystem hosting the repository. Treating it as a small incidental
addition would under-design it.

#### Suggested direction — not a decision

Do not add an ad hoc lock to this repair pull request. Instead:

- prominently document that mutating R3 commands and rebuild must not overlap;
- keep all individual operations internally interruption-safe under that assumption;
- prepare a separate repository-concurrency design, currently expected after this pull
  request;
- consider moving it earlier only if concurrent writers are already a realistic normal
  workflow rather than an unsupported misuse.

Until that follow-up exists, the single-writer condition remains an operational
limitation, not an invariant enforced by R3.

## Answers to the requested review questions

### 1. Is full archive re-transfer for content verification sound?

Yes. It is the appropriate conservative default for the first durable implementation.
On fetch, hashing can happen during the required download and therefore adds no second
network transfer. On move, the verification GET is expensive, but it provides the
end-to-end guarantee the design claims. Multipart ETags and echoed user metadata are
not substitutes for verification of the stored bytes.

Modern AWS SDKs support additional checksum mechanisms, but behavior depends on SDK
configuration, and `WHEN_REQUIRED` intentionally does less automatic checksum handling
than the current `WHEN_SUPPORTED` default. See
[AWS SDK data-integrity protections](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html).
Ceph documents multipart support, but this does not by itself establish compatibility
with every modern checksum header for the exact deployed RGW release. See
[Ceph Object Gateway S3 API](https://docs.ceph.com/en/latest/radosgw/s3/).

A fast path can be considered later if a capability probe and live tests prove a
server-validated checksum path on the actual CEPH deployment. User-supplied SHA-256
metadata alone only proves that the metadata round-tripped, not that the service checked
the object against it. The manifest itself must also be covered by content verification.

### 2. Is the documented single-writer assumption acceptable for now?

Acceptable as an explicit temporary scope restriction, but not as a permanent concurrency
story. Repository locking should be designed separately and is not a blocker for this
repair pull request. The limitation must be visible enough that users do not reasonably
infer that concurrent mutation is supported.

### 3. What multipart/CEPH issues should the implementation preempt?

The implementation and live test should:

- explicitly set multipart threshold, part size, and maximum concurrency rather than
  relying on evolving SDK defaults;
- use at least three unequal parts so the final short-part path is exercised;
- confirm that multipart was actually used, then perform a full GET and SHA-256 check;
- run with the production endpoint, addressing style, TLS, and checksum configuration;
- exercise failure after one or more uploaded parts and verify abort/list permissions;
- exercise failure around multipart completion and per-object deletion errors;
- start with conservative concurrency for RGW;
- detect abandoned multipart uploads through `r3 remote check`;
- arrange a bucket lifecycle rule for old incomplete uploads where deployment policy
  permits it.

Relevant references are
[Boto3 file transfer configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#file-transfer-configuration)
and
[AWS lifecycle cleanup of incomplete multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html).

Exact checksum-header behavior should be treated as a property of the lab's specific
Ceph release and configuration. A small recorded capability matrix is more useful than
assuming parity with current Amazon S3.

### 4. Do all interruption-recovery tables currently hold?

Not yet:

- **Move:** an existing manifest can remain visible while fixed payload keys are
  overwritten, and the newly uploaded manifest is not content-verified.
- **Fetch:** restoration protects the data reasonably well, but cleanup can leave
  undiscoverable manifestless objects or an unrepresented duplicate.
- **Rebuild:** atomic file replacement is sound, but a logically incomplete rebuild can
  still replace a more complete old index.
- **Remove:** no equivalent interruption table exists yet.

The tables should also state whether “interruption” means process termination only or
includes host power loss.

## Recommended disposition

Revise the design before generating the final implementation plan. The necessary work
is focused rather than architectural:

1. repair manifest invalidation, publication, and verification;
2. make rebuild fail closed instead of silently omitting remote jobs;
3. make fetch/remove cleanup discoverable and idempotent, including duplicate-ID
   handling;
4. specify the mutable-output boundary and remove recovery;
5. clarify remote-configuration and filesystem assumptions;
6. retain repository locking as a separately designed follow-up.

After those changes, the design should be a solid basis for implementation and live
CEPH validation.
