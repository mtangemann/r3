# Known Limitations

Sharp edges and intentional scope limits in R3, in one visible place. Each entry
says what it means in practice and where the design detail lives — the reasoning and
invariants belong in the design docs, not here.

Read this before relying on R3 for anything you can't afford to lose, and before
pointing multiple processes or people at one repository.

---

## No concurrency control (single writer per repository)

**Impact.** R3 assumes one mutating process per repository at a time. Running
mutating commands concurrently on the same repository — e.g. a batch `r3 move`
script while you `r3 commit`, or `r3 rebuild-index` overlapping a `move`/`fetch` — is
**unsupported and carries no data-safety or automatic-recovery guarantee.** It can
corrupt the index, and some interleavings can cause loss R3 cannot recover from — for
example, `r3 remote remove` seeing an empty prefix while a concurrent `move` is still
uploading can drop the remote configuration; the move then publishes and deletes the
local copy, and the surviving bucket objects have no recorded endpoint/bucket/prefix
by which R3 can reach them. (Under correct single-writer use, the bucket is
authoritative and `rebuild-index` + `r3 remote check` recover index drift — but that
guarantee does **not** extend to concurrent mutation.)

**Workaround.** Do not run overlapping mutating R3 commands (including
`rebuild-index`) on one repository. Treat single-writer as a hard operating rule.

**Status.** Deliberately not addressed in the remote-storage work. Enforced locking
is a separate design (complicated by network filesystems such as the cluster's,
where `flock` semantics are unreliable). Most relevant once concurrent writers become
normal — i.e. the shared-remote/collaboration feature — and best designed together
with it. Not scheduled otherwise.

**Details:** see the remote-storage durable-design notes.

---

## Durability is against process interruption, not power loss

**Impact.** R3 operations are safe to interrupt (crash, `Ctrl-C`, kill) and re-run.
They are **not** yet guaranteed durable across a host power loss mid-operation: newly
written files and their parent directories are not explicitly `fsync`-ed, so a power
cut can in principle lose a just-completed local rename.

**Mitigation in practice.** The authoritative stores already cover the important
cases — SQLite commits are `fsync`-durable by default, and S3/CEPH is service-durable
— so the exposed surface is the transient local staging/rename/`.trash` steps, which
are recoverable by re-running the operation (and `rebuild-index`).

**Workaround.** After an unclean host shutdown mid-operation, re-run the interrupted
command and, if in doubt, `r3 rebuild-index` + `r3 remote check`.

**Status.** Follow-up: add explicit file+directory `fsync` at commit points if
power-loss durability is required.

**Details:** see the remote-storage durable-design notes.

---

## Moving a job requires it to be quiescent

**Impact.** `r3 move` captures the job directory, uploads it, then deletes the local
copy. If the job is still **running** (e.g. a slurm job still writing `output/`),
output produced after the capture but before deletion would be lost.

**Safeguard.** `move` re-checks the job's file set immediately before deleting local
and aborts if it changed since capture — so a mutating job is detected rather than
silently truncated. This is a detection, not a lock: don't rely on racing it.

**Workaround.** Only move jobs that have finished.

**Status.** A stronger active-job marker / run-lifecycle integration is possible but
out of scope for the remote-storage work.

**Details:** see the remote-storage durable-design notes.

---

## Remotes are single-owner (no shared/collaborator remotes yet)

**Impact.** Each remote prefix is assumed to belong to exactly one repository. R3
lifecycle operations (`remove`'s orphan sweep, `rebuild-index`) assume no other
repository is reading or writing the same remote objects. Pointing two repositories
at the same remote prefix is unsupported and can let one repository's cleanup affect
another's jobs.

**Workaround.** Give each repository its own remote (bucket or prefix).

**Status.** Shared remotes for collaboration are a planned future feature; they
require both a concurrency design and revised lifecycle/ownership rules.

**Details:** see the remote-storage durable-design notes.

---

## Interrupted move/fetch can leave orphaned remote objects

**Impact.** An interruption during the cleanup phase of `move`/`fetch` can leave
remote objects that no index row references (dead weight; on a versioned bucket, also
delete markers). These waste storage but are not data loss.

**Detection & recovery.** `r3 remote check` reports orphaned remote objects and
incomplete multipart uploads. `r3 remove` cleans a job's remote objects. Automatic
reclamation (a mutating `gc`) is deferred.

**Status.** `r3 remote check` (read-only) is in scope; the mutating `r3 gc` (delete
orphans, abort incomplete multipart uploads) is a documented follow-up, to be
designed together with shared-remote semantics.

**Details:** see the remote-storage durable-design notes.

---

## Empty directories are not preserved

**Impact.** A job's logical file set is a list of files; empty directories are not
represented (with the exception of the conventional `output/`, which is always
present). A dependency on an otherwise-empty directory will not behave consistently
across local and remote jobs.

**Workaround.** Don't depend on empty directories; ensure a directory you depend on
contains at least one file.

**Details:** see the remote-storage durable-design notes.

---

## Remote configuration is durable metadata — back it up

**Impact.** A remote's endpoint/bucket/prefix live in the repository's `r3.yaml`, not
in the bucket. The SQLite index is disposable (rebuildable), but this configuration
is **not**: lose it and you lose the pointer to where your archived jobs are stored.

**Workaround.** Back up `r3.yaml` alongside your data. `r3 remote remove` refuses to
drop a remote while complete jobs still exist under its prefix.

**Details:** see the remote-storage durable-design notes.

---

## `r3.yaml` is managed by R3 — don't hand-format it

**Impact.** `r3.yaml` is an R3-managed file. `r3 remote add`/`r3 remote remove` and
format migrations rewrite it by re-serializing the parsed config, which does **not**
preserve comments, blank lines, or key order. Any hand-formatting you add there is
lost on the next such rewrite (`remote add`/`remove` now warn when they detect
comments, but they still overwrite).

**Workaround.** Don't rely on comments or a particular layout in `r3.yaml`. Keep any
notes about your setup elsewhere.

**Status.** Round-trip preservation via a comment-aware YAML library is a possible
future improvement — see [ROADMAP.md](ROADMAP.md).
