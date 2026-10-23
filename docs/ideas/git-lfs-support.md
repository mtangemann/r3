# Idea: git-LFS support for git dependencies

*Status: idea, 2026-09-13. From a real checkout failure on a dependency
(`Susmit-A/DeepGaze3.5-VL`, whose `data/**.pkl` files are LFS-backed). Not a spec.
Tracked in [issue #84](https://github.com/mtangemann/r3/issues/84).*

## The problem

R3 mirrors a git dependency by cloning it into the repository:

```
git clone --bare <url> <repo>/git/github.com/<owner>/<repo>
```

A bare clone copies the git object store — commits, trees, and blobs, including the small
git-LFS **pointer** files — but *not* the LFS blobs themselves. LFS objects live outside the
object store (under `lfs/objects/`) and are only transferred by `git lfs fetch`/`pull`/smudge.

Checkout ([`r3/storage.py`](../../r3/storage.py), `checkout_git_dependency`) then does, in a
temp dir:

```
git init && git remote add origin <mirror> && git fetch --depth=1 origin <commit> && git checkout FETCH_HEAD
```

At `git checkout FETCH_HEAD`, git runs the LFS smudge filter for LFS-tracked files. The filter
reads the pointer and tries to download the real bytes from its remote — `origin`, i.e. the
local bare mirror — which has no LFS objects. Result:

```
Downloading data/centerbias/MIT/0985.pkl (3.1 MB)
Error downloading object: ... Smudge error: ... remote missing object <oid>
error: external filter 'git-lfs filter-process' failed
fatal: data/centerbias/MIT/0985.pkl: smudge filter lfs failed
```

R3 has **no** git-LFS handling anywhere (`grep -ri lfs r3/` is empty).

## Why it matters

Two distinct problems, not one:

1. **Checkout is blocked.** Any dependency on an LFS-backed repo fails — commit, checkout, and
   dev-checkout all route through the same bare mirror.
2. **Provenance gap.** Even once checkout is worked around, the LFS data is not stored in the R3
   repository. A committed job that depends on LFS content is therefore *not self-contained* — if
   the upstream repo or GitHub's LFS storage disappears, the job can no longer be reconstructed.
   For a tool whose reason to exist is reliable, reproducible research, this is the more important
   half: the mirror should capture the LFS blobs the same way it captures git objects.

## The data does exist upstream (this is fixable)

The failing "remote missing object" refers to the *local mirror*, not GitHub. Querying GitHub's
LFS batch API directly for the failing object returns a valid download action:

```bash
curl -s -X POST "https://github.com/Susmit-A/DeepGaze3.5-VL.git/info/lfs/objects/batch" \
  -H "Accept: application/vnd.git-lfs+json" -H "Content-Type: application/vnd.git-lfs+json" \
  -d '{"operation":"download","transfers":["basic"],
       "objects":[{"oid":"bb5a5cd8...ac65","size":3145908}]}'
# -> objects[0].actions.download.href = <signed S3 URL>   (object is present upstream)
```

So the objects are published; R3 just never fetches them. (Worth keeping as a diagnostic habit:
a "remote missing object" error names the endpoint that *looked*, not necessarily the one at
fault — probe the authoritative upstream before concluding the data is gone.)

## Immediate workaround (no code change)

Check out with the smudge filter disabled, then pull the LFS data straight from the source repo:

```bash
GIT_LFS_SKIP_SMUDGE=1 xr3 dev-checkout .          # writes pointer files, no download
cd <destination>/<repo>
git config lfs.url https://github.com/<owner>/<repo>.git/info/lfs
git lfs pull
```

This proves out the end state but bypasses the mirror, so it neither fixes provenance nor
generalizes to plain (non-dev) checkouts.

## Solution options

### Option A — LFS-complete mirror (recommended)

Make the bare mirror hold the LFS blobs, so R3 stays self-contained and works offline.

- **Populate at mirror creation / update.** Wherever the mirror is created or topped up
  ([`r3/repository.py`](../../r3/repository.py): the `git clone --bare` in `__contains__` and in
  `_resolve_git_dependency`, plus the `git fetch origin *:* --force` refresh), follow it with an
  LFS fetch into the mirror, e.g. `git -C <mirror> lfs fetch origin <commit>` (or `--all`). A bare
  repo stores fetched objects under `<mirror>/lfs/objects/`.
- **Materialize at checkout from the mirror.** The temp checkout must smudge from the mirror
  rather than the network. Either rely on git-lfs resolving objects from a local-path remote's
  `lfs/objects` (needs verification — see open questions), or make it explicit: check out with
  `GIT_LFS_SKIP_SMUDGE=1`, then `git -C <tempdir> lfs fetch <mirror> <commit>` + `git lfs checkout`
  (deterministic, no dependence on smudge auto-resolution).

Trade-offs: preserves provenance and offline reproducibility; grows the mirror by the LFS payload;
adds a hard-ish dependency on the `git-lfs` binary being installed (detect and degrade gracefully).

### Option B — smudge from origin (GitHub) at checkout

Point the temp checkout's LFS endpoint at the original GitHub repo so smudge downloads from there.
Smallest change, unblocks checkout — but it requires network + GitHub availability at every
checkout and stores nothing locally, so it does **not** fix the provenance gap. Reasonable only as
an interim behavior or an explicit `--no-mirror-lfs` escape hatch.

### Option C — opt-out / pointers-only

Expose `GIT_LFS_SKIP_SMUDGE`-style behavior (leave pointer files) as a supported mode for users who
manage LFS data themselves. Not a fix, but a cheap, honest fallback that turns a hard failure into a
documented, recoverable state — and a sensible default when `git-lfs` is not installed.

A likely end state combines A (the default, for provenance) with C (graceful degradation when
git-lfs is unavailable), and possibly B behind a flag.

## Open questions

- **Does git-lfs smudge/fetch resolve objects from a local *bare* mirror's `lfs/objects`?** This is
  the crux of Option A's checkout half. Quick test: `git -C <mirror> lfs fetch --all`, then run the
  normal R3 checkout and see whether smudge succeeds against `origin=<mirror>`. If not, use the
  explicit skip-smudge + `lfs fetch/checkout` path.
- **Fetch scope.** Per-commit (`lfs fetch origin <commit>`) keeps the mirror small but must run on
  every newly-pinned commit; `--all` is simpler but can be large. Per-commit matches how R3 already
  pins dependencies.
- **git-lfs availability.** Detect via `git lfs version`; if absent, warn and fall back to Option C
  rather than failing. Document git-lfs as an optional dependency.
- **Where in the lifecycle to capture.** `_resolve_git_dependency` runs at commit (pinning the
  commit) — the natural point to also capture that commit's LFS objects for provenance.
- **Interaction with immutability/hashing.** The dependency is referenced by commit and is not part
  of the depending job's own content hash, so materializing LFS content should not change any job
  hash — worth confirming when implementing.
- **Storage growth / dedup.** LFS payloads can dwarf the git history; consider whether mirrors
  should be shared/dedup'd across jobs (they already are, being keyed by `owner/repo`).

## Evidence

- Failure: `dev-checkout` of a job depending on `https://github.com/Susmit-A/DeepGaze3.5-VL.git`
  aborts at `smudge filter lfs failed` for `data/centerbias/MIT/0985.pkl` (oid `bb5a5cd8…ac65`,
  size 3145908).
- Pointer confirmed via the raw file URL (`version https://git-lfs.github.com/spec/v1`).
- Upstream availability confirmed via the LFS batch API (download action returned).
- `grep -ri lfs r3/` → no matches (no LFS handling in R3 today).
