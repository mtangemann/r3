# Remote Storage Repair — Finding Disposition Summary

> Final disposition for the `feature/remote-storage` durable-storage repair. Maps every
> original review finding to how it was resolved, for use as (or folded into) the PR
> description. **Not pushed.** Companion documents:
> [durable design](specs/2026-08-04-remote-storage-durable-design.md) (authoritative;
> §14 is the finding map) and the [repair plan](plans/2026-08-04-remote-storage-repair.md)
> (phase-by-phase execution-status notes are the primary record of resolutions).

## 1. Summary

An independent review of the remote-storage feature branch raised **14 findings**
(F-01…F-14: 2 Blocker, 6 High, 5 Medium, 1 Low). This branch resolves all 14 through a
sequenced repair, phases A–H:

- **A — Integrate `main` (F-07):** merge `main` (no rebase — published branch), revert
  the interim `Job.files` API, raise the Python floor to 3.10.
- **B — Manifest primitive, archive-only representation, safe extraction (F-01, F-05):**
  `r3.manifest` (integrity/listing record), `r3.archive` (files-only `tar.zst` builder +
  adversarial `safe_extract`), `Job` remote projection with `FilesUnavailableError`.
- **C — Migrations (F-02):** rewrite the beta.8/beta.9 migrations crash-safe — migration-local
  SQLite, version marker written last via `os.replace`, `.bak`-preserving.
- **D — Crash-safe move/fetch (F-04, F-05):** archive-only `S3Remote` transport (4-object
  layout, manifest published via verified staging-copy) and crash-safe `move`/`fetch` state
  machines (content-verify every object, quiescence re-check, atomic local delete; fetch
  receipt + idempotent finalize).
- **E — Index durability (F-08, F-09, F-10, F-11):** `Transaction` rollback-on-exception;
  atomic, bucket-backed, fail-closed `rebuild`; explicit `location` (absence = corruption);
  lazy file-list; bound `location` parameter.
- **F — Dependencies & checkout (F-06):** directory-membership `__contains__`, location-aware
  `find_dependents`, traversal-matched transitive-checkout preflight.
- **G — CLI + lifecycle (F-03, F-13):** idempotent "gone everywhere" `remove` protocol,
  read-only `r3 remote check` reconciliation, validated `remote add`/`remove` (+ CEPH flags,
  atomic `r3.yaml` writes), `r3 edit` refuses remote jobs.
- **H — Failure tests, docs, live-S3, final gate (F-12, F-14):** failure-mode assertions,
  live-S3/multipart tests rewritten (opt-in), docs + `ROADMAP.md`/`LIMITATIONS.md`, this
  disposition, final gate.

Between phases D and E an **independent A–D code-review remediation batch** (2026-08-11) ran
a 5-way review over the shipped A–D code and fixed five genuinely-new defects (atomic +
version-stamped migration backup; re-added absolute extraction cap + hardened manifest/path
validation; `publish_manifest` `CopyObject`-result validation + staging cleanup; `move`
round-trips the archive before any remote mutation, fetch persists a receipt before finalize,
fetched job dirs write-protected; F-11 bound `location` param pulled forward). Confirmation and
third-pass design reviews (C-01…C-06, T-01…T-05) were resolved in the frozen design and its
implementation (§3 below).

**Final gate (2026-08-17), on `feature/remote-storage`, not pushed:**

- `AWS_CONFIG_FILE=/dev/null python -m pytest -m "not live_s3" -q` → **390 passed, 6 deselected**
  (the `live_s3` suite), 19 warnings.
- `make lint` → ruff **Passed**; mypy **Success: no issues found in 31 source files**.
- The `live_s3` suite is **opt-in** and was **not** run here — it must be run manually against
  CEPH before deployment (see §5).

## 2. Finding disposition (F-01 … F-14)

All 14 findings are **Fixed**; none were deferred. Descriptions are from the design's §14
finding map; resolutions are from the plan's execution-status notes and the commits.

| # | Sev | Finding (short) | Resolution |
|---|-----|-----------------|------------|
| F-01 | Blocker | Downloads allow writes outside the destination (path traversal) | **Fixed** — Phase B3: streaming per-member `safe_extract` into a staging dir (validate before write; no abs/`..`/outside-root; files-only; name must match a non-sidecar manifest entry); individual-key download path removed. Absolute extraction cap re-added in the A–D remediation. (design §11) |
| F-02 | Blocker | beta.7→beta.8 migration fails at branch HEAD | **Fixed** — Phase C: beta.8/beta.9 rewritten crash-safe via a migration-local SQLite helper (never constructs `Repository`), version marker written last, `.bak`-preserving; backup made atomic + version-stamped in the A–D remediation. (design §12) |
| F-03 | High | Remote config mutable while jobs depend on it (strands jobs) | **Fixed** — Phase G: representation pinned per-job in the manifest; `remote remove` refuses while complete manifests exist under the prefix (bucket check, not index) and guards residual debris unless `--force`. (design §9) |
| F-04 | High | `move`/`fetch` not crash-safe | **Fixed** — Phase D + A–D remediation: invalidate-stale-manifest-first, content-verify-all before publish, quiescence re-check, atomic local delete; idempotent-finalize `fetch` with index-flip-last, unconditional cleanup, and a local receipt; `move` round-trips the archive before any remote mutation. (design §5/§6) |
| F-05 | High | `exists()` ≠ completeness/integrity | **Fixed** — Phase B/D: manifest is the completion marker (uploaded strictly last, via verified staging-copy); every object — archive, both sidecars, and the manifest itself — content-verified before publish. (design §4.1/§5) |
| F-06 | High | Remote dependency handling incomplete | **Fixed** — Phase F: directory-membership `__contains__` (via cached file list), location-aware `find_dependents` (projection, not `storage.get`), traversal-matched transitive-checkout preflight run before `resolve()`. (design §10) |
| F-07 | High | Integration with current `main` unresolved | **Fixed** — Phase A: merged `main`; manifest built from single-pass hashing (not `Job.files`); took `main`'s `/output` exclusion, safer `Storage.__contains__`, packaging, and CLI. |
| F-08 | High | Index authoritative but treated as a rebuildable cache | **Fixed** — Phase E1/E2: `Transaction.__exit__` rolls back on exception; `rebuild` is atomic (`index.sqlite.new` + `os.replace`), bucket-backed (remote rows reconstructed from manifests/sidecars), and **fails closed** (structural validation incl. `job_id==key`, sidecar size+sha256, HEAD `archive_size`); rejects duplicate job IDs across remotes. (design §7.1/§7.2) |
| F-09 | Med | Missing local data silently represented as remote-like | **Fixed** — Phase E3: `get`/`find` select explicit `location`; a `local` row with a missing dir or missing `r3.yaml` raises a clear corruption error; the remote projection raises rather than returning empty. (design §7.3/§8) |
| F-10 | Med | File manifests eagerly loaded into every query result | **Fixed** — Phase E4: `find` is lazy about the `files` column (regression test pins the laziness). (design §7.4) |
| F-11 | Med | `location` filter interpolated into SQL | **Fixed** — A–D remediation (pulled forward from E4): `location` bound as a parameter in `Index.find`. Broader `query.py` review noted out of scope. (design §7.5) |
| F-12 | Med | Promised failure-mode coverage incomplete | **Fixed** — Phase H1/H2: failure-mode assertions (no finalized manifest after failed verify; no `jobs/<id>` after failed fetch; corruption raises; per-object `delete_objects` `Errors` inspected). A live multipart (>threshold) `move`/`fetch` test exists but is **opt-in** and must be run against CEPH (see §4/§5). (design §5/§6) |
| F-13 | Med | Remote-management CLI bypasses repository validation | **Fixed** — Phase G3: `remote add` validates via `Remote.from_config()` before an atomic (`temp`+`os.replace`) `r3.yaml` write; CEPH flags exposed; `archive_format` is not a CLI flag. (design §9) |
| F-14 | Low | Python 3.9 vs the new boto dependency misaligned | **Fixed** — Phase A/§13: `requires-python = ">=3.10,<3.13"`; `pyzstd` moved to required deps. (design §13) |

## 3. Confirmation / third-pass design-review findings (C-/T-)

These were raised against the durable **design** (not the shipped code) during two review
rounds after the first design revision, and were resolved in the frozen design and its
implementation. All are **resolved**; none deferred.

| # | Sev | Finding (short) | Resolution |
|---|-----|-----------------|------------|
| C-01 | Blocker | Final manifest publication had an unverified-visible window | **Resolved** — verified staging-copy: PUT `manifest.json.staging` → GET+byte-compare → `copy_object` (`CopySourceIfMatch`) → delete staging. Confirmed by T-01. (design §5 step 5; Phase B/D) |
| C-02 | — | Rebuild could omit a remote job instead of failing | **Resolved** (already, in the reviewed revision) — fail-closed rebuild with an explicit validation boundary. Confirmed by T-05. (Phase E2) |
| C-03 | Med | Directory members vs the file-only manifest disagreed | **Resolved** — one symmetric files-only model: files-only tar members, directories recreated locally. Refined by T-04. (design §11; Phase B/D) |
| C-04 | Med | Structural checks missed an archive missing beneath a manifest | **Resolved** — HEAD `ContentLength == archive_size` check in rebuild and `remote check`. Confirmed by T-05. (design §7.2; Phase E2/G2) |
| C-05 | Med | Remote removal could strand manifestless storage debris | **Resolved** — `remote remove` refuses residual debris unless `--force`, which reports exactly what becomes unmanaged. Confirmed by T-05. (design §9; Phase G3) |
| C-06 | High (doc) | Unsupported concurrency implied a data-safety guarantee | **Resolved** — design and `LIMITATIONS.md` narrowed to promise no data-safety/auto-recovery guarantee for unsupported concurrent mutation. Confirmed by T-05. |
| T-01 | — | Verified staging-copy closes C-01 | **Confirmed resolved** — no further action; I8 wording clarified (indirect final-marker verification). |
| T-02 | — | Fetch receipt provides complete retry evidence | **Confirmed resolved** — receipt persisted for every post-rename state where the remote manifest may be absent. (Phase D2) |
| T-03 | High | `remove` omitted recovery artifacts (staging manifest, receipt, fetch-staging, `.trash`) | **Resolved** — Phase G1: `remove` sweeps `.fetch`/`.trash` recovery artifacts (glob `<id>*`) and the remote staging manifest across every remote; `remote check` (G2) reports leftover staging manifests. (design §9) |
| T-04 | Med | Files-only archive contract incorrectly included the two sidecars | **Resolved** — archive member set = manifest files − `{r3.yaml, metadata.yaml}`; fetch rejects sidecar names as archive members. (design §11; Phase B/D) |
| T-05 | — | Rebuild, remote removal, limitations internally consistent | **Confirmed resolved** — no further action. |

*(Ancestry note: the first-pass design review's D-findings — including D2-01 stale-manifest
overwrite and D2-02 — were addressed in the revision that the confirmation review (C-) then
checked; the C-/T- rounds are the surviving record.)*

## 4. Deferred / out of scope

Intentionally **not** done in this branch (see [`ROADMAP.md`](../../ROADMAP.md) and
[`LIMITATIONS.md`](../../LIMITATIONS.md); design §14):

- **Live-S3 / multipart validation against CEPH** — the `live_s3` suite (incl. the
  `>multipart_threshold` `move`→`fetch` test) is opt-in and must be run manually against CEPH
  before deployment; not part of the local/CI gate.
- **`job.file_paths` / mounting API** — separate proposal
  ([spec](specs/2026-08-04-job-file-manifest-and-access-proposal.md)); no-fetch mounts (ratarmount) deferred.
- **Filesystem-backed remote** (`FilesystemRemote`) — deferred (ROADMAP).
- **Mutating `gc` / reclamation** — delete orphaned objects + abort incomplete multipart
  uploads; the read-only `r3 remote check` is its foundation. Deferred (ROADMAP / LIMITATIONS).
- **Shared / multi-owner remotes** — the single-owner assumption underpins `remove`'s sweep;
  revisit with a concurrency design. Deferred (ROADMAP / LIMITATIONS).
- **Enforced repository lock / concurrency safety** — single-writer is a documented, *unenforced*
  operating rule; no data-safety guarantee under concurrent mutation (LIMITATIONS).
- **Power-loss (`fsync`) durability** — crash model covers process interruption, not power loss
  (ROADMAP / LIMITATIONS).
- **Python 3.13 support** — floor raised to 3.10, upper bound kept `<3.13`; 3.13 is a separate
  follow-up branch (ROADMAP).
- **`r3 remote check` of rows pointing at a no-longer-configured remote** — only configured
  remotes are reconciled today (ROADMAP).
- **`ruamel`/comment-preservation in `r3.yaml`** — rewrites re-serialize and drop comments
  (they now warn); comment-aware round-trip deferred (ROADMAP / LIMITATIONS).
- Also deferred (design §14): `r3 copy`/replicas, query-based batch `move`, manifest v2 per-file
  offsets.

## 5. Verification

Reproduce the gate from the repo root (branch `feature/remote-storage`, venv active):

```bash
source .venv/bin/activate
AWS_CONFIG_FILE=/dev/null python -m pytest -m "not live_s3" -q   # 390 passed, 6 deselected
make lint                                                        # ruff Passed; mypy Success (31 files)
git diff --check                                                 # clean
```

`AWS_CONFIG_FILE=/dev/null` avoids reading any ambient AWS profile (per the CEPH memo). The six
deselected tests are the `live_s3` suite.

**Live-S3 (manual, before deployment).** The live suite is not run by the gate and must be run
against the lab CEPH endpoint:

```bash
# Credentials come from the AWS profile / standard AWS credential chain — there is
# no access-key/secret-key env var. Required:
export R3_TEST_S3_ENDPOINT_URL=https://ceph.example.com   # CEPH endpoint URL
export R3_TEST_S3_BUCKET=my-r3-test-bucket                # a bucket you can write to
# Optional; a realistic CEPH RGW setup:
export R3_TEST_S3_PROFILE=ceph                            # AWS profile holding the credentials
export R3_TEST_S3_ADDRESSING_STYLE=path                   # CEPH RGW typically needs path-style
export R3_TEST_S3_REQUEST_CHECKSUM_CALCULATION=when_required  # older CEPH RGW builds need this
# Also optional: R3_TEST_S3_PREFIX, R3_TEST_S3_RESPONSE_CHECKSUM_VALIDATION
python -m pytest -m live_s3 -q
```

It exercises the real transport, including a multipart (`>multipart_threshold`) `move`→`fetch`
round-trip and per-object delete-error handling (F-12). Run it against CEPH before relying on the
remote-storage feature in production.
