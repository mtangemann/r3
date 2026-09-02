# Path promotion — design

**Status:** Draft for review
**Date:** 2026-09-02
**Branch:** `feature/path-promotion`

## Motivation

`metadata.path` has, over years of use, become a load-bearing convention: jobs
are organized in a virtual filesystem addressed by a single slash-separated
`path` string (`gold-standard/experiments/2026-02-16-foo/report`). It is
project-prefixed, globally unique by that prefix, and **mutable** (a job can be
re-`path`ed later; dependencies pin job ids, not paths, so re-pathing is safe).
The leaf segment is effectively the job's name — there is no separate `name`
key in practice. Paths are deliberately **not unique**: several jobs may share a
path (sweep variants, re-commits), which is what "revisions" means.

Today R3 privileges only `tags`: `r3 find --tag` filters on them and `r3 find -l`
shows them. `path` is invisible to the CLI, so querying it requires the Python
API or awkward JSON. This forces a workaround (encoding the path into nested
tags) that this feature aims to retire.

**Promotion means giving `path` first-class tooling — not enforcing or
normalizing it.** R3's minimal-assumptions philosophy stands: the engine still
never parses `path`; we only add display, filtering, and browsing on top of the
existing metadata query layer.

### Prior art (this is largely upstreaming a proven prototype)

- **`xr3 find`** (house wrapper) already implements `-p/--path GLOB`, `-l` with
  `id | datetime | path | tags` (path before tags), `--tags/--no-tags`,
  `--latest/--all`. Battle-tested; we upstream it into core.
- **Foreman's web `browse`** already implements the virtual-FS directory view
  that `r3 ls` mirrors. We adopt its query/partition model but fix its one flaw
  (see below).
- The query engine already supports `$glob` (SQLite `GLOB`) over any field
  including `path`, so the engine work is done; this is CLI glue plus a small
  safety fix.

## Scope

**In (this spec, one PR):**
- `r3 find`: `path` column in `-l`, `-p/--path` filter, `--tags/--no-tags`.
- `r3 find`: escape string literals in `query.py` so values can't break the SQL.
- `r3 ls`: new command — the virtual-filesystem browser.

**Separate / deferred (explicitly out):**
- **Docs cleanup** (stale `commands:` run/done block in `docs/tutorial.md`) —
  its own small PR, independent of this work.
- **`r3 find -q/--query`** (raw Mongo JSON) — a separate feature; do soon.
- **Parameter-bound queries** in `query.py` (replacing string interpolation
  wholesale) — the proper robustness fix; follow-up. This PR only does the
  minimal quote-escaping so nothing breaks in the meantime.
- **Dependency path sugar** for `find_latest`/`find_all` — deferred (kickoff
  called it "probably not too critical").
- **Path-hygiene lint** (detect corrupt paths, e.g. repeated segments or
  `path != origin` drift) — a real future capability, motivated by corruption
  already present in the store; out of scope here.

## Part A — `r3 find` display + filter

### A1. Path column in `-l`

`-l/--long` output becomes:

```
<job_id> | <YYYY-MM-DD HH:MM:SS> | <path> | <#tags>
```

- `path` is placed **before** tags. When a job has no `path`, the column is
  empty (the pipes stay, so columns line up): `<id> | <datetime> |  | #tag`.
- Timestamp format unchanged (`%Y-%m-%d %H:%M:%S`).

### A2. `-p/--path GLOB`

- New option `--path` / `-p`, taking a single pattern.
- Builds `{"path": {"$glob": <pattern>}}` and merges it (AND) with any `--tag`
  filter, i.e. `{"tags": {"$all": [...]}, "path": {"$glob": <pattern>}}`.
- The pattern is a **literal SQLite GLOB**: `*`, `?`, `[...]` are wildcards, and
  the user adds them explicitly. No implicit `*` wrapping. Examples:
  - `-p '*DAEMONS*'` — substring
  - `-p 'gold-standard/experiments/*'` — subtree
  - `-p gold-standard/models/main` — exact path
- Ordering/`--latest`/`--all` semantics unchanged from current `find`.

### A3. `--tags/--no-tags`

- Boolean toggle, **default on** (tags column shown in `-l`).
- `--no-tags` drops the tags column from `-l` output: `<id> | <datetime> | <path>`.
- Rationale: with a `path` column, tags are often redundant for locating a job;
  a natural consequence of the new output. No effect on `--short`.

### A4. Robustness — escape string literals in `query.py`

`query.py` builds SQL by interpolating values directly, e.g.
`f"{field} GLOB '{self.pattern}'"` (and the same pattern in `Eq`, `Ne`, `In`,
`Nin`, array `value = '...'` branches). A value containing `'` breaks the SQL
and is an injection vector — now reachable via `-p` from the CLI.

**This PR:** add a single helper that renders a SQL string literal safely
(double any `'` → `''`) and route every string interpolation in `query.py`
through it. Numeric values are unaffected. This is the minimal "does not break"
fix.

**Follow-up (not here):** replace interpolation with parameter binding (`?`
placeholders + a params list threaded through `to_sql`). Tracked separately.

## Part B — `r3 ls`

### B1. Purpose

`r3 ls <prefix>` shows **one level** of the virtual filesystem under `<prefix>`,
the way you'd browse a directory tree. It is a navigation tool; `find -p` remains
the tool for glob search. The `<prefix>` argument is a **literal path**, not a
glob.

### B2. Entities shown

Under `<prefix>`, three kinds of entry:

1. **Self** — job(s) at *exactly* `<prefix>`. Rendered as a **`.`** entry.
2. **Child leaf** — a job at `<prefix>/<name>` (no deeper segment). Rendered as
   `<name>`.
3. **Child directory** — jobs exist deeper under `<prefix>/<name>/…`. Rendered
   as `<name>/`.

A name that is both a leaf and a directory (a job at `<prefix>/<name>` *and*
jobs beneath it) produces **both** lines — `<name>` and `<name>/`. Nothing is
ever hidden. This is the fix for foreman's browse, which silently drops a job
sitting exactly at a browsed prefix.

Jobs with no `path` are not part of the virtual filesystem and never appear.

### B3. Revisions

Multiple jobs at the same exact path are revisions. A `.` or leaf entry shows
the **latest** revision's timestamp and, when there is more than one,
`(N revisions)`. A single revision shows no annotation. To enumerate the actual
revisions, use `r3 find -p <path> -l --all`.

### B4. Rendering

Default (`--short`, the default):

- Job rows (`.` and leaves): `<name>` padded to align, then the latest
  timestamp, then `(N revisions)` when N > 1.
- Directory rows: `<name>/` (no timestamp).

Example — a prefix that is itself a job, with children and a collision:

```
r3 ls gold-standard/models/main

.                 2026-06-01 09:00:00 (2 revisions)
eval              2026-06-02 10:00:00
eval/
tasks/
```

`-l/--long`: job rows carry the latest revision's job id and (unless `--no-tags`)
its tags, in `find`-like columns; directory rows are unchanged in v1 (no
aggregate detail):

```
r3 ls -l gold-standard/models/main

.       <job_id> | 2026-06-01 09:00:00 | #main #v2 (2 revisions)
eval    <job_id> | 2026-06-02 10:00:00 | #eval
eval/
tasks/
```

### B5. Ordering

- **Alphabetical, interleaved** — leaves and directories are *not* grouped into
  separate blocks; they sort together by name. (An experiment split into
  `compute` / `eval` / `report` is one logical unit; grouping files vs. folders
  would scatter it.)
- Sort key ignores the trailing `/`, so a colliding `<name>` and `<name>/` are
  adjacent; the job line precedes its `/` line.
- The `.` entry always sorts first (it naturally does in byte order; guaranteed
  regardless).

### B6. Flags

- `-l/--long` — add job id and tags to job rows (see B4); honors `--no-tags`.
- `-d` — self entry only: show the `.` line (the job[s] at the prefix) without
  expanding children. The explicit, discoverable form of "don't descend" —
  replacing any trailing-slash magic.
- `-t` — sort by timestamp instead of alphabetically. A leaf/`.` uses its latest
  revision time; a directory uses the max timestamp among its descendants (all
  already fetched). Default remains alphabetical.
- `--repository` / `R3_REPOSITORY` — as for other commands.

### B7. Argument handling and edge cases

- **Trailing slash insignificant**: `proj/exp` and `proj/exp/` are identical
  (strip trailing slashes on input).
- **No argument** → root (empty prefix): list top-level directories and any
  top-level single-segment jobs.
- **No match** (neither a self job nor any child): print nothing, exit 0.
- The prefix is literal; **escape SQLite GLOB metacharacters** (`*`, `?`, `[`)
  in it before constructing the children glob, so a stray metacharacter in a
  path can't turn navigation into a wildcard match.

### B8. Query construction

Given normalized `<prefix>`:

- **Self:** `{"path": <prefix>}` (exact; skipped when prefix is empty).
- **Children:** `{"path": {"$glob": <escaped-prefix> + "/*"}}` for a non-empty
  prefix, or `{"path": {"$glob": "*"}}` at root.

Fetch both, then partition in Python: strip the prefix (and separating `/`) from
each job's path; empty remainder → self bucket; a remainder with a `/` → its
first segment is a directory; otherwise → a leaf keyed by the remainder (grouping
revisions). Build the sorted, interleaved listing from the buckets.

## Testing

- **`query.py`**: unit tests for the quote-escaping helper (values with `'`,
  with GLOB metacharacters, numeric values unaffected); a regression test that a
  `$glob`/`$eq` value containing `'` matches literally and does not error.
- **`r3 find`**: `-l` includes the path column in the right position and blanks
  it when absent; `-p` builds the expected query and ANDs with `-t`; literal
  GLOB semantics (no auto-`*`); `--no-tags` drops the column.
- **`r3 ls`**: fixtures covering — pure directory; prefix-as-job (`.` entry);
  leaf/dir collision (both lines); revisions annotation and latest timestamp;
  root listing; empty result; trailing-slash equivalence; a prefix containing a
  GLOB metacharacter (escaped, no over-match); `-d`, `-t`, `-l`/`--no-tags`.
- Follows the repo's pytest + `pyfakefs` conventions and fixtures in
  `test/data/jobs/`.

## Non-goals / deferred (recap)

`-q/--query`; parameter-bound `query.py`; dependency `find_latest`/`find_all`
path sugar; path-hygiene lint; single-child directory-chain collapsing; the
`docs/tutorial.md` cleanup (separate PR).
