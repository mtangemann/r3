# Known Limitations

Sharp edges and intentional scope limits in R3, in one visible place. Each entry
says what it means in practice. Read this before relying on R3 for anything you
can't afford to lose, and before pointing multiple processes or people at one
repository.

---

## No concurrency control (single writer per repository)

R3 assumes one mutating process per repository at a time. Running mutating commands
concurrently — two `r3 commit`s, or `rebuild-index` overlapping a `commit`/`remove`
— is unsupported and can corrupt the index, so treat single-writer as a hard rule.
The index is disposable: `r3 rebuild-index` rebuilds it from the jobs on disk.
(Enforced locking is unscheduled — `flock` is unreliable on the network filesystems
R3 often runs on.)

---

## Durability is against process interruption, not power loss

R3 operations are safe to interrupt (crash, `Ctrl-C`, kill) and re-run, but are not
yet durable across a host power loss mid-operation: just-written files and their
parent directories are not `fsync`-ed, so a power cut can lose a just-written file or
leave a partial one. The exposed surface is small — SQLite commits are `fsync`-durable
and the index is rebuildable, so the risk is the transient local copy steps,
recoverable by re-running the command (and `r3 rebuild-index`). After an unclean
shutdown mid-operation, re-run the interrupted command. Adding explicit `fsync` at
commit points is on the [roadmap](ROADMAP.md).

---

## Empty directories are not preserved

A job's file set is a list of files, so empty directories are not represented (except
the conventional `output/`, which is always present). Don't depend on an otherwise-empty
directory — make sure any directory you depend on holds at least one file.

---

## Symlinks and special files are not supported

R3 stores regular files only and does not preserve symbolic links. At commit, a file
symlink is silently dereferenced (its target's content is stored as a plain file), a
directory symlink is followed and flattened into the job, and a broken symlink is
silently dropped — the link itself is never kept. Keep job directories to regular
files, materializing anything reached through a symlink before committing. Making
commit fail closed on symlinks/special files, and eventually preserving them properly,
are on the [roadmap](ROADMAP.md).

---

## `r3.yaml` is managed by R3 — don't hand-format it

`r3.yaml` is an R3-managed file: `r3 init` and format migrations rewrite it by
re-serializing the parsed config, which does not preserve comments, blank lines, or key
order. Don't rely on hand-formatting there — it's lost on the next rewrite; keep notes
about your setup elsewhere. Round-tripping through a comment-aware YAML library is a
possible future improvement (see [ROADMAP.md](ROADMAP.md)).

---

## Git dependencies: GitHub only, and git-LFS data is not included

A `git` dependency must be a GitHub repository — R3 recognizes only
`https://github.com/<owner>/<repo>` and `git@github.com:<owner>/<repo>` URLs; other hosts
(GitLab, Bitbucket, self-hosted, or arbitrary `git://`/`file://` remotes) are rejected with
`Unrecognized git url`.

R3 also does not handle **git-LFS**. Dependencies are mirrored with `git clone --bare`,
which copies the LFS *pointer* files but not the LFS blobs, so checking out a dependency on
an LFS-backed repo fails at the smudge step (`remote missing object …` /
`smudge filter lfs failed`). It is also a provenance gap: a committed job's LFS data is not
captured in the repository, so the job is not self-contained. A workaround and a design
sketch for proper support are in
[docs/ideas/git-lfs-support.md](docs/ideas/git-lfs-support.md) (tracked in
[#84](https://github.com/mtangemann/r3/issues/84)).
