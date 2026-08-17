# Roadmap

The remote-storage extension is alpha. The items below are non-binding future
directions, not commitments or a schedule. For the *current* sharp edges and scope
limits, see [LIMITATIONS.md](LIMITATIONS.md).

## Future directions

- **Preserve comments/formatting in `r3.yaml`.** Round-trip the config through a
  comment-aware YAML library (`ruamel.yaml`) so `remote add`/`remove` and migrations
  keep comments, blank lines, and key order — deferred to avoid a new dependency for
  now.
- **Fail closed on symlinks/special files at commit.** `commit` currently handles
  symlinks silently — it dereferences a file symlink (storing the target's content),
  follows and flattens a directory symlink, and drops a broken symlink. A near-term
  guard should make `commit` refuse a job containing any symlink or special file
  (FIFO/socket/device) with a clear error, so nothing is silently altered or lost and
  the rule is uniform with `move`. This also turns `move`'s existing symlink rejection
  into a consequence of a general R3 rule rather than a remote-specific limitation.
- **Proper symlink support.** Preserve symbolic links as links through commit,
  checkout, and the remote archive, rather than dereferencing or dropping them. Started
  on the unmerged `feat-symlinks` branch.
- **`r3 remote check`: report unreachable jobs.** Also flag index rows that point at a
  no-longer-configured remote — currently only configured remotes are reconciled.
- **Mutating `gc` / reclamation command.** Delete orphaned bucket objects and abort
  incomplete multipart uploads — the read-only `r3 remote check` is the foundation for
  it.
- **Filesystem-backed remote / no-fetch mounts.** A filesystem-backed remote, and/or
  mounting an archived job's files without a full fetch.
- **Edit an archived job's metadata in place.** Metadata is stored as a separate
  object by design, which should make in-place edits feasible.
- **Shared / multi-owner remotes.** The current single-owner assumption lets `remove`
  sweep every configured remote; that must be revisited before remotes can be shared.
- **Power-loss durability (`fsync`).** The current crash model covers process
  interruption, not power loss.
- **Python 3.13 support.**
