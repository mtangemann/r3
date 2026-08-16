# Roadmap

The remote-storage extension is alpha. The items below are non-binding future
directions, not commitments or a schedule. For the *current* sharp edges and scope
limits, see [LIMITATIONS.md](LIMITATIONS.md).

## Future directions

- **Preserve comments/formatting in `r3.yaml`.** Round-trip the config through a
  comment-aware YAML library (`ruamel.yaml`) so `remote add`/`remove` and migrations
  keep comments, blank lines, and key order — deferred to avoid a new dependency for
  now.
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
