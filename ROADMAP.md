# Roadmap

Non-binding future directions for R3 — not commitments or a schedule. For the
*current* sharp edges and scope limits, see [LIMITATIONS.md](LIMITATIONS.md).

## Future directions

- **Preserve comments/formatting in `r3.yaml`.** Round-trip the config through a
  comment-aware YAML library (`ruamel.yaml`) so migrations and future config writes
  keep comments, blank lines, and key order — deferred to avoid a new dependency for
  now.
- **Fail closed on symlinks/special files at commit.** `commit` currently handles
  symlinks silently — it dereferences a file symlink (storing the target's content),
  follows and flattens a directory symlink, and drops a broken symlink. A near-term
  guard should make `commit` refuse a job containing any symlink or special file
  (FIFO/socket/device) with a clear error, so nothing is silently altered or lost.
- **Proper symlink support.** Preserve symbolic links as links through commit and
  checkout, rather than dereferencing or dropping them. Started on the unmerged
  `feat-symlinks` branch.
- **Power-loss durability (`fsync`).** The current crash model covers process
  interruption, not power loss.
- **Python 3.13 support.** Blocked on the `executor` dependency, which imports the
  `pipes` stdlib module (removed in 3.13) at import time, so r3 can't be imported on
  3.13 at all. r3's own code is 3.13-clean; unblocking needs a 3.13-compatible
  `executor` release (23.2 still imports `pipes`) or replacing that dependency.
