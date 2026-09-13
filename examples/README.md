# Examples

Small, self-contained scripts for use cases r3's core deliberately does not
commit to. They are **starting points to copy and adapt**, not r3 commands and
not API-stable — each runs against the current release with no compatibility
promise. r3's core stays unopinionated; the opinions live here, and you are
expected to fork an example and make it yours.

Each script documents, in its own comments, the decisions it makes and what a
fuller variant might add.

## Available examples

- **`dev_checkout.py`** — materialize a job's dependencies into its working
  directory for the local dev loop (`checkout`), and remove them again
  (`cleanup`). The starting point for developing a job that has dependencies.
