# Repository Format

Version: 1.0.0-beta.3

This document describes the format that R3 uses internally for storing jobs. The format
specification is intended to guide the development of R3's core features but is not part
of the public API. Rather, the Python API and CLI provide methods that should be used
for any interaction with repositories and stored jobs.

- This specification uses semantic versioning.

- A repository is a folder with the following contents:
  - `r3.yaml`: Contains a single key `version` mapping to the version of this
    specification.
  - `index.yaml` or `index.sqlite`: Optional cache of job metadata for fast retrieval.
  - `git/`: Cloned git repositories.
  - `jobs/`: Committed jobs.

- The `git` directory contains cloned git repositories, structured by their url (e.g.,
  `git/github.com/mtangemann/r3`). Repositories may be pulled, but only if no commit is
  removed that any job depends on.

- The `jobs` directory contains all committed jobs structured by hash (e.g. 
  `jobs/123abc.../`). See below for how the hashes are computed.

- Each job directory `job/$hash/` is write protected and has the following contents:
  - `r3.yaml`: Job metadata used by R3 (write protected).
  - `metadata.yaml`: Custom job metadata that may be changed at any time.
  - `output/`: Directory for all job outputs. Contents may be changed.
  - Other files and directories required by the job (write protected).

- Job hashes are computed using SHA-256 as follows:
  - The byte stream for all files is hashed, except for `r3.yaml`, `metadata.yaml` and
    the `output/` folder.
  - The contents from `r3.yaml` are loaded. The top level `ignore` key and any `query`
    keys in dependencies are removed. A dictionary of relative file names mapping to
    hashes for all files is added as `files`.
  - The final hash is computed by converting the modifed contents of `r3.yaml` to
    [canonical json](https://gibson042.github.io/canonicaljson-spec/) and hashing the
    resulting string.

- The config file `r3.yaml` may contain the following keys.
  - `dependencies`: A list of other jobs or repositories that this job depends on. Each
    dependency is specified as a dict with the following keys:
    - `item`: Relative path of the job or repository. E.g. `jobs/123abc...` or
        `git/github.com/mtangemann/r3`.
    - `commit`: Required if the item is a git repository. The full commit id.
    - `source`: Optional. A path relative to the item if only a specific subfolder
      or file is needed. For example: `output/checkpoints/best.pth`. Defaults to `.`.
    - `destination`: Optional. A path relative to the directory where the job is checked
      out. The source will be symlinked given this mail. For example:
      `pretrained_weights.pth`. Default: the same as `source`.
    - `query`: Optional. The original query that was resolved to this dependency.
  - `ignore`: A list of ignore patterns as used by git. The given patterns must not
    match any file belonging to the job.
  - `files`: The hash dictionary for all files as created for computing the job hash.

- The custom metadata file may contain arbitrary metadata. Tools building on R3 may
  further specify parts of the information provided in that file but should fail
  gracefully if their specification is not obeyed.
