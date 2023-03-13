# Repository Format

Version: 1.0.0-beta.2

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
  - The hash of the file `r3.yaml` is computed by converting the contents to
    [canonical json](https://gibson042.github.io/canonicaljson-spec/) and hashing the
    resulting string. Any `query` keys in dependencies are ignored.
  - The `metadata.yaml` file and the `output/` folder are ignored.
  - For all other files, the original byte stream is hashed.
  - To compute the final hash, a multiline string is hashed that contains in each line
    first the relative file path and the respective hash, ordered lexicographically and
    separated by a single space character. For example:
    ```
    r3.yaml 123abc...
    some/nested/script.py 456def...
    ```

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

- The custom metadata file may contain arbitrary metadata. Tools building on R3 may
  further specify parts of the information provided in that file but should fail
  gracefully if their specification is not obeyed.
