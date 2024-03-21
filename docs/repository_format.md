# Repository Format

Version: 1.0.0-beta.6

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
  `git/github.com/mtangemann/r3`). All clones are bare repositories. A lightweight tag
  `r3/$job_id` is present for each commit that is used by a job.

- The `jobs` directory contains all committed jobs structured by their uuid (i.e. 
  `jobs/$uuid/`). Each job is assigned a uuid version 4 when committed to the
  repository.

- Each job directory `job/$uuid/` is write protected and has the following contents:
  - `r3.yaml`: Job metadata used by R3 (write protected).
  - `metadata.yaml`: Custom job metadata that may be changed at any time.
  - `output/`: Directory for all job outputs. Contents may be changed.
  - Other files and directories required by the job (write protected).

- Job hashes are computed using SHA-256 as follows
  - The byte stream for all files is hashed, except for `r3.yaml`, `metadata.yaml` and
    the `output/` folder.
  - For each dependency, a string is hashed that defines the dependencies using the
    following pattern: `<item>[@<commit>]/<source>`
  - To compute the final hash, a multiline string is hashed that contains in each line
    first the relative file path and the respective hash, ordered lexicographically and
    separated by a single space character. Dependencies use the destination as path.
     For example:
    ```
    run.py 123abc...
    some/dependency 456def...
    ```

- The config file `r3.yaml` may contain the following keys.
  - `dependencies`: A list of other jobs or repositories that this job depends on. Each
    dependency may be either a job or a git dependency, and is specified by the
    following keys:
    - `job` (job only): Job id.
    - `repository` (git only): Repository url.
    - `commit` (git only): Full commit id.
    - `source`: Optional. A path relative to the item if only a specific subfolder
      or file is needed. For example: `output/checkpoints/best.pth`. Defaults to the
      empty string.
    - `destination`: A path relative to the job directory where the dependency will be
       checked out. For example: `pretrained_weights.pth`.
    - `query` (job only): Optional. The original query that was resolved to this
       dependency.
  - `ignore`: A list of ignore patterns as used by git. The given patterns must not
    match any file belonging to the job.
  - `hashes`: A dictionary apping paths to hashes (as specified aboce). The key `.`
    maps to the overall job hash.

- The custom metadata file may contain arbitrary metadata. Tools building on R3 may
  further specify parts of the information provided in that file but should fail
  gracefully if their specification is not obeyed.
