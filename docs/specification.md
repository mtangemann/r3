# R3 Repository Format

Version: 1.0.0-beta.1

- This specification uses semantic versioning.

- A repository is a folder with the following contents:
  - `r3repository.yaml`: Contains a single key `version` mapping to the version of this
    specification.
  - `metadata.yaml` or `metadata.sqlite`: Optional cache of job metadata for fast
    retrieval.
  - `git/`: Cloned git repositories.
  - `jobs/`: Committed jobs.

- The `git` directory contains cloned git repositories, preferable structured by their
  url (e.g., `git/github.com/mtangemann/r3`). Repositories may be pulled, but only if no
  commit is removed that any job depends on.

- The `job` directory contains all committed jobs structured by hash (e.g. 
  `jobs/123abc.../`). See below for how the hashes are computed.

- Each job directory `job/$hash/` has the following contents. If not stated otherwise,
  all contents may not be modified after the job has been committed and must be write
  protected.
  - `r3.yaml`: Main configuration file with. See below.
  - `r3metadata.yaml`: Arbitrary job metadata. Common keys are `tags`, `date`, `source`,
    `description`. May be modified after the job has been committed.
  - `output/`: A folder to which all job outputs are written. May be modified after the
    job has been committed.
  - Other files and directories required by the job.

- Job hashes are computed using SHA-256 and the following scheme.

  - The hash of the file `r3.yaml` is the computed by converting the contents to
    [canonical json](https://gibson042.github.io/canonicaljson-spec/) and hashing the
    resulting string. The `source` keys in dependencies are ignored.
  - The `r3metadata.yaml` file and the contents of the `output/` folder are not hashed.
  - For all otherfiles, the original bytestream is hashed.
  - To compute the final hash, a multiline string is hashed that contains in each line
    first the relative file path, a single space and the respective has
    ```
    r3.yaml 123abc...
    some/nested/script.py 456def...
    ```

- The main config file `r3.yaml` may contain the following keys.
  - `dependencies`: A list of other jobs or repositories that this job depends on. Each
    dependency is specified as a dict with the following keys:
    - `item`: Relative path of the job or repository. E.g. `jobs/123abc...` or
        `git/github.com/mtangemann/r3`.
    - `commit`: Required when the item is a git repository. The full commit id.
    - `source_path`: Optional. A path relative to the item if only a specific subfolder
      or file is needed. For example: `output/checkpoints/best.pth`. Default `.`.
    - `target_path`: Optional. A path relative to the directory where the job is checked
      out. The source will be symlinked given this mail. For example:
      `pretrained_weights.pth`. Default: the same as `source_path`.
    - `origin`: Optional. The original query that was resolved to this dependency.
  - `environment`: Constraints on the environment used to execute the job. For example:
    `cpus: 4` or `gpus: 2xV100`. Not further specified as of now.
  - `commands`: Dictionary of shell commands. May contain `run` and `done`. For example:
    ```
    commands:
      run: python train.py
      done: ls output/final_checkpoint.pth
    ```
  - `parameters`: A dictionary with parameters used by the job.
