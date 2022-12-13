# R3 - **R**epository of **R**eproducible **R**esearch

## Usage
Create a new repository using `r3 init`:

```
r3 init path/to/repository
```

Add data manually to the data directory. Files may not be renamed after being added to
the repository! For example:

```bash
mkdir path/to/repository/data/containers
cp container-v1.sif path/to/repository/data/containers
```

Prepare your job in a directory, including a config file. For example:

```yaml
# config.yaml
dependencies:
  - &container data/containers/container-v1.sif

environment:
  container: *container
  gpus: none

commands:
  run: python run.py
  done: ls output/test

parameters:
  name: World
```

```python
# run.py
import yaml

with open("config.yaml", "r") as config_file:
    parameters = yaml.safe_load(config_file).get("parameters", {})

name = parameters.get("name", "World")

with open("output/test", "w") as output_file:
    output_file.write(f"Hello {name}!")
```

Now commit your job to the repository:
```
r3 commit path/to/job path/to/repository
```

Per default, the repository path is read from the environment variable `R3_REPOSITORY`.
So the following works as well:
```
export R3_REPOSITORY=path/to/repository
r3 commit path/to/job
```

Checking out jobs from the repository will copy the job files and symlink the output
directory and dependencies:
```
r3 checkout /repository/jobs/by_hash/123abc... work/dir
```
