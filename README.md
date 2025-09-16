# R3 - **R**epository for **R**eliable **R**esearch

## Usage
Use the `stable` branch to get the latest release of R3:

```bash
git clone -b stable https://github.com/mtangemann/r3.git

# or
git clone https://github.com/mtangemann/r3.git
cd r3
git switch stable
```

The recommended way to install R3 is by using [uv](), but other package mangers will
work as well:

```bash
cd r3
uv pip install -e .
```

Have a look at the documentation for more information:
```bash
mkdocs serve
```

Some parts of the documentation are accessible without a build step:
- [Tutorial](docs/tutorial.md)


## Contributing
All contributions are welcome! Feel free to open issues or PRs at any time.

To avoid friction, however, please take a minute to read the following guidelines
before working on the R3 code.

Set up R3 for development. The instructions below use
[uv](https://docs.astral.sh/uv/), but other package managers will work as well.

```bash
uv sync
source .venv/bin/activate
pre-commit install
```

R3 uses [ruff](https://docs.astral.sh/ruff/) and
[mypy](https://mypy.readthedocs.io/en/latest/) to foster code quality. Please make sure
these tools run with errors before submitting a PR:

```bash
make lint  # Runs ruff and mypy
make ruff
make mypy
```

Please add test cases for all functionality that you added or changed, and make sure you
don't accidentally break existings tests:

```bash
make pytest
```

Please use [gitmojis](https://gitmoji.dev/) for commit messages. Here are the most
commonly used prefixes:

- ✨ - `:sparkles:` for new features.
- 🐛 - `:bug:` for bug fixes.
- 📝 - `:memo:` for documentation updates.
- 🔨 - `:hammer:` for dev tooling updates.
