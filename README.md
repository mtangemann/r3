# R3 - **R**epository for **R**eliable **R**esearch

## Usage
Have a look at the documentation:
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
