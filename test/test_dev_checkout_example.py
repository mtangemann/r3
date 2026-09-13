"""Smoke-test for examples/dev_checkout.py — keeps the example honest in CI."""

import importlib.util
from pathlib import Path
from types import ModuleType

import yaml
from click.testing import CliRunner

from r3.job import Job
from r3.repository import Repository

DATA_PATH = Path(__file__).parent / "data"
EXAMPLE_PATH = Path(__file__).parent.parent / "examples" / "dev_checkout.py"


def _load_example() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dev_checkout_example", EXAMPLE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dev_checkout_materializes_and_cleans_up(tmp_path: Path) -> None:
    repository = Repository.init(tmp_path / "repository")
    upstream = repository.commit(Job(DATA_PATH / "jobs" / "base"))
    assert upstream.id is not None

    job_dir = tmp_path / "dev"
    job_dir.mkdir()
    (job_dir / "r3.yaml").write_text(
        yaml.dump(
            {
                "dependencies": [
                    {"job": upstream.id, "source": "output", "destination": "dep"}
                ]
            }
        )
    )

    example = _load_example()
    runner = CliRunner()

    checkout = runner.invoke(
        example.cli,
        ["checkout", str(job_dir), "--repository", str(repository.path)],
    )
    assert checkout.exit_code == 0, checkout.output
    assert (job_dir / "dep").is_symlink()

    cleanup = runner.invoke(example.cli, ["cleanup", str(job_dir)])
    assert cleanup.exit_code == 0, cleanup.output
    assert not (job_dir / "dep").exists()
    assert not (job_dir / "dep").is_symlink()
