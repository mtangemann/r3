from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

import r3


def test_init_fails_if_path_exists(fs: FakeFilesystem) -> None:
    path = "/path/to/repository"
    fs.create_dir(path)

    repository = r3.Repository(path)
    with pytest.raises(FileExistsError):
        repository.init()


def test_init_creates_directories(fs: FakeFilesystem) -> None:
    root = Path("/test/repository")
    repository = r3.Repository(root)
    repository.init()

    assert root.exists()
    assert (root / "git").exists()
    assert (root / "jobs").exists()


def test_init_creates_config_file(fs: FakeFilesystem) -> None:
    root = Path("/test/repository")
    repository = r3.Repository(root)
    repository.init()

    assert (root / "r3repository.yaml").exists()

    with open(root / "r3repository.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    assert "version" in config
