from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

import r3


def test_create_fails_if_path_exists(fs: FakeFilesystem) -> None:
    path = "/rest/repository"
    fs.create_dir(path)

    with pytest.raises(FileExistsError):
        r3.Repository.create(path)


def test_create_creates_directories(fs: FakeFilesystem) -> None:
    path = Path("/test/repository")
    r3.Repository.create(path)

    assert path.exists()
    assert (path / "git").exists()
    assert (path / "jobs").exists()


def test_create_creates_config_file_with_version(fs: FakeFilesystem) -> None:
    path = Path("/test/repository")
    r3.Repository.create(path)

    assert (path / "r3repository.yaml").exists()

    with open(path / "r3repository.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    assert "version" in config
