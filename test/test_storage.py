"""Unit tests for `r3.storage`."""

import filecmp
import os
import tempfile
from pathlib import Path

import pytest
import yaml
from executor import execute
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from r3.job import GitDependency, Job, JobDependency
from r3.storage import Storage

DATA_PATH = Path(__file__).parent / "data"


# REVIEW: This should be offered centrally.
def get_dummy_job(fs: FakeFilesystem, name: str) -> Job:
    path = DATA_PATH / "jobs" / name
    fs.add_real_directory(path, read_only=True)
    return Job(path)


def test_storage_constructor_raises_if_root_does_not_exist(fs: FakeFilesystem):
    with pytest.raises(FileNotFoundError):
        Storage("/does/not/exist")


def test_storage_constructor_raises_if_root_is_not_a_directory(fs: FakeFilesystem):
    fs.create_file("/not/a/directory")
    with pytest.raises(NotADirectoryError):
        Storage("/not/a/directory")


def test_storage_init_creates_directories(fs: FakeFilesystem):
    fs.create_dir("/repository")
    Storage.init("/repository")
    assert Path("/repository/jobs").is_dir()
    assert Path("/repository/git").is_dir()


def test_storage_add_updates_job_path(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.parent == storage.root / "jobs"


def test_storage_add_creates_job_folder(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.is_dir()


def test_storage_add_assigns_job_id(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.id is not None
    assert job.path.name == job.id


def test_storage_add_copies_source_files(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")
    
    original_job = get_dummy_job(fs, "base")

    # REVIEW: Job should have a method to return all source files.
    source_files = [
        path.relative_to(original_job.path) for path in original_job.path.rglob("*")
        if path.name not in ["r3.yaml", "metadata.yaml"]
    ]

    committed_job = storage.add(original_job)

    for source_file in source_files:
        assert (committed_job.path / source_file).exists()
        assert filecmp.cmp(
            original_job.path / source_file, committed_job.path / source_file
        )


def test_storage_add_saves_metadata(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job.metadata["test"] = "value"

    job = storage.add(job)
    assert job.metadata["test"] == "value"
    
    assert (job.path / "metadata.yaml").exists()
    with open(job.path / "metadata.yaml", "r") as metadata_file:
        metadata = yaml.safe_load(metadata_file)
    assert metadata["test"] == "value"


def test_storage_add_saves_hashes(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert (job.path / "r3.yaml").exists()
    with open(job.path / "r3.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)
    assert "hashes" in config


def test_storage_contains(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    assert original_job not in storage

    committed_job = storage.add(original_job)
    assert committed_job.id is not None
    assert original_job not in storage
    assert committed_job in storage
    assert committed_job.id in storage


def test_storage_contains_works_with_relative_paths(fs: FakeFilesystem):
    fs.create_dir("/path")
    storage = Storage.init("/path/repository")
    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    assert job in storage
    assert job.path.is_absolute()

    os.chdir("/path")
    storage = Storage("repository")
    assert job in storage

    os.chdir("/")
    assert job in storage


def test_storage_get(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)
    assert committed_job.id is not None

    retrieved_job = storage.get(committed_job.id)
    assert retrieved_job.id == committed_job.id
    assert retrieved_job.path == committed_job.path


def test_storage_get_raises_if_job_id_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    with pytest.raises(FileNotFoundError):
        storage.get("non-existent")


def test_storage_get_raises_if_job_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    with pytest.raises(FileNotFoundError):
        storage.get("non-existent")


def test_storage_jobs_returns_all_jobs(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    jobs = list(storage.jobs())
    assert len(jobs) == 0

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)

    jobs = list(storage.jobs())
    assert len(jobs) == 1
    assert jobs[0].id == committed_job.id
    assert jobs[0].path == committed_job.path

    storage.add(original_job)
    jobs = list(storage.jobs())
    assert len(jobs) == 2


def test_storage_remove_deletes_job_folder(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job = storage.add(job)

    assert job.path.exists()
    storage.remove(job)
    assert not job.path.exists()


def test_storage_remove_raises_if_job_does_not_exist(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    with pytest.raises(FileNotFoundError):
        storage.remove(job)


def test_checkout_delegates_to_specific_checkout_method(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)

    checkout_job_called = False
    def _checkout_job(item, path):
        nonlocal checkout_job_called
        checkout_job_called = True
    storage.checkout_job = _checkout_job  # type: ignore

    storage.checkout(committed_job, "/checkout")
    assert checkout_job_called

    checkout_job_dependency_called = False
    def _checkout_job_dependency(item, path):
        nonlocal checkout_job_dependency_called
        checkout_job_dependency_called = True
    storage.checkout_job_dependency = _checkout_job_dependency  # type: ignore

    storage.checkout(JobDependency("123abc", "source"), "/checkout")
    assert checkout_job_dependency_called

    checkout_git_dependency_called = False
    def _checkout_git_dependency(item, path):
        nonlocal checkout_git_dependency_called
        checkout_git_dependency_called = True
    storage.checkout_git_dependency = _checkout_git_dependency  # type: ignore

    storage.checkout(GitDependency("https://...", "123abc", "source"), "/checkout")
    assert checkout_git_dependency_called


def test_checkout_job_copies_source_files(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)

    checkout_path = Path("/checkout")
    storage.checkout_job(committed_job, checkout_path)

    source_files = [
        path.relative_to(original_job.path) for path in original_job.path.rglob("*")
        if path.name not in ["r3.yaml", "metadata.yaml"]
    ]

    for source_file in source_files:
        assert (checkout_path / source_file).exists()
        assert filecmp.cmp(
            committed_job.path / source_file, checkout_path / source_file
        )


def test_checkout_job_symlinks_output_files(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    committed_job = storage.add(original_job)

    checkout_path = Path("/checkout")
    storage.checkout_job(committed_job, checkout_path)

    assert (checkout_path / "output").exists()
    assert (checkout_path / "output").is_symlink()
    assert (checkout_path / "output").resolve() == (committed_job.path / "output").resolve()  # noqa: E501


def test_checkout_job_checks_out_job_dependencies(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    original_job._config["dependencies"] = [
        {"job": "123abc", "destination": "dependency_path"}
    ]
    committed_job = storage.add(original_job)

    calls_to_checkout = []
    storage.checkout = lambda item, path: calls_to_checkout.append((item, path))  # type: ignore

    checkout_path = Path("/checkout")
    storage.checkout_job(committed_job, checkout_path)

    assert len(calls_to_checkout) == 1
    assert isinstance(calls_to_checkout[0][0], JobDependency)
    assert calls_to_checkout[0][0].job == "123abc"
    assert calls_to_checkout[0][1] == checkout_path


def test_checkout_job_checks_out_git_dependencies(
    fs: FakeFilesystem, mocker: MockerFixture,
):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    original_job = get_dummy_job(fs, "base")
    original_job._config["dependencies"] = [{
        "repository": "https://github.com/user/model.git",
        "commit": "123abc",
        "destination": "dependency_path",
    }]

    # Prevent calling `git tag` when using the fake filesystem.
    def patched_execute(command: str, **kwargs):
        if command.startswith("git tag"):
            return
        return execute(command, **kwargs)
    mocker.patch("r3.storage.execute", new=patched_execute)

    committed_job = storage.add(original_job)

    calls_to_checkout = []
    storage.checkout = lambda item, path: calls_to_checkout.append((item, path))  # type: ignore

    checkout_path = Path("/checkout")
    storage.checkout_job(committed_job, checkout_path)

    assert len(calls_to_checkout) == 1
    assert isinstance(calls_to_checkout[0][0], GitDependency)
    assert calls_to_checkout[0][0].repository == "https://github.com/user/model.git"
    assert calls_to_checkout[0][0].commit == "123abc"
    assert calls_to_checkout[0][1] == checkout_path


def test_checkout_job_dependency_symlinks_files(fs: FakeFilesystem):
    fs.create_dir("/repository")
    storage = Storage.init("/repository")

    job = get_dummy_job(fs, "base")
    job = storage.add(job)
    assert job.id is not None

    dependency = JobDependency("destination", job.id)
    fs.makedir("/checkout1")
    storage.checkout_job_dependency(dependency, "/checkout1")
    assert Path("/checkout1/destination").is_symlink()
    assert Path("/checkout1/destination").resolve() == job.path.resolve()

    dependency = JobDependency("original_run.py", job.id, "run.py")
    fs.makedir("/checkout2")
    storage.checkout_job_dependency(dependency, "/checkout2")
    assert Path("/checkout2/original_run.py").is_symlink()
    assert Path("/checkout2/original_run.py").resolve() == job.path.resolve() / "run.py"

    dependency = JobDependency("destination", job.id, "output")
    fs.makedir("/checkout3")
    storage.checkout_job_dependency(dependency, "/checkout3")
    assert Path("/checkout3/destination").is_symlink()
    assert Path("/checkout3/destination").resolve() == job.path.resolve() / "output"


def test_checkout_git_dependency_clones_repository():
    # We cannot use the fake filesystem here, since checkout_git_dependency uses
    # executor to run git commands.
    with tempfile.TemporaryDirectory() as tempdir:
        os.mkdir(f"{tempdir}/repository")
        storage = Storage.init(f"{tempdir}/repository")

        r3_path = Path(__file__).parent.parent
        assert (r3_path / ".git").is_dir()
        repository_path = Path(f"{tempdir}/repository/git/github.com/mtangemann/r3")
        execute(f"git clone {r3_path} {repository_path}")

        expected_content_path = Path(f"{tempdir}/expected_content")
        execute(f"git clone {r3_path} {expected_content_path}")
        execute(
            "git checkout c2397aac3fbdca682150faf721098b6f5a47806b",
            directory=expected_content_path,
        )

        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination",
        )

        checkout_path = Path(tempdir) / "checkout1"
        os.mkdir(checkout_path)
        storage.checkout_git_dependency(dependency, checkout_path)
        assert (checkout_path / "destination").is_dir()
        for child in expected_content_path.iterdir():
            assert (checkout_path / "destination" / child.name).exists()
            if child.is_dir():
                assert (checkout_path / "destination" / child.name).is_dir()
            else:
                assert (checkout_path / "destination" / child.name).is_file()

        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination",
            source="test",
        )

        checkout_path = Path(tempdir) / "checkout2"
        os.mkdir(checkout_path)
        storage.checkout_git_dependency(dependency, checkout_path)
        assert (checkout_path / "destination").is_dir()
        for child in (expected_content_path / "test").iterdir():
            assert (checkout_path / "destination" / child.name).exists()
            if child.is_dir():
                assert (checkout_path / "destination" / child.name).is_dir()
            else:
                assert (checkout_path / "destination" / child.name).is_file()
        
        dependency = GitDependency(
            repository="https://github.com/mtangemann/r3.git",
            commit="c2397aac3fbdca682150faf721098b6f5a47806b",
            destination="destination",
            source="test/test_storage.py",
        )

        checkout_path = Path(tempdir) / "checkout3"
        os.mkdir(checkout_path)
        storage.checkout_git_dependency(dependency, checkout_path)
        assert (checkout_path / "destination").is_file()
        assert filecmp.cmp(
            expected_content_path / "test" / "test_storage.py",
            checkout_path / "destination",
        )
