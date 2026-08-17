"""Shared pytest fixtures for the R3 test suite."""

import pytest


@pytest.fixture(autouse=True)
def _isolated_git_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give every test a self-contained git identity, independent of the machine.

    Several tests create throwaway git repos and commit into them (via
    ``executor.execute``), which needs an author/committer identity. Nulling the
    global and system git config makes a LOCAL run reproduce the bare-CI state (no
    ambient ``~/.gitconfig`` identity), so a green local run genuinely proves the CI
    fix -- identity then comes only from the ``GIT_*`` env vars set below.

    ``executor.execute(...)`` spawns subprocesses that inherit ``os.environ``, so
    ``monkeypatch.setenv`` is sufficient; no per-call plumbing is needed. This fixture
    is function-scoped because it uses the function-scoped ``monkeypatch`` fixture
    (which auto-reverts), so the env changes never leak between tests.
    """
    monkeypatch.setenv("GIT_AUTHOR_NAME", "R3 Test")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "R3 Test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "r3-test@example.com")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "r3-test@example.com")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", "/dev/null")
