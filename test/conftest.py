"""Shared pytest fixtures for the R3 test suite."""

import pytest


@pytest.fixture(autouse=True)
def _isolated_git_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give every test a self-contained git identity, independent of the machine.

    Also nulls ``GIT_CONFIG_GLOBAL``/``GIT_CONFIG_SYSTEM`` so machine-local settings
    (e.g. ``commit.gpgsign``) can't make ``git commit`` fail regardless of identity.
    """
    monkeypatch.setenv("GIT_AUTHOR_NAME", "R3 Test")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "R3 Test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "r3-test@example.com")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "r3-test@example.com")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", "/dev/null")
