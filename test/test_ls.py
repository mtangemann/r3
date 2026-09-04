import datetime
from types import SimpleNamespace

from r3.ls import (
    Entry,
    build_listing,
    escape_glob_literal,
    format_listing,
    normalize_prefix,
    query_for_prefix,
)


def _job(path, ts, id="id", tags=None):
    return SimpleNamespace(
        metadata={"path": path, "tags": tags or []},
        timestamp=datetime.datetime(2026, 1, 1) + datetime.timedelta(days=ts),
        id=id,
    )


def test_normalize_prefix_strips_trailing_slashes():
    assert normalize_prefix("a/b/") == "a/b"
    assert normalize_prefix("a/b//") == "a/b"
    assert normalize_prefix("a/b") == "a/b"
    assert normalize_prefix("") == ""
    assert normalize_prefix("/") == ""


def test_escape_glob_literal_wraps_metacharacters():
    assert escape_glob_literal("a*b?c[d") == "a[*]b[?]c[[]d"
    assert escape_glob_literal("proj/exp") == "proj/exp"


def test_query_for_prefix_root_matches_any_path():
    assert query_for_prefix("") == {"path": {"$glob": "*"}}


def test_query_for_prefix_non_root_is_self_or_children():
    assert query_for_prefix("proj/exp/") == {
        "$or": [
            {"path": "proj/exp"},
            {"path": {"$glob": "proj/exp/*"}},
        ]
    }


def test_query_for_prefix_escapes_children_glob():
    assert query_for_prefix("a*b") == {
        "$or": [
            {"path": "a*b"},
            {"path": {"$glob": "a[*]b/*"}},
        ]
    }


def test_build_listing_partitions_self_leaf_dir():
    jobs = [
        _job("proj/exp", 5, id="self"),
        _job("proj/exp/pilot", 1, id="pilot"),
        _job("proj/exp/grid/run1", 2, id="g1"),
        _job("proj/exp/grid/run2", 3, id="g2"),
    ]
    entries = build_listing(jobs, "proj/exp")
    kinds = [(e.kind, e.name) for e in entries]
    # self first, then alphabetical interleave: "grid" (dir) sorts before "pilot"
    assert kinds == [("self", "."), ("dir", "grid"), ("leaf", "pilot")]


def test_build_listing_collision_shows_both_lines():
    jobs = [
        _job("proj/exp/analysis", 5, id="a"),
        _job("proj/exp/analysis/report", 6, id="r"),
        _job("proj/exp/pilot", 1, id="p"),
    ]
    entries = build_listing(jobs, "proj/exp")
    kinds = [(e.kind, e.name) for e in entries]
    # analysis is both a leaf and a dir; leaf comes just before its dir
    assert kinds == [
        ("leaf", "analysis"),
        ("dir", "analysis"),
        ("leaf", "pilot"),
    ]


def test_build_listing_groups_revisions_latest_wins():
    jobs = [
        _job("proj/exp/pilot", 1, id="old", tags=["v1"]),
        _job("proj/exp/pilot", 3, id="new", tags=["v2"]),
        _job("proj/exp/pilot", 2, id="mid", tags=["v1b"]),
    ]
    entries = build_listing(jobs, "proj/exp")
    (leaf,) = [e for e in entries if e.kind == "leaf"]
    assert leaf.revisions == 3
    assert leaf.job_id == "new"          # latest by timestamp
    assert leaf.tags == ["v2"]
    assert leaf.timestamp == datetime.datetime(2026, 1, 4)  # day 3


def test_build_listing_root_uses_full_path_segments():
    jobs = [_job("proj/exp/pilot", 1), _job("single", 2)]
    entries = build_listing(jobs, "")
    kinds = [(e.kind, e.name) for e in entries]
    assert ("dir", "proj") in kinds
    assert ("leaf", "single") in kinds


def test_build_listing_ignores_jobs_without_path():
    jobs = [
        _job("proj/exp/pilot", 1),
        SimpleNamespace(metadata={}, timestamp=None, id="x"),
    ]
    entries = build_listing(jobs, "proj/exp")
    assert [e.name for e in entries] == ["pilot"]


def test_build_listing_by_time_orders_newest_first_self_still_first():
    jobs = [
        _job("proj/exp", 9, id="self"),
        _job("proj/exp/old", 1),
        _job("proj/exp/new", 8),
    ]
    entries = build_listing(jobs, "proj/exp", by_time=True)
    assert [e.name for e in entries] == [".", "new", "old"]


def test_format_listing_default_and_long():
    ts1 = datetime.datetime(2026, 6, 1, 9, 0, 0)
    ts2 = datetime.datetime(2026, 6, 2, 10, 0, 0)
    entries = [
        Entry(kind="self", name=".", timestamp=ts1,
              revisions=2, job_id="jid", tags=["main"]),
        Entry(kind="leaf", name="eval", timestamp=ts2,
              revisions=1, job_id="jid2", tags=["eval"]),
        Entry(kind="dir", name="tasks"),
    ]
    short = format_listing(entries, long=False)
    assert "." in short and "(2 revisions)" in short
    assert "2026-06-01 09:00:00" in short
    assert "tasks/" in short
    assert "(1 revisions)" not in short          # singular not annotated

    long = format_listing(entries, long=True, show_tags=True)
    assert "jid | 2026-06-01 09:00:00 | #main (2 revisions)" in long
    assert "#eval" in long

    no_tags = format_listing(entries, long=True, show_tags=False)
    assert "#main" not in no_tags
