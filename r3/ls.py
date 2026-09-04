"""Virtual-filesystem listing over the `metadata.path` convention (r3 ls)."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class Entry:
    """One row of an `r3 ls` listing."""

    kind: str  # "self" | "leaf" | "dir"
    name: str  # "." for self; the leaf/dir segment otherwise (no trailing slash)
    timestamp: Optional[datetime] = None
    revisions: int = 0
    job_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    sort_time: Optional[datetime] = None


def normalize_prefix(prefix: str) -> str:
    """Strips trailing slashes; a bare '/' or '' both mean the root."""
    return prefix.rstrip("/")


def escape_glob_literal(literal: str) -> str:
    """Escapes SQLite GLOB metacharacters (* ? [) so a literal path is matched
    literally (SQLite GLOB has no backslash escape; wrap each in a class)."""
    return "".join(f"[{ch}]" if ch in "*?[" else ch for ch in literal)


def query_for_prefix(prefix: str) -> Dict[str, Any]:
    """Query matching the job(s) at exactly `prefix` plus everything one or more
    levels below it. At the root, matches every job that has a path."""
    prefix = normalize_prefix(prefix)
    if prefix == "":
        return {"path": {"$glob": "*"}}
    return {
        "$or": [
            {"path": prefix},
            {"path": {"$glob": escape_glob_literal(prefix) + "/*"}},
        ]
    }


def build_listing(
    jobs: Iterable[Any], prefix: str, by_time: bool = False
) -> List[Entry]:
    """Partitions matched jobs into a one-level listing under `prefix`.

    `jobs` are the results of `query_for_prefix(prefix)`; each is read only for
    `.metadata` (`path`, `tags`), `.timestamp`, and `.id`. Jobs without a `path`
    are ignored. Revisions (jobs sharing an exact path) collapse to one entry.
    """
    prefix = normalize_prefix(prefix)

    self_jobs: List[Any] = []
    leaves: Dict[str, List[Any]] = {}
    dirs: Dict[str, List[Any]] = {}

    for job in jobs:
        path = job.metadata.get("path")
        if path is None:
            continue

        if prefix == "":
            remainder = path
        elif path == prefix:
            self_jobs.append(job)
            continue
        elif path.startswith(prefix + "/"):
            remainder = path[len(prefix) + 1:]
        else:
            continue  # path is not under prefix: skip

        if "/" in remainder:
            dirs.setdefault(remainder.split("/", 1)[0], []).append(job)
        else:
            leaves.setdefault(remainder, []).append(job)

    entries: List[Entry] = []

    def _job_entry(kind: str, name: str, group: List[Any]) -> Entry:
        latest = max(group, key=lambda j: j.timestamp)
        return Entry(
            kind=kind,
            name=name,
            timestamp=latest.timestamp,
            revisions=len(group),
            job_id=latest.id,
            tags=list(latest.metadata.get("tags", [])),
            sort_time=latest.timestamp,
        )

    if self_jobs:
        entries.append(_job_entry("self", ".", self_jobs))
    for name, group in leaves.items():
        entries.append(_job_entry("leaf", name, group))
    for name, group in dirs.items():
        entries.append(
            Entry(kind="dir", name=name, sort_time=max(j.timestamp for j in group))
        )

    def sort_key(entry: Entry):
        self_first = entry.kind != "self"
        if by_time:
            # newest first among non-self; sort_time is always set here
            assert entry.sort_time is not None
            return (self_first, -entry.sort_time.timestamp())
        # alphabetical; on a leaf/dir name tie, the leaf precedes its dir
        return (self_first, entry.name, 0 if entry.kind == "leaf" else 1)

    entries.sort(key=sort_key)
    return entries


def format_listing(
    entries: Iterable[Entry], long: bool = False, show_tags: bool = True
) -> str:
    """Renders entries as aligned text (job rows carry a timestamp; dir rows are
    just `name/`)."""
    entries = list(entries)

    def display_name(entry: Entry) -> str:
        return entry.name + "/" if entry.kind == "dir" else entry.name

    width = max((len(display_name(e)) for e in entries), default=0)

    lines: List[str] = []
    for entry in entries:
        name = display_name(entry)
        if entry.kind == "dir":
            lines.append(name)
            continue

        assert entry.timestamp is not None
        timestamp = entry.timestamp.strftime(r"%Y-%m-%d %H:%M:%S")
        revisions = f" ({entry.revisions} revisions)" if entry.revisions > 1 else ""
        if long:
            tags = ""
            if show_tags:
                tags = " | " + " ".join(f"#{tag}" for tag in entry.tags)
            lines.append(
                f"{name:<{width}}  {entry.job_id} | {timestamp}{tags}{revisions}"
            )
        else:
            lines.append(f"{name:<{width}}  {timestamp}{revisions}")

    return "\n".join(lines)
