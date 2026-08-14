"""High-level interface to R3 repositories.

The `Repository` class should be imported not from this module but from the top-level
`r3` package.
"""

import os
import shutil
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import yaml
from executor import execute

import r3
import r3.archive
import r3.manifest
import r3.utils
from r3.index import Index
from r3.job import (
    Dependency,
    FindAllDependency,
    FindLatestDependency,
    GitDependency,
    Job,
    JobDependency,
    QueryAllDependency,
    QueryDependency,
)
from r3.manifest import SIDECAR_PATHS, FileEntry
from r3.remote import Remote, RemoteError
from r3.storage import Storage

R3_FORMAT_VERSION = "1.0.0-beta.9"


class Repository:
    """A repository of jobs."""

    def __init__(self, path: Union[str, os.PathLike]) -> None:
        """Initializes the repository instance.

        Parameters:
            path: The path to the repository.

        Raises:
            FileNotFoundError: If the given path does not exist.
            NotADirectoryError: If the given path exists but is not a directory.
        """
        self.path = Path(path)

        if not self.path.exists():
            raise FileNotFoundError(f"No such directory: {self.path}")

        if not self.path.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.path}")

        if not (self.path / "r3.yaml").exists():
            raise ValueError(f"Invalid repository: {self.path}")

        with open(self.path / "r3.yaml") as config_file:
            config = yaml.safe_load(config_file)
            if config["version"] != R3_FORMAT_VERSION:
                raise ValueError(
                    f"Invalid repository version: {config['version']}. Please migrate "
                    f"to {R3_FORMAT_VERSION}."
                )

        self._storage = Storage(self.path)

        # Remotes must be built before the index: Index.rebuild reconstructs remote
        # rows from the bucket, and Index.__init__ auto-rebuilds when the index file
        # is missing — so the remotes have to be available at construction time.
        self._remotes: Dict[str, Remote] = {}
        for name, remote_config in config.get("remotes", {}).items():
            self._remotes[name] = Remote.from_config(remote_config)

        self._index = Index(self._storage, self._remotes)

    @property
    def remotes(self) -> Dict[str, "Remote"]:
        """Returns the configured remotes."""
        return self._remotes

    @staticmethod
    def init(path: Union[str, os.PathLike]) -> "Repository":
        """Creates a new repository at the given path.

        Returns:
            The newly created repository.

        Raises:
            FileExistsError: If the given path exists alreay.
        """
        path = Path(path)

        if path.exists():
            raise FileExistsError(f"Path exists already: {path}")

        os.makedirs(path)
        Storage.init(path)

        r3config = {"version": R3_FORMAT_VERSION}

        with open(path / "r3.yaml", "w") as config_file:
            yaml.dump(r3config, config_file)

        return Repository(path)

    def jobs(self) -> Iterable[Job]:
        """Returns an iterator over all jobs in this repository."""
        return self.find({}, latest=False)

    def __contains__(self, item: Union[Job, Dependency]) -> bool:
        """Checks whether a job or dependency is contained in this repository.

        Parameters:
            item: The job or dependency to check for.

        Returns:
            Whether the given job or dependency is contained in this repository.
        """
        if isinstance(item, Job):
            return item in self._storage

        try:
            resolved_item = self.resolve(item)
        except ValueError:
            return False

        if isinstance(resolved_item, list):
            return all(dependency in self for dependency in resolved_item)

        if isinstance(resolved_item, JobDependency):
            target = self.path / "jobs" / resolved_item.job / resolved_item.source
            if target.exists():
                return True
            file_list = self._index.get_file_list(resolved_item.job)
            if file_list is not None:
                source = resolved_item.source
                if source == Path("."):
                    return len(file_list) > 0
                # Empty directories leave no manifest entry, but every complete job
                # conceptually has an output/. Treat an output/ source as present so
                # it cannot spuriously flip a job's dependency-satisfied state.
                if source == Path("output"):
                    return True
                # A directory source is present if any entry lies beneath it; a file
                # source is present on an exact match.
                return any(
                    entry == source or source in entry.parents for entry in file_list
                )
            return False

        if isinstance(resolved_item, GitDependency):
            assert resolved_item.commit is not None
            repository_path = self.path / resolved_item.repository_path

            if not repository_path.exists():
                execute(
                    f"git clone --bare {resolved_item.repository} {repository_path}"
                )

            if not r3.utils.git_commit_exists(repository_path, resolved_item.commit):
                execute("git fetch origin *:* --force", directory=repository_path)

            return r3.utils.git_path_exists(
                repository_path,
                resolved_item.commit,
                resolved_item.source,
            )

        return False

    def commit(self, job: Job) -> Job:
        """Commits a job to the repository.

        Parameters:
            job: The job to commit.

        Returns:
            The committed job. Compared to the original job, the returned job has an id
            and the path is changed to the location in the repository.
        """
        job = self.resolve(job)  # type: ignore

        # REVIEW It would be nice if `resolve` would check whether the dependencies
        #        exist in the repository.
        for dependency in job.dependencies:
            if dependency not in self:
                raise ValueError(f"Missing dependency: {dependency}")

        job = self._storage.add(job)
        self._index.add(job)

        return job

    def checkout(
        self, item: Union[Dependency, Job], path: Union[str, os.PathLike]
    ) -> None:
        """Checks out a job or dependency to the given path.

        Parameters:
            item: The job or dependency to check out.
            path: The path to check out the job or dependency to.

        Raises:
            ValueError: If the job or any of its dependencies is archived.
        """
        # A remote top-level job is a metadata-only projection whose `.dependencies`
        # raises FilesUnavailableError. resolve() would touch it and surface that raw
        # error, so refuse an archived top-level job first, before resolve() runs.
        if isinstance(item, Job) and item.id is not None:
            self._check_job_is_local(item.id)

        resolved_item = self.resolve(item)

        # Confirm every job the checkout will dereference or symlink is local BEFORE
        # any side effect, so a refusal never leaves a partial checkout behind.
        self._preflight_checkout_locality(resolved_item)

        if isinstance(resolved_item, list):
            for dependency in resolved_item:
                self._storage.checkout(dependency, path)
        else:
            self._storage.checkout(resolved_item, path)

    def _preflight_checkout_locality(
        self, resolved_item: Union[Job, Dependency, List[JobDependency]]
    ) -> None:
        """Confirms every job the checkout will reference is stored locally.

        This mirrors the traversal of ``Storage.checkout_job`` /
        ``checkout_job_dependency`` and must be kept in sync with them: a job must be
        local exactly when the checkout touches ``jobs/<id>/…`` — the top-level job,
        and the target job of every ``JobDependency`` edge (a recursive edge
        dereferences it via ``Storage.get``; a non-recursive edge symlinks
        ``jobs/<job>/<source>`` into it — either way an archived target breaks). Only a
        recursive edge (``source == "."`` and ``recursive_checkout``) descends into the
        target's own dependencies, because only then does ``checkout_job`` recurse. A
        job's ``.dependencies`` is read only after that job is confirmed local, so a
        remote projection never trips ``FilesUnavailableError`` here.
        """
        visited: Set[str] = set()
        if isinstance(resolved_item, list):
            for dependency in resolved_item:
                self._preflight_check_dependency(dependency, visited)
        elif isinstance(resolved_item, Job):
            # The top-level job was already confirmed local in `checkout`.
            self._preflight_check_job(resolved_item, visited)
        else:
            self._preflight_check_dependency(resolved_item, visited)

    def _preflight_check_job(self, job: Job, visited: Set[str]) -> None:
        """Walks the dependencies of a job being checked out. `job` must already be
        confirmed local; its `.dependencies` is only read here. `visited` guards
        against dependency cycles."""
        assert job.id is not None
        if job.id in visited:
            return
        visited.add(job.id)
        for dependency in job.dependencies:
            self._preflight_check_dependency(dependency, visited)

    def _preflight_check_dependency(
        self, dependency: Dependency, visited: Set[str]
    ) -> None:
        """Checks a single dependency edge for the checkout preflight."""
        if not isinstance(dependency, JobDependency):
            return  # Git dependencies reference the git store, not jobs/<id>.

        self._check_job_is_local(dependency.job)

        if str(dependency.source) == "." and dependency.recursive_checkout:
            child = self._index.get(dependency.job)
            self._preflight_check_job(child, visited)

    def _check_job_is_local(self, job_id: str) -> None:
        """Raises ValueError if a job is not stored locally."""
        location = self._index.get_location(job_id)
        if location != "local":
            raise ValueError(
                f"Job {job_id} is archived on remote \"{location}\". "
                f"Run `r3 fetch {job_id}` to retrieve it first."
            )

    def remove(self, job: Union[Job, str]) -> None:
        """Removes a job from the repository, everywhere it is stored.

        This runs an ordered, idempotent, retryable protocol so the job ends up gone
        from every configured remote, from local storage, from the local recovery
        artifacts, and from the index — and re-running after a crash completes whatever
        a partial run left behind. It operates from the job id and direct probes rather
        than from a materialized `Job`, so it works even in the retry state where the
        index row still says local but `jobs/<id>` is already gone.

        Parameters:
            job: The job, or its id, to remove.

        Raises:
            ValueError: If the job exists nowhere, or if other jobs depend on it.
            RemoteError: If a remote reports a per-object deletion failure. The index
                row is left intact so the removal can be retried.
        """
        job_id = job.id if isinstance(job, Job) else job
        if job_id is None:
            raise ValueError("Cannot remove a job without an id.")

        try:
            self._index.get_location(job_id)
            indexed = True
        except KeyError:
            indexed = False

        job_dir = self._storage.root / "jobs" / job_id

        # Preconditions. The job must exist somewhere: an index row, a local
        # directory, or ANY object under its prefix on some remote. Probing for any
        # object (not just the manifest) is what keeps a retry recoverable after a
        # delete_job that removed the manifest but crashed before the rest: the
        # leftover archive/sidecars still count as "exists", so the retry completes the
        # sweep instead of refusing. A row short-circuits the remote probes, so the
        # common (indexed) path makes no network calls.
        if (
            not indexed
            and not job_dir.exists()
            and not any(
                remote.has_objects(job_id) for remote in self._remotes.values()
            )
        ):
            raise ValueError(f"Job {job_id} is not contained in this repository.")

        # Referenced-by guard, ahead of any deletion. The reference only carries the
        # id; find_dependents never touches the (possibly absent) directory.
        dependents = self._index.find_dependents(Job(job_dir, job_id))
        if len(dependents) > 0:
            dependent_ids = sorted(str(dependent.id) for dependent in dependents)
            raise ValueError(
                "Cannot remove job since other jobs depend on it:\n"
                + "\n".join(f"  - {dependent_id}" for dependent_id in dependent_ids)
            )

        # 1. Remote sweep across EVERY configured remote, unconditionally. A single
        #    remote can hold leftovers the index does not point at (e.g. after a
        #    fetch-interruption -> rebuild -> move-to-another-remote sequence); a
        #    single-owner remote model makes sweeping all of them safe. delete_job
        #    inspects its per-object Errors, so a reported failure aborts here, before
        #    the index row is touched, leaving the removal retryable.
        for remote in self._remotes.values():
            remote.delete_job(job_id)

        # 2. Local deletion, then the job's local recovery artifacts.
        self._atomic_remove_local(job_id)
        self._remove_recovery_artifacts(job_id)

        # 3. Drop the index row and its dependency edges.
        self._index.remove_by_id(job_id)

    def _remove_recovery_artifacts(self, job_id: str) -> None:
        """Removes the job's fetch/move recovery artifacts so "gone everywhere" holds.

        Covers the fetch receipt, any leftover fetch staging directories, and any
        trash entries (including one an interrupted `_force_rmtree` of this very
        removal may have left, so a retry that finds no live `jobs/<id>` still finishes
        the cleanup). Every target is tolerated absent."""
        fetch_dir = self.path / ".fetch"
        trash_dir = self.path / ".trash"
        # In .fetch this job owns `<id>.receipt.json`, its `.tmp-<uuid>` write sibling
        # (a crash before the atomic os.replace leaves it), and `<id>-<uuid>` staging
        # dirs — so match `<id>*`. In .trash it owns only `<id>-<uuid>` entries. Job
        # ids are fixed-length, so no id is a prefix of another and `<id>*` cannot
        # bleed into a different job's artifacts.
        for entry in list(fetch_dir.glob(f"{job_id}*")) + list(
            trash_dir.glob(f"{job_id}-*")
        ):
            if entry.is_dir():
                _force_rmtree(entry)
            else:
                entry.unlink(missing_ok=True)

    def __getitem__(self, key: str) -> Job:
        """Get jobs by their ID with the repository[job_id] syntax."""
        return self.get_job_by_id(key)

    def get_job_by_id(self, job_id: str) -> Job:
        """Returns the job with the given ID.

        For remote jobs, returns a Job with cached_file_paths populated from the
        index (no local files). For unknown IDs, raises KeyError.
        """
        return self._index.get(job_id)

    def find(
        self,
        query: Dict[str, Any],
        latest: bool = False,
        location: Optional[str] = None,
    ) -> List[Job]:
        """Finds jobs by a query.

        Parameters:
            query: The mongo-style query document to find jobs by.
            latest: Whether to return the latest job or all jobs with the given tags.
            location: Optional location filter. When provided, only jobs with the
                given location are returned.

        Returns:
            The jobs that match the given tags.
        """
        return self._index.find(query, latest, location=location)

    def find_dependents(self, job: Job, recursive: bool = False) -> Set[Job]:
        """Finds jobs that depend on the given job.

        Parameters:
            job: The job to find dependents for.
            recursive: Whether to find dependents recursively.

        Returns:
            The jobs that depend on the given job.
        """
        return self._index.find_dependents(job, recursive)

    def move(self, job_id: str, remote_name: str) -> Set[Job]:
        """Moves a job to a remote, deleting the local copy.

        The job is archived (files-only) and its sidecars/archive are uploaded and
        **content-verified**; only then is the manifest published (verified
        staging-copy) as the completion marker, the index flipped to the remote, and
        the local files deleted via an atomic rename. A stale manifest is invalidated
        before any payload is overwritten, and the job is re-checked for quiescence
        before local deletion.

        Returns:
            The jobs that depend on the moved job (informational).

        Raises:
            ValueError: If the remote is unknown or the job is not local.
            KeyError: If the job does not exist.
            RemoteError: If an uploaded object fails content verification.
            RuntimeError: If the job changed during the move (not quiescent), or the
                built archive does not round-trip.
        """
        if remote_name not in self._remotes:
            raise ValueError(f"Unknown remote: {remote_name}")
        remote = self._remotes[remote_name]

        if self._index.get_location(job_id) != "local":
            raise ValueError(f"Job {job_id} is not local; cannot move it.")

        # A prior fetch's receipt is stale for a fresh move: archive_sha256 is not
        # deterministic across re-moves (tar headers embed mtimes), so a leftover
        # would make a later fetch step-0 spuriously report remote/receipt
        # disagreement. Invalidate it now (missing_ok also tolerates no .fetch dir).
        (self.path / ".fetch" / f"{job_id}.receipt.json").unlink(missing_ok=True)

        job = self._storage.get(job_id)
        job_dir = job.path

        # 1. Capture: snapshot the job dir, build the archive (files-only, hashing
        #    members) and sidecar hashes, and assemble the manifest.
        snapshot = _dir_snapshot(job_dir)
        member_paths = [
            path for path in snapshot if path.as_posix() not in SIDECAR_PATHS
        ]
        temp_dir = Path(tempfile.mkdtemp(prefix="r3-move-"))
        try:
            archive_path = temp_dir / "data.tar.zst"
            result = r3.archive.create_archive(job_dir, member_paths, archive_path)
            entries = list(result.entries)
            sidecar_bytes: Dict[str, bytes] = {}
            for name in SIDECAR_PATHS:
                data = (job_dir / name).read_bytes()
                sidecar_bytes[name] = data
                entries.append(FileEntry(name, len(data), r3.utils.hash_bytes(data)))
            manifest = r3.manifest.build_manifest(
                job_id, entries, result.sha256, result.size
            )
            manifest_bytes = r3.manifest.dumps(manifest)

            # 1b. Local round-trip check: prove the just-built archive + sidecars
            #     reconstruct the job (safe_extract + verify_directory, exactly as
            #     fetch does) BEFORE any remote mutation. move deletes the sole local
            #     copy, so a non-restorable archive must abort here, local untouched.
            roundtrip_dir = temp_dir / "roundtrip"
            expected = _payload_sizes(manifest)
            try:
                r3.archive.safe_extract(archive_path, roundtrip_dir, expected)
                for name in SIDECAR_PATHS:
                    (roundtrip_dir / name).write_bytes(sidecar_bytes[name])
                r3.manifest.verify_directory(roundtrip_dir, manifest)
            except (r3.archive.ArchiveError, r3.manifest.ManifestError) as error:
                raise RuntimeError(
                    f"Built archive for job {job_id} does not round-trip "
                    f"(safe_extract + verify_directory failed): {error}. Aborted "
                    "before any remote mutation; local files are untouched."
                ) from error

            # 2. Invalidate any stale publication before overwriting payload keys.
            remote.delete_manifest(job_id)

            # 3. Upload the payload objects.
            remote.put_archive(job_id, archive_path)
            for name, data in sidecar_bytes.items():
                remote.put_sidecar(job_id, name, data)

            # 4. Content-verify every uploaded object before publishing.
            self._verify_upload(remote, job_id, result.sha256, sidecar_bytes, temp_dir)

            # 5. Publish the manifest (verified staging-copy) — the completion marker.
            remote.publish_manifest(job_id, manifest_bytes)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        # 6. Quiescence re-check: abort if the job changed since the capture.
        if _dir_snapshot(job_dir) != snapshot:
            try:
                remote.delete_manifest(job_id)
            except RemoteError as error:
                raise RemoteError(
                    f"Job {job_id} changed during move and the stale published "
                    f"manifest could not be removed ({error}); manual cleanup needed."
                ) from error
            raise RuntimeError(
                f"Job {job_id} changed during move (still running?). Aborted before "
                "deleting local files; move a quiescent job (see LIMITATIONS.md)."
            )

        dependents = self._index.find_dependents(job)

        # 7. Commit the index transition, then 8. delete local atomically.
        self._index.set_location(job_id, remote_name)
        if remote.cache_file_list:
            self._index.set_file_list(job_id, r3.manifest.file_paths(manifest))
        self._atomic_remove_local(job_id)
        return dependents

    def fetch(self, job_id: str) -> None:
        """Fetches a remote job back to local storage, deleting the remote copy.

        The inverse of move: download and content-verify the archive,
        extract into a staging directory, place the sidecars, verify the whole
        directory against the manifest, atomically publish it locally, then delete
        the remote copy and flip the index to local last. A local manifest receipt
        makes the operation retryable even after the remote manifest is deleted.

        Raises:
            ValueError: If the job is already local.
            KeyError: If the job's remote is not configured.
            FileNotFoundError: If the remote has no manifest for the job.
        """
        location = self._index.get_location(job_id)
        if location == "local":
            raise ValueError(f"Job {job_id} is already local.")
        if location not in self._remotes:
            raise KeyError(
                f"Job {job_id} is on remote '{location}', which is not configured."
            )
        remote = self._remotes[location]

        job_dir = self._storage.root / "jobs" / job_id
        fetch_dir = self.path / ".fetch"
        fetch_dir.mkdir(exist_ok=True)
        receipt_path = fetch_dir / f"{job_id}.receipt.json"

        # 0. Idempotent finalize: a prior fetch/move may have left a complete jobs/<id>.
        if job_dir.exists():
            manifest_bytes = self._finalize_manifest(remote, job_id, receipt_path)
            manifest = r3.manifest.loads(manifest_bytes)
            try:
                r3.manifest.verify_directory(job_dir, manifest)
            except r3.manifest.ManifestError as error:
                raise RuntimeError(
                    f"jobs/{job_id} exists but does not match its manifest "
                    f"(corruption): {error}. Manual intervention required."
                ) from error
            # Persist a recovery receipt before the remote is deleted, so a crash
            # between the remote delete and the index flip stays finalizable on rerun.
            # (The main path writes it earlier; a move crash between its index flip and
            # local delete reaches here with none.)
            if not receipt_path.exists():
                _atomic_write_bytes(receipt_path, manifest_bytes)
            # Restore committed-job immutability (idempotent if already protected).
            self._storage.protect_job(job_dir)
            self._finalize_fetch(remote, job_id, receipt_path)
            return

        # 1. Read the manifest; persist a local recovery receipt.
        manifest_bytes = remote.get_manifest(job_id)
        manifest = r3.manifest.loads(manifest_bytes)
        _atomic_write_bytes(receipt_path, manifest_bytes)

        temp_dir = Path(tempfile.mkdtemp(prefix="r3-fetch-"))
        staging_dir = fetch_dir / f"{job_id}-{uuid.uuid4().hex}"
        try:
            # 2. Download + verify the archive.
            archive_path = temp_dir / "data.tar.zst"
            remote.download_archive(job_id, archive_path)
            if r3.utils.hash_file(archive_path) != manifest["archive_sha256"]:
                raise RemoteError(f"Archive checksum mismatch fetching job {job_id}.")

            # 3-4. Extract into staging, then place the sidecars.
            expected = _payload_sizes(manifest)
            r3.archive.safe_extract(archive_path, staging_dir, expected)
            for name in SIDECAR_PATHS:
                (staging_dir / name).write_bytes(remote.get_sidecar(job_id, name))

            # 5. Verify the reconstructed directory against the manifest.
            r3.manifest.verify_directory(staging_dir, manifest)

            # 6. Atomically publish locally.
            os.replace(staging_dir, job_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(staging_dir, ignore_errors=True)

        # 7. Restore committed-job immutability on the freshly published directory.
        self._storage.protect_job(job_dir)

        # 8-9. Delete the remote copy, flip the index to local, drop the receipt.
        self._finalize_fetch(remote, job_id, receipt_path)

    def _verify_upload(
        self,
        remote: Remote,
        job_id: str,
        archive_sha256: str,
        sidecar_bytes: Dict[str, bytes],
        temp_dir: Path,
    ) -> None:
        """Downloads every uploaded object back and content-verifies it before the
        manifest is published. Raises RemoteError on any mismatch."""
        verify_path = temp_dir / "verify.tar.zst"
        remote.download_archive(job_id, verify_path)
        if r3.utils.hash_file(verify_path) != archive_sha256:
            raise RemoteError(f"Archive content verification failed for job {job_id}.")
        verify_path.unlink()
        for name, data in sidecar_bytes.items():
            if remote.get_sidecar(job_id, name) != data:
                raise RemoteError(
                    f"Sidecar {name!r} content verification failed for job {job_id}."
                )

    def _finalize_manifest(
        self, remote: Remote, job_id: str, receipt_path: Path
    ) -> bytes:
        """Returns the chosen manifest BYTES for the step-0 finalize, from the remote
        or the local receipt (requiring agreement if both exist). The caller parses
        them with ``r3.manifest.loads`` and, when no receipt exists yet, persists
        these bytes as the recovery receipt before finalizing."""
        try:
            remote_bytes: Optional[bytes] = remote.get_manifest(job_id)
        except FileNotFoundError:
            remote_bytes = None
        receipt_bytes = receipt_path.read_bytes() if receipt_path.exists() else None

        if remote_bytes is not None and receipt_bytes is not None:
            if remote_bytes != receipt_bytes:
                raise RuntimeError(
                    f"Remote manifest and local receipt for job {job_id} disagree. "
                    "Manual intervention required."
                )
            chosen = remote_bytes
        elif remote_bytes is not None:
            chosen = remote_bytes
        elif receipt_bytes is not None:
            chosen = receipt_bytes
        else:
            raise RuntimeError(
                f"jobs/{job_id} exists and is indexed remote, but neither the remote "
                "manifest nor a local receipt is available to verify it. Manual "
                "intervention required."
            )
        return chosen

    def _finalize_fetch(self, remote: Remote, job_id: str, receipt_path: Path) -> None:
        """Deletes the remote copy, flips the index to local, drops the receipt."""
        remote.delete_job(job_id)
        self._index.set_location(job_id, "local")
        try:
            receipt_path.unlink(missing_ok=True)
        except OSError:
            pass  # receipt is cleanup debris; the local transition already committed

    def _atomic_remove_local(self, job_id: str) -> None:
        """Removes jobs/<id> via an atomic rename into .trash, then rmtree. The rename
        is instantaneous, so a complete jobs/<id> never lingers after the index says
        remote."""
        job_dir = self._storage.root / "jobs" / job_id
        if not job_dir.exists():
            return
        trash_dir = self.path / ".trash"
        trash_dir.mkdir(exist_ok=True)
        target = trash_dir / f"{job_id}-{uuid.uuid4().hex}"
        # A committed job dir is write-protected; renaming it to a different parent
        # updates its ".." entry and so needs write permission on the dir itself.
        os.chmod(job_dir, stat.S_IRWXU)
        os.replace(job_dir, target)
        _force_rmtree(target)

    def rebuild_index(self) -> None:
        """Rebuilds the job index.

        The job index is used to efficiently query for jobs. The index is automatically
        updated when jobs are added or removed. This method has to be called manually
        if the metadata file of a job has been updated manually.
        """
        self._index.rebuild()

    def resolve(
        self,
        item: Union[Job, Dependency],
    ) -> Union[Job, Dependency, List[JobDependency]]:
        """Resolves a job or dependency.

        A job or dependency is resolved by replacing query dependencies with concrete
        dependencies.

        Parameters:
            item: The job or dependency to resolve.

        Returns:
            The resolved job or dependency. A query dependency might resolve to multiple
            concrete dependencies, in which case a list of dependencies is returned.
        """
        if item.is_resolved():
            return item

        if isinstance(item, Job):
            return self._resolve_job(item)
        if isinstance(item, FindLatestDependency):
            return self._resolve_find_latest_dependency(item)
        if isinstance(item, FindAllDependency):
            return self._resolve_find_all_dependency(item)
        if isinstance(item, QueryDependency):
            return self._resolve_query_dependency(item)
        if isinstance(item, QueryAllDependency):
            return self._resolve_query_all_dependency(item)
        if isinstance(item, GitDependency):
            return self._resolve_git_dependency(item)

        raise ValueError(f"Cannot resolve {item}")

    def _resolve_job(self, job: Job) -> Job:
        if not isinstance(job.dependencies, list):
            raise ValueError("Dependencies are not writeable.")

        resolved_dependencies: List[Dependency] = []

        for dependency in job.dependencies:
            resolved_dependency = self.resolve(dependency)
            if isinstance(resolved_dependency, list):
                resolved_dependencies.extend(resolved_dependency)
            else:
                assert isinstance(resolved_dependency, Dependency)
                resolved_dependencies.append(resolved_dependency)

        job._dependencies = resolved_dependencies
        job._config["dependencies"] = [  # type: ignore
            dependency.to_config() for dependency in job.dependencies
        ]
        return job

    def _resolve_find_latest_dependency(
        self,
        dependency: FindLatestDependency,
    ) -> JobDependency:
        result = self.find(dependency.query, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        return JobDependency(
            destination=dependency.destination,
            job=result[0],
            source=dependency.source,
            find_latest=dependency.query,
            recursive_checkout=dependency.recursive_checkout,
        )

    def _resolve_find_all_dependency(
        self, dependency: FindAllDependency
    ) -> List[JobDependency]:
        result = self.find(dependency.query)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        resolved_dependencies = []
        for job in result:
            assert job.id is not None
            resolved_dependencies.append(JobDependency(
                destination=dependency.destination / job.id,
                job=job,
                find_all=dependency.query,
                recursive_checkout=dependency.recursive_checkout,
            ))

        return resolved_dependencies

    def _resolve_query_dependency(
        self,
        dependency: QueryDependency,
    ) -> JobDependency:
        tags = dependency.query.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query}")

        tags = [tag[1:] for tag in tags]
        query = { "tags": { "$all": tags } }
        result = self.find(query, latest=True)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query}")

        return JobDependency(
            dependency.destination, result[0], dependency.source, query=dependency.query
        )

    def _resolve_query_all_dependency(
        self,
        dependency: QueryAllDependency,
    ) -> List[JobDependency]:
        tags = dependency.query_all.strip().split(" ")

        if not all(tag.startswith("#") for tag in tags):
            raise ValueError(f"Invalid query: {dependency.query_all}")

        tags = [tag[1:] for tag in tags]
        query = { "tags": { "$all": tags } }
        result = self.find(query)

        if len(result) < 1:
            raise ValueError(f"Cannot resolve dependency: {dependency.query_all}")

        resolved_dependencies = []
        for job in result:
            assert job.id is not None
            resolved_dependencies.append(JobDependency(
                dependency.destination / job.id, job, query_all=dependency.query_all)
            )

        return resolved_dependencies

    def _resolve_git_dependency(self, dependency: GitDependency) -> GitDependency:
        repository_path = self.path / dependency.repository_path
        if not repository_path.exists():
            execute(f"git clone --bare {dependency.repository} {repository_path}")

        if dependency.branch is not None:
            commit = r3.utils.git_get_remote_branch_head(
                repository_path, dependency.branch
            )
            if commit is None:
                raise ValueError(f"Branch not found: {dependency.branch}")
        elif dependency.tag is not None:
            commit = r3.utils.git_get_remote_tag_head(repository_path, dependency.tag)
            if commit is None:
                raise ValueError(f"Tag not found: {dependency.tag}")
        else:
            commit = r3.utils.git_get_remote_head(repository_path)

        return GitDependency(
            dependency.destination,
            dependency.repository,
            commit,
            source=dependency.source,
        )


def _payload_sizes(manifest: Dict[str, Any]) -> Dict[str, int]:
    """Returns {path: size} for the archive-resident files (manifest minus sidecars).

    This is the ``expected_sizes`` mapping ``r3.archive.safe_extract`` is bounded by,
    used identically by the move round-trip check and by fetch.
    """
    return {
        entry["path"]: entry["size"]
        for entry in manifest["files"]
        if entry["path"] not in SIDECAR_PATHS
    }


def _dir_snapshot(job_dir: Path) -> Dict[Path, Tuple[int, int]]:
    """Returns {relative path: (size, mtime_ns)} for every file under job_dir.

    Used both to enumerate the job's files and to detect mutation between the move
    capture and the local deletion (the quiescence re-check before deleting local).
    """
    snapshot: Dict[Path, Tuple[int, int]] = {}
    for child in job_dir.rglob("*"):
        if child.is_file():
            stat_result = child.stat()
            snapshot[child.relative_to(job_dir)] = (
                stat_result.st_size,
                stat_result.st_mtime_ns,
            )
    return snapshot


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Writes bytes to path via a temp file + os.replace (crash-safe)."""
    tmp_path = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex}")
    tmp_path.write_bytes(data)
    os.replace(tmp_path, path)


def _force_rmtree(path: Path) -> None:
    """Removes a directory tree, first making directories writable so entries in a
    write-protected committed job can be unlinked."""
    for root, _dirs, _files in os.walk(path):
        os.chmod(root, stat.S_IRWXU)
    shutil.rmtree(path)
