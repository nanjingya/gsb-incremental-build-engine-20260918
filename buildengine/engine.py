"""Resumable concurrent incremental build engine (stdlib only).

Cache layout under ``cache_dir``::

    blobs/<2-hex shard>/<sha256-of-content>   immutable content-addressed blobs
    tasks/<sha256-of-task-name>.json          metadata: key + blob reference

A task cache key covers the task name, its ``version`` string, and the
ordered (dependency name, dependency content hash) pairs, serialized with
length-prefixing so keys cannot be confused by concatenation.  Because only
dependency *content* feeds downstream keys, rebuilding an unchanged
dependency lets dependents reuse their cache.

All cache writes are atomic: data goes to a ``.tmp-*`` file in the same
directory, is fsynced, then ``os.replace``-d into place.  Readers therefore
never observe a half-written file.  No inter-process locks are used: blobs
are immutable and metadata files are replaced atomically, so concurrent
processes may duplicate computation but can never corrupt each other's
cache.  Cross-process deduplication is explicitly *not* guaranteed.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

__all__ = [
    "Task",
    "Engine",
    "BuildEngineError",
    "ValidationError",
    "BuildFailed",
    "TaskRecord",
    "BuildReport",
]

CACHED = "cached"
EXECUTED = "executed"
FAILED = "failed"
SKIPPED = "skipped"

_TMP_PREFIX = ".tmp-"
_KEY_DOMAIN = "buildengine-key-v1"


class BuildEngineError(Exception):
    """Base class for all errors raised by buildengine."""


class ValidationError(BuildEngineError):
    """The task graph or requested targets are invalid.

    Raised before any action runs.  The message names the offending nodes.
    """


class BuildFailed(BuildEngineError):
    """One or more actions raised during :meth:`Engine.build`.

    Attributes:
        failures: mapping of task name -> the original exception raised by
            its action (traceback and ``__cause__`` chain preserved).
        report: the :class:`BuildReport` describing every reachable task.
    """

    def __init__(self, failures, report):
        self.failures = dict(failures)
        self.report = report
        names = ", ".join(sorted(self.failures))
        super().__init__(f"build failed; failed task(s): {names}")


@dataclass(frozen=True)
class Task:
    name: str
    deps: tuple[str, ...]
    action: Callable[[dict[str, bytes]], bytes]
    version: str = "1"


@dataclass
class TaskRecord:
    name: str
    status: str  # CACHED | EXECUTED | FAILED | SKIPPED
    detail: str = ""


@dataclass
class BuildReport:
    """Structured outcome of one :meth:`Engine.build` call."""

    records: dict[str, TaskRecord] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def _names(self, status):
        return sorted(n for n, r in self.records.items() if r.status == status)

    @property
    def cached(self):
        return self._names(CACHED)

    @property
    def executed(self):
        return self._names(EXECUTED)

    @property
    def failed(self):
        return self._names(FAILED)

    @property
    def skipped(self):
        return self._names(SKIPPED)


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_name(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def _task_key(task: Task, dep_blob_hashes: dict[str, str]) -> str:
    """Length-prefixed canonical serialization -> hex digest.

    Length prefixing makes the encoding injective: no two distinct
    (name, version, deps) tuples can serialize to the same byte stream.
    """
    h = hashlib.sha256()

    def feed(text: str) -> None:
        raw = text.encode("utf-8")
        h.update(len(raw).to_bytes(8, "big"))
        h.update(raw)

    feed(_KEY_DOMAIN)
    feed(task.name)
    feed(task.version)
    h.update(len(task.deps).to_bytes(8, "big"))
    for dep in task.deps:
        feed(dep)
        feed(dep_blob_hashes[dep])
    return h.hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically (temp file + fsync + replace)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=_TMP_PREFIX)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:  # best-effort directory durability
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class Engine:
    """Dependency-based incremental build engine.

    Args:
        tasks: iterable of :class:`Task`.  Names must be unique (checked at
            build time, before any action runs).
        cache_dir: directory for the on-disk cache; shared safely across
            Engine instances, threads and processes.
        max_workers: maximum concurrently running actions in one ``build``
            call.  Defaults to ``min(32, cpu_count + 4)``.

    Thread-safety: ``build`` keeps all mutable state in per-call locals, so
    concurrent ``build`` calls on the same Engine instance are safe provided
    the actions themselves are thread-safe.
    """

    def __init__(self, tasks, cache_dir, max_workers=None):
        self._task_list = list(tasks)
        self.tasks = {task.name: task for task in self._task_list}
        self.cache_dir = Path(cache_dir)
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        if self.max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._blobs_dir = self.cache_dir / "blobs"
        self._tasks_dir = self.cache_dir / "tasks"
        self._blobs_dir.mkdir(parents=True, exist_ok=True)
        self._tasks_dir.mkdir(parents=True, exist_ok=True)
        self._sweep_stale_temp_files()

    # ------------------------------------------------------------------ API

    def build(self, targets, *, force=False, return_report=False):
        """Build ``targets`` and return ``{target_name: bytes}``.

        Args:
            targets: iterable of task names.  Duplicates are built once;
                an empty iterable validates the graph and returns ``{}``.
            force: re-execute every reachable task, ignoring cached results
                (results are still published to the cache).
            return_report: when true, return ``(outputs, BuildReport)``.

        Raises:
            ValidationError: invalid graph or unknown targets (no action ran).
            BuildFailed: at least one action raised; dependents were skipped
                while independent tasks were allowed to finish and cache.
        """
        targets = list(dict.fromkeys(targets))
        report = BuildReport()
        self._validate_graph()
        reachable = self._reachable_subgraph(targets)
        outputs: dict[str, bytes] = {}
        if reachable:
            outputs = self._run(reachable, force, report)
        result = {name: outputs[name] for name in targets}
        if return_report:
            return result, report
        return result

    # ----------------------------------------------------------- validation

    def _validate_graph(self):
        counts = Counter(task.name for task in self._task_list)
        duplicates = sorted(name for name, n in counts.items() if n > 1)
        if duplicates:
            raise ValidationError(
                "duplicate task name(s): " + ", ".join(repr(n) for n in duplicates)
            )
        problems = []
        for task in self._task_list:
            for dep in task.deps:
                if dep not in self.tasks:
                    problems.append(f"task {task.name!r} depends on missing task {dep!r}")
        if problems:
            raise ValidationError("missing dependencies: " + "; ".join(problems))

    def _reachable_subgraph(self, targets):
        """Return reachable task names; raise on unknown target or cycle."""
        missing = [t for t in targets if t not in self.tasks]
        if missing:
            raise ValidationError(
                "unknown target(s): " + ", ".join(repr(t) for t in missing)
            )
        color: dict[str, int] = {}  # 1 = on stack, 2 = done
        reachable: set[str] = set()
        for root in targets:
            if color.get(root) == 2:
                continue
            color[root] = 1
            path = [root]
            stack = [(root, iter(self.tasks[root].deps))]
            while stack:
                node, deps_iter = stack[-1]
                descended = False
                for dep in deps_iter:
                    state = color.get(dep, 0)
                    if state == 1:
                        cycle = path[path.index(dep):] + [dep]
                        raise ValidationError(
                            "dependency cycle detected: " + " -> ".join(
                                repr(n) for n in cycle
                            )
                        )
                    if state == 0:
                        color[dep] = 1
                        path.append(dep)
                        stack.append((dep, iter(self.tasks[dep].deps)))
                        descended = True
                        break
                if not descended:
                    color[node] = 2
                    reachable.add(node)
                    stack.pop()
                    path.pop()
        return reachable

    # ------------------------------------------------------------ scheduling

    def _run(self, reachable, force, report):
        dependents: dict[str, list[str]] = {name: [] for name in reachable}
        remaining: dict[str, int] = {}
        for name in reachable:
            deps = [d for d in self.tasks[name].deps if d in reachable]
            remaining[name] = len(deps)
            for dep in deps:
                dependents[dep].append(name)

        results: dict[str, bytes] = {}
        blob_hashes: dict[str, str] = {}
        failures: dict[str, BaseException] = {}
        ready = [n for n in reachable if remaining[n] == 0]
        in_flight: dict = {}

        def resolve(name):
            for dependent in dependents[name]:
                remaining[dependent] -= 1
                if remaining[dependent] == 0:
                    ready.append(dependent)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while ready or in_flight:
                while ready:
                    name = ready.pop()
                    task = self.tasks[name]
                    bad = [
                        d
                        for d in task.deps
                        if d in reachable
                        and report.records[d].status in (FAILED, SKIPPED)
                    ]
                    if bad:
                        report.records[name] = TaskRecord(
                            name, SKIPPED, "upstream failed: " + ", ".join(sorted(bad))
                        )
                        resolve(name)
                        continue
                    dep_hashes = {d: blob_hashes[d] for d in task.deps if d in reachable}
                    key = _task_key(task, dep_hashes)
                    if not force:
                        cached = self._cache_lookup(task, key, report)
                        if cached is not None:
                            data, blob_hash = cached
                            results[name] = data
                            blob_hashes[name] = blob_hash
                            report.records[name] = TaskRecord(name, CACHED)
                            resolve(name)
                            continue
                    dep_values = {d: results[d] for d in task.deps if d in reachable}
                    future = pool.submit(self._execute_action, task, dep_values)
                    in_flight[future] = (name, key)
                if not in_flight:
                    break
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    name, key = in_flight.pop(future)
                    try:
                        output = future.result()
                    except BaseException as exc:  # noqa: BLE001 - reported, not swallowed
                        failures[name] = exc
                        report.records[name] = TaskRecord(
                            name, FAILED, f"{type(exc).__name__}: {exc}"
                        )
                    else:
                        blob_hash = self._cache_publish(self.tasks[name], key, output)
                        results[name] = output
                        blob_hashes[name] = blob_hash
                        report.records[name] = TaskRecord(name, EXECUTED)
                    resolve(name)

        if failures:
            raise BuildFailed(failures, report)
        return results

    @staticmethod
    def _execute_action(task, dep_values):
        output = task.action(dep_values)
        if not isinstance(output, bytes):
            raise TypeError(
                f"action for task {task.name!r} must return bytes, "
                f"got {type(output).__name__}"
            )
        return output

    # ----------------------------------------------------------------- cache

    def _blob_path(self, blob_hash: str) -> Path:
        return self._blobs_dir / blob_hash[:2] / blob_hash

    def _meta_path(self, name: str) -> Path:
        return self._tasks_dir / f"{_hash_name(name)}.json"

    def _cache_lookup(self, task, key, report):
        """Return ``(data, blob_hash)`` on a verified hit, else ``None``."""
        meta_path = self._meta_path(task.name)
        try:
            raw = meta_path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            report.warnings.append(f"task {task.name!r}: unreadable metadata: {exc}")
            return None
        try:
            meta = json.loads(raw.decode("utf-8"))
            assert isinstance(meta, dict)
            blob_hash = meta["blob"]
            assert meta["format"] == 1 and isinstance(blob_hash, str)
            assert meta["key"] and meta["name"] == task.name
        except (ValueError, UnicodeDecodeError, KeyError, AssertionError):
            report.warnings.append(
                f"task {task.name!r}: corrupt cache metadata at {meta_path}; rebuilding"
            )
            return None
        if meta["key"] != key:
            return None  # normal invalidation: inputs changed
        blob_path = self._blob_path(blob_hash)
        try:
            data = blob_path.read_bytes()
        except OSError:
            report.warnings.append(
                f"task {task.name!r}: missing blob {blob_hash} at {blob_path}; rebuilding"
            )
            return None
        if _hash_bytes(data) != blob_hash:
            report.warnings.append(
                f"task {task.name!r}: corrupt blob {blob_hash} at {blob_path}; rebuilding"
            )
            return None
        return data, blob_hash

    def _cache_publish(self, task, key, output: bytes) -> str:
        blob_hash = _hash_bytes(output)
        blob_path = self._blob_path(blob_hash)
        if not blob_path.exists():  # content-addressed: identical writes are no-ops
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write(blob_path, output)
        meta = json.dumps(
            {"format": 1, "name": task.name, "key": key, "blob": blob_hash},
            sort_keys=True,
        ).encode("utf-8")
        _atomic_write(self._meta_path(task.name), meta)
        return blob_hash

    def _sweep_stale_temp_files(self):
        """Remove ``.tmp-*`` leftovers from crashed processes (never linked)."""
        for directory in (self._blobs_dir, self._tasks_dir):
            for root, _dirs, files in os.walk(directory):
                for filename in files:
                    if filename.startswith(_TMP_PREFIX):
                        try:
                            os.unlink(Path(root) / filename)
                        except OSError:
                            pass
