"""Resumable, concurrent, content-addressed incremental build engine.

Public surface (backward compatible with the original engine):

    Task(name, deps, action, version="1")
    Engine(tasks, cache_dir, max_workers=None)
    Engine.build(targets) -> dict[str, bytes]

Extensions:

    Engine.build(targets, force=False, return_report=False)
    BuildReport / TaskStatus / TaskFailure / CacheEvent
    BuildGraphError, BuildFailed, InvalidActionOutputError

Only the Python standard library is used.
"""

from __future__ import annotations

import enum
import hashlib
import itertools
import json
import os
import tempfile
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:  # POSIX only; other platforms degrade to no inter-process locking.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None

CACHE_FORMAT_VERSION = 1
_META_DIR = "meta"
_BLOB_DIR = "blobs"
_LOCK_DIR = "locks"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BuildEngineError(Exception):
    """Base class for all buildengine specific exceptions."""


class BuildGraphError(BuildEngineError):
    """Raised before any action runs when the task graph or targets are invalid.

    Multiple problems are reported together. Structured diagnostics are
    available on the attributes:

    * ``duplicate_names`` -- task names appearing more than once
    * ``missing_dependencies`` -- {task_name: [missing dep names]}
    * ``missing_targets`` -- requested target names that do not exist
    * ``cycles`` -- list of cycles, each a list of node names
    """

    def __init__(self, problems, duplicate_names=(), missing_dependencies=None,
                 missing_targets=(), cycles=()):
        self.problems = list(problems)
        self.duplicate_names = list(duplicate_names)
        self.missing_dependencies = {
            name: list(deps) for name, deps in (missing_dependencies or {}).items()
        }
        self.missing_targets = list(missing_targets)
        self.cycles = [list(cycle) for cycle in cycles]
        message = "invalid build graph:\n  " + "\n  ".join(self.problems)
        super().__init__(message)


class InvalidActionOutputError(BuildEngineError, TypeError):
    """Raised as a node failure when an action returns a non-bytes value.

    Inherits TypeError for compatibility with the original engine.
    """

    def __init__(self, task_name, value):
        self.task_name = task_name
        self.value = value
        super().__init__(
            f"task {task_name!r} action must return bytes, "
            f"got {type(value).__name__}"
        )


class BuildFailed(BuildEngineError):
    """Raised when one or more tasks fail during a build.

    The structured BuildReport is available as ``report``. Each TaskFailure
    in ``report.failures`` preserves its original exception chain.
    """

    def __init__(self, report):
        self.report = report
        names = ", ".join(failure.task for failure in report.failures)
        details = "; ".join(
            f"{failure.task}: {type(failure.error).__name__}: {failure.error}"
            for failure in report.failures
        )
        super().__init__(
            f"build failed for {len(report.failures)} task(s): {names}; "
            f"{len(report.skipped)} downstream task(s) skipped "
            f"[{details}]"
        )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


class TaskStatus(enum.Enum):
    CACHED = "cached"        # output reused from disk cache
    EXECUTED = "executed"    # action ran and succeeded
    FORCED = "forced"        # action ran because force=True
    FAILED = "failed"        # action raised or returned invalid output
    SKIPPED = "skipped"      # not run because an upstream task failed


@dataclass(frozen=True)
class TaskFailure:
    task: str
    error: BaseException
    traceback: str


@dataclass(frozen=True)
class CacheEvent:
    kind: str                # corrupt_metadata | corrupt_blob |
    #                        # missing_blob | temp_swept
    key: str
    detail: str = ""
    path: str = ""


@dataclass(frozen=True)
class BuildReport:
    targets: tuple[str, ...]
    statuses: dict
    failures: tuple
    skipped: dict            # skipped task -> responsible failed ancestor
    outputs: dict            # every reached task name -> output bytes
    cache_hits: frozenset
    cache_events: tuple
    elapsed: float

    @property
    def ok(self):
        return not self.failures

    @property
    def executed(self):
        return frozenset(
            name for name, status in self.statuses.items()
            if status in (TaskStatus.EXECUTED, TaskStatus.FORCED)
        )

    def as_dict(self):
        return {
            "targets": list(self.targets),
            "statuses": {
                name: status.value for name, status in self.statuses.items()
            },
            "failed": [failure.task for failure in self.failures],
            "skipped": dict(self.skipped),
            "cache_hits": sorted(self.cache_hits),
            "cache_events": [
                {"kind": event.kind, "key": event.key,
                 "detail": event.detail, "path": event.path}
                for event in self.cache_events
            ],
            "elapsed": self.elapsed,
        }


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Task:
    name: str
    deps: tuple
    action: Callable[[dict], bytes]
    version: str = "1"


# ---------------------------------------------------------------------------
# Cache keys
# ---------------------------------------------------------------------------


def _digest_hex(data):
    return hashlib.sha256(data).hexdigest()


def build_key(task, dep_digests):
    """Cache key for *task* given dependency output digests.

    *dep_digests* maps dependency name -> sha256 hex digest of that
    dependency's output bytes. The key covers the cache format version, task
    name, explicit task ``version`` (action code is never introspected --
    bumping ``version`` is how action changes are signalled), the ordered
    dependency list and every dependency content digest.

    Encoding is an unambiguous length-prefixed frame: each string becomes
    ``<8-byte big-endian length><utf-8 bytes>`` and structured sections carry
    their own element counts, so no two distinct inputs serialize to the same
    bytes (unlike naive separators/JSON string concatenation).
    """
    key = hashlib.sha256()

    def put(value):
        raw = str(value).encode("utf-8")
        key.update(len(raw).to_bytes(8, "big"))
        key.update(raw)

    put("buildengine-cache-v%d" % CACHE_FORMAT_VERSION)
    put(task.name)
    put(str(task.version))
    deps = list(task.deps)
    key.update(len(deps).to_bytes(8, "big"))
    for dep_name in deps:
        put(dep_name)
        put(dep_digests[dep_name])
    return key.hexdigest()


# ---------------------------------------------------------------------------
# Content-addressed on-disk cache
# ---------------------------------------------------------------------------


class _CacheCorrupt(Exception):
    """Internal: a cache entry is truncated, damaged or inconsistent."""


class ContentCache:
    """On-disk content-addressed result cache.

    Layout::

        cache_dir/
            blobs/<sha256-hex>   immutable output bytes (content addressed)
            meta/<key>.json      metadata binding a build key to a blob
            locks/<key>.lock     cross-process advisory locks
            .*.tmp-*             publish temp files (never observed as final)

    The blob is fully written and fsynced first; the metadata file -- the only
    thing a reader needs to find a result -- is published afterwards. Both go
    through ``os.replace`` from uniquely named temp files in the same
    directory, so publication is atomic and readers never see partial data.
    Blobs are content addressed and therefore immutable.
    """

    def __init__(self, cache_dir):
        self.root = Path(cache_dir)
        self.blobs = self.root / _BLOB_DIR
        self.meta = self.root / _META_DIR
        self.locks = self.root / _LOCK_DIR
        for directory in (self.root, self.blobs, self.meta, self.locks):
            directory.mkdir(parents=True, exist_ok=True)

    # -- housekeeping ------------------------------------------------------

    def sweep_tempfiles(self):
        """Remove publish temp files left by crashed processes.

        Temp files carry the publishing process PID. Files owned by *this*
        live process (including ones being published by a concurrent build in
        the same process) are never touched.
        """
        removed = 0
        own_marker = "-" + str(os.getpid()) + "-"
        for directory in (self.meta, self.blobs):
            try:
                entries = list(directory.iterdir())
            except FileNotFoundError:
                continue
            for entry in entries:
                if entry.name.startswith(".tmp-") and own_marker not in entry.name:
                    try:
                        entry.unlink()
                        removed += 1
                    except FileNotFoundError:
                        pass
                    except OSError:
                        pass
        return removed

    # -- reads -------------------------------------------------------------

    def _meta_path(self, key):
        return self.meta / (key + ".json")

    def _blob_path(self, digest_hex):
        return self.blobs / digest_hex

    def _load_meta(self, key):
        path = self._meta_path(key)
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise _CacheCorrupt(f"metadata unreadable: {exc}") from exc
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _CacheCorrupt(f"metadata is not valid UTF-8 JSON: {exc}") from exc
        expected = {"format", "name", "version", "dependencies", "sha256", "size"}
        if not isinstance(doc, dict) or set(doc) != expected:
            raise _CacheCorrupt("metadata has wrong shape/fields")
        if doc["format"] != CACHE_FORMAT_VERSION:
            raise _CacheCorrupt(f"unsupported cache format: {doc['format']!r}")
        if not isinstance(doc["name"], str) or not isinstance(doc["version"], str):
            raise _CacheCorrupt("metadata name/version have wrong types")
        deps = doc["dependencies"]
        if not isinstance(deps, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in deps.items()
        ):
            raise _CacheCorrupt("metadata dependencies have wrong shape")
        if isinstance(doc["size"], bool) or not isinstance(doc["size"], int) \
                or doc["size"] < 0:
            raise _CacheCorrupt("metadata size is invalid")
        if not isinstance(doc["sha256"], str):
            raise _CacheCorrupt("metadata sha256 has wrong type")
        return doc

    def read(self, task, key, dep_digests, events):
        """Return cached bytes for *key*, or None on a clean miss.

        Truncated/corrupt metadata or blobs, missing blobs and inconsistent
        metadata are quarantined (and reported via *events*) and treated as a
        miss, so a damaged cache never masquerades as a successful result.
        """
        meta_path = self._meta_path(key)
        doc = None
        try:
            doc = self._load_meta(key)
            if doc is None:
                return None
            if doc["name"] != task.name \
                    or str(doc["version"]) != str(task.version) \
                    or doc["dependencies"] != dict(dep_digests):
                raise _CacheCorrupt("metadata payload does not match key inputs")
            blob_path = self._blob_path(doc["sha256"])
            try:
                data = blob_path.read_bytes()
            except FileNotFoundError:
                raise _CacheCorrupt("referenced blob is missing")
            if len(data) != doc["size"]:
                raise _CacheCorrupt(
                    f"blob size {len(data)} != metadata size {doc['size']}"
                )
            if _digest_hex(data) != doc["sha256"]:
                raise _CacheCorrupt("blob digest mismatch")
            return data
        except _CacheCorrupt as exc:
            detail = str(exc)
            kind = ("missing_blob" if detail == "referenced blob is missing"
                    else "corrupt_blob" if "blob" in detail
                    else "corrupt_metadata")
            events.append(CacheEvent(kind, key, detail, str(meta_path)))
            self._quarantine(meta_path)
            if kind != "corrupt_metadata" and isinstance(doc, dict) \
                    and isinstance(doc.get("sha256"), str):
                events.append(CacheEvent(
                    "corrupt_blob", key, "quarantined with metadata",
                    str(self._blob_path(doc["sha256"]))))
                self._quarantine(self._blob_path(doc["sha256"]))
            return None

    def _quarantine(self, path):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    # -- writes ------------------------------------------------------------

    @staticmethod
    def _atomic_put(final_path, data):
        """Write *data* to *final_path* atomically and durably.

        Temp file is created in the same directory (same filesystem rename),
        fsynced, then os.replace'd. Unique name embeds pid + uuid so
        concurrent processes and crashed-process leftovers never collide.
        """
        directory = final_path.parent
        fd, tmp_name = tempfile.mkstemp(
            prefix=".tmp-", suffix="-" + str(os.getpid()) + "-" + uuid.uuid4().hex,
            dir=str(directory),
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, final_path)
            ContentCache._fsync_dir(directory)
        except BaseException:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(directory):
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        try:
            fd = os.open(str(directory), flags)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    def publish(self, task, key, dep_digests, data):
        """Atomically publish a successful result. Returns (digest, size)."""
        digest_hex = _digest_hex(data)
        size = len(data)
        blob_path = self._blob_path(digest_hex)
        if not blob_path.exists():
            # Content-addressed: same bytes always map to the same path; an
            # existing file (published by another process) is identical.
            self._atomic_put(blob_path, data)
        doc = {
            "format": CACHE_FORMAT_VERSION,
            "name": task.name,
            "version": str(task.version),
            "dependencies": dict(dep_digests),
            "sha256": digest_hex,
            "size": size,
        }
        payload = json.dumps(doc, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self._atomic_put(self._meta_path(key), payload)
        return digest_hex, size


# ---------------------------------------------------------------------------
# Cross-process / in-process per-key locks
# ---------------------------------------------------------------------------


_LOCK_REGISTRY = {}
_LOCK_REGISTRY_GUARD = threading.Lock()


def _get_key_locks(cache):
    """Return the process-wide KeyLocks for a resolved cache directory.

    Two Engine instances pointed at the same cache_dir inside one process
    therefore also deduplicate work.
    """
    root = os.path.realpath(str(cache.root))
    with _LOCK_REGISTRY_GUARD:
        locks = _LOCK_REGISTRY.get(root)
        if locks is None:
            locks = _KeyLocks(cache)
            _LOCK_REGISTRY[root] = locks
        return locks


class _KeyLocks:
    """Per-cache-key locks: one in-process Lock plus one flock file.

    The flock is an advisory exclusive lock held for the
    "recheck cache -> run action -> publish" critical section, which gives
    cross-process *deduplication* for cooperating processes: the second
    process finds the freshly published metadata and does not run the action.

    The lock file itself is never deleted (which would be unsafe under
    concurrent holders) and never needed to observe a result; readers only
    open blob/meta files, which are published atomically.
    """

    def __init__(self, cache):
        self._cache = cache
        # key -> (threading.Lock, active-holder count)
        self._locks = {}
        self._guard = threading.Lock()

    def hold(self, key):
        with self._guard:
            entry = self._locks.get(key)
            if entry is None:
                tlock = threading.Lock()
                entry = [tlock, 0]
                self._locks[key] = entry
            entry[1] += 1
        return _KeyLockHandle(self, key, entry)

    def _release(self, key, entry):
        with self._guard:
            entry[1] -= 1
            if entry[1] == 0:
                self._locks.pop(key, None)


class _KeyLockHandle:
    def __init__(self, owner, key, entry):
        self._owner = owner
        self._key = key
        self._entry = entry
        self._tlock = entry[0]
        self._file = None

    def __enter__(self):
        self._tlock.acquire()
        if fcntl is not None:
            path = self._owner._cache.locks / (self._key + ".lock")
            try:
                self._file = open(path, "a+b")
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
            except OSError:
                # Locking unavailable (e.g. exotic FS): degrade safely. Atomic
                # publication still prevents corruption; only dedup is lost.
                if self._file is not None:
                    self._file.close()
                    self._file = None
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._file is not None and fcntl is not None:
                try:
                    fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
                finally:
                    self._file.close()
        finally:
            self._tlock.release()
            self._owner._release(self._key, self._entry)
        return False


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class Engine:
    """Dependency-based incremental build engine with a disk cache.

    Parameters
    ----------
    tasks:
        Iterable of :class:`Task` objects.
    cache_dir:
        Directory for the content-addressed cache; created if missing. Task
        names never appear in cache paths (keys are hex digests), so Unicode
        names and path separators are safe.
    max_workers:
        Worker thread count passed to ThreadPoolExecutor. ``None`` selects the
        executor default. Each build uses its own isolated executor and
        in-memory state; the Engine itself is immutable after construction.
    """

    def __init__(self, tasks, cache_dir, max_workers=None):
        self._task_list = list(tasks)
        for task in self._task_list:
            if not isinstance(task, Task):
                raise TypeError(f"expected Task, got {type(task).__name__}")
        self.tasks = {}
        for task in self._task_list:
            self.tasks[task.name] = task
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self._cache = ContentCache(self.cache_dir)
        self._key_locks = _get_key_locks(self._cache)

    # -- validation --------------------------------------------------------

    def validate(self, targets=None):
        """Validate task names, dependencies, targets and reachable cycles.

        Raises BuildGraphError describing every detected problem; returns None
        when valid. With *targets* given, only the target-reachable subgraph is
        checked for cycles (an unreachable cycle elsewhere is legal). With
        ``targets=None`` the whole graph is checked.
        """
        if targets is None:
            target_names = []
            whole_graph = True
        else:
            target_names = self._normalize_targets(targets)
            whole_graph = False
        self._validate(target_names, whole_graph=whole_graph)

    @staticmethod
    def _normalize_targets(targets):
        if isinstance(targets, (str, bytes)):
            return [targets]

        names = []
        for target in targets:
            if not isinstance(target, str):
                raise BuildGraphError(
                    [f"target names must be str, got {type(target).__name__}: "
                     f"{target!r}"]
                )
            if target not in names:
                names.append(target)
        return names

    def _validate(self, target_names, whole_graph=False):
        problems = []
        duplicate_names = []
        seen = set()
        for task in self._task_list:
            if task.name in seen and task.name not in duplicate_names:
                duplicate_names.append(task.name)
            seen.add(task.name)
        for name in duplicate_names:
            problems.append(f"duplicate task name: {name!r}")

        missing_targets = [t for t in target_names if t not in self.tasks]
        for name in missing_targets:
            problems.append(f"target does not exist: {name!r}")

        if whole_graph:
            reachable = set(self.tasks)
        else:
            reachable = set()
            stack = [t for t in target_names if t in self.tasks]
            while stack:
                name = stack.pop()
                if name in reachable:
                    continue
                reachable.add(name)
                task = self.tasks.get(name)
                if task is not None:
                    stack.extend(task.deps)

        missing_dependencies = {}
        for name in sorted(reachable):
            task = self.tasks.get(name)
            if task is None:
                continue
            missing = [dep for dep in task.deps if dep not in self.tasks]
            if missing:
                missing_dependencies[name] = missing
                for dep in missing:
                    problems.append(
                        f"task {name!r} depends on missing task {dep!r}"
                    )

        cycles = []
        if not missing_dependencies:
            cycles = self._find_cycles(reachable)
            for cycle in cycles:
                problems.append(
                    "dependency cycle in target-reachable subgraph: "
                    + " -> ".join(repr(node) for node in cycle)
                )

        if problems:
            raise BuildGraphError(
                problems,
                duplicate_names=duplicate_names,
                missing_dependencies=missing_dependencies,
                missing_targets=missing_targets,
                cycles=cycles,
            )

    def _find_cycles(self, nodes):
        """Iterative 3-color DFS; returns every discovered back-edge cycle."""
        edges = {name: self.tasks[name].deps for name in nodes if name in self.tasks}
        color = {name: 0 for name in nodes}
        cycles = []
        for root in sorted(nodes):
            if color.get(root, 2) != 0:
                continue
            color[root] = 1
            path = [root]
            stack = [(root, 0)]
            while stack:
                node, index = stack[-1]
                deps = edges.get(node, ())
                if index < len(deps):
                    stack[-1] = (node, index + 1)
                    dep = deps[index]
                    if dep not in color:
                        continue  # missing dependency; already reported
                    if color[dep] == 0:
                        color[dep] = 1
                        path.append(dep)
                        stack.append((dep, 0))
                    elif color[dep] == 1:
                        start = path.index(dep)
                        cycles.append(path[start:] + [dep])
                else:
                    color[node] = 2
                    path.pop()
                    stack.pop()
        return cycles

    # -- build -------------------------------------------------------------

    def build(self, targets, force=False, return_report=False):
        """Build *targets* and return ``{target_name: output_bytes}``.

        Parameters
        ----------
        targets:
            Iterable of target task names. Duplicate names are built once and
            returned once; an empty iterable yields ``{}``.
        force:
            When True, every reachable task is executed regardless of cache
            state; fresh successful results republish cache entries.
        return_report:
            When True, return a :class:`BuildReport` instead of the dict.

        Raises
        ------
        BuildGraphError:
            Graph/target validation failed (nothing was executed).
        BuildFailed:
            One or more tasks failed; ``e.report`` holds the structured
            report (including independent tasks that completed and cached).
        """
        if self.max_workers is not None and self.max_workers < 1:
            raise ValueError("max_workers must be >= 1 or None")
        target_names = self._normalize_targets(targets)
        self._validate(target_names)
        started = time.perf_counter()

        cache_events = []
        swept = self._cache.sweep_tempfiles()
        if swept:
            cache_events.append(CacheEvent(
                "temp_swept", "", f"{swept} stale temp file(s) from a crashed "
                f"process removed at build start"))

        if not target_names:
            report = BuildReport(
                targets=(), statuses={}, failures=(), skipped={},
                outputs={}, cache_hits=frozenset(),
                cache_events=tuple(cache_events),
                elapsed=time.perf_counter() - started,
            )
            return report if return_report else {}

        reachable = set()
        stack = list(target_names)
        while stack:
            name = stack.pop()
            if name in reachable:
                continue
            reachable.add(name)
            stack.extend(self.tasks[name].deps)

        dependents = {name: [] for name in reachable}
        for name in reachable:
            for dep in self.tasks[name].deps:
                if dep in dependents:
                    dependents[dep].append(name)
        remaining = {name: len(self.tasks[name].deps) for name in reachable}

        outputs = {}          # resolved name -> bytes
        digests = {}          # resolved name -> output content hex digest
        statuses = {}
        cache_hits = set()
        failures = []
        skipped = {}          # name -> responsible failed ancestor
        scheduled = set()

        def run_node(task, dep_outputs, dep_digests):
            key = build_key(task, dep_digests)
            local_events = []
            with self._key_locks.hold(key):
                # Recheck under the lock: another thread/process may have
                # published this exact result while we were queued.
                data = None if force else self._cache.read(
                    task, key, dep_digests, local_events)
                if data is not None:
                    return data, _digest_hex(data), TaskStatus.CACHED, local_events
                output = task.action(dep_outputs)
                if not isinstance(output, bytes):
                    raise InvalidActionOutputError(task.name, output)
                digest_hex, _size = self._cache.publish(
                    task, key, dep_digests, output)
                status = TaskStatus.FORCED if force else TaskStatus.EXECUTED
            return output, digest_hex, status, local_events

        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = {}
        try:
            def submit(name):
                if name in scheduled or name in outputs or name in skipped:
                    return
                scheduled.add(name)
                task = self.tasks[name]
                dep_outputs = {dep: outputs[dep] for dep in task.deps}
                dep_digests = {dep: digests[dep] for dep in task.deps}
                future = executor.submit(run_node, task, dep_outputs, dep_digests)
                futures[future] = name

            for name in reachable:
                if remaining[name] == 0:
                    submit(name)

            while futures:
                done, _pending = wait(tuple(futures), return_when=FIRST_COMPLETED)
                for future in done:
                    name = futures.pop(future)
                    scheduled.discard(name)
                    try:
                        output, digest_hex, status, local_events = future.result()
                    except BaseException as exc:  # noqa: BLE001 - recorded, not swallowed
                        failures.append(TaskFailure(
                            task=name,
                            error=exc,
                            traceback="".join(traceback.format_exception(
                                type(exc), exc, exc.__traceback__)),
                        ))
                        statuses[name] = TaskStatus.FAILED
                        # Block every transitive dependent; independent tasks
                        # already running are left to finish and cache.
                        block_stack = list(dependents.get(name, ()))
                        while block_stack:
                            child = block_stack.pop()
                            if child in outputs or child in skipped:
                                continue
                            if statuses.get(child) == TaskStatus.FAILED:
                                continue
                            if child not in skipped:
                                skipped[child] = name
                                statuses[child] = TaskStatus.SKIPPED
                                block_stack.extend(dependents.get(child, ()))
                        continue
                    cache_events.extend(local_events)
                    outputs[name] = output
                    digests[name] = digest_hex
                    statuses[name] = status
                    if status is TaskStatus.CACHED:
                        cache_hits.add(name)
                    for child in dependents.get(name, ()):
                        if child in outputs or child in skipped:
                            continue
                        remaining[child] -= 1
                        if remaining[child] == 0:
                            submit(child)
        finally:
            executor.shutdown(wait=True)

        report = BuildReport(
            targets=tuple(target_names),
            statuses=statuses,
            failures=tuple(failures),
            skipped=skipped,
            outputs=outputs,
            cache_hits=frozenset(cache_hits),
            cache_events=tuple(cache_events),
            elapsed=time.perf_counter() - started,
        )
        if failures:
            error = BuildFailed(report)
            # Preserve the original chain of (one of) the failing action(s).
            raise error from failures[0].error
        if return_report:
            return report
        return {name: outputs[name] for name in target_names}
