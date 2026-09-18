# buildengine — resumable concurrent incremental builds

A small, dependency-free Python 3.11+ library for dependency-based artifact
builds. It extends the original two-class library with a concurrent scheduler,
a content-addressed disk cache that survives across processes, recoverable
failures and structured build reports — without replacing the original public
interface.

* Standard library only (threads, `fcntl` advisory locks on POSIX).
* No external services or network access.

## API

```python
from buildengine import Engine, Task

@dataclass(frozen=True)
class Task:
    name: str                                   # any str: Unicode, "/", etc.
    deps: tuple[str, ...]
    action: Callable[[dict[str, bytes]], bytes]
    version: str = "1"                          # bump when action code changes

Engine(tasks, cache_dir, max_workers=None)

# Backward-compatible call; still returns {target_name: bytes}:
engine.build(targets)

# New options:
engine.build(targets, force=False, return_report=False)
```

`build()` accepts any iterable of target names. Duplicate target names are
built once and returned once; empty targets return `{}` (with a valid empty
report). Outputs must be `bytes`; zero-length `b""` is a perfectly valid,
cacheable output. A non-bytes return value is a task failure
(`InvalidActionOutputError`, also a `TypeError`), not a process crash.

`Engine(tasks, cache_dir)` and `Task(name, deps, action, version="1")` are
unchanged; `max_workers` and the two `build()` keyword arguments are optional
additions.

### Reports

`build(..., return_report=True)` returns a `BuildReport`; the default return
value remains the plain `{target: bytes}` dict.

| Field | Meaning |
| --- | --- |
| `statuses` | per reached task: `CACHED`, `EXECUTED`, `FORCED`, `FAILED`, `SKIPPED` |
| `cache_hits` | names reused from disk |
| `executed` | names whose action actually ran (`EXECUTED` + `FORCED`) |
| `failures` | `TaskFailure(task, error, traceback)` for every failed task |
| `skipped` | skipped task -> name of the failed ancestor responsible |
| `outputs` | all reached outputs, including independent successes after a failure |
| `cache_events` | corruption quarantines and stale-temp sweeps (diagnostics) |
| `targets`, `elapsed`, `ok` | requested targets, wall time, success flag |

`report.as_dict()` gives a JSON-serialisable view.

## Scheduler and concurrency

* The main thread runs a DAG event loop with a private
  `ThreadPoolExecutor(max_workers)` per build. A task is submitted only once
  all its dependencies are resolved, so a shared dependency in one build runs
  at most once (diamond DAG).
* The main thread never occupies a worker thread and never blocks a worker
  waiting for a dependency that has not been scheduled yet; it only waits in
  `concurrent.futures.wait(..., FIRST_COMPLETED)`. Therefore deep chains and
  diamond graphs cannot deadlock, even with `max_workers=1` (verified with a
  1,500-node chain test).
* The action receives exactly `{dep_name: output_bytes}` for its declared
  dependencies.
* Each `build()` call creates its own executor and in-memory state. Concurrent
  calls to `build()` on the **same** `Engine` instance are supported and
  isolated (no shared memo). The `Engine` itself is immutable; actions, as
  user code, must themselves be thread-safe if they mutate shared state.

## Cache format

Task names never touch the filesystem as paths: all cache paths are hex
digests, so Unicode names and path separators are harmless.

```
cache_dir/
  blobs/<sha256-hex>      immutable output bytes (content addressed)
  meta/<key>.json         metadata binding a build key to a blob
  locks/<key>.lock        advisory cross-process locks (never deleted)
```

**Build key.** `build_key(task, dep_digests)` is SHA-256 over an unambiguous
length-prefixed frame containing: the cache format tag
(`buildengine-cache-v1`), task `name`, explicit task `version`, the ordered
dependency list, and each dependency name with the SHA-256 hex digest of that
dependency's output bytes. Every string is encoded as
`<8-byte big-endian length><utf-8 bytes>` and structured sections carry their
own element counts, so distinct inputs (e.g. names `"ab"/"c"` vs `"a"/"bc"`)
can never collide — unlike raw separator joins or concatenated JSON strings.

Action source code is intentionally **not** hashed: action changes are managed
explicitly by bumping `Task.version`. Cache format changes are gated by the
embedded format version.

**Metadata** (`meta/<key>.json`, JSON with sorted keys):

```json
{"format": 1, "name": "...", "version": "...",
 "dependencies": {"dep": "<sha256-hex>"},
 "sha256": "<blob digest>", "size": 123}
```

A hit requires valid metadata with exactly the expected shape/version, a
payload matching the requested name/version/dependency digests, and an
existing blob whose length matches `size` and whose SHA-256 matches
`sha256`. Binary content is read and written as raw bytes and round-trips
byte-for-byte.

Because cache keys depend on **content** digests of dependencies, rebuilding a
dependency with a new `version` but identical output bytes lets all downstream
tasks reuse their existing cache entries. Reusing a cache directory with a new
`Engine` instance works out of the box.

### Atomic publication and corruption handling

* Results are written to uniquely named temp files (`.tmp-<pid>-<uuid>`) in
  the target directory, `fsync`ed, then moved into place with `os.replace`;
  the parent directory is fsynced afterwards. The blob is published first and
  the metadata — the only pointer readers follow — afterwards. Readers can
  therefore only ever see "absent" or "complete and verified", never a
  half-written file.
* At build start, temp files belonging to other PIDs (crashed processes) are
  swept and reported as a `temp_swept` cache event; temp files of the live
  process are left untouched, so concurrent builds cannot delete each other's
  in-flight writes.
* Truncated/invalid-JSON metadata, truncated or digest-mismatching blobs, and
  metadata referencing a missing blob are quarantined (deleted), recorded as
  `corrupt_metadata` / `corrupt_blob` / `missing_blob` events, and rebuilt. A
  damaged entry is never returned as a successful hit.

### Cross-process locking

During the critical section "recheck cache -> run action -> publish", a build
holds a per-key `fcntl.flock(LOCK_EX)` (POSIX; lock files live under
`locks/`) plus an in-process per-key lock. Consequences:

* **Cooperating processes using this library get cross-process
  deduplication**: two processes building the same task in the same
  `cache_dir` run that action exactly once; the loser of the lock race finds
  the published metadata on recheck and gets a cache hit. Two `Engine`
  instances in one process pointed at the same resolved `cache_dir` are
  deduplicated the same way.
* The guarantee applies only to processes cooperating via these locks (it is
  advisory). On platforms without `fcntl`, locking degrades to in-process
  only: duplicate computation may happen, but atomic content-addressed
  publication still prevents corruption — processes can never observe or
  overwrite each other's partial files.
* Locks are never removed while they might be held; stale lock files are
  harmless (the kernel releases flock on process exit).

## Failure semantics

* If an action raises (or returns non-bytes), the task is `FAILED`; every
  transitive dependent is marked `SKIPPED` (with the responsible failed
  ancestor recorded) and never runs.
* Independent tasks already running (or later becoming ready) are allowed to
  finish and their successes are cached. The raised `BuildFailed` carries the
  full `BuildReport` in `.report`; each failure keeps the original exception
  (also chained via `raise ... from`) and a formatted traceback string.
  Several tasks failing concurrently are all reported.
* Nothing about a failed build is written to the cache or into any state that
  outlives the call, so an immediate retry on the same `Engine` can succeed;
  in-memory build state is per-call and discarded afterwards.

## Validation

Before any action runs, `build()` checks in one pass and raises
`BuildGraphError` naming the concrete nodes (structured attributes
`duplicate_names`, `missing_dependencies`, `missing_targets`, `cycles`):

* duplicate task names;
* dependencies that do not exist (for every task reachable from a target);
* requested targets that do not exist;
* cycles found by 3-color DFS in the **target-reachable** subgraph.

A cycle that is not reachable from any requested target does not block an
otherwise legal build. `Engine.validate()` with no arguments checks the whole
graph instead. When validation fails, no action executes.

## Demo

```bash
python3 demo.py
```

Offline, no arguments; runs in well under a second (well inside the 90-second
budget) and shows: a first concurrent build, a full cache-hit rebuild, a
dependency `version` change with downstream reuse, a failing action with
skip / independent-success diagnostics, and a successful retry.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The suite covers cache hits/invalidation/key ambiguity, binary round-trips,
diamond and 1,500-deep chains with one worker, the `max_workers` cap and real
parallelism (using `threading.Barrier`/`Event` rendezvous rather than sleeps),
failure propagation and retry isolation, cycle and graph diagnostics,
truncated/missing cache recovery, stale-temp sweeps, atomic publication, and
cross-process deduplication/atomicity using real `subprocess` children.
