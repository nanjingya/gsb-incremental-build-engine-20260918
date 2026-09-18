# Incremental Build Engine

A small Python 3.11+ library for dependency-based artifact builds. Standard
library only, no external services. Features: resumable concurrent
incremental builds, a content-addressed on-disk cache shared across
processes, atomic cache publication, and structured build reports.

Run the tests: `python3 -m unittest discover -s tests -v`
Run the demo (offline, seconds): `python3 demo.py`

## API

```python
from buildengine import Engine, Task, BuildFailed, ValidationError

task = Task(
    name="compile/a",            # any unicode str; path separators are fine
    deps=("prepare",),           # tuple of task names
    action=lambda deps: b"...",  # deps: {dep_name: bytes}; must return bytes
    version="1",                 # bump when the action's code changes
)

engine = Engine(tasks, cache_dir, max_workers=None)  # default: min(32, cpu+4)
outputs = engine.build(["compile/a"])                # -> {target_name: bytes}
outputs, report = engine.build(["compile/a"], return_report=True)
outputs = engine.build(["compile/a"], force=True)    # ignore cache, still publish
```

- `build(targets, *, force=False, return_report=False)` — the default return
  value is still `{target_name: bytes}` (backwards compatible).
- Duplicate targets are built once; an empty target list validates the graph
  and returns `{}`; zero-length outputs (`b""`) are valid and cached.
- Actions returning non-`bytes` fail the task with a `TypeError`.

### Validation

Before any action runs, the engine rejects (with `ValidationError`, naming
the offending nodes): duplicate task names, dependencies on missing tasks,
unknown targets, and cycles **reachable from the requested targets**. Cycles
in unreachable parts of the graph do not block a build.

### Scheduling

One `build` call runs each shared dependency at most once. Ready independent
tasks run concurrently on a thread pool of `max_workers`; the scheduler never
occupies a worker waiting for unscheduled dependencies, so a single worker
handles arbitrarily deep chains and diamonds without deadlock. Concurrent
`build` calls on the same `Engine` are safe (all mutable state is per-call)
provided your actions are thread-safe.

### Failure semantics

If an action raises, its dependents are marked `skipped`, already-started
independent tasks finish and are cached, and `build` raises `BuildFailed`
with `.failures` (`{task_name: original_exception}` — traceback and
`__cause__` chain preserved) and `.report`. Failures are never cached, so
fixing the action and retrying just works; multiple concurrent failures are
all preserved.

### Build report

`BuildReport.records` maps each reachable task to a `TaskRecord` with status
`cached` / `executed` / `failed` / `skipped`; `.cached`, `.executed`,
`.failed`, `.skipped` give sorted name lists; `.warnings` lists cache
corruption diagnostics.

## Cache format

```
cache_dir/
  blobs/<2-hex>/<sha256(content)>   immutable content-addressed outputs
  tasks/<sha256(task name)>.json    {"format":1,"name","key","blob"}
```

- Task names are never used as paths (they are hashed), so any unicode name
  is safe.
- A task's `key` is a SHA-256 over a length-prefixed serialization of
  `(name, version, [(dep name, dep content hash), ...])`. Length prefixing
  makes key serialization unambiguous. Because keys depend on dependency
  *content*, rebuilding a dependency with identical output lets downstream
  tasks reuse their cache; changing a dependency's output invalidates them.
  Action code changes are your responsibility: bump `version`.
- Cache hits verify the blob's hash before use. Truncated/corrupt metadata,
  missing or corrupt blobs, and `.tmp-*` leftovers from crashed writers are
  detected, reported via `report.warnings`, and rebuilt — never treated as
  success.

## Concurrency & atomicity across processes

Multiple processes may share one `cache_dir`. All writes go to a `.tmp-*`
file in the same directory, are fsynced, then `os.replace`d into place, so
readers never see a half-written file. Blobs are immutable and
content-addressed; metadata files are replaced atomically. **No locks are
taken and cross-process deduplication is not guaranteed**: two processes may
compute the same task simultaneously, but the last atomic rename wins and
the cache is never corrupted. Stale `.tmp-*` files are swept when an
`Engine` is constructed.

## Design tradeoffs

- **Content over timestamps**: correctness does not depend on clocks or
  mtimes; identical outputs are stored once and shared by all tasks.
- **Explicit versioning of code**: the engine hashes task *inputs*, not your
  Python source. Bumping `version` is an explicit, reviewable invalidation.
- **No lock manager**: atomic rename gives crash safety and cross-process
  integrity without lock files, at the cost of occasionally duplicated
  computation.
- **Thread pool + central scheduler**: simple, deadlock-free by construction;
  actions should be CPU-light or release the GIL (subprocess, I/O, native
  code) for best parallelism.
