#!/usr/bin/env python3
"""Offline demo for the buildengine incremental build library.

Run:  python3 demo.py
No network access or third-party packages are required.

The demo walks through five scenarios with a small pipeline:

  config ---> compile ---> package
     |                       ^
     +---> test              |
  resources -----------------+

config and resources are independent roots and synchronize on a barrier to
demonstrate that the scheduler runs independent tasks at the same time.

  1. first build (everything executes)
  2. second identical build (everything is a cache hit)
  3. dependency version bump -> only changed chain re-executes
  4. failing action -> failure with diagnostics, downstream skipped
  5. retry after fixing the action -> recovered build

It deliberately forces real concurrency with a small barrier so you can see
independent tasks running in parallel, and uses a cache directory under
./.cache-demo (delete it to start over).
"""

import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

from buildengine import (
    BuildFailed,
    Engine,
    Task,
    TaskStatus,
)


def banner(title):
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def summarize(report, labels):
    for name in labels:
        status = report.statuses.get(name)
        marker = {
            TaskStatus.CACHED: "cache hit ",
            TaskStatus.EXECUTED: "executed  ",
            TaskStatus.FORCED: "forced    ",
            TaskStatus.FAILED: "FAILED    ",
            TaskStatus.SKIPPED: "skipped   ",
        }[status]
        extra = ""
        if name in report.skipped:
            extra = f" (blocked by {report.skipped[name]!r})"
        print(f"  [{marker}] {name}{extra}")
    if report.cache_events:
        for event in report.cache_events:
            print(f"  cache event: {event.kind}: {event.detail}")


def main():
    cache_dir = Path(__file__).resolve().parent / ".cache-demo"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True)

    pipeline_labels = ["config", "resources", "compile", "test",
                        "package"]

    # A barrier proves the two independent roots run at the same time.
    start_barrier = threading.Barrier(2)

    def make_engine(config_version="1", compile_version="1",
                    fail_compile=False, max_workers=4, use_barrier=True):
        def config(_, version=config_version, do_sync=use_barrier):
            if do_sync:
                start_barrier.wait(timeout=10)
            time.sleep(0.05)
            return b"<config-payload>"  # identical bytes regardless of version

        def resources(_, do_sync=use_barrier):
            if do_sync:
                start_barrier.wait(timeout=10)
            time.sleep(0.05)
            return b"<resources>"

        def compile_(deps, fail=fail_compile):
            time.sleep(0.05)
            if fail:
                raise RuntimeError("simulated compiler crash")
            return deps["config"] + b" <compiled>"

        def test(deps):
            time.sleep(0.05)
            return deps["config"] + b" <tested>"

        def package(deps):
            time.sleep(0.05)
            return deps["compile"] + deps["resources"] + b" <packaged>"

        return Engine(
            [
                Task("config", (), config, version=config_version),
                Task("resources", (), resources),
                Task("compile", ("config",), compile_, version=compile_version),
                Task("test", ("config",), test),
                Task("package", ("compile", "resources"), package),
            ],
            cache_dir,
            max_workers=max_workers,
        )

    # 1. First build ------------------------------------------------------
    banner("1. First build: independent tasks execute concurrently")
    t0 = time.perf_counter()
    report = make_engine().build(["package", "test"], return_report=True)
    dt = time.perf_counter() - t0
    summarize(report, pipeline_labels)
    print(f"  result: {report.outputs['package']!r}")
    print(f"  config output: {report.outputs['config']!r}")
    print(f"  elapsed: {dt:.2f}s (serial sleep budget would be ~0.20s)")
    assert report.executed == set(pipeline_labels)
    assert dt < 0.9, "independent work did not run concurrently"

    # 2. Second build -----------------------------------------------------
    banner("2. Second build: every task is a disk cache hit, no action runs")
    report = make_engine().build(["package", "test"], return_report=True)
    summarize(report, pipeline_labels)
    assert report.cache_hits == set(pipeline_labels)

    # 3. Dependency version change ---------------------------------------
    banner("3. Bump 'config' version with identical content")
    report = make_engine(config_version="2", use_barrier=False).build(
        ["package", "test"], return_report=True)
    summarize(report, pipeline_labels)
    print("  -> config re-executed (new version); identical bytes let the")
    print("     downstream tasks reuse their cached outputs.")
    assert report.statuses["config"] is TaskStatus.EXECUTED
    assert report.statuses["compile"] is TaskStatus.CACHED

    # 4. Failure ----------------------------------------------------------
    banner("4. Failing action: compile fails; package is skipped; test caches")
    try:
        make_engine(config_version="2", compile_version="broken-1",
                        fail_compile=True, use_barrier=False).build(
            ["package", "test"])
    except BuildFailed as exc:
        report = exc.report
        failure = report.failures[0]
        summarize(report, pipeline_labels)
        print(f"  raised: {type(exc).__name__}: {exc}")
        print(f"  original error chain preserved: "
              f"{type(failure.error).__name__}: {failure.error}")
        assert report.statuses["package"] is TaskStatus.SKIPPED
        assert report.statuses["test"] is TaskStatus.CACHED
        assert report.failures[0].task == "compile"
    else:
        raise AssertionError("expected BuildFailed")

    # 5. Recovery ---------------------------------------------------------
    banner("5. Retry with the action fixed: build succeeds, no poisoned cache")
    report = make_engine(config_version="2", compile_version="fixed-2",
                        fail_compile=False, use_barrier=False).build(
        ["package", "test"], return_report=True)
    summarize(report, pipeline_labels)
    print(f"  result: {report.outputs['package']!r}")
    assert report.ok
    assert report.outputs["package"] == (
        b"<config-payload> <compiled><resources> <packaged>")

    banner("All demo scenarios completed successfully.")
    print(f"Cache directory left at: {cache_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
