"""Offline demo of the incremental build engine (runs in seconds).

Shows: first build -> cache hits -> version-driven rebuild with downstream
cache reuse -> failure with skipped dependents -> recovery.
"""

import tempfile

from buildengine import BuildFailed, Engine, Task

state = {"flaky_ok": False}


def source(_deps):
    print("  [run] source")
    return b"hello"


def left(deps):
    print("  [run] left")
    return deps["source"] + b" L"


def right(deps):
    print("  [run] right")
    return deps["source"] + b" R"


def join(deps):
    print("  [run] join")
    return deps["left"] + b" | " + deps["right"]


def flaky(_deps):
    print("  [run] flaky")
    if not state["flaky_ok"]:
        raise RuntimeError("flaky step exploded")
    return b"recovered"


def after_flaky(deps):
    print("  [run] after_flaky")
    return deps["flaky"] + b"!"


def show(title, report):
    print(f"{title}")
    print(f"  executed: {report.executed}")
    print(f"  cached:   {report.cached}")
    print(f"  failed:   {report.failed}")
    print(f"  skipped:  {report.skipped}")


def graph(source_version="1", include_flaky=False):
    tasks = [
        Task("source", (), source, version=source_version),
        Task("left", ("source",), left),
        Task("right", ("source",), right),
        Task("join", ("left", "right"), join),
    ]
    if include_flaky:
        tasks += [
            Task("flaky", (), flaky),
            Task("after_flaky", ("flaky",), after_flaky),
        ]
    return tasks


def main():
    with tempfile.TemporaryDirectory() as cache:
        print("== 1. first build (everything executes) ==")
        engine = Engine(graph(), cache, max_workers=4)
        _, report = engine.build(["join"], return_report=True)
        show("", report)

        print("== 2. second build, new Engine, same cache_dir (all cache hits) ==")
        _, report = Engine(graph(), cache).build(["join"], return_report=True)
        show("", report)

        print("== 3. source version bump: source reruns, same content -> downstream reused ==")
        _, report = Engine(graph(source_version="2"), cache).build(["join"], return_report=True)
        show("", report)

        print("== 4. a failing task: dependents skipped, error is diagnosable ==")
        engine = Engine(graph(source_version="2", include_flaky=True), cache)
        try:
            engine.build(["join", "after_flaky"], return_report=True)
        except BuildFailed as exc:
            show("", exc.report)
            print(f"  failure: {type(exc.failures['flaky']).__name__}: {exc.failures['flaky']}")

        print("== 5. fix the flaky task and retry: failure did not poison the cache ==")
        state["flaky_ok"] = True
        result, report = engine.build(["join", "after_flaky"], return_report=True)
        show("", report)
        print(f"  outputs: {result}")


if __name__ == "__main__":
    main()
