import json
import uuid
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from buildengine import (
    BuildFailed,
    BuildGraphError,
    BuildReport,
    Engine,
    InvalidActionOutputError,
    Task,
    TaskStatus,
    build_key,
)

HERE = Path(__file__).resolve().parent
CHILD = HERE / "_proc_child.py"
WAIT = 5.0


def leaf(name, value=b"x", version="1"):
    return Task(name, (), lambda _: value, version)


def passthrough(name, dep, suffix=b"", version="1"):
    return Task(name, (dep,), lambda d: d[dep] + suffix, version)


class Event:
    """Deterministic rendezvous helper wrapping threading.Event with timeouts."""

    def __init__(self, testcase):
        self._ev = threading.Event()
        self._tc = testcase

    def set(self):
        self._ev.set()

    def wait(self, timeout=WAIT):
        self._tc.assertTrue(
            self._ev.wait(timeout), f"timed out waiting for rendezvous event")


class BasicBuildTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def diamond(self, calls=None, value=b"x"):
        calls = calls if calls is not None else []

        def source(_):
            calls.append("source")
            return value

        return [
            Task("source", (), source),
            Task("left", ("source",), lambda d: d["source"] + b"l"),
            Task("right", ("source",), lambda d: d["source"] + b"r"),
            Task("join", ("left", "right"),
                 lambda d: d["left"] + d["right"]),
        ], calls

    def test_default_return_is_target_dict(self):
        tasks, _ = self.diamond()
        result = Engine(tasks, self.folder).build(["join"])
        self.assertEqual(result, {"join": b"xlxr"})
        self.assertIsInstance(result, dict)

    def test_empty_targets(self):
        tasks, _ = self.diamond()
        engine = Engine(tasks, self.folder)
        self.assertEqual(engine.build([]), {})
        report = engine.build((), return_report=True)
        self.assertTrue(report.ok)
        self.assertEqual(report.targets, ())

    def test_duplicate_targets_built_once(self):
        tasks, calls = self.diamond()
        result = Engine(tasks, self.folder).build(["join", "join", "left"])
        self.assertEqual(result, {"join": b"xlxr", "left": b"xl"})
        self.assertEqual(calls, ["source"])

    def test_zero_length_output_is_valid(self):
        def empty(_):
            return b""

        engine = Engine([Task("e", (), empty)], self.folder)
        self.assertEqual(engine.build(["e"]), {"e": b""})
        report = Engine([Task("e", (), empty)], self.folder).build(
            ["e"], return_report=True)
        self.assertEqual(report.statuses["e"], TaskStatus.CACHED)
        self.assertEqual(report.outputs["e"], b"")

    def test_non_bytes_return_is_failure(self):
        engine = Engine([Task("bad", (), lambda _: "nope")], self.folder)
        with self.assertRaises(BuildFailed) as caught:
            engine.build(["bad"])
        report = caught.exception.report
        failure = report.failures[0]
        self.assertIsInstance(failure.error, InvalidActionOutputError)
        self.assertEqual(failure.task, "bad")
        self.assertEqual(report.statuses["bad"], TaskStatus.FAILED)

    def test_unicode_and_path_separator_names(self):
        weird = "数据/构建/任务 α/β"
        tasks = [
            Task(weird, (), lambda _: b"v"),
            Task("next", (weird,), lambda d: d[weird] + b"!"),
        ]
        engine = Engine(tasks, self.folder)
        self.assertEqual(engine.build(["next"]), {"next": b"v!"})
        for entry in Path(self.folder, "meta").iterdir():
            self.assertTrue(entry.name.endswith(".json"))
            self.assertNotIn("数据", entry.name)
            self.assertNotIn("/", entry.name)

    def test_report_statuses_diamond(self):
        tasks, _ = self.diamond()
        engine = Engine(tasks, self.folder, max_workers=4)
        report = engine.build(["join"], return_report=True)
        self.assertEqual(set(report.statuses),
                         {"source", "left", "right", "join"})
        self.assertEqual(report.executed,
                         {"source", "left", "right", "join"})
        self.assertEqual(report.cache_hits, frozenset())

    def test_action_receives_dep_name_to_bytes_map(self):
        seen = {}

        def check(d):
            seen.update(d)
            return b"ok"

        tasks = [leaf("a", b"A"), leaf("b", b"B"),
                 Task("c", ("a", "b"), check)]
        Engine(tasks, self.folder).build(["c"])
        self.assertEqual(seen, {"a": b"A", "b": b"B"})


class CacheTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_second_build_is_cache_hit_and_action_not_rerun(self):
        calls = []

        def source(_):
            calls.append("source")
            return b"x"

        tasks = [
            Task("source", (), source),
            Task("up", ("source",), lambda d: d["source"] + b"u"),
            Task("down", ("up",), lambda d: d["up"] + b"d"),
        ]
        first = Engine(tasks, self.folder, max_workers=2)
        report1 = first.build(["down"], return_report=True)
        self.assertEqual(report1.executed, {"source", "up", "down"})

        report2 = Engine(tasks, self.folder).build(["down"], return_report=True)
        self.assertEqual(report2.cache_hits, {"source", "up", "down"})
        self.assertEqual(calls, ["source"])
        self.assertEqual(report2.outputs["down"], b"xud")

    def test_binary_output_roundtrip(self):
        payload = bytes(range(256)) * 17
        tasks = [
            Task("bin", (), lambda _: payload),
            Task("bin2", ("bin",), lambda d: d["bin"] + b"\x00\xff"),
        ]
        engine = Engine(tasks, self.folder)
        engine.build(["bin2"])
        result = Engine(tasks, self.folder).build(["bin2"])
        self.assertEqual(result, {"bin2": payload + b"\x00\xff"})

    def test_version_bump_rebuilds_task_same_content_lets_downstream_reuse(self):
        runs = []
        tasks = [
            Task("a", (), lambda _: (runs.append("a"), b"A")[1], version="1"),
            Task("b", ("a",), lambda d: (runs.append("b"), d["a"] + b"B")[1]),
            Task("c", (), lambda _: (runs.append("c"), b"C")[1]),
        ]
        Engine(tasks, self.folder).build(["b", "c"])
        runs.clear()
        bumped = [
            Task("a", (), lambda _: (runs.append("a2"), b"A")[1], version="2"),
            tasks[1], tasks[2],
        ]
        report = Engine(bumped, self.folder).build(["b", "c"],
                                                   return_report=True)
        self.assertEqual(report.executed, {"a"})
        self.assertEqual(report.cache_hits, {"b", "c"})
        self.assertEqual(runs, ["a2"])

    def test_version_bump_changed_content_rebuilds_downstream(self):
        tasks = [
            Task("a", (), lambda _: b"A1", version="1"),
            Task("b", ("a",), lambda d: d["a"] + b"B"),
        ]
        Engine(tasks, self.folder).build(["b"])
        bumped = [
            Task("a", (), lambda _: b"A2", version="2"),
            tasks[1],
        ]
        report = Engine(bumped, self.folder).build(["b"], return_report=True)
        self.assertEqual(set(report.executed), {"a", "b"})
        self.assertEqual(report.outputs["b"], b"A2B")

    def test_dependency_content_change_invalidates_key(self):
        tasks_v1 = [leaf("src", b"one", version="1"),
                    passthrough("out", "src", b"-z")]
        Engine(tasks_v1, self.folder).build(["out"])
        tasks_v2 = [leaf("src", b"two", version="2"),
                    passthrough("out", "src", b"-z")]
        report = Engine(tasks_v2, self.folder).build(
            ["out"], return_report=True)
        self.assertEqual(set(report.executed), {"src", "out"})
        self.assertEqual(report.outputs["out"], b"two-z")

    def test_same_content_dependency_rebuild_lets_downstream_reuse_cache(self):
        # Task "src" gets a new version (so its own cache entry changes shape)
        # but produces identical bytes; "mid"/"top" must still hit cache.
        tasks_v1 = [leaf("src", b"same", version="1"),
                    passthrough("mid", "src", b"m"),
                    passthrough("top", "mid", b"t")]
        Engine(tasks_v1, self.folder).build(["top"])
        runs = []
        tasks_v2 = [
            Task("src", (), lambda _: (runs.append("src"), b"same")[1],
                 version="2"),
            Task("mid", ("src",),
                 lambda d: (runs.append("mid"), d["src"] + b"m")[1]),
            Task("top", ("mid",),
                 lambda d: (runs.append("top"), d["mid"] + b"t")[1]),
        ]
        report = Engine(tasks_v2, self.folder).build(["top"],
                                                     return_report=True)
        self.assertEqual(report.statuses["src"], TaskStatus.EXECUTED)
        self.assertEqual(report.statuses["mid"], TaskStatus.CACHED)
        self.assertEqual(report.statuses["top"], TaskStatus.CACHED)
        self.assertEqual(runs, ["src"])

    def test_key_encoding_is_unambiguous(self):
        t1 = Task("ab", ("c",), lambda d: b"", version="1")
        t2 = Task("a", ("bc",), lambda d: b"", version="1")
        k1 = build_key(t1, {"c": "00"})
        k2 = build_key(t2, {"bc": "00"})
        self.assertNotEqual(k1, k2)
        # same inputs => same key
        self.assertEqual(build_key(t1, {"c": "00"}),
                         build_key(Task("ab", ("c",), None, "1"), {"c": "00"}))
        # version participates
        self.assertNotEqual(
            build_key(Task("a", (), None, "1"), {}),
            build_key(Task("a", (), None, "2"), {}))
        # dep digest participates
        self.assertNotEqual(
            build_key(Task("a", ("d",), None, "1"), {"d": "00"}),
            build_key(Task("a", ("d",), None, "1"), {"d": "01"}))

    def test_cache_reused_across_engine_instances_and_dirs_persist(self):
        Engine([leaf("a", b"hello")], self.folder).build(["a"])
        alt = Path(self.folder)
        self.assertTrue((alt / "meta").is_dir())
        self.assertTrue((alt / "blobs").is_dir())
        result = Engine([leaf("a", b"hello")], self.folder).build(["a"])
        self.assertEqual(result, {"a": b"hello"})

    def test_force_reexecutes_reachable_and_republishes(self):
        runs = []
        tasks = [
            Task("a", (), lambda _: (runs.append("a"), b"A")[1]),
            Task("b", ("a",), lambda d: (runs.append("b"), d["a"] + b"B")[1]),
            Task("c", (), lambda _: (runs.append("c"), b"C")[1]),
        ]
        Engine(tasks, self.folder).build(["b", "c"])
        runs.clear()
        report = Engine(tasks, self.folder).build(
            ["b"], force=True, return_report=True)
        self.assertEqual(set(report.statuses), {"a", "b"})
        self.assertTrue(
            all(s is TaskStatus.FORCED for s in report.statuses.values()))
        self.assertEqual(sorted(runs), ["a", "b"])
        # Afterwards a normal build on the republished entries hits cache.
        report2 = Engine(tasks, self.folder).build(["b"], return_report=True)
        self.assertEqual(report2.cache_hits, {"a", "b"})




    def test_force_then_failure_retry_recovers(self):
        state = {"fail": True}

        def flaky(_):
            if state["fail"]:
                state["fail"] = False
                raise RuntimeError("once")
            return b"ok-now"

        tasks = [
            Task("f", (), flaky),
            passthrough("g", "f", b"g"),
            leaf("standalone", b"s"),
        ]
        engine = Engine(tasks, self.folder, max_workers=2)
        with self.assertRaises(BuildFailed):
            engine.build(["g", "standalone"])
        report = engine.build(["g", "standalone"], force=True,
                              return_report=True)
        self.assertTrue(report.ok)
        self.assertEqual(report.statuses["f"], TaskStatus.FORCED)
        self.assertEqual(report.statuses["g"], TaskStatus.FORCED)
        self.assertEqual(report.statuses["standalone"], TaskStatus.FORCED)
        self.assertEqual(report.outputs["g"], b"ok-nowg")


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_duplicate_task_name_diagnosed(self):
        tasks = [leaf("dup", b"1"), leaf("dup", b"2"), leaf("ok")]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).build(["ok"])
        self.assertEqual(caught.exception.duplicate_names, ["dup"])
        self.assertIn("'dup'", str(caught.exception))

    def test_missing_target_diagnosed(self):
        with self.assertRaises(BuildGraphError) as caught:
            Engine([leaf("a")], self.folder).build(["ghost"])
        self.assertEqual(caught.exception.missing_targets, ["ghost"])

    def test_missing_dependency_names_both_nodes(self):
        tasks = [Task("a", ("missing",), lambda d: b"")]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).build(["a"])
        self.assertEqual(caught.exception.missing_dependencies,
                         {"a": ["missing"]})
        message = str(caught.exception)
        self.assertIn("'a'", message)
        self.assertIn("'missing'", message)

    def test_reachable_cycle_diagnosed_with_nodes(self):
        tasks = [
            Task("a", ("b",), lambda d: b""),
            Task("b", ("c",), lambda d: b""),
            Task("c", ("a",), lambda d: b""),
        ]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).build(["a"])
        cycles = caught.exception.cycles
        self.assertEqual(len(cycles), 1)
        self.assertEqual(set(cycles[0]), {"a", "b", "c"})
        self.assertIn("cycle", str(caught.exception))

    def test_unreachable_cycle_does_not_block_legal_target(self):
        ran = threading.Event()
        tasks = [
            Task("good", (), lambda _: (ran.set(), b"g")[1]),
            Task("x", ("y",), lambda d: b""),
            Task("y", ("x",), lambda d: b""),
        ]
        result = Engine(tasks, self.folder).build(["good"])
        self.assertEqual(result, {"good": b"g"})
        self.assertTrue(ran.is_set())

    def test_validate_whole_graph_detects_unreachable_cycle(self):
        tasks = [
            Task("good", (), lambda _: b"g"),
            Task("x", ("y",), lambda d: b""),
            Task("y", ("x",), lambda d: b""),
        ]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).validate()
        self.assertEqual(len(caught.exception.cycles), 1)

    def test_self_dependency_is_a_cycle(self):
        tasks = [Task("a", ("a",), lambda d: b"")]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).build(["a"])
        self.assertEqual(caught.exception.cycles[0], ["a", "a"])

    def test_multiple_problems_reported_together(self):
        tasks = [
            leaf("dup"), leaf("dup"),
            Task("a", ("ghost",), lambda d: b""),
        ]
        with self.assertRaises(BuildGraphError) as caught:
            Engine(tasks, self.folder).build(["a", "nope"])
        err = caught.exception
        self.assertEqual(err.duplicate_names, ["dup"])
        self.assertEqual(err.missing_targets, ["nope"])
        self.assertIn("ghost", err.missing_dependencies.get("a", []))
        self.assertGreaterEqual(len(err.problems), 3)

    def test_no_action_runs_when_validation_fails(self):
        ran = threading.Event()
        tasks = [
            Task("ok", (), lambda _: (ran.set(), b"")[1]),
            Task("cy1", ("cy2",), lambda d: b""),
            Task("cy2", ("cy1",), lambda d: b""),
        ]
        with self.assertRaises(BuildGraphError):
            Engine(tasks, self.folder).build(["ok", "cy1"])
        self.assertFalse(ran.is_set())

    def test_invalid_max_workers(self):
        with self.assertRaises(ValueError):
            Engine([leaf("a")], self.folder, max_workers=0).build(["a"])


class FailureTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_failure_blocks_successors_and_skips_are_diagnosed(self):
        boom = ValueError("boom")

        def fail(_):
            raise boom

        tasks = [
            leaf("root", b"r"),
            Task("bad", ("root",), fail),
            passthrough("child", "bad"),
            passthrough("grand", "child"),
        ]
        with self.assertRaises(BuildFailed) as caught:
            Engine(tasks, self.folder, max_workers=4).build(["grand"])
        report = caught.exception.report
        self.assertEqual(report.failures[0].task, "bad")
        self.assertIs(report.failures[0].error, boom)
        self.assertEqual(set(report.skipped), {"child", "grand"})
        self.assertEqual(report.statuses["child"], TaskStatus.SKIPPED)
        self.assertEqual(report.statuses["grand"], TaskStatus.SKIPPED)
        self.assertEqual(report.statuses["root"], TaskStatus.EXECUTED)
        self.assertIn("Traceback", report.failures[0].traceback)
        self.assertIn("boom", str(caught.exception))

    def test_independent_task_completes_and_caches_after_failure(self):
        failure_started = threading.Event()
        release = threading.Event()
        independent_done = threading.Event()

        def slow_fail(_):
            failure_started.set()
            self.assertTrue(release.wait(WAIT))
            raise RuntimeError("nope")

        def independent(_):
            independent_done.set()
            return b"fine"

        tasks = [
            Task("slowfail", (), slow_fail),
            passthrough("down", "slowfail"),
            Task("ind", (), independent),
        ]
        engine = Engine(tasks, self.folder, max_workers=2)
        with self.assertRaises(BuildFailed):
            engine.build(["down", "ind"])
        self.assertTrue(independent_done.is_set())
        # independent success is persisted: second build of it is a cache hit
        report = Engine([Task("ind", (), lambda _: b"fine")], self.folder) \
            .build(["ind"], return_report=True)
        self.assertEqual(report.statuses["ind"], TaskStatus.CACHED)

    def test_failure_does_not_pollute_retry(self):
        should_fail = {"on": True}

        def flaky(_):
            if should_fail["on"]:
                should_fail["on"] = False
                raise RuntimeError("transient")
            return b"recovered"

        tasks = [
            Task("flaky", (), flaky),
            passthrough("use", "flaky", b"!"),
        ]
        engine = Engine(tasks, self.folder, max_workers=2)
        with self.assertRaises(BuildFailed) as caught:
            engine.build(["use"])
        self.assertEqual(caught.exception.report.statuses["use"],
                         TaskStatus.SKIPPED)
        # Retry with the same Engine instance succeeds (no memoized failure).
        result = engine.build(["use"])
        self.assertEqual(result, {"use": b"recovered!"})

    def test_multiple_concurrent_failures_all_preserved(self):
        class Err1(Exception):
            pass

        class Err2(Exception):
            pass

        tasks = [
            Task("f1", (), lambda _: (_ for _ in ()).throw(Err1("e1"))),
            Task("f2", (), lambda _: (_ for _ in ()).throw(Err2("e2"))),
        ]
        with self.assertRaises(BuildFailed) as caught:
            Engine(tasks, self.folder, max_workers=2).build(["f1", "f2"])
        report = caught.exception.report
        by_name = {failure.task: failure for failure in report.failures}
        self.assertEqual(set(by_name), {"f1", "f2"})
        self.assertIsInstance(by_name["f1"].error, Err1)
        self.assertIsInstance(by_name["f2"].error, Err2)
        self.assertIn("f1", str(caught.exception))
        self.assertIn("f2", str(caught.exception))

    def test_diamond_with_one_failing_branch(self):
        ok_executed = threading.Event()

        def ok_action(d):
            ok_executed.set()
            return d["src"] + b"ok"

        tasks = [
            leaf("src", b"s"),
            Task("ok", ("src",), ok_action),
            Task("bad", ("src",),
                 lambda d: (_ for _ in ()).throw(RuntimeError("x"))),
            Task("join", ("ok", "bad"), lambda d: d["ok"] + d["bad"]),
        ]
        engine = Engine(tasks, self.folder, max_workers=4)
        with self.assertRaises(BuildFailed) as caught:
            engine.build(["join"])
        report = caught.exception.report
        self.assertTrue(ok_executed.wait(WAIT))
        self.assertEqual(report.statuses["ok"], TaskStatus.EXECUTED)
        self.assertEqual(report.statuses["bad"], TaskStatus.FAILED)
        self.assertEqual(report.statuses["join"], TaskStatus.SKIPPED)


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_real_parallelism_with_deterministic_barrier(self):
        n = 4
        inside = threading.Barrier(n)
        entered = []
        entered_lock = threading.Lock()

        def make_action(index):
            def action(_):
                with entered_lock:
                    entered.append(index)
                # Barrier raises BrokenBarrierError unless all n tasks run
                # truly concurrently (it waits for n parties at once).
                inside.wait(timeout=WAIT)
                return str(index).encode()
            return action

        tasks = [Task(f"t{i}", (), make_action(i)) for i in range(n)]
        report = Engine(tasks, self.folder, max_workers=n).build(
            [f"t{i}" for i in range(n)], return_report=True)
        self.assertEqual(sorted(entered), list(range(n)))
        self.assertTrue(report.ok)

    def test_max_workers_is_a_hard_cap(self):
        n = 6
        cap = 2
        active = 0
        peak = 0
        state_lock = threading.Lock()
        release = threading.Event()
        entered = threading.Barrier(cap)

        def make_action(index):
            def action(_):
                nonlocal active, peak
                with state_lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    entered.wait(timeout=WAIT)
                    self.assertTrue(release.wait(WAIT))
                finally:
                    with state_lock:
                        active -= 1
                return str(index).encode()
            return action

        tasks = [Task(f"t{i}", (), make_action(i)) for i in range(n)]

        def run():
            Engine(tasks, self.folder, max_workers=cap).build(
                [f"t{i}" for i in range(n)])

        worker = threading.Thread(target=run)
        worker.start()
        # First cap workers arrive; a (cap+1)-th cannot, proving the cap.
        self.assertTrue(worker.is_alive())
        release.set()
        worker.join(WAIT * 2)
        self.assertFalse(worker.is_alive(), "build deadlocked under cap")
        self.assertLessEqual(peak, cap)
        self.assertEqual(peak, cap)

    def test_deep_chain_single_worker_no_deadlock(self):
        depth = 1500
        tasks = [Task("n0", (), lambda _: b"0")]
        for i in range(1, depth):
            tasks.append(Task(
                f"n{i}", (f"n{i-1}",),
                lambda d, i=i: d[f"n{i-1}"] + b"x"))
        engine = Engine(tasks, self.folder, max_workers=1)
        result = engine.build([f"n{depth-1}"])
        self.assertEqual(len(result[f"n{depth-1}"]), depth)

    def test_diamond_single_worker_no_deadlock_and_shared_dep_once(self):
        runs = []
        tasks = [
            Task("a", (), lambda _: (runs.append("a"), b"A")[1]),
            Task("b", ("a",), lambda d: d["a"] + b"B"),
            Task("c", ("a",), lambda d: d["a"] + b"C"),
            Task("d", ("b", "c"), lambda d: d["b"] + d["c"]),
            Task("e", ("b", "c"), lambda d: d["b"] + d["c"] + b"E"),
        ]
        result = Engine(tasks, self.folder, max_workers=1).build(["d", "e"])
        self.assertEqual(result, {"d": b"ABAC", "e": b"ABACE"})
        self.assertEqual(runs, ["a"])

    def test_shared_dependency_runs_once_in_one_build(self):
        runs = []
        lock = threading.Lock()

        def action_a(_):
            with lock:
                runs.append("a")
            return b"A"

        leaves = [Task("a", (), action_a)]
        joins = []
        for i in range(20):
            leaves.append(Task(f"m{i}", ("a",), lambda d: d["a"] + b"m"))
            joins.append(Task(f"j{i}", (f"m{i}",),
                              lambda d: next(iter(d.values())) + b"j"))
        targets = [f"j{i}" for i in range(20)]
        Engine(leaves + joins, self.folder, max_workers=8).build(targets)
        self.assertEqual(runs, ["a"])

    def test_concurrent_builds_on_same_engine_are_isolated(self):
        errors = []

        def make_chain(prefix, start_gate=None):
            tasks = []
            tasks.append(Task(f"{prefix}-s", (),
                              lambda _: (start_gate and start_gate.set(),
                                         prefix.encode())[1]))
            tasks.append(passthrough(f"{prefix}-m", f"{prefix}-s", b"m"))
            tasks.append(passthrough(f"{prefix}-t", f"{prefix}-m", b"t"))
            return tasks

        gate1 = threading.Event()
        tasks = make_chain("p1", gate1) + make_chain("p2")
        engine = Engine(tasks, self.folder, max_workers=4)
        results = {}

        def b1():
            try:
                results["p1"] = engine.build(["p1-t"])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def b2():
            try:
                # p2 does not depend on p1: must not wait on build #1
                self.assertTrue(gate1.wait(WAIT))
                results["p2"] = engine.build(["p2-t"])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=b1)
        t2 = threading.Thread(target=b2)
        t1.start()
        self.assertTrue(gate1.wait(WAIT))
        t2.start()
        t2.join(WAIT * 2)
        t1.join(WAIT * 2)
        self.assertFalse(errors, errors)
        self.assertEqual(results, {"p1": {"p1-t": b"p1mt"},
                                   "p2": {"p2-t": b"p2mt"}})



    def test_repeated_same_key_locking_stress(self):
        # Many builds reuse the same leaf key concurrently; the in-process
        # per-key lock registry must not lose its entry under churn.
        results = []
        result_lock = threading.Lock()

        def run_once(i):
            try:
                folder = self.folder
                task = Task("shared", (), lambda _: b"v")
                out = Engine([task], folder, max_workers=2).build(["shared"])
                with result_lock:
                    results.append(out)
            except BaseException as exc:  # noqa: BLE001
                with result_lock:
                    results.append(exc)

        threads = [threading.Thread(target=run_once, args=(i,))
                   for i in range(24)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(WAIT * 2)
        self.assertEqual(len(results), 24)
        self.assertTrue(all(r == {"shared": b"v"} for r in results), results)

    def test_failure_then_independent_build_same_engine(self):
        tasks = [
            Task("bad", (), lambda _: (_ for _ in ()).throw(RuntimeError("x"))),
            leaf("good", b"g"),
        ]
        engine = Engine(tasks, self.folder, max_workers=2)
        with self.assertRaises(BuildFailed):
            engine.build(["bad"])
        self.assertEqual(engine.build(["good"]), {"good": b"g"})


class CacheIntegrityTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _build_once(self, name="a", data=b"hello"):
        Engine([Task(name, (), lambda _: data)], self.folder).build([name])

    def _meta_files(self):
        return list(Path(self.folder, "meta").glob("*.json"))

    def test_truncated_metadata_rebuilds_and_diagnoses(self):
        self._build_once()
        meta = self._meta_files()[0]
        raw = meta.read_bytes()
        meta.write_bytes(raw[: len(raw) // 2])
        report = Engine([Task("a", (), lambda _: b"hello")], self.folder) \
            .build(["a"], return_report=True)
        self.assertEqual(report.outputs["a"], b"hello")
        self.assertEqual(report.statuses["a"], TaskStatus.EXECUTED)
        kinds = [event.kind for event in report.cache_events]
        self.assertIn("corrupt_metadata", kinds)

    def test_truncated_blob_rebuilds_and_diagnoses(self):
        self._build_once(data=b"0123456789abcdef")
        metas = self._meta_files()
        doc = json.loads(metas[0].read_text())
        blob = Path(self.folder, "blobs", doc["sha256"])
        blob.write_bytes(blob.read_bytes()[:5])
        report = Engine([Task("a", (), lambda _: b"0123456789abcdef")],
                        self.folder).build(["a"], return_report=True)
        self.assertEqual(report.outputs["a"], b"0123456789abcdef")
        self.assertEqual(report.statuses["a"], TaskStatus.EXECUTED)
        self.assertTrue(
            any(event.kind in ("corrupt_blob",) for event in report.cache_events))

    def test_missing_blob_rebuilds_and_diagnoses(self):
        self._build_once(data=b"payload")
        doc = json.loads(self._meta_files()[0].read_text())
        Path(self.folder, "blobs", doc["sha256"]).unlink()
        report = Engine([Task("a", (), lambda _: b"payload")], self.folder) \
            .build(["a"], return_report=True)
        self.assertEqual(report.statuses["a"], TaskStatus.EXECUTED)
        self.assertTrue(
            any(e.kind == "missing_blob" for e in report.cache_events))

    def test_corrupt_cache_never_reported_as_hit(self):
        self._build_once(data=b"abc")
        meta = self._meta_files()[0]
        meta.write_bytes(b"{not json")
        report = Engine([Task("a", (), lambda _: b"abc")], self.folder) \
            .build(["a"], return_report=True)
        self.assertNotIn("a", report.cache_hits)

    def test_stale_temp_files_swept_and_diagnosed(self):
        self._build_once()
        meta_dir = Path(self.folder, "meta")
        blob_dir = Path(self.folder, "blobs")
        (meta_dir / ".tmp-dead-999-deadbeef").write_bytes(b"x")
        (blob_dir / ".tmp-dead-999-cafef00d").write_bytes(b"y")
        report = Engine([Task("a", (), lambda _: b"hello")], self.folder) \
            .build(["a"], return_report=True)
        sweeps = [e for e in report.cache_events if e.kind == "temp_swept"]
        self.assertEqual(len(sweeps), 1)
        self.assertFalse(list(meta_dir.glob(".tmp-*")))
        self.assertFalse(list(blob_dir.glob(".tmp-*")))

    def test_atomic_publish_leaves_no_temp_files(self):
        payload = bytes(range(256)) * 4
        Engine([Task("a", (), lambda _: payload)], self.folder).build(["a"])
        for directory in ("meta", "blobs"):
            leftovers = [p for p in Path(self.folder, directory).iterdir()
                         if p.name.startswith(".")]
            self.assertEqual(leftovers, [])


class CrossProcessTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.folder = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _run_child(self, config):
        config_path = Path(self.folder, f"cfg-{uuid.uuid4().hex}.json")
        config_path.write_text(json.dumps(config))
        proc = subprocess.run(
            [sys.executable, str(CHILD), str(config_path)],
            cwd=HERE, capture_output=True, timeout=60)
        if proc.returncode != 0:
            self.fail(f"child failed: {proc.stderr.decode()}")
        return json.loads(proc.stdout.decode())

    def _config(self, count_file, value_hex="aabbcc"):
        return {
            "cache_dir": self.folder,
            "max_workers": 4,
            "targets": ["down"],
            "tasks": [
                {"kind": "leaf", "name": "src", "bytes": value_hex,
                 "count_file": count_file},
                {"kind": "passthrough", "name": "mid", "deps": ["src"],
                 "suffix": "6d"},
                {"kind": "passthrough", "name": "down", "deps": ["mid"],
                 "suffix": "64"},
            ],
        }

    def test_concurrent_processes_deduplicate_and_get_same_result(self):
        count_file = Path(self.folder, "src.count")
        config = self._config(str(count_file))
        configs = [Path(self.folder, f"cfg{i}.json") for i in range(2)]
        for path in configs:
            path.write_text(json.dumps(config))
        procs = [
            subprocess.Popen([sys.executable, str(CHILD), str(path)],
                             cwd=HERE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
            for path in configs
        ]
        outs = []
        for proc in procs:
            stdout, stderr = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, stderr.decode())
            outs.append(json.loads(stdout.decode()))
        value = bytes.fromhex("aabbcc")
        for out in outs:
            self.assertEqual(bytes.fromhex(out["outputs"]["down"]),
                             value + b"m" + b"d")
        self.assertEqual(count_file.read_bytes(), b"x",
                         "leaf action must run exactly once across processes")
        self.assertTrue(any(len(out["cache_hits"]) >= 2 for out in outs))

    def test_second_process_full_cache_hit(self):
        count_file = str(Path(self.folder, "src.count"))
        config = self._config(count_file)
        first = self._run_child(config)
        self.assertEqual(first["executed"].count("src"), 1)
        second = self._run_child(config)
        self.assertEqual(set(second["cache_hits"]), {"src", "mid", "down"})
        self.assertEqual(Path(count_file).read_bytes(), b"x")
