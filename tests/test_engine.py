import json
import multiprocessing
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

from buildengine import (
    BuildFailed,
    Engine,
    Task,
    ValidationError,
)


def const(value):
    return lambda _deps: value


class EngineTests(unittest.TestCase):
    def test_diamond(self):
        calls = []

        def source(_):
            calls.append("source")
            return b"x"

        tasks = [
            Task("source", (), source),
            Task("left", ("source",), lambda d: d["source"] + b"l"),
            Task("right", ("source",), lambda d: d["source"] + b"r"),
            Task("join", ("left", "right"), lambda d: d["left"] + d["right"]),
        ]
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(Engine(tasks, folder).build(["join"]), {"join": b"xlxr"})
        self.assertEqual(calls, ["source"])
        # A single worker must not deadlock on a diamond either.
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(
                Engine(tasks, folder, max_workers=1).build(["join"]), {"join": b"xlxr"}
            )

    def test_cache_hit_across_engine_instances(self):
        calls = []

        def action(_):
            calls.append(1)
            return b"payload"

        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(Engine([Task("a", (), action)], folder).build(["a"]), {"a": b"payload"})
            # A brand-new Engine over the same cache_dir reuses the result.
            result, report = Engine([Task("a", (), action)], folder).build(
                ["a"], return_report=True
            )
            self.assertEqual(result, {"a": b"payload"})
            self.assertEqual(report.cached, ["a"])
        self.assertEqual(len(calls), 1)

    def test_version_change_invalidates(self):
        with tempfile.TemporaryDirectory() as folder:
            Engine([Task("a", (), const(b"v1"), version="1")], folder).build(["a"])
            result, report = Engine(
                [Task("a", (), const(b"v2"), version="2")], folder
            ).build(["a"], return_report=True)
            self.assertEqual(result, {"a": b"v2"})
            self.assertEqual(report.executed, ["a"])

    def test_same_content_rebuild_lets_downstream_reuse_cache(self):
        runs = {"src": 0, "down": 0}

        def src(_):
            runs["src"] += 1
            return b"same-content"

        def down(deps):
            runs["down"] += 1
            return deps["src"] + b"!"

        with tempfile.TemporaryDirectory() as folder:
            Engine([Task("src", (), src, "1"), Task("down", ("src",), down)], folder).build(["down"])
            # src version bump forces re-execution, but identical output content
            # means downstream keys are unchanged -> downstream cache hit.
            _, report = Engine(
                [Task("src", (), src, "2"), Task("down", ("src",), down)], folder
            ).build(["down"], return_report=True)
            self.assertEqual(report.executed, ["src"])
            self.assertEqual(report.cached, ["down"])
        self.assertEqual(runs, {"src": 2, "down": 1})

    def test_dep_content_change_invalidates_downstream(self):
        with tempfile.TemporaryDirectory() as folder:

            def make(payload):
                return [
                    Task("src", (), const(payload), version=payload.decode()),
                    Task("down", ("src",), lambda d: d["src"] + b"!"),
                ]

            self.assertEqual(Engine(make(b"one"), folder).build(["down"]), {"down": b"one!"})
            result, report = Engine(make(b"two"), folder).build(["down"], return_report=True)
            self.assertEqual(result, {"down": b"two!"})
            self.assertEqual(sorted(report.executed), ["down", "src"])

    def test_binary_output_roundtrip(self):
        payload = bytes(range(256)) * 64
        with tempfile.TemporaryDirectory() as folder:
            Engine([Task("bin", (), const(payload))], folder).build(["bin"])
            self.assertEqual(Engine([Task("bin", (), const(payload))], folder).build(["bin"])["bin"], payload)

    def test_zero_length_output(self):
        with tempfile.TemporaryDirectory() as folder:
            result, report = Engine([Task("z", (), const(b""))], folder).build(["z"], return_report=True)
            self.assertEqual(result, {"z": b""})
            _, report2 = Engine([Task("z", (), const(b""))], folder).build(["z"], return_report=True)
            self.assertEqual(report2.cached, ["z"])

    def test_non_bytes_return_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("bad", (), lambda _: "not-bytes")], folder)
            with self.assertRaises(BuildFailed) as ctx:
                engine.build(["bad"])
            self.assertIsInstance(ctx.exception.failures["bad"], TypeError)

    def test_duplicate_and_empty_targets(self):
        calls = []
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), lambda _: calls.append(1) or b"a")], folder)
            self.assertEqual(engine.build(["a", "a", "a"]), {"a": b"a"})
            self.assertEqual(len(calls), 1)
            self.assertEqual(engine.build([]), {})
            result, report = engine.build([], return_report=True)
            self.assertEqual((result, report.records), ({}, {}))

    def test_force_reexecutes(self):
        calls = []
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), lambda _: calls.append(1) or b"a")], folder)
            engine.build(["a"])
            _, report = engine.build(["a"], force=True, return_report=True)
            self.assertEqual(report.executed, ["a"])
            self.assertEqual(len(calls), 2)

    def test_unicode_and_path_separator_names(self):
        names = ["src/工具", "a\\b", "emoji-\U0001f680"]
        tasks = [Task(n, (), const(n.encode("utf-8"))) for n in names]
        tasks.append(Task("join", tuple(names), lambda d: b"|".join(d[n] for n in names)))
        with tempfile.TemporaryDirectory() as folder:
            result = Engine(tasks, folder).build(["join"])
            self.assertEqual(result["join"], "|".join(names).encode("utf-8"))
            # Cache files must not embed raw task names as paths.
            for meta in (Path(folder) / "tasks").iterdir():
                self.assertNotIn("工具", meta.name)
                self.assertNotIn("/", meta.name)

    def test_real_concurrency_with_barrier(self):
        # Both actions block until both have started: impossible sequentially.
        barrier = threading.Barrier(2)

        def guarded(tag):
            def action(_):
                barrier.wait(timeout=10)
                return tag

            return action

        with tempfile.TemporaryDirectory() as folder:
            engine = Engine(
                [
                    Task("a", (), guarded(b"a")),
                    Task("b", (), guarded(b"b")),
                ],
                folder,
                max_workers=2,
            )
            self.assertEqual(engine.build(["a", "b"]), {"a": b"a", "b": b"b"})

    def test_concurrency_limit_respected(self):
        lock = threading.Lock()
        current = [0]
        peak = [0]
        entered = [threading.Event() for _ in range(4)]

        def make(i):
            def action(_):
                with lock:
                    current[0] += 1
                    peak[0] = max(peak[0], current[0])
                    entered[i].set()
                # Hold each slot until a sibling is also running, forcing overlap
                # if (and only if) more than one worker is active.
                others = [e for j, e in enumerate(entered) if j != i]
                self.assertTrue(any(e.wait(timeout=10) for e in others))
                with lock:
                    current[0] -= 1
                return bytes([i])

            return action

        with tempfile.TemporaryDirectory() as folder:
            engine = Engine(
                [Task(f"t{i}", (), make(i)) for i in range(4)], folder, max_workers=2
            )
            engine.build([f"t{i}" for i in range(4)])
        self.assertEqual(peak[0], 2)

    def test_single_worker_deep_chain_no_deadlock(self):
        depth = 300
        tasks = [Task("n0", (), const(b""))]
        for i in range(1, depth):
            tasks.append(Task(f"n{i}", (f"n{i-1}",), lambda d, i=i: d[f"n{i-1}"] + b"x"))
        with tempfile.TemporaryDirectory() as folder:
            result = Engine(tasks, folder, max_workers=1).build([f"n{depth-1}"])
            self.assertEqual(result[f"n{depth-1}"], b"x" * (depth - 1))

    def test_failure_blocks_dependents_allows_independent_and_recovers(self):
        should_fail = {"on": True}
        ran = []

        def flaky(_):
            if should_fail["on"]:
                raise RuntimeError("boom")
            return b"fixed"

        tasks = [
            Task("flaky", (), flaky),
            Task("dependent", ("flaky",), lambda d: ran.append("dependent") or d["flaky"]),
            Task("independent", (), lambda _: ran.append("independent") or b"ok"),
        ]
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine(tasks, folder, max_workers=3)
            with self.assertRaises(BuildFailed) as ctx:
                engine.build(["dependent", "independent"])
            exc = ctx.exception
            self.assertEqual(sorted(exc.failures), ["flaky"])
            self.assertIsInstance(exc.failures["flaky"], RuntimeError)
            report = exc.report
            self.assertEqual(report.failed, ["flaky"])
            self.assertEqual(report.skipped, ["dependent"])
            self.assertEqual(report.executed, ["independent"])
            self.assertNotIn("dependent", ran)
            # Failure must not poison the cache: fixing the action and retrying works.
            should_fail["on"] = False
            result, report2 = engine.build(["dependent", "independent"], return_report=True)
            self.assertEqual(result, {"dependent": b"fixed", "independent": b"ok"})
            self.assertEqual(report2.cached, ["independent"])  # success was cached
            self.assertEqual(report2.executed, ["dependent", "flaky"])

    def test_multiple_concurrent_failures_preserved(self):
        def boom(tag):
            def action(_):
                raise ValueError(tag)

            return action

        with tempfile.TemporaryDirectory() as folder:
            engine = Engine(
                [Task("f1", (), boom("one")), Task("f2", (), boom("two"))],
                folder,
                max_workers=2,
            )
            with self.assertRaises(BuildFailed) as ctx:
                engine.build(["f1", "f2"])
            failures = ctx.exception.failures
            self.assertEqual(sorted(failures), ["f1", "f2"])
            self.assertEqual(str(failures["f1"]), "one")
            self.assertEqual(str(failures["f2"]), "two")

    def test_cycle_diagnostic_and_unreachable_cycle_ignored(self):
        cyclic = [
            Task("a", ("c",), const(b"a")),
            Task("b", ("a",), const(b"b")),
            Task("c", ("b",), const(b"c")),
        ]
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValidationError) as ctx:
                Engine(cyclic, folder).build(["a"])
            message = str(ctx.exception)
            self.assertIn("cycle", message)
            for node in ("a", "b", "c"):
                self.assertIn(node, message)
            # The same cycle, unreachable from the target, must not block the build.
            tasks = cyclic + [Task("ok", (), const(b"fine"))]
            self.assertEqual(Engine(tasks, folder).build(["ok"]), {"ok": b"fine"})

    def test_missing_dep_target_and_duplicate_names(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValidationError, "missing"):
                Engine([Task("a", ("ghost",), const(b""))], folder).build(["a"])
            with self.assertRaisesRegex(ValidationError, "unknown target"):
                Engine([Task("a", (), const(b""))], folder).build(["nope"])
            with self.assertRaisesRegex(ValidationError, "duplicate"):
                Engine(
                    [Task("a", (), const(b"1")), Task("a", (), const(b"2"))], folder
                ).build(["a"])

    # ---------------------------------------------------------- corruption

    def _meta_files(self, folder):
        return list((Path(folder) / "tasks").iterdir())

    def test_truncated_metadata_rebuilds(self):
        calls = []
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), lambda _: calls.append(1) or b"a")], folder)
            engine.build(["a"])
            meta = self._meta_files(folder)[0]
            meta.write_bytes(meta.read_bytes()[:5])  # truncated JSON
            _, report = engine.build(["a"], return_report=True)
            self.assertEqual(report.executed, ["a"])
            self.assertTrue(any("corrupt" in w for w in report.warnings))
            self.assertEqual(len(calls), 2)

    def test_corrupt_blob_rebuilds(self):
        calls = []
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), lambda _: calls.append(1) or b"a")], folder)
            engine.build(["a"])
            meta = json.loads(self._meta_files(folder)[0].read_text())
            blob = Path(folder) / "blobs" / meta["blob"][:2] / meta["blob"]
            blob.write_bytes(b"tampered")
            _, report = engine.build(["a"], return_report=True)
            self.assertEqual(report.executed, ["a"])
            self.assertTrue(any("corrupt blob" in w for w in report.warnings))
            self.assertEqual(len(calls), 2)

    def test_missing_blob_rebuilds(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), const(b"a"))], folder)
            engine.build(["a"])
            meta = json.loads(self._meta_files(folder)[0].read_text())
            os.unlink(Path(folder) / "blobs" / meta["blob"][:2] / meta["blob"])
            _, report = engine.build(["a"], return_report=True)
            self.assertEqual(report.executed, ["a"])
            self.assertTrue(any("missing blob" in w for w in report.warnings))

    def test_stale_temp_files_swept(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine([Task("a", (), const(b"a"))], folder)
            engine.build(["a"])
            junk = Path(folder) / "tasks" / ".tmp-crashed-writer"
            junk.write_bytes(b"partial")
            Engine([Task("a", (), const(b"a"))], folder)  # new engine sweeps
            self.assertFalse(junk.exists())

    def test_no_temp_files_left_after_build(self):
        with tempfile.TemporaryDirectory() as folder:
            tasks = [Task(f"t{i}", (), const(bytes([i]))) for i in range(8)]
            Engine(tasks, folder, max_workers=4).build([f"t{i}" for i in range(8)])
            leftovers = [
                p for p in Path(folder).rglob("*") if p.name.startswith(".tmp-")
            ]
            self.assertEqual(leftovers, [])

    # ------------------------------------------------------- multiprocessing

    def test_concurrent_build_calls_same_engine(self):
        # "shared" runs once per build call; both calls must overlap inside it.
        barrier = threading.Barrier(2)

        def shared(_):
            barrier.wait(timeout=10)
            return b"s"

        tasks = [
            Task("shared", (), shared),
            Task("left", ("shared",), lambda d: d["shared"] + b"l"),
            Task("right", ("shared",), lambda d: d["shared"] + b"r"),
        ]
        with tempfile.TemporaryDirectory() as folder:
            engine = Engine(tasks, folder, max_workers=4)
            outcomes = {}
            threads = [
                threading.Thread(
                    target=lambda t=t: outcomes.setdefault(t, engine.build([t])),
                    args=(t,),
                )
                for t in ("left", "right")
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            self.assertEqual(outcomes, {"left": {"left": b"sl"}, "right": {"right": b"sr"}})

    def test_cross_process_race_same_cache_dir(self):
        with tempfile.TemporaryDirectory() as folder:
            ctx = multiprocessing.get_context("fork")
            start = ctx.Event()
            errors = ctx.Queue()

            def worker():
                try:
                    start.wait(timeout=10)
                    tasks = [
                        Task("base", (), lambda _: b"base"),
                        Task("top", ("base",), lambda d: d["base"] + b"+top"),
                    ]
                    result = Engine(tasks, folder, max_workers=2).build(["top"])
                    assert result == {"top": b"base+top"}, result
                except BaseException as exc:  # noqa: BLE001
                    errors.put(repr(exc))

            procs = [ctx.Process(target=worker) for _ in range(4)]
            for proc in procs:
                proc.start()
            start.set()
            for proc in procs:
                proc.join(timeout=60)
                self.assertEqual(proc.exitcode, 0)
            collected = []
            while not errors.empty():
                collected.append(errors.get())
            self.assertEqual(collected, [])
            # Cache left in a consistent, reusable state.
            _, report = Engine(
                [Task("base", (), const(b"base")), Task("top", ("base",), lambda d: d["base"] + b"+top")],
                folder,
            ).build(["top"], return_report=True)
            self.assertEqual(sorted(report.cached), ["base", "top"])


if __name__ == "__main__":
    unittest.main()
