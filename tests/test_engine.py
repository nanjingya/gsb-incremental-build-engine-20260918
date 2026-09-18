import tempfile
import unittest
from buildengine import Engine, Task

class EngineTests(unittest.TestCase):
    def test_diamond(self):
        calls = []
        def source(_):
            calls.append("source")
            return b"x"
        tasks = [Task("source", (), source), Task("left", ("source",), lambda d: d["source"] + b"l"), Task("right", ("source",), lambda d: d["source"] + b"r"), Task("join", ("left", "right"), lambda d: d["left"] + d["right"])]
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(Engine(tasks, folder).build(["join"]), {"join": b"xlxr"})
        self.assertEqual(calls, ["source"])

if __name__ == "__main__":
    unittest.main()
