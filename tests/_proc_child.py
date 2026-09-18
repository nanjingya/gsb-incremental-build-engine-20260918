"""Helper used by cross-process cache tests. Invoked as a subprocess:

    python _proc_child.py <json-config-path>

Config JSON: {"cache_dir": ..., "tasks": [...], "targets": [...]}

A task descriptor is one of:
  {"kind": "leaf", "name": ..., "bytes": "<hex>", "count_file": "<path>"}
  {"kind": "passthrough", "name": ..., "deps": [...], "suffix": "<hex>"}
  {"kind": "join", "name": ..., "deps": [...]}

Each leaf increments its count_file exactly once per action invocation
(atomic O_APPEND write of one byte), so the parent can detect duplicate work.
Prints a JSON result object and exits 0.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildengine import Engine, Task  # noqa: E402


def make_task(spec):
    kind = spec["kind"]
    if kind == "leaf":
        data = bytes.fromhex(spec["bytes"])
        count_file = spec["count_file"]

        def leaf(_d, data=data, count_file=count_file):
            fd = os.open(count_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            try:
                os.write(fd, b"x")
            finally:
                os.close(fd)
            return data

        return Task(spec["name"], (), leaf, spec.get("version", "1"))

    if kind == "passthrough":
        dep = spec["deps"][0]
        suffix = bytes.fromhex(spec["suffix"])

        def passthrough(d, dep=dep, suffix=suffix):
            return d[dep] + suffix

        return Task(spec["name"], tuple(spec["deps"]), passthrough,
                    spec.get("version", "1"))

    if kind == "join":
        deps = tuple(spec["deps"])

        def join(d, deps=deps):
            return b"".join(d[dep] for dep in deps)

        return Task(spec["name"], deps, join, spec.get("version", "1"))

    raise ValueError(f"unknown task kind: {kind}")


def main():
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        config = json.load(handle)
    tasks = [make_task(spec) for spec in config["tasks"]]
    engine = Engine(tasks, config["cache_dir"],
                    max_workers=config.get("max_workers"))
    report = engine.build(config["targets"], return_report=True)
    result = {
        "outputs": {name: report.outputs[name].hex()
                    for name in config["targets"]},
        "cache_hits": sorted(report.cache_hits),
        "executed": sorted(report.executed),
        "ok": report.ok,
    }
    sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    main()
