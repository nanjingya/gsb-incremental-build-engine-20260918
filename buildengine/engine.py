from dataclasses import dataclass
from pathlib import Path
from typing import Callable

@dataclass(frozen=True)
class Task:
    name: str
    deps: tuple[str, ...]
    action: Callable[[dict[str, bytes]], bytes]
    version: str = "1"

class Engine:
    def __init__(self, tasks, cache_dir):
        self.tasks = {task.name: task for task in tasks}
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def build(self, targets):
        memo = {}
        def visit(name):
            if name in memo:
                return memo[name]
            task = self.tasks[name]
            values = {dep: visit(dep) for dep in task.deps}
            output = task.action(values)
            if not isinstance(output, bytes):
                raise TypeError("actions must return bytes")
            memo[name] = output
            return output
        return {name: visit(name) for name in targets}
