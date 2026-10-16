from collections import Counter
from threading import Lock


class Metrics:
    """Local operational counters, intentionally not a transactional ledger."""
    def __init__(self):
        self._lock = Lock()
        self._counts = Counter()
        self._timings = {}

    def counter(self, name, value=1):
        with self._lock:
            self._counts[name] += value

    def observe(self, name, seconds):
        with self._lock:
            count, total, maximum = self._timings.get(name, (0, 0.0, 0.0))
            self._timings[name] = (count + 1, total + seconds, max(maximum, seconds))

    def snapshot(self):
        with self._lock:
            return {"counters": dict(self._counts), "timings": {
                k: {"count": c, "total": t, "max": m} for k, (c, t, m) in self._timings.items()}}

