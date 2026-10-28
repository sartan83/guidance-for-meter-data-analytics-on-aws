from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor


def run_parallel(jobs: Mapping[str, Callable[[], object]], max_workers: int = 8) -> dict[str, object]:
    """Run independent Spark actions concurrently from the driver (Spark schedules them in parallel)."""
    if not jobs:
        return {}
    with ThreadPoolExecutor(max_workers=min(max_workers, len(jobs))) as pool:
        futures = {name: pool.submit(fn) for name, fn in jobs.items()}
        return {name: f.result() for name, f in futures.items()}
