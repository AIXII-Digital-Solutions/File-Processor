"""
Shared process pool for CPU-bound ingestion work (pandas parse / Excel ``to_sql``).

A ProcessPoolExecutor gives TRUE multi-core parallelism for CPU-bound work, unlike a
thread pool which is GIL-bound. The queue workers process files from different groups
concurrently (one event loop), so offloading the heavy pandas/Excel work to separate
processes lets those concurrent files actually run in parallel.

Toggle with ``FP_USE_PROCESS_POOL`` (default true). When false, work runs in a thread via
``asyncio.to_thread`` instead — a safe fallback for environments where spawning subprocesses
is undesirable. Pool size is ``FP_PROCESS_WORKERS`` (default: CPU count).

Functions submitted to the pool MUST be importable top-level callables and their arguments
must be picklable (pass DSN strings / file paths, never live DB sessions or engines).
"""
import asyncio
import os
from concurrent.futures import ProcessPoolExecutor

_pool: ProcessPoolExecutor | None = None

USE_PROCESS_POOL: bool = os.getenv("FP_USE_PROCESS_POOL", "true").lower() in ("1", "true", "yes", "on")


def _max_workers() -> int:
    try:
        configured = int(os.getenv("FP_PROCESS_WORKERS", "0"))
    except ValueError:
        configured = 0
    return max(1, configured or (os.cpu_count() or 2))


def _init_worker() -> None:
    """Runs once in every pool worker. Under fork (the Linux default) a child inherits the
    parent's already-configured loggers IN MEMORY and never re-runs setup_logger, so it would
    keep the parent's RotatingFileHandler and race its doRollover() on the shared log file.
    Strip every inherited file handler so children only log to console (captured by the
    container); the parent_process() guard in Logger.py covers the spawn case."""
    import logging
    names = ["", *list(logging.root.manager.loggerDict)]
    for name in names:
        lg = logging.getLogger(name)
        for h in list(getattr(lg, "handlers", [])):
            if isinstance(h, logging.FileHandler):
                try:
                    lg.removeHandler(h)
                except Exception:
                    pass


def get_process_pool() -> ProcessPoolExecutor:
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=_max_workers(), initializer=_init_worker)
    return _pool


async def run_cpu(func, *args):
    """Run a picklable CPU-bound function off the event loop and await the result.

    Uses the process pool (true parallelism) unless FP_USE_PROCESS_POOL is disabled, in
    which case it falls back to a worker thread.
    """
    if not USE_PROCESS_POOL:
        return await asyncio.to_thread(func, *args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_process_pool(), func, *args)


def shutdown_pool() -> None:
    global _pool
    if _pool is not None:
        try:
            _pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        _pool = None


__all__ = ["run_cpu", "get_process_pool", "shutdown_pool", "USE_PROCESS_POOL"]
