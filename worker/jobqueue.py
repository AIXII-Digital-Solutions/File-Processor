"""
Per-user grouped, durable processing queue over Redis.

Files are grouped by a caller-supplied ``group`` key (e.g. a user id). Within a group
files are processed strictly FIFO and never interleaved with another group's files;
across DIFFERENT groups up to FP_WORKERS run concurrently. A user's batch therefore stays
contiguous while distinct users still parallelise.

Data model (every key self-cleans when its group goes idle):
  fp:u:{group}      LIST   — pending job blobs for one group (RPUSH tail, LMOVE head = FIFO).
  fp:ready          STREAM (consumer group ``fp-workers``) — a doorbell: one entry per group
                    that transitions idle->busy, so a busy group is signalled exactly once.
  fp:lease:{group}  STRING (TTL) — the consumer currently draining a group; guarantees a
                    single worker per group. Heartbeat-renewed while a file is processed and
                    released with an ownership-checked compare-and-delete (no stealing).
  fp:proc:{consumer} LIST  — the file a consumer is mid-processing (durability; re-injected
                    on restart so a hard crash does not lose an in-flight file).

Durability / recovery:
  * a restarted worker re-injects its own in-flight file (``_recover_processing``) — use a
    STABLE per-instance id (FP_INSTANCE_ID, else hostname) so a worker recovers its OWN proc;
  * ``reclaim_loop`` (XAUTOCLAIM) re-rings doorbells stuck under a dead/removed consumer so a
    live worker drains them — it never drains itself, so files only ever live under a
    recoverable fp:proc:{worker} key;
  * on handler failure the doorbell is NOT acked, so the reclaimer retries the group.
  At-least-once on crash: ingest must be idempotent (a file may be re-processed after a hard
  crash that occurred mid-ingest).

Pause: set the ``fp:paused`` Redis key to stop workers picking up new groups (in-flight
drains finish); delete it to resume. Requires Redis >= 6.2 (LMOVE).
"""
import asyncio
import json

STREAM = "fp:ready"
GROUP = "fp-workers"
PAUSED_KEY = "fp:paused"

LEASE_TTL = 600          # seconds; per-group lease, heartbeat-renewed while a file processes
RECLAIM_MS = 120_000     # reclaim doorbells idle longer than this (dead / removed consumer)
READY_MAXLEN = 10_000    # approximate cap on the ready stream


def _ukey(group: str) -> str:
    return f"fp:u:{group}"


def _lease_key(group: str) -> str:
    return f"fp:lease:{group}"


def _proc_key(consumer: str) -> str:
    return f"fp:proc:{consumer}"


# RPUSH a job; only on the idle->busy (len==1) transition ring the doorbell for the group.
_ENQUEUE_LUA = """
local n = redis.call('RPUSH', KEYS[1], ARGV[1])
if n == 1 then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*', 'group', ARGV[2])
end
return n
"""

# Ownership-checked lease ops (avoid stealing/deleting another worker's lease).
_RENEW_LUA = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
_RELEASE_LUA = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"


async def ensure_group(redis) -> None:
    """Create the ready stream + consumer group if they don't exist."""
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception as e:  # BUSYGROUP if it already exists
        if "BUSYGROUP" not in str(e):
            raise


async def enqueue(redis, *, group: str, kind: str, path: str, job_id: str, ref: str) -> None:
    """Append a job to its group's FIFO list and (only if the group was idle) ring the doorbell."""
    blob = json.dumps({"group": group, "kind": kind, "path": path, "job_id": job_id, "ref": ref})
    await redis.eval(_ENQUEUE_LUA, 2, _ukey(group), STREAM, blob, group, str(READY_MAXLEN))


async def _signal(redis, group: str) -> None:
    await redis.xadd(STREAM, {"group": group}, maxlen=READY_MAXLEN, approximate=True)


async def _is_paused(redis) -> bool:
    try:
        return bool(await redis.exists(PAUSED_KEY))
    except Exception:
        return False


async def _renew_lease(redis, lease: str, consumer: str) -> None:
    """Background heartbeat: keep OUR lease alive while a (possibly long) file processes."""
    interval = max(5, LEASE_TTL // 3)
    while True:
        await asyncio.sleep(interval)
        try:
            await redis.eval(_RENEW_LUA, 1, lease, consumer, str(LEASE_TTL))
        except Exception:
            pass


async def _drain_group(redis, group: str, consumer: str, handler, logger) -> None:
    """Hold the per-group lease and process its files strictly FIFO until the list is empty."""
    lease = _lease_key(group)
    if not await redis.set(lease, consumer, nx=True, ex=LEASE_TTL):
        return  # another worker already owns (is draining) this group
    proc = _proc_key(consumer)
    heartbeat = asyncio.create_task(_renew_lease(redis, lease, consumer))
    try:
        while True:
            blob = await redis.lmove(_ukey(group), proc, "LEFT", "RIGHT")  # head -> processing (durable)
            if blob is None:
                await redis.eval(_RELEASE_LUA, 1, lease, consumer)
                # a job may have arrived between the empty LMOVE and releasing the lease
                if await redis.llen(_ukey(group)) > 0:
                    await _signal(redis, group)
                return
            job = json.loads(blob)
            try:
                await handler(kind=job.get("kind"), path=job.get("path"),
                              job_id=job.get("job_id"), ref=job.get("ref"))
            except Exception:
                logger.exception("job failed (group=%s job_id=%s)", group, job.get("job_id"))
            finally:
                await redis.lrem(proc, 1, blob)  # done (success or recorded error) -> drop from processing
    finally:
        heartbeat.cancel()
        try:
            await heartbeat
        except (asyncio.CancelledError, Exception):
            pass
        try:
            await redis.eval(_RELEASE_LUA, 1, lease, consumer)  # ownership-checked
        except Exception:
            pass


async def _recover_processing(redis, consumer: str, logger) -> None:
    """On startup re-inject any file this consumer was mid-processing when it died, so it is
    not lost. A STABLE per-instance consumer id means a restarted worker recovers its own."""
    proc = _proc_key(consumer)
    try:
        leftovers = await redis.lrange(proc, 0, -1)
        for blob in leftovers:
            try:
                group = json.loads(blob).get("group")
            except Exception:
                group = None
            if group:
                await redis.lpush(_ukey(group), blob)  # back to the FRONT (preserve FIFO order)
                await _signal(redis, group)
        if leftovers:
            await redis.delete(proc)
            logger.info("recovered %d in-flight job(s) for %s", len(leftovers), consumer)
    except Exception:
        logger.exception("processing-recovery failed for %s", consumer)


async def _process_ready(redis, consumer: str, entries, handler, logger) -> None:
    for msg_id, fields in entries:
        group = fields.get("group")
        ok = False
        if not group:
            ok = True  # malformed doorbell -> drop it
        else:
            try:
                await _drain_group(redis, group, consumer, handler, logger)
                ok = True
            except Exception:
                # leave the doorbell UNACKED -> reclaim_loop re-rings the group after RECLAIM_MS
                logger.exception("drain failed (doorbell=%s group=%s)", msg_id, group)
        if ok:
            try:
                await redis.xack(STREAM, GROUP, msg_id)
            except Exception:
                logger.exception("xack failed for %s", msg_id)


async def consume(redis, consumer: str, handler, logger) -> None:
    """One worker: recover own in-flight file, replay own pending doorbells, then read live.

    count=1: a worker holds at most ONE doorbell at a time, so a burst of N groups fans out
    across up to FP_WORKERS workers instead of one worker hoarding them.
    """
    await _recover_processing(redis, consumer, logger)
    backlog = "0"  # "0" = this consumer's pending doorbells (recovery); ">" = new ones
    while True:
        if await _is_paused(redis):
            await asyncio.sleep(2)
            continue
        try:
            live = backlog == ">"
            resp = await redis.xreadgroup(GROUP, consumer, {STREAM: backlog}, count=1,
                                          block=(5000 if live else None))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("xreadgroup error")
            await asyncio.sleep(1)
            continue
        entries = resp[0][1] if resp else []
        if not live and not entries:
            backlog = ">"  # pending drained -> switch to live
            continue
        try:
            await _process_ready(redis, consumer, entries, handler, logger)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("process_ready error")
            await asyncio.sleep(1)


async def reclaim_loop(redis, consumer: str, logger) -> None:
    """Re-ring doorbells stuck under a dead/removed consumer (XAUTOCLAIM) so a LIVE worker
    (with a recoverable proc list) drains them. Never drains itself — so an in-flight file
    is never stranded under this non-recoverable name."""
    while True:
        try:
            if not await _is_paused(redis):
                res = await redis.xautoclaim(STREAM, GROUP, consumer, min_idle_time=RECLAIM_MS, count=10)
                claimed = res[1] if res and len(res) > 1 else []
                for msg_id, fields in claimed:
                    group = fields.get("group")
                    if group:
                        await _signal(redis, group)  # re-ring; a normal worker picks it up
                    try:
                        await redis.xack(STREAM, GROUP, msg_id)
                    except Exception:
                        logger.exception("reclaim xack failed for %s", msg_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("reclaim error")
        await asyncio.sleep(RECLAIM_MS / 1000)


__all__ = ["STREAM", "GROUP", "PAUSED_KEY", "ensure_group", "enqueue", "consume", "reclaim_loop"]
