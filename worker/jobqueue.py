"""
Durable processing queue over Redis Streams.

POST /process saves the file then ENQUEUES a job here; a consumer group of bounded
workers pulls jobs and processes them. Properties:
- durable: Redis persists the stream + per-consumer pending entries, so a restart does
  not lose queued (or in-flight) jobs;
- bounded / ordered: N workers (FP_WORKERS) -> at most N files processed at once;
- scalable: multiple file-processor instances share ONE consumer group.

Crash recovery uses STABLE consumer names (fp-0..fp-N): on restart a worker first
replays its own un-acked (pending) messages, then switches to new ones.
"""
import asyncio

STREAM = "fp:process"
GROUP = "fp-workers"


async def ensure_group(redis) -> None:
    """Create the stream + consumer group if they don't exist."""
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception as e:  # redis.exceptions.ResponseError BUSYGROUP if it already exists
        if "BUSYGROUP" not in str(e):
            raise


async def enqueue(redis, *, kind: str, path: str, job_id: str, ref: str) -> str:
    """Add one job to the stream; returns the message id."""
    return await redis.xadd(STREAM, {"kind": kind, "path": path, "job_id": job_id, "ref": ref})


async def _handle(redis, msg_id, fields, handler, logger) -> None:
    try:
        await handler(
            kind=fields.get("kind"),
            path=fields.get("path"),
            job_id=fields.get("job_id"),
            ref=fields.get("ref"),
        )
    except Exception:
        logger.exception("job %s failed", msg_id)
    finally:
        # ack so the message is not redelivered (errors are recorded in job_statuses)
        try:
            await redis.xack(STREAM, GROUP, msg_id)
        except Exception:
            logger.exception("xack failed for %s", msg_id)


async def consume(redis, consumer: str, handler, logger) -> None:
    """One worker: replay this consumer's pending entries, then read new ones forever."""
    backlog = "0"  # "0" = this consumer's pending (crash recovery); ">" = new messages
    while True:
        try:
            live = backlog == ">"
            resp = await redis.xreadgroup(
                GROUP, consumer, {STREAM: backlog}, count=10, block=(5000 if live else None)
            )
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
        for msg_id, fields in entries:
            await _handle(redis, msg_id, fields, handler, logger)


__all__ = ["STREAM", "GROUP", "ensure_group", "enqueue", "consume"]
