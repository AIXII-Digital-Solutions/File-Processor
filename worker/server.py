"""
file-processor HTTP service.

core-api saves an uploaded file and POSTs it here (`POST /process`, service-token
auth). The file is stored in THIS service's own local storage and ingested into the
DB — there is NO shared filesystem with core-api. Local drop folders are also watched
(Finder loops) in the app lifespan. Status is published to the shared `job_statuses`
table + Redis `status:events` so core-api can read it back.
"""
import asyncio
import shutil
import uuid as _uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response, UploadFile, File, Form, Depends, status
from redis.asyncio import Redis

from settings import FILES_PATH, EXCEL_FILES_PATH, CIRIUM_FILES_PATH, INTAKE_PATH, FP_WORKERS
from Config import setup_logger, DBSettings
from Database import DatabaseClient
from Schemas.Enums.service import FilesExtensionEnum
from status import publish_status
from service_auth import verify_service_token
import jobqueue
from ingest import (
    Finder,
    process_csv_file,
    process_json_file,
    process_excel_file,
    process_cirium_file,
)

logger = setup_logger("file_processor")

# kind -> (processor, watch_path, extension, db_name)
PROCESSORS = {
    "json": (process_json_file, FILES_PATH, FilesExtensionEnum.JSON, "service"),
    "csv": (process_csv_file, FILES_PATH, FilesExtensionEnum.CSV, "main"),
    "excel": (process_excel_file, EXCEL_FILES_PATH, FilesExtensionEnum.EXCEL, "main"),
    "cirium": (process_cirium_file, CIRIUM_FILES_PATH, FilesExtensionEnum.CIRIUM, "cirium"),
}


async def process_one(db_client, redis, path: str, kind: str, job_id: str) -> None:
    """Ingest one file, publishing running -> success/error status."""
    if kind not in PROCESSORS:
        await publish_status(db_client, redis, job_id=job_id, kind="file", ref=path,
                             state="error", message=f"Unknown kind: {kind}")
        raise ValueError(f"Unknown kind: {kind}")
    func, _, _, db = PROCESSORS[kind]
    await publish_status(db_client, redis, job_id=job_id, kind="file", ref=path,
                         state="running", progress=0)
    try:
        async with db_client.session(db) as session:
            await func(session, path)
    except Exception as e:
        await publish_status(db_client, redis, job_id=job_id, kind="file", ref=path,
                             state="error", message=str(e))
        raise
    await publish_status(db_client, redis, job_id=job_id, kind="file", ref=path,
                         state="success", progress=100)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_client = DatabaseClient()
    username, password, host, port = DBSettings().get_reddis_credentials()
    app.state.redis = Redis(username=username or None, password=password or None,
                            host=host, port=port, decode_responses=True)
    app.state.tasks = []

    # processing queue (Redis Streams) + bounded worker pool
    await jobqueue.ensure_group(app.state.redis)

    async def _handler(*, kind, path, job_id, ref):
        await process_one(app.state.db_client, app.state.redis, path, kind, job_id)

    for i in range(FP_WORKERS):
        app.state.tasks.append(asyncio.create_task(
            jobqueue.consume(app.state.redis, f"fp-{i}", _handler, logger)
        ))

    # folder-watch loops (manual local drops — processed inline)
    for kind, (func, path, ext, db) in PROCESSORS.items():
        finder = Finder()  # one Finder per loop — avoids shared mutable state
        app.state.tasks.append(asyncio.create_task(
            finder.start_loop(db_client=app.state.db_client, func=func, path=path,
                              extension=ext, db=db, redis=app.state.redis)
        ))
    logger.info("file-processor up; %d queue workers + watch loops: %s", FP_WORKERS, list(PROCESSORS))
    yield
    for t in app.state.tasks:
        t.cancel()
    for t in app.state.tasks:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await app.state.redis.aclose()
    except Exception:
        pass
    await app.state.db_client.dispose()
    logger.info("file-processor down")


app = FastAPI(title="file-processor", lifespan=lifespan)


def _save_sync(upload_file, dest: Path) -> None:
    with dest.open("wb") as out:
        shutil.copyfileobj(upload_file, out)


@app.get("/health/")
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/process", dependencies=[Depends(verify_service_token)])
async def process(
    request: Request,
    response: Response,
    file: UploadFile = File(...),
    kind: str = Form(...),
    job_id: str = Form(default=None),
):
    if kind not in PROCESSORS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": f"Unknown kind '{kind}'. Allowed: {sorted(PROCESSORS)}"}
    safe_name = Path(file.filename or "").name
    if not safe_name:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": "Missing or invalid filename"}

    job_id = job_id or _uuid.uuid4().hex
    INTAKE_PATH.mkdir(parents=True, exist_ok=True)
    dest = INTAKE_PATH / f"{_uuid.uuid4().hex}__{safe_name}"
    await asyncio.to_thread(_save_sync, file.file, dest)

    db_client, redis = request.app.state.db_client, request.app.state.redis
    # enqueue onto the durable Redis-Streams queue; a bounded worker pool processes it
    await jobqueue.enqueue(redis, kind=kind, path=str(dest), job_id=job_id, ref=str(dest))
    await publish_status(db_client, redis, job_id=job_id, kind="file", ref=str(dest), state="queued")

    response.status_code = status.HTTP_202_ACCEPTED
    return {"job_id": job_id, "kind": kind, "filename": safe_name}
