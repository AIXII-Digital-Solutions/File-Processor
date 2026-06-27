# file-processor

Independent **HTTP service** that ingests files (CSV/JSON/Excel/Cirium; PDF/Word
planned) into the database. core-api saves an uploaded file and **POSTs it here**
(`POST /process`, `X-Service-Token`); there is NO shared filesystem. It also watches
local drop folders (Finder loops) for manual drops. Per-file status is published to the
shared `job_statuses` table + Redis `status:events` (read back via core-api).

It vendors only the model files for the databases it uses (`main`, `service`, `cirium`)
— not the whole schema.

## Layout
```
worker/
  server.py      # FastAPI app: POST /process, GET /health, folder-watch in lifespan
  main.py        # uvicorn entry
  service_auth.py, status.py, settings.py
  ingest/        # CSV/JSON/Excel/Cirium processors
  Database/      # vendored model subset (main/service/cirium)
  Config, Schemas, Utils
Dockerfile, docker-compose.yml, entrypoint.sh, .env.example
```

## API
- `POST /process` — multipart `file` + `kind` (`json|csv|excel|cirium`) + optional `job_id`.
  Requires header `X-Service-Token` == this service's `SERVICE_TOKEN`. Saves the file to
  local storage and **enqueues** it on a durable **Redis-Streams queue** (returns 202 + job_id).
- `GET /health`.

## Processing queue
Received files go on a Redis-Streams queue (`fp:process`, group `fp-workers`); a bounded
pool of `FP_WORKERS` (default 2) consumers ingests them — so multiple files **queue**
instead of all running at once. Durable (survives restarts; in-flight jobs are replayed
via stable consumer names) and scalable (run several file-processor instances on one group).
Folder-watch drops are still processed inline.

## Run (Docker)
```bash
cp .env.example .env     # DB_*/REDIS_* + SERVICE_TOKEN (must equal core-api FILE_PROCESSOR_TOKEN)
docker compose up -d --build     # serves on host :8001 -> container :8000
```
Use the **same Redis** as core-api (for the `status:events` channel). No shared file volume.

## Updating models
Models are owned by **core-api** (`db-contract`). This repo holds a vendored subset; when
the schema changes, copy the updated model files from core-api and commit. No sync script.
