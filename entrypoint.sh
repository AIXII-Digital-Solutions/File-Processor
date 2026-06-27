#!/usr/bin/env sh
# file-processor entrypoint: wait for DB + Redis, then start the HTTP service (uvicorn).
set -e

python - <<'PY'
import os, socket, time
targets = [
    ("database", os.getenv("DB_HOST", "localhost"), int(os.getenv("DB_PORT", "5432"))),
    ("redis", os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))),
]
for name, host, port in targets:
    for _ in range(60):
        try:
            socket.create_connection((host, port), 2).close()
            print(f"[entrypoint] {name} reachable"); break
        except OSError:
            time.sleep(2)
    else:
        print(f"[entrypoint] WARNING: {name} not reachable after timeout")
PY

cd /app/worker
echo "[entrypoint] starting file-processor HTTP service on :${PORT:-8000}"
exec python main.py
