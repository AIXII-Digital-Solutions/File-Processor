"""file-processor entry point (uvicorn).

settings is imported first so this service's own .env is loaded before the local
Config (and anything importing it) is initialised.
"""
import settings  # noqa: F401

import uvicorn

from settings import HOST, PORT
from server import app

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
