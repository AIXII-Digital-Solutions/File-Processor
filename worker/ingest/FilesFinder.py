import asyncio
import os
import uuid
from glob import glob
from pathlib import Path
from typing import Callable, Awaitable

from Config import setup_logger
from Database import DatabaseClient
from Schemas.Enums.service import FilesExtensionEnum
from status import publish_status

logger = setup_logger(name="file_finder")


class Finder:
    def __init__(self):
        self.count: int = 0
        self.files: list[str] | None = None
        logger.debug("Initialized finder(-s)")

    async def find(self, path: Path, extension: str) -> tuple[list[str], int]:
        if extension.lower() == "cirium":
            files = sorted(glob(os.path.join(path, f"*.xlsx")), key=os.path.getmtime)
        else:
            files = sorted(glob(os.path.join(path, f"*.{extension}")), key=os.path.getmtime)
        self.files = files
        self.count = len(files)
        logger.debug(f"[{extension.upper()}] Found {self.count} files")

        return files, self.count

    async def start_loop(self,
                         *,
                         path: Path,
                         extension: FilesExtensionEnum,
                         db_client: DatabaseClient,
                         db: str,
                         func: Callable[..., Awaitable[None]],
                         redis=None,
                         interval: int = 5,
                         **kwargs
                         ):
        while True:
            try:
                _files, _count = await self.find(path=path, extension=extension.value)
                for _file in _files:
                    job_id = str(uuid.uuid4())
                    await publish_status(db_client, redis, job_id=job_id, kind="file",
                                         ref=_file, state="running", progress=0)
                    try:
                        async with db_client.session(db) as session:
                            logger.debug(
                                f"[{extension.value.upper()}] Sending '{os.path.basename(_file)}' "
                                f"to {func.__name__} function"
                            )
                            await func(session, _file, **kwargs)
                    except Exception as _ex:
                        # one bad file must not kill the whole watcher loop
                        logger.exception(f"[{extension.value.upper()}] Failed processing '{_file}': {_ex}")
                        await publish_status(db_client, redis, job_id=job_id, kind="file",
                                             ref=_file, state="error", message=str(_ex))
                    else:
                        await publish_status(db_client, redis, job_id=job_id, kind="file",
                                             ref=_file, state="success", progress=100)

                logger.debug(f"[{extension.value.upper()}] Waiting for new files...")
            except asyncio.CancelledError:
                logger.info(f"[{extension.value.upper()}] Watcher loop cancelled")
                raise
            except Exception as _ex:
                logger.exception(f"[{extension.value.upper()}] Watcher loop error: {_ex}")

            await asyncio.sleep(interval)
