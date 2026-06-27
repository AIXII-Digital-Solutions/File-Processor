import asyncio
import json
import os

from pydantic import ValidationError

from Config import setup_logger
from Schemas import JsonFileSchema
from .Queueing import add_to_queue

logger = setup_logger("json_processor")


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


async def process_json_file(session, json_file):
    try:
        content = await asyncio.to_thread(_read_text, json_file)
        file_data = json.loads(content)
        validated = JsonFileSchema(**file_data)

        for filename in validated.filename.split(','):
            await add_to_queue(
                filename=filename.strip(),
                user_email=validated.user_email,
                _type=validated.type,
                session=session
            )
            await session.commit()
        logger.info(f"[JSON] Added {file_data['filename']} in queue")
    except json.JSONDecodeError as _ex:
        logger.error(f"[JSON] File error: {json_file} - {_ex}")
        raise
    except ValidationError as _ex:
        logger.error(f"[JSON] File structure error: {json_file} - {_ex} \n\n Expected: {JsonFileSchema.model_json_schema()}")
        raise
    finally:
        if os.path.exists(json_file):
            os.remove(json_file)
            logger.debug(f"[JSON] Removed {json_file}")


__all__ = ["process_json_file"]
