import asyncio
import os

import pandas as pd
from sqlalchemy import create_engine

from Config import setup_logger, DBSettings

logger = setup_logger(name="excel_processor")


def _write_sheets_sync(excel_file: str, sync_db_url: str) -> list[str]:
    """Blocking pandas work, executed in a worker thread."""
    tables: list[str] = []
    sync_engine = create_engine(sync_db_url)
    xls = pd.ExcelFile(excel_file)
    try:
        logger.info(f"[XLSX] File loaded. Sheets count: {len(xls.sheet_names)}")
        for sheet_name in xls.sheet_names:
            if sheet_name.upper() == "README":
                logger.info("[XLSX] Readme file skipped")
                continue
            df = pd.read_excel(xls, sheet_name=sheet_name)
            logger.debug(f"[XLSX] Row count: {len(df)}")

            df.columns = [c.strip() for c in df.columns]
            df.to_sql(sheet_name.lower(), sync_engine, if_exists="replace", index=False)

            tables.append(sheet_name.lower())
            logger.info(f"[XLSX] Sheet '{sheet_name}' saved to DB with name: {sheet_name.lower()}")
    finally:
        try:
            sync_engine.dispose()
            xls.close()
        except Exception:
            pass
    return tables


async def process_excel_file(session, excel_file: str):
    db_settings = DBSettings()
    try:
        async_engine = session.get_bind()
        sync_db_url = async_engine.url.set(drivername="postgresql+psycopg2", password=db_settings.DB_PASSWORD)

        tables = await asyncio.to_thread(_write_sheets_sync, excel_file, sync_db_url)
        logger.info(f"[XLSX] Saved sheets: {tables}")
    except Exception as _ex:
        logger.error(f"[XLSX] Error processing: {_ex}")
        raise
    finally:
        if os.path.exists(excel_file):
            os.remove(excel_file)
            logger.debug(f"[XLSX] Removed {os.path.basename(excel_file)}")
