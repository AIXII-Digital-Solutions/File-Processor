import asyncio
import csv
import io
import os

from sqlalchemy import update, insert, delete

from Config import setup_logger
from Database.Models import Registrations, Airlines
from Utils import to_bool

logger = setup_logger("csv_processor")


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


async def process_csv_file(session, csv_file: str):
    try:
        content = await asyncio.to_thread(_read_text, csv_file)
        reader = csv.DictReader(io.StringIO(content))

        if 'aircrafts' in csv_file:  # LEGACY
            for row in reader:
                stmt = (
                    update(Registrations)
                    .where(Registrations.reg == row["reg"])
                    .values(
                        reg=row["reg"],
                        msn=int(row["msn"]) if row["msn"] != "" else None,
                        aircraft_type=row["aircraft"],
                        indashboard=to_bool(row["indashboard"]),
                        status=row["status"]
                    )
                    .execution_options(synchronize_session="fetch")
                )
                result = await session.execute(stmt)

                if result.rowcount == 0:
                    await session.execute(
                        insert(Registrations).values(
                            reg=row["reg"],
                            msn=int(row["msn"]) if row["msn"] != "" else None,
                            aircraft_type=row["aircraft"],
                            indashboard=to_bool(row["indashboard"]),
                            status=row["status"]
                        )
                    )

        if 'airlines' in csv_file:
            await session.execute(delete(Airlines))

            batch = [
                {"airline_name": row["airline_name"], "icao": row["icao"]}
                for row in reader
            ]
            if batch:
                await session.execute(insert(Airlines), batch)

        logger.info(f"[CSV] Processed {csv_file}")
    except Exception as e:
        logger.error(f"[CSV] Error processing {csv_file}: {e}")
        raise
    finally:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            logger.debug(f"[CSV] Removed {csv_file}")


__all__ = ["process_csv_file"]
