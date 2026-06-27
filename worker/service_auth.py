"""Server-to-server auth: the token core-api must present to POST files here."""
import hmac

from fastapi import Header, HTTPException, status

from settings import SERVICE_TOKEN


async def verify_service_token(x_service_token: str | None = Header(default=None)) -> None:
    if not SERVICE_TOKEN or not x_service_token or not hmac.compare_digest(x_service_token, SERVICE_TOKEN):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing service token",
        )


__all__ = ["verify_service_token"]
