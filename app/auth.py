from fastapi import Header, HTTPException
from .config import settings

async def require_token(x_api_token: str | None = Header(default=None)):
    if not x_api_token or x_api_token != settings.api_token:
        raise HTTPException(status_code=401, detail="Unauthorized")
