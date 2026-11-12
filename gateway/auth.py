"""Notion OAuth endpoints.
The extension opens Notion's consent screen, but the code-for-token exchange needs the
OAuth client secret. The secret stays here on the server; the extension never sees it.
"""
import base64
import logging
from typing import Optional
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from common.config import get_settings

logger = logging.getLogger("keyframe.gateway.auth")

NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token"

router = APIRouter(prefix="/auth/notion", tags=["auth"])


class NotionConfigResponse(BaseModel):
    client_id: str


class NotionTokenRequest(BaseModel):
    code: str = Field(..., min_length=1)
    redirect_uri: str = Field(..., min_length=1)

    @field_validator("redirect_uri")
    @classmethod
    def must_be_extension_redirect(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme != "https" or not (parsed.hostname or "").endswith(".chromiumapp.org"):
            raise ValueError("redirect_uri must be a chrome.identity redirect URL.")
        return value


class NotionTokenResponse(BaseModel):
    access_token: str
    workspace_name: Optional[str] = None


@router.get("/config", response_model=NotionConfigResponse)
async def notion_config():
    """Public part of the OAuth setup: the client ID the extension needs to start sign-in."""
    client_id = get_settings().notion_oauth_client_id
    if not client_id:
        raise HTTPException(status_code=503, detail="Notion sign-in is not configured on the server.")
    return NotionConfigResponse(client_id=client_id)


@router.post("/token", response_model=NotionTokenResponse)
async def exchange_token(payload: NotionTokenRequest):
    """Exchanges the one-time authorization code for a Notion access token."""
    settings = get_settings()
    if not settings.notion_oauth_client_id or not settings.notion_oauth_client_secret:
        raise HTTPException(status_code=503, detail="Notion sign-in is not configured on the server.")

    credentials = f"{settings.notion_oauth_client_id}:{settings.notion_oauth_client_secret}"
    headers = {
        "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "grant_type": "authorization_code",
        "code": payload.code,
        "redirect_uri": payload.redirect_uri,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(NOTION_TOKEN_URL, headers=headers, json=body)
    except httpx.HTTPError as exc:
        logger.error("Could not reach Notion for token exchange: %r", exc)
        raise HTTPException(status_code=503, detail="Could not reach Notion. Try again shortly.")

    if response.status_code != 200:
        logger.warning("Notion rejected token exchange: %s %s", response.status_code, response.text[:200])
        raise HTTPException(status_code=400, detail="Notion rejected the sign-in. Please try connecting again.")

    data = response.json()
    return NotionTokenResponse(
        access_token=data["access_token"],
        workspace_name=data.get("workspace_name"),
    )