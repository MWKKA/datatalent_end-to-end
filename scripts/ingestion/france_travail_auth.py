"""
Authentification OAuth2 Client Credentials pour l'API France Travail.
"""

from __future__ import annotations

import os
import time
from typing import NamedTuple
from pathlib import Path
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.core.common import load_project_env

TOKEN_URL = "https://authentification-partenaire.francetravail.io/connexion/oauth2/access_token"
REALM = "/partenaire"
DEFAULT_SCOPE = "api_offresdemploiv2 o2dsoffre"
_cache: tuple[str, float] | None = None
BUFFER_SECONDS = 60


class TokenResponse(NamedTuple):
    access_token: str
    expires_in: int
    token_type: str
    scope: str


def fetch_token(client_id: str, client_secret: str, scope: str = DEFAULT_SCOPE) -> TokenResponse:
    resp = requests.post(
        TOKEN_URL,
        params={"realm": REALM},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return TokenResponse(
        access_token=data["access_token"],
        expires_in=data["expires_in"],
        token_type=data.get("token_type", "Bearer"),
        scope=data.get("scope", scope),
    )


def get_access_token(
    client_id: str | None = None,
    client_secret: str | None = None,
    scope: str = DEFAULT_SCOPE,
    *,
    force_refresh: bool = False,
) -> str:
    global _cache
    client_id = client_id or os.environ.get("FRANCE_TRAVAIL_CLIENT_ID")
    client_secret = client_secret or os.environ.get("FRANCE_TRAVAIL_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("FRANCE_TRAVAIL_CLIENT_ID et FRANCE_TRAVAIL_CLIENT_SECRET doivent etre definis")

    now = time.monotonic()
    if not force_refresh and _cache is not None:
        token, expires_at = _cache
        if now < expires_at:
            return token

    result = fetch_token(client_id, client_secret, scope)
    _cache = (result.access_token, now + result.expires_in - BUFFER_SECONDS)
    return result.access_token


if __name__ == "__main__":
    load_project_env()
    token = get_access_token()
    print("Token obtenu (debut):", token[:30] + "...")

