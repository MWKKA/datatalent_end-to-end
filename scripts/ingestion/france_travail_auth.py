"""Authentification OAuth2 France Travail (client credentials)."""

from __future__ import annotations

import os
import time

import requests

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
SCOPE = "api_offresdemploiv2 o2dsoffre"
REQUEST_TIMEOUT_SECONDS = 30
_TOKEN_CACHE: dict[str, float | str] = {}


def _require_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"La variable d'environnement {name} est requise.")
    return value


def _is_cached_token_valid() -> bool:
    token = _TOKEN_CACHE.get("access_token")
    expires_at = float(_TOKEN_CACHE.get("expires_at", 0))
    return bool(token) and time.time() < expires_at


def get_access_token() -> str:
    """Retourne un token d'acces France Travail, avec cache en memoire."""
    if _is_cached_token_valid():
        return str(_TOKEN_CACHE["access_token"])

    client_id = _require_env("FRANCE_TRAVAIL_CLIENT_ID")
    client_secret = _require_env("FRANCE_TRAVAIL_CLIENT_SECRET")

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": SCOPE,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    payload = response.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 0))
    if not access_token:
        raise RuntimeError("Token France Travail absent de la reponse OAuth2.")

    # Marge de securite pour eviter d'utiliser un token presque expire.
    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = time.time() + max(expires_in - 60, 60)
    return str(access_token)
