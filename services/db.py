"""
JSON-file-based database service for backend-chatbot.
All data is persisted in the data/ directory as JSON files.
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_ROUTING_FILE  = os.path.join(_DATA_DIR, "routing.json")
_SESSIONS_FILE = os.path.join(_DATA_DIR, "fb_sessions.json")

# TTL for fb_sessions (seconds)
_SESSION_TTL = 3600


def _ensure_data_dir() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)


def _read_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _write_json(path: str, data: dict) -> None:
    _ensure_data_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def init_db() -> None:
    """Ensure data directory exists. No external connection needed."""
    _ensure_data_dir()


async def close_db() -> None:
    pass  # nothing to close


def generate_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Facebook OAuth sessions (temporary, for code exchange) ──────────────────

async def save_fb_session(session_id: str, data: dict) -> None:
    """Save temporary OAuth session (user access token + pages list)."""
    sessions = _read_json(_SESSIONS_FILE)
    sessions[session_id] = {
        "id": session_id,
        "created_at": now_iso(),
        "created_at_ts": time.time(),
        **data,
    }
    _write_json(_SESSIONS_FILE, sessions)


async def get_fb_session(session_id: str) -> Optional[dict]:
    sessions = _read_json(_SESSIONS_FILE)
    entry = sessions.get(session_id)
    if not entry:
        return None
    # Honour TTL
    if time.time() - entry.get("created_at_ts", 0) > _SESSION_TTL:
        sessions.pop(session_id)
        _write_json(_SESSIONS_FILE, sessions)
        return None
    return entry


async def delete_fb_session(session_id: str) -> None:
    sessions = _read_json(_SESSIONS_FILE)
    if session_id in sessions:
        sessions.pop(session_id)
        _write_json(_SESSIONS_FILE, sessions)


async def clear_fb_sessions() -> None:
    _write_json(_SESSIONS_FILE, {})


# ── Routing table: { page_id → { platform_id, worker_uuid, ... } } ──────────

async def get_routing() -> dict:
    """Return full routing table as a plain dict: { page_id → entry }."""
    return _read_json(_ROUTING_FILE)


async def set_routing_entry(page_id: str, entry: dict) -> None:
    """Upsert a single routing entry."""
    routing = _read_json(_ROUTING_FILE)
    routing[page_id] = {k: v for k, v in entry.items() if k != "page_id"}
    _write_json(_ROUTING_FILE, routing)


async def delete_routing_entry(page_id: str) -> None:
    routing = _read_json(_ROUTING_FILE)
    if page_id in routing:
        routing.pop(page_id)
        _write_json(_ROUTING_FILE, routing)


async def clear_routing() -> None:
    _write_json(_ROUTING_FILE, {})

