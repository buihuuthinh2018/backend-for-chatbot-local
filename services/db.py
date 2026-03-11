"""
JSON-file based database.
Lưu dữ liệu vào data/*.json như một mini database.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def _ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _file_path(collection: str) -> str:
    _ensure_dir()
    return os.path.join(DATA_DIR, f"{collection}.json")


def _read(collection: str) -> list[dict]:
    path = _file_path(collection)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write(collection: str, data: list[dict]):
    path = _file_path(collection)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def generate_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Facebook OAuth sessions (temporary, for code exchange) ──────────────────

def save_fb_session(session_id: str, data: dict):
    """Save temporary OAuth session (user access token + pages list)."""
    sessions = _read("fb_sessions")
    record = {"id": session_id, "created_at": now_iso(), **data}
    # Replace if exists
    sessions = [s for s in sessions if s["id"] != session_id]
    sessions.append(record)
    _write("fb_sessions", sessions)


def get_fb_session(session_id: str) -> dict | None:
    for s in _read("fb_sessions"):
        if s["id"] == session_id:
            return s
    return None


def delete_fb_session(session_id: str):
    sessions = _read("fb_sessions")
    sessions = [s for s in sessions if s["id"] != session_id]
    _write("fb_sessions", sessions)

