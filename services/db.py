"""
MongoDB-based database service for backend-chatbot.
Replaces JSON file storage with Motor (async MongoDB driver).
"""
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import motor.motor_asyncio

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("BACKEND_DB_NAME", "chatbot_backend")

_client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
_db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None


async def init_db() -> None:
    """Connect to MongoDB and create required indexes."""
    global _client, _db
    _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    _db = _client[MONGO_DB]
    # fb_sessions: TTL index — auto-expire documents after 3600 s
    await _db.fb_sessions.create_index(
        "created_at_ts", expireAfterSeconds=3600, name="ttl_fb_sessions"
    )
    # routing: unique on page_id
    await _db.routing.create_index("page_id", unique=True, name="uq_routing_page_id")


async def close_db() -> None:
    if _client:
        _client.close()


def generate_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Facebook OAuth sessions (temporary, for code exchange) ──────────────────

async def save_fb_session(session_id: str, data: dict) -> None:
    """Save temporary OAuth session (user access token + pages list)."""
    doc = {
        "id": session_id,
        "created_at": now_iso(),
        "created_at_ts": time.time(),  # numeric field used by TTL index
        **data,
    }
    await _db.fb_sessions.replace_one({"id": session_id}, doc, upsert=True)


async def get_fb_session(session_id: str) -> Optional[dict]:
    return await _db.fb_sessions.find_one({"id": session_id}, {"_id": 0})


async def delete_fb_session(session_id: str) -> None:
    await _db.fb_sessions.delete_one({"id": session_id})


async def clear_fb_sessions() -> None:
    await _db.fb_sessions.delete_many({})


# ── Routing table: { page_id → { platform_id, worker_uuid, ... } } ──────────

async def get_routing() -> dict:
    """Return full routing table as a plain dict: { page_id → entry }."""
    docs = await _db.routing.find({}, {"_id": 0}).to_list(length=None)
    return {d["page_id"]: {k: v for k, v in d.items() if k != "page_id"} for d in docs}


async def set_routing_entry(page_id: str, entry: dict) -> None:
    """Upsert a single routing entry."""
    await _db.routing.replace_one(
        {"page_id": page_id},
        {"page_id": page_id, **{k: v for k, v in entry.items() if k != "page_id"}},
        upsert=True,
    )


async def delete_routing_entry(page_id: str) -> None:
    await _db.routing.delete_one({"page_id": page_id})


async def clear_routing() -> None:
    await _db.routing.delete_many({})

