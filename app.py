"""
backend-chatbot — FastAPI backend for chatbot configuration.

Endpoints:
  POST /api/v1/auth/facebook          — Exchange FB code → user token → list pages
  POST /api/v1/auth/facebook/connect   — User chọn page → lấy page token, subscribe webhook, lưu DB
  GET  /api/v1/platforms               — List connected platforms
  DELETE /api/v1/platforms/:id         — Disconnect platform (unsubscribe + delete)
"""
import os
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Request, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from services import facebook, db, mqtt_publisher

# ── Routing table: { page_id → { platform_id, worker_uuid } } ───────────────────
# Full platform data (tokens, config) lives in worker SQLite.
# Backend only needs this lightweight map to route webhooks.
import httpx

_ROUTING_FILE = os.path.join(os.path.dirname(__file__), "data", "routing.json")
_routing: dict = {}  # { page_id: { "platform_id": str, "worker_uuid": str } }
WORKER_API_URL = os.getenv("WORKER_API_URL", "http://localhost:8001")


def _load_routing() -> None:
    global _routing
    if os.path.exists(_ROUTING_FILE):
        try:
            with open(_ROUTING_FILE, "r", encoding="utf-8") as f:
                _routing = json.load(f)
            logger.info("📂 Loaded routing for %d page(s)", len(_routing))
        except Exception as e:
            logger.warning("Failed to load routing.json: %s", e)
            _routing = {}


def _save_routing() -> None:
    os.makedirs(os.path.dirname(_ROUTING_FILE), exist_ok=True)
    try:
        with open(_ROUTING_FILE, "w", encoding="utf-8") as f:
            json.dump(_routing, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Failed to save routing.json: %s", e)


def _find_routing_by_id(platform_id: str):
    """Find routing entry by platform_id. Returns (page_id, entry) or (None, None).
    Handles both: entry['platform_id'] (new format) and entry['id'] (legacy fat format).
    """
    for pid, entry in _routing.items():
        eid = entry.get("platform_id") or entry.get("id")
        if eid == platform_id:
            return pid, entry
    return None, None


async def _get_worker_platform(platform_id: str) -> dict | None:
    """Call worker /api/v1/platforms/{id}/token to get page_access_token + page_id."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{WORKER_API_URL}/api/v1/platforms/{platform_id}/token")
            if r.status_code == 200:
                return r.json()
            logger.warning("Worker token lookup returned %d for %s", r.status_code, platform_id)
    except Exception as e:
        logger.warning("Failed to get platform token from worker: %s", e)
    return None


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("backend-chatbot")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 backend-chatbot starting")
    logger.info("   FB_APP_ID: %s", os.getenv("FB_APP_ID", "(not set)"))
    logger.info("   FB_REDIRECT_URI: %s", os.getenv("FB_REDIRECT_URI", "(not set)"))
    logger.info("   FB_WEBHOOK_VERIFY_TOKEN: %s", "✅ set" if os.getenv("FB_WEBHOOK_VERIFY_TOKEN") else "⚠️  NOT SET")
    logger.info("   MQTT broker: %s:%s", os.getenv("MQTT_BROKER_URL", "localhost"), os.getenv("MQTT_BROKER_PORT", "1883"))
    logger.info("   WORKER_UUID: %s", os.getenv("WORKER_UUID", "(not set)"))
    logger.info("   JWT_SECRET: %s", "\u2705 set" if os.getenv("JWT_SECRET_KEY") else "\u26a0\ufe0f  NOT SET")
    _load_routing()
    yield
    logger.info("👋 backend-chatbot shutting down")


FB_WEBHOOK_VERIFY_TOKEN = os.getenv("FB_WEBHOOK_VERIFY_TOKEN", "")

# ── Webhook raw-payload logger ────────────────────────────────────────────────
_FB_LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "fb_webhooks")
os.makedirs(_FB_LOG_DIR, exist_ok=True)
_MAX_LOG_FILES = 500  # rotate oldest when exceeded


def _save_fb_webhook(data: dict, raw_bytes: int) -> None:
    """Pretty-print the full FB webhook payload to logger + save JSON file."""
    received_at = datetime.now(timezone.utc).isoformat()
    record = {
        "received_at": received_at,
        "payload_bytes": raw_bytes,
        "body": data,
    }
    # ── Log to stdout/file ────────────────────────────────────────────────────
    logger.info(
        "📥 [FB WEBHOOK RAW]\n%s",
        json.dumps(record, ensure_ascii=False, indent=2, default=str),
    )
    # ── Save JSON file ────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    object_type = data.get("object", "unknown")
    fname = f"{ts}_{object_type}.json"
    try:
        with open(os.path.join(_FB_LOG_DIR, fname), "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)
        # Rotate old files
        files = sorted(os.listdir(_FB_LOG_DIR))
        for old in files[: max(0, len(files) - _MAX_LOG_FILES)]:
            os.remove(os.path.join(_FB_LOG_DIR, old))
    except Exception as exc:
        logger.warning("Failed to save FB webhook log: %s", exc)


def _log_fb_event(entry_id: str, event: dict, event_type: str) -> None:
    """Log a single messaging event before MQTT publish."""
    logger.info(
        "📨 [FB EVENT] page_id=%s type=%s\n%s",
        entry_id,
        event_type,
        json.dumps(event, ensure_ascii=False, indent=2, default=str),
    )


# ── Facebook Connect flow logger ──────────────────────────────────────────────
_FB_CONNECT_LOG_DIR = os.path.join(os.path.dirname(__file__), "data", "fb_connect_logs")
os.makedirs(_FB_CONNECT_LOG_DIR, exist_ok=True)
_MAX_CONNECT_LOG_FILES = 200


def _save_fb_connect_log(action: str, data: dict) -> None:
    """
    Log toàn bộ payload của user khi thực hiện kết nối Facebook.

    action: "auth_code"       — user gửi OAuth code, nhận token + danh sách pages
            "connect_request" — user chọn page và submit kết nối
            "connect_result"  — platform đã được tạo thành công

    Lưu file vào data/fb_connect_logs/YYYYMMDD_HHMMSS_ms_<action>.json
    """
    logged_at = datetime.now(timezone.utc).isoformat()
    record = {
        "logged_at": logged_at,
        "action": action,
        "payload": data,
    }
    # ── Log to stdout/file ────────────────────────────────────────────────────
    logger.info(
        "🔗 [FB CONNECT] action=%s\n%s",
        action,
        json.dumps(record, ensure_ascii=False, indent=2, default=str),
    )
    # ── Save JSON file ────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    fname = f"{ts}_{action}.json"
    try:
        with open(os.path.join(_FB_CONNECT_LOG_DIR, fname), "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)
        files = sorted(os.listdir(_FB_CONNECT_LOG_DIR))
        for old in files[: max(0, len(files) - _MAX_CONNECT_LOG_FILES)]:
            os.remove(os.path.join(_FB_CONNECT_LOG_DIR, old))
    except Exception as exc:
        logger.warning("Failed to save FB connect log: %s", exc)


app = FastAPI(
    title="backend-chatbot",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow frontend dev server
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3458")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "backend-chatbot"}


# ── Facebook Webhook ──────────────────────────────────────────────────────────

@app.get("/webhook/facebook", response_class=PlainTextResponse)
async def fb_webhook_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    """
    Facebook gọi endpoint này để xác minh webhook.
    Cấu hình trong App Dashboard → Webhooks → Verify Token = FB_WEBHOOK_VERIFY_TOKEN
    """
    if hub_mode == "subscribe" and hub_verify_token == FB_WEBHOOK_VERIFY_TOKEN:
        logger.info("✅ Webhook verified by Facebook")
        return hub_challenge or ""
    logger.warning("❌ Webhook verification failed (token mismatch)")
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook/facebook")
async def fb_webhook_event(request: Request):
    """
    Facebook gửi message events về đây.
    Backend chỉ verify signature rồi forward toàn bộ sang Worker.
    Worker sẽ xử lý logic + reply.
    """
    body = await request.body()
    logger.info("=" * 50)
    logger.info("🔔 Facebook webhook event received (%d bytes)", len(body))

    # Verify signature (bỏ qua nếu APP_SECRET chưa set — chỉ dùng trong dev)
    sig_header = request.headers.get("X-Hub-Signature-256", "")
    if sig_header and not facebook.verify_signature(body, sig_header):
        logger.warning("❌ Invalid webhook signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    data = await request.json()

    # 📸 Log nguyên bản toàn bộ webhook payload + lưu file JSON
    _save_fb_webhook(data, len(body))

    if data.get("object") != "page":
        return {"status": "ignored"}

    published = 0

    for fb_entry in data.get("entry", []):
        page_id = fb_entry.get("id")

        # Look up routing (page_id → platform_id)
        routing_entry = _routing.get(page_id)
        if not routing_entry:
            logger.warning("No routing found for page_id %s — skipping", page_id)
            continue

        # Minimal platform dict: worker has full data in SQLite
        # Supports both new format (platform_id key) and legacy fat format (id key)
        platform = {
            "id": routing_entry.get("platform_id") or routing_entry.get("id"),
            "page_id": page_id,
        }

        for event in fb_entry.get("messaging", []):
            sender_psid  = event.get("sender",    {}).get("id", "")
            recipient_id = event.get("recipient",  {}).get("id", "")
            is_echo = "message" in event and event["message"].get("is_echo", False)

            # ── Detect event type for logging ─────────────────────────────────
            if is_echo:
                raw_event_type = "message_echo"
            elif "message" in event:
                raw_event_type = "message"
            elif "postback" in event:
                raw_event_type = "postback"
            elif "read" in event:
                raw_event_type = "message_reads"
            elif "delivery" in event:
                raw_event_type = "message_deliveries"
            elif "referral" in event:
                raw_event_type = "messaging_referrals"
            elif "pass_thread_control" in event:
                raw_event_type = "messaging_handovers"
            elif "take_thread_control" in event:
                raw_event_type = "messaging_handovers_take"
            elif "request_thread_control" in event:
                raw_event_type = "messaging_handovers_request"
            elif "account_linking" in event:
                raw_event_type = "messaging_account_linking"
            elif "policy-enforcement" in event:
                raw_event_type = "messaging_policy_enforcement"
            elif "optin" in event:
                raw_event_type = "messaging_optins"
            else:
                raw_event_type = "unknown"

            # 📝 Log EVERY event regardless of whether we process it
            _log_fb_event(page_id, event, raw_event_type)

            # ── Route to MQTT only for processable event types ────────────────
            if is_echo:
                event_type    = "message_echo"
                customer_psid = recipient_id   # the human who received the message
            elif raw_event_type in ("message", "postback"):
                # Skip if the page itself is the sender (non-echo case)
                if sender_psid == page_id:
                    logger.debug("Skipping non-echo event from page itself (sender=%s)", sender_psid)
                    continue
                event_type    = raw_event_type
                customer_psid = sender_psid
            elif raw_event_type == "message_deliveries":
                # Forward delivery event — worker fetches message content from FB Graph API
                delivery = event.get("delivery", {})
                mids = delivery.get("mids", [])
                watermark = delivery.get("watermark", 0)
                if mids:
                    ok = await mqtt_publisher.publish_delivery_event(
                        platform=platform,
                        customer_id=sender_psid,
                        mids=mids,
                        watermark=watermark,
                    )
                    if ok:
                        published += 1
                    else:
                        logger.error("🚨 MQTT publish FAILED for delivery from %s", sender_psid)
                else:
                    logger.debug("delivery event has no mids — skipping")
                continue
            else:
                # Not yet handled — already logged above, skip MQTT publish
                logger.debug("Event type '%s' received but not forwarded to worker", raw_event_type)
                continue

            logger.info("📤 Publishing to MQTT → broker=%s:%s worker=%s",
                os.getenv("MQTT_BROKER_URL", "localhost"),
                os.getenv("MQTT_BROKER_PORT", "1883"),
                os.getenv("WORKER_UUID", "NOT SET"),
            )
            ok = await mqtt_publisher.publish_message_event(
                platform=platform,
                sender_psid=customer_psid,
                event_type=event_type,
                event_data=event,
            )
            if ok:
                published += 1
            else:
                logger.error("🚨 MQTT publish FAILED for %s from %s", event_type, customer_psid)

    logger.info("✅ Webhook processed: published=%d", published)
    return {"status": "ok", "published": published}


# ── Facebook OAuth ────────────────────────────────────────────────────────────

class FacebookCodeRequest(BaseModel):
    code: str


class FacebookConnectRequest(BaseModel):
    session_id: str
    page_id: str


@app.post("/api/v1/auth/facebook")
async def facebook_auth(body: FacebookCodeRequest):
    """
    Frontend gửi authorization code từ Facebook Login.
    Backend:
    1. Đổi code → User Access Token
    2. Đổi tiếp → Long-lived Token (60 ngày)
    3. Gọi /me/accounts → danh sách Pages
    4. Trả về session_id + pages list cho frontend hiển thị
    """
    try:
        # Step 1: Code → Short-lived User Token
        token_data = await facebook.exchange_code_for_token(body.code)
        short_token = token_data["access_token"]
        logger.info("✅ Got short-lived user token")

        # Step 2: Short → Long-lived Token
        long_data = await facebook.get_long_lived_token(short_token)
        user_token = long_data["access_token"]
        logger.info("✅ Got long-lived user token (expires_in: %s)", long_data.get("expires_in"))

        # Step 3: Get user's pages
        pages = await facebook.get_user_pages(user_token)
        logger.info("✅ Found %d page(s)", len(pages))

        # 📝 Log toàn bộ thông tin user + pages vừa nhận từ Facebook
        _save_fb_connect_log("auth_code", {
            "short_token_data": token_data,
            "long_token_data": long_data,
            "user_access_token": user_token,
            "pages_count": len(pages),
            "pages": pages,
        })

        # Filter out pages already connected (use routing as source of truth)
        connected_page_ids = set(_routing.keys())

        pages_response = []
        for page in pages:
            pages_response.append({
                "page_id": page["page_id"],
                "name": page["name"],
                "category": page["category"],
                "picture_url": page["picture_url"],
                "fan_count": page["fan_count"],
                "is_published": page["is_published"],
                "already_connected": page["page_id"] in connected_page_ids,
            })

        # Save session temporarily (user_token + pages with their tokens)
        session_id = db.generate_id()
        db.save_fb_session(session_id, {
            "user_access_token": user_token,
            "pages": pages,  # includes page access_tokens
        })

        return {
            "success": True,
            "session_id": session_id,
            "pages": pages_response,
        }

    except Exception as e:
        logger.error("Facebook auth failed: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/v1/auth/facebook/connect")
async def facebook_connect(body: FacebookConnectRequest):
    """
    User đã chọn page → Backend:
    1. Lấy Page Access Token từ session
    2. Subscribe webhook
    3. Lưu platform vào DB
    """
    # Get session
    session = db.get_fb_session(body.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session expired or not found. Please login again.")

    # Find selected page in session
    selected_page = None
    for page in session.get("pages", []):
        if page["page_id"] == body.page_id:
            selected_page = page
            break

    if not selected_page:
        raise HTTPException(status_code=400, detail="Page not found in your account list.")

    # 📝 Log toàn bộ payload request kết nối của user
    _save_fb_connect_log("connect_request", {
        "session_id": body.session_id,
        "page_id": body.page_id,
        "selected_page": selected_page,
        "user_access_token": session.get("user_access_token"),
    })

    # Check if already connected
    if body.page_id in _routing:
        raise HTTPException(status_code=409, detail="This page is already connected.")

    page_token = selected_page["access_token"]

    try:
        # Subscribe webhook
        webhook_ok = await facebook.subscribe_page_webhook(body.page_id, page_token)
        if not webhook_ok:
            raise HTTPException(status_code=500, detail="Failed to subscribe webhook. Check page permissions.")

        # Build platform record
        created_at = datetime.now(timezone.utc).isoformat()
        platform_id = db.generate_id()
        platform = {
            "id": platform_id,
            "platform_type": "facebook",
            "page_id": body.page_id,
            "page_name": selected_page["name"],
            "page_category": selected_page.get("category", ""),
            "page_picture_url": selected_page.get("picture_url", ""),
            "fan_count": selected_page.get("fan_count", 0),
            "page_access_token": page_token,
            "user_access_token": session["user_access_token"],
            "webhook_subscribed": True,
            "status": "active",
            "created_at": created_at,
        }

        # ① Save minimal routing entry (page_id → platform_id + worker_uuid)
        _routing[body.page_id] = {
            "platform_id": platform_id,
            "worker_uuid": os.getenv("WORKER_UUID", ""),
        }
        _save_routing()

        # ② Send full platform data to worker SQLite via MQTT
        await mqtt_publisher.publish_save_platform(platform)

        # ③ Send default bot config to worker
        await mqtt_publisher.publish_bot_config({
            **platform,
            "chatbot_name": selected_page["name"],
            "chatbot_description": "",
            "personality": "friendly",
            "tone": "informal",
            "greeting": "",
            "rules": [],
            "prohibited_topics": [],
            "language": "vi",
        })

        # Cleanup session
        db.delete_fb_session(body.session_id)

        logger.info("✅ Connected page '%s' (ID: %s)", selected_page["name"], body.page_id)

        # 📝 Log kết quả
        _save_fb_connect_log("connect_result", {
            "page_id": platform["page_id"],
            "page_name": platform["page_name"],
            "platform_id": platform_id,
            "created_at": created_at,
        })

        return {
            "success": True,
            "platform": {
                "id": platform_id,
                "platform_type": "facebook",
                "page_id": body.page_id,
                "page_name": selected_page["name"],
                "page_picture_url": selected_page.get("picture_url", ""),
                "fan_count": selected_page.get("fan_count", 0),
                "status": "active",
                "webhook_subscribed": True,
                "created_at": created_at,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to connect page: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ── Platforms management ─────────────────────────────────────────────────────
# GET / detail / patch → served by worker at /api/v1/platforms
# DELETE         → backend handles (needs to unsubscribe FB webhook + update routing)

@app.delete("/api/v1/platforms/{platform_id}")
async def disconnect_platform(platform_id: str):
    page_id, entry = _find_routing_by_id(platform_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Platform not found")

    # Get page_access_token from worker to unsubscribe webhook
    worker_data = await _get_worker_platform(platform_id)
    if worker_data and worker_data.get("page_access_token"):
        try:
            ok = await facebook.unsubscribe_page_webhook(
                worker_data["page_id"],
                worker_data["page_access_token"],
            )
            if ok:
                logger.info("✅ Page unsubscribed: page_id=%s", page_id)
            else:
                logger.warning("⚠️  FB unsubscribe returned false for page_id=%s", page_id)
        except Exception as e:
            logger.warning("❌ Failed to unsubscribe: %s", str(e))
    else:
        logger.warning("⚠️  No token from worker for page_id=%s — skipping FB unsubscribe", page_id)

    # Remove from routing
    _routing.pop(page_id, None)
    _save_routing()

    # Tell worker to purge all local data for this page
    from services.mqtt_publisher import publish_delete_page_data
    try:
        await publish_delete_page_data(page_id=page_id, platform_id=platform_id)
    except Exception as e:
        logger.warning("Failed to send delete_page_data MQTT: %s", str(e))

    return {"success": True, "message": "Platform disconnected"}


@app.get("/api/v1/platforms/{platform_id}/webhook-status")
async def webhook_status(platform_id: str):
    """
    Kiểm tra subscription thực tế trên Facebook.
    Token được lấy từ worker SQLite.
    """
    routing_page_id, entry = _find_routing_by_id(platform_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Platform not found")

    worker_data = await _get_worker_platform(platform_id)
    if not worker_data or not worker_data.get("page_access_token"):
        raise HTTPException(status_code=400, detail="Token not available from worker")

    page_token = worker_data["page_access_token"]
    page_id = worker_data.get("page_id") or routing_page_id
    page_name = worker_data.get("page_name", "")

    # Check what FB actually has subscribed
    fb_data = await facebook.get_page_subscriptions(page_id, page_token)

    # Count saved webhook log files
    log_files = sorted(os.listdir(_FB_LOG_DIR)) if os.path.isdir(_FB_LOG_DIR) else []

    subscribed_apps = fb_data.get("data", [])
    subscribed_fields = []
    for app_entry in subscribed_apps:
        subscribed_fields = app_entry.get("subscribed_fields", [])

    return {
        "page_id": page_id,
        "page_name": page_name,
        "fb_response": fb_data,
        "subscribed_fields": subscribed_fields,
        "webhook_logs_count": len(log_files),
        "last_webhook_files": log_files[-5:],
        "warning": (
            "App đang ở Development Mode — chỉ admin/tester mới nhận được webhook. "
            "Vào App Dashboard → App Mode → Live để nhận từ tất cả người dùng."
        ),
    }


@app.post("/api/v1/platforms/{platform_id}/resubscribe-webhook")
async def resubscribe_webhook(platform_id: str):
    """Force re-subscribe Facebook webhook. Token fetched from worker SQLite."""
    _, entry = _find_routing_by_id(platform_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Platform not found")

    worker_data = await _get_worker_platform(platform_id)
    if not worker_data or not worker_data.get("page_access_token"):
        raise HTTPException(status_code=400, detail="Token not available from worker")

    page_id = worker_data["page_id"]
    page_token = worker_data["page_access_token"]
    ok = await facebook.subscribe_page_webhook(page_id, page_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Facebook returned failure.")
    # Update extra in worker SQLite
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.patch(
                f"{WORKER_API_URL}/api/v1/platforms/{platform_id}",
                json={"webhook_subscribed": True},
            )
    except Exception:
        pass  # Non-critical
    return {
        "success": True,
        "page_id": page_id,
        "page_name": worker_data.get("page_name", ""),
        "message": "Webhook re-subscribed successfully.",
    }

@app.post("/api/v1/admin/resync-platforms")
async def resync_platforms():
    """
    Admin endpoint: re-syncs all platforms to worker SQLite.
    Reads platforms.json if available, otherwise uses routing.json (fat format from legacy migration).
    Call once after migration or when worker loses its SQLite.
    """
    worker_uuid = os.getenv("WORKER_UUID", "")
    platforms: list = []

    # Try platforms.json first
    platforms_file = os.path.join(os.path.dirname(__file__), "data", "platforms.json")
    if os.path.exists(platforms_file):
        with open(platforms_file, "r", encoding="utf-8") as f:
            platforms = json.load(f)
    else:
        # Fall back to routing.json entries that have token data (legacy fat format)
        for page_id, entry in _routing.items():
            if entry.get("page_access_token"):
                p = dict(entry)
                p["page_id"] = page_id
                if not p.get("id"):
                    p["id"] = p.get("platform_id", "")
                platforms.append(p)

    if not platforms:
        return {"message": "No platform data found to sync", "synced": 0}

    synced = 0
    for p in platforms:
        page_id = p.get("page_id")
        if not page_id:
            continue
        # Upsert routing with minimal format
        if page_id not in _routing or not _routing[page_id].get("platform_id"):
            _routing[page_id] = {
                "platform_id": p.get("id") or p.get("platform_id"),
                "worker_uuid": worker_uuid,
            }
        # Push full platform data to worker
        try:
            await mqtt_publisher.publish_save_platform(p)
            synced += 1
        except Exception as e:
            logger.warning("Failed to sync page_id=%s: %s", page_id, e)

    _save_routing()
    logger.info("✅ Resync complete: %d platform(s)", synced)
    return {"message": f"Synced {synced} platform(s) to worker", "synced": synced}


# ── Worker config endpoint ────────────────────────────────────────────────────────────
# Onboarding questions live here so all workers fetch the same list.
# Bump _WORKER_CONFIG_VERSION whenever you change onboarding questions —
# workers check the X-Config-Version header every 4 hours and reload when it changes.

_WORKER_CONFIG_VERSION = 1

_ONBOARDING_QUESTIONS = [
    {
        "id": "business_name",
        "question": "Tên doanh nghiệp / thương hiệu của bạn là gì?",
        "placeholder": "VD: Siêu Nhân Điện Quang",
        "type": "text",
        "required": True,
    },
    {
        "id": "business_type",
        "question": "Doanh nghiệp bạn kinh doanh lĩnh vực gì?",
        "placeholder": "VD: Bán lẻ thiết bị điện, dịch vụ lắp đặt",
        "type": "text",
        "required": True,
    },
    {
        "id": "products",
        "question": "Liệt kê chi tiết sản phẩm / dịch vụ (kèm giá nếu có)? (tuỳ chọn)",
        "placeholder": "VD:\n- Bóng đèn LED 9W — 45.000đ\n- Dịch vụ sửa điện tại nhà — liên hệ báo giá\n- Dây điện đồng 1.5mm — 12.000đ/m",
        "type": "textarea",
        "required": False,
    },
    {
        "id": "target_customers",
        "question": "Khách hàng mục tiêu của bạn là ai?",
        "placeholder": "VD: Hộ gia đình, thợ điện, chủ công trình",
        "type": "text",
        "required": False,
    },
    {
        "id": "tone",
        "question": "Chatbot nên giao tiếp theo phong cách nào?",
        "type": "choice",
        "required": True,
        "choices": ["Thân thiện, gần gũi", "Chuyên nghiệp, lịch sự", "Vui vẻ, hài hước"],
    },
    {
        "id": "contact_info",
        "question": "Số điện thoại hoặc địa chỉ liên hệ của shop?",
        "placeholder": "VD: 0901 234 567 — 123 Nguyễn Văn A, Q.1",
        "type": "text",
        "required": False,
    },
    {
        "id": "faq",
        "question": "Câu hỏi thường gặp nhất của khách hàng? (tuỳ chọn)",
        "placeholder": "VD: Giờ mở cửa? Có giao hàng không? Bảo hành bao lâu?",
        "type": "textarea",
        "required": False,
    },
]


@app.get("/api/v1/worker/config")
async def get_worker_config(response: Response):
    """
    Worker configuration endpoint — polled by ChatbotConfigManager at startup
    and every 4 hours.  Workers read `onboarding_questions` from here so all
    parallel workers stay in sync.

    Response header X-Config-Version lets workers skip a full reload when the
    version hasn't changed (HEAD request from _check_version()).
    """
    response.headers["X-Config-Version"] = str(_WORKER_CONFIG_VERSION)
    return {
        "version": _WORKER_CONFIG_VERSION,
        "scenarios": [],
        "knowledge_base": [],
        "platforms": [],
        "business_info": {},
        "onboarding_questions": _ONBOARDING_QUESTIONS,
    }

# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8082"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
