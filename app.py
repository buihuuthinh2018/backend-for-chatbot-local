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

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from services import facebook, db, mqtt_publisher

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

    for entry in data.get("entry", []):
        page_id = entry.get("id")

        # Tìm platform trong DB
        platform = db.get_platform_by_page_id(page_id)
        if not platform:
            logger.warning("No platform found for page_id %s — skipping", page_id)
            continue

        if platform.get("status") != "active":
            logger.info("Platform %s not active — skipping", page_id)
            continue

        for event in entry.get("messaging", []):
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

        # Filter out pages already connected
        existing_platforms = db.get_platforms()
        connected_page_ids = {p["page_id"] for p in existing_platforms if p.get("platform_type") == "facebook"}

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
    existing = db.get_platform_by_page_id(body.page_id)
    if existing:
        raise HTTPException(status_code=409, detail="This page is already connected.")

    page_token = selected_page["access_token"]

    try:
        # Subscribe webhook
        webhook_ok = await facebook.subscribe_page_webhook(body.page_id, page_token)
        if not webhook_ok:
            raise HTTPException(status_code=500, detail="Failed to subscribe webhook. Check page permissions.")

        # Save to DB (identity only — no chatbot config)
        platform = db.create_platform({
            "platform_type": "facebook",
            "page_id": body.page_id,
            "page_name": selected_page["name"],
            "page_category": selected_page.get("category", ""),
            "page_picture_url": selected_page.get("picture_url", ""),
            "fan_count": selected_page.get("fan_count", 0),
            "page_access_token": page_token,  # In production: encrypt this!
            "user_access_token": session["user_access_token"],
            "webhook_subscribed": True,
            "status": "active",
        })

        # Send default bot config to worker via MQTT
        await mqtt_publisher.publish_bot_config({
            **platform,
            "chatbot_name": selected_page["name"],
            "page_name": selected_page["name"],
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

        # 📝 Log kết quả platform được tạo
        _save_fb_connect_log("connect_result", {
            "page_id": platform["page_id"],
            "page_name": platform["page_name"],
            "page_category": platform.get("page_category"),
            "fan_count": platform.get("fan_count"),
            "page_picture_url": platform.get("page_picture_url"),
            "page_access_token": platform.get("page_access_token"),
            "user_access_token": platform.get("user_access_token"),
            "webhook_subscribed": platform.get("webhook_subscribed"),
            "status": platform.get("status"),
            "platform_id": platform.get("id"),
            "created_at": platform.get("created_at"),
        })

        return {
            "success": True,
            "platform": {
                "id": platform["id"],
                "platform_type": "facebook",
                "page_id": platform["page_id"],
                "page_name": platform["page_name"],
                "page_picture_url": platform["page_picture_url"],
                "fan_count": platform["fan_count"],
                "status": "active",
                "webhook_subscribed": True,
                "created_at": platform["created_at"],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to connect page: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ── Platforms CRUD ────────────────────────────────────────────────────────────

@app.get("/api/v1/platforms")
async def list_platforms():
    """List all connected platforms."""
    platforms = db.get_platforms()
    # Don't expose tokens
    safe_list = []
    for p in platforms:
        safe = {k: v for k, v in p.items() if "token" not in k.lower()}
        safe["access_token_status"] = "valid" if p.get("page_access_token") else "missing"
        safe_list.append(safe)
    return {"data": safe_list}


@app.get("/api/v1/platforms/{platform_id}")
async def get_platform(platform_id: str):
    """Get single platform detail."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    safe = {k: v for k, v in platform.items() if "token" not in k.lower()}
    safe["access_token_status"] = "valid" if platform.get("page_access_token") else "missing"
    return {"data": safe}


class PatchPlatformRequest(BaseModel):
    status: str | None = None
    access_token_status: str | None = None


@app.patch("/api/v1/platforms/{platform_id}")
async def patch_platform(platform_id: str, body: PatchPlatformRequest):
    """Partial update: status, access_token_status, etc."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    changes = {k: v for k, v in body.dict().items() if v is not None}
    if changes:
        db.update_platform(platform_id, changes)

    # When status changes, notify the worker to enable/disable the global chatbot
    if body.status is not None:
        from services.mqtt_publisher import publish_set_bot_enabled
        try:
            await publish_set_bot_enabled(
                page_id=platform["page_id"],
                enabled=(body.status == "active"),
            )
        except Exception as e:
            logger.warning("Failed to send set_bot_enabled MQTT: %s", str(e))

    updated = db.get_platform(platform_id)
    safe = {k: v for k, v in updated.items() if "token" not in k.lower()}
    return {"success": True, "data": safe}


@app.delete("/api/v1/platforms/{platform_id}")
async def disconnect_platform(platform_id: str):
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")

    # Hủy đăng ký page khỏi app (xóa subscribed_apps) — luôn thử nếu có token
    if platform.get("page_access_token"):
        try:
            ok = await facebook.unsubscribe_page_webhook(
                platform["page_id"],
                platform["page_access_token"],
            )
            if ok:
                logger.info("✅ Page unsubscribed from app: page_id=%s", platform["page_id"])
            else:
                logger.warning("⚠️  FB unsubscribe returned false for page_id=%s", platform["page_id"])
        except Exception as e:
            logger.warning("❌ Failed to unsubscribe page from app: %s", str(e))

    db.delete_platform(platform_id)

    # Notify worker to purge all local data for this page
    from services.mqtt_publisher import publish_delete_page_data
    try:
        await publish_delete_page_data(
            page_id=platform["page_id"],
            platform_id=platform_id,
        )
    except Exception as e:
        logger.warning("Failed to send delete_page_data MQTT: %s", str(e))

    return {"success": True, "message": "Platform disconnected"}


@app.get("/api/v1/platforms/{platform_id}/webhook-status")
async def webhook_status(platform_id: str):
    """
    Kiểm tra subscription thực tế trên Facebook:
    - Gọi GET /{page_id}/subscribed_apps — xem fields nào đang active
    - Trả về app mode hint và webhook log count
    """
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    page_token = platform.get("page_access_token")
    if not page_token:
        raise HTTPException(status_code=400, detail="No page access token stored")

    page_id = platform["page_id"]

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
        "page_name": platform.get("page_name"),
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
    """Force re-subscribe Facebook webhook — use when events stop arriving."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    page_token = platform.get("page_access_token")
    if not page_token:
        raise HTTPException(status_code=400, detail="No page access token stored")
    page_id = platform["page_id"]
    ok = await facebook.subscribe_page_webhook(page_id, page_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Facebook returned failure.")
    db.update_platform(platform_id, {"webhook_subscribed": True})
    return {
        "success": True,
        "page_id": page_id,
        "page_name": platform.get("page_name"),
        "message": "Webhook re-subscribed successfully.",
    }

# ── Agent send message ──────────────────────────────────────────────────────────

class AgentSendRequest(BaseModel):
    page_id: str
    customer_id: str
    message_text: str
    conversation_id: str = ""


@app.post("/api/v1/agent-send")
async def agent_send_message(body: AgentSendRequest):
    """
    Gửi tin nhắn từ agent (UI) tới một customer cụ thể.
    Backend tìm page bằng page_id, publish lệnh send_message qua MQTT tới Worker.
    Worker gửi qua FB API và lưu vào lịch sử chat với sender_type='agent'.
    """
    platform = db.get_platform_by_page_id(body.page_id)
    if not platform:
        raise HTTPException(status_code=404, detail=f"Platform not found for page_id={body.page_id}")
    if not platform.get("page_access_token"):
        raise HTTPException(status_code=400, detail="No page access token stored for this platform")

    ok = await mqtt_publisher.publish_agent_send(
        platform=platform,
        customer_id=body.customer_id,
        message_text=body.message_text,
        conversation_id=body.conversation_id,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to publish message to worker via MQTT")

    logger.info(
        "✅ Agent send queued: page=%s customer=%s text=%s",
        body.page_id, body.customer_id, body.message_text[:80],
    )
    return {"success": True}


# ── Agent / Bot control toggle ────────────────────────────────────────────────

class AgentControlRequest(BaseModel):
    conversation_id: str
    page_id: str
    agent_controlled: bool   # true → agent takes over; false → bot resumes


@app.post("/api/v1/agent-control")
async def set_agent_control(body: AgentControlRequest):
    """
    Toggle kiểm soát cuộc trò chuyện giữa Bot và Agent.

    Khi agent_controlled=true:
      - Worker tắt bot reply cho conversation này
      - Worker hủy slot-filling session đang chạy (nếu có)
    Khi agent_controlled=false:
      - Worker bật lại bot reply bình thường
    """
    ok = await mqtt_publisher.publish_set_agent_control(
        conversation_id=body.conversation_id,
        page_id=body.page_id,
        agent_controlled=body.agent_controlled,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to publish agent-control command to worker")

    mode = "agent" if body.agent_controlled else "bot"
    logger.info(
        "✅ Agent control set: conv=%s mode=%s",
        body.conversation_id, mode,
    )
    return {"success": True, "agent_controlled": body.agent_controlled, "mode": mode}


# ── Auto-Reply Config ─────────────────────────────────────────────────────────

class AutoReplyRule(BaseModel):
    trigger: str
    response: str


class AutoReplyConfigRequest(BaseModel):
    auto_replies: list[AutoReplyRule]


@app.put("/api/v1/platforms/{platform_id}/auto-replies")
async def update_auto_replies(platform_id: str, body: AutoReplyConfigRequest):
    """
    Save auto-reply (keyword → response) rules for a platform.
    1. Persist to platform record in JSON DB
    2. Publish to Worker via MQTT (worker stores in SQLite)
    """
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")

    rules = [{"trigger": r.trigger, "response": r.response} for r in body.auto_replies]

    # 1. Save to backend JSON DB
    db.update_platform(platform_id, {"quick_replies": rules})
    logger.info("✅ Auto-replies saved to DB: platform=%s rules=%d", platform_id, len(rules))

    # 2. Publish to Worker via MQTT
    page_id = platform.get("page_id", "")
    ok = await mqtt_publisher.publish_auto_reply_config(
        platform_id=platform_id,
        page_id=page_id,
        auto_replies=rules,
    )
    if not ok:
        logger.warning("⚠️  MQTT publish failed — worker will NOT have updated auto-replies until next sync")

    return {
        "success": True,
        "rules_count": len(rules),
        "mqtt_published": ok,
    }


@app.get("/api/v1/platforms/{platform_id}/auto-replies")
async def get_auto_replies(platform_id: str):
    """Get auto-reply rules for a platform."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")

    rules = platform.get("quick_replies", [])
    return {"data": rules}


# ── Slot-filling Scenarios ─────────────────────────────────────────────────────

class ScenariosRequest(BaseModel):
    scenarios: list = []


@app.get("/api/v1/platforms/{platform_id}/scenarios")
async def get_scenarios(platform_id: str):
    """Get slot-filling scenario definitions for a platform."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    return {"data": platform.get("slot_scenarios", [])}


@app.put("/api/v1/platforms/{platform_id}/scenarios")
async def update_scenarios(platform_id: str, body: ScenariosRequest):
    """Save slot-filling scenarios and push them to the worker via MQTT."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")

    db.update_platform(platform_id, {"slot_scenarios": body.scenarios})
    logger.info("✅ Scenarios saved: platform=%s count=%d", platform_id, len(body.scenarios))

    updated = db.get_platform(platform_id)
    ok = await mqtt_publisher.publish_scenarios(platform=updated)
    if not ok:
        logger.warning("⚠️  MQTT scenarios publish failed — worker will receive on next bot_config update")

    return {"success": True, "count": len(body.scenarios), "mqtt_published": ok}

class AIConfigRequest(BaseModel):
    personality: str = "friendly"
    tone: str = "informal"
    greeting: str = ""
    demographics: dict | None = None
    # Chatbot identity
    chatbot_name: str = ""
    chatbot_description: str = ""
    rules: list = []
    prohibited_topics: list = []
    language: str = "vi"
    # Business knowledge
    business_name: str = ""
    business_address: str = ""
    business_hours: str = ""
    business_phone: str = ""
    business_website: str = ""
    products: list = []
    # Slot-filling scenarios
    slot_scenarios: list = []


@app.get("/api/v1/platforms/{platform_id}/ai-config")
async def get_ai_config(platform_id: str):
    """Get saved AI config for a platform (personality, tone, greeting, business knowledge, scenarios)."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")
    return {
        "personality":          platform.get("personality",          "friendly"),
        "tone":                 platform.get("tone",                 "informal"),
        "greeting":             platform.get("greeting",             ""),
        "chatbot_name":         platform.get("chatbot_name",         ""),
        "chatbot_description":  platform.get("chatbot_description",  ""),
        "rules":                platform.get("rules",                []),
        "prohibited_topics":    platform.get("prohibited_topics",    []),
        "language":             platform.get("language",             "vi"),
        "demographics":         platform.get("demographics",         {}),
        "business_name":        platform.get("business_name",        ""),
        "business_address":     platform.get("business_address",     ""),
        "business_hours":       platform.get("business_hours",       ""),
        "business_phone":       platform.get("business_phone",       ""),
        "business_website":     platform.get("business_website",     ""),
        "products":             platform.get("products",             []),
        "slot_scenarios":       platform.get("slot_scenarios",       []),
    }


@app.put("/api/v1/platforms/{platform_id}/ai-config")
async def update_ai_config(platform_id: str, body: AIConfigRequest):
    """Save AI personality, tone, greeting, demographics for a platform."""
    platform = db.get_platform(platform_id)
    if not platform:
        raise HTTPException(status_code=404, detail="Platform not found")

    changes = {
        "personality": body.personality,
        "tone": body.tone,
        "greeting": body.greeting,
        "chatbot_name": body.chatbot_name,
        "chatbot_description": body.chatbot_description,
        "rules": body.rules,
        "prohibited_topics": body.prohibited_topics,
        "language": body.language,
        "business_name": body.business_name,
        "business_address": body.business_address,
        "business_hours": body.business_hours,
        "business_phone": body.business_phone,
        "business_website": body.business_website,
        "products": body.products,
        "slot_scenarios": body.slot_scenarios,
    }
    if body.demographics is not None:
        changes["demographics"] = body.demographics

    db.update_platform(platform_id, changes)
    logger.info("✅ AI config saved: platform=%s", platform_id)

    # Publish config to Worker via MQTT so it can use updated personality/tone/greeting
    updated_platform = db.get_platform(platform_id)
    ok = await mqtt_publisher.publish_bot_config(platform=updated_platform)
    if not ok:
        logger.warning("⚠️  MQTT bot_config publish failed — worker will use stale config until restart")

    return {"success": True, "mqtt_published": ok}


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8082"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
