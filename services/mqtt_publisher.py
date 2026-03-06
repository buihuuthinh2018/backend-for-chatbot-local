"""
MQTT Publisher — gửi Facebook webhook events tới Worker qua MQTT.

Thay thế HTTP forward (POST /api/v1/webhook/facebook).
Backend publish JWT-signed command lên topic w/tasks/{worker_uuid}.
Worker nhận và xử lý qua WorkflowExecutor (đầy đủ scenario DAG, SQLite, AI...).

Command format giống hệt TADAGRAM production:
{
    "command_id": "uuid",
    "type": "new_message",
    "jwt_token": "eyJ...",          ← HS256 signed, worker verify trước khi xử lý
    "payload": {
        "platform_type": "facebook",
        "platform_id": "...",
        "page_id": "...",
        "customer_id": "psid_...",
        "customer_name": "",
        "message": {
            "content_type": "text",
            "content_text": "..."
        },
        "platform_message_id": "mid_..."
    }
}
"""
import asyncio
import json
import logging
import os
import time
import uuid

import jwt
import paho.mqtt.publish as mqtt_publish

logger = logging.getLogger(__name__)

# ── Config từ .env ────────────────────────────────────────────────────────────
MQTT_BROKER_URL  = os.getenv("MQTT_BROKER_URL", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_USERNAME    = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD    = os.getenv("MQTT_PASSWORD", "")
JWT_SECRET_KEY   = os.getenv("JWT_SECRET_KEY", "")
WORKER_UUID      = os.getenv("WORKER_UUID", "")


def _build_jwt(worker_uuid: str) -> str:
    """
    Tạo JWT token để Worker verify.
    Payload: worker_uuid + iat + exp (5 phút).
    Algorithm: HS256 — phải khớp JWT_SECRET_KEY của Worker.
    """
    payload = {
        "worker_uuid": worker_uuid,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,  # 5 phút
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")


def _publish_sync(topic: str, message: dict) -> None:
    """
    Synchronous MQTT publish dùng paho single-publish helper.
    Chạy trong executor thread để không block async event loop.
    """
    auth = None
    if MQTT_USERNAME:
        auth = {"username": MQTT_USERNAME, "password": MQTT_PASSWORD}

    mqtt_publish.single(
        topic=topic,
        payload=json.dumps(message, ensure_ascii=False),
        hostname=MQTT_BROKER_URL,
        port=MQTT_BROKER_PORT,
        auth=auth,
        qos=1,
        retain=False,
    )


async def publish_message_event(
    platform: dict,
    sender_psid: str,
    event_type: str,
    event_data: dict,
) -> bool:
    """
    Publish một new_message command lên MQTT topic w/tasks/{worker_uuid}.

    Args:
        platform:    Platform dict từ DB (có id, page_id, ...)
        sender_psid: Page-Scoped User ID của người gửi
        event_type:  "message" hoặc "postback"
        event_data:  Raw Facebook event object

    Returns:
        True nếu publish thành công, False nếu lỗi.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env — không thể ký JWT")
        return False

    # ── Trích xuất nội dung tin nhắn ─────────────────────────────────────────
    content_text = ""
    platform_msg_id = ""
    command_type = "new_message"   # default — override for echo below

    if event_type == "message":
        msg_obj = event_data.get("message", {})
        content_text    = msg_obj.get("text", "")
        platform_msg_id = msg_obj.get("mid", "")
    elif event_type == "message_echo":
        msg_obj = event_data.get("message", {})
        content_text    = msg_obj.get("text", "")
        platform_msg_id = msg_obj.get("mid", "")
        command_type = "agent_message"
    elif event_type == "postback":
        pb = event_data.get("postback", {})
        content_text = pb.get("title", "") or pb.get("payload", "")

    # ── Build command ─────────────────────────────────────────────────────────
    command = {
        "command_id": str(uuid.uuid4()),
        "type": command_type,
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "platform_type": "facebook",
            "platform_id": platform["id"],
            "page_id": platform["page_id"],
            "customer_id": sender_psid,
            "customer_name": "",
            "message": {
                "content_type": "text",
                "content_text": content_text,
            },
            "platform_message_id": platform_msg_id,
            # Standalone extension: worker dùng khi không có token trong SQLite
            "_standalone": {
                "page_access_token": platform.get("page_access_token", ""),
            },
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT published → %s | type=%s | text=%s",
            topic, event_type, content_text[:80],
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT publish failed (%s:%s): %s", MQTT_BROKER_URL, MQTT_BROKER_PORT, e)
        return False


async def publish_agent_send(
    platform: dict,
    customer_id: str,
    message_text: str,
    conversation_id: str,
) -> bool:
    """
    Công bố lệnh gửi tin nhắn từ agent (UI) tới Worker qua MQTT.
    Worker nhận, gửi qua FB API, lưu vào DB với sender_type='agent'.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "send_message",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "platform_type": "facebook",
            "platform_id": platform["id"],
            "page_id": platform["page_id"],
            "customer_id": customer_id,
            "conversation_id": conversation_id,
            "sender_type": "agent",
            "message": {
                "text": message_text,
            },
            "_standalone": {
                "page_access_token": platform.get("page_access_token", ""),
            },
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT agent_send → %s | customer=%s | text=%s",
            topic, customer_id, message_text[:80],
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT agent_send failed: %s", e)
        return False


async def publish_scenarios(
    platform: dict,
) -> bool:
    """
    Publish slot-filling scenario definitions to the Worker via MQTT.
    Worker caches them per page_id for keyword matching at message time.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    page_id   = platform.get("page_id", "")
    scenarios = platform.get("slot_scenarios", [])

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "update_scenarios",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "page_id":   page_id,
            "scenarios": scenarios,
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT scenarios → %s | page=%s count=%d",
            topic, page_id, len(scenarios),
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT scenarios publish failed: %s", e)
        return False


async def publish_bot_config(
    platform: dict,
) -> bool:
    """
    Publish bot config (personality, tone, greeting, chatbot_name)
    cho một page cụ thể tới Worker qua MQTT.
    Worker lưu config này vào bộ nhớ và dùng khi xử lý tin nhắn của page đó.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "update_bot_config",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "page_id":              platform.get("page_id",             ""),
            "chatbot_name":         platform.get("chatbot_name",        ""),
            "page_name":            platform.get("page_name",           ""),
            "chatbot_description":  platform.get("chatbot_description", ""),
            "personality":          platform.get("personality",         "friendly"),
            "tone":                 platform.get("tone",                "informal"),
            "greeting":             platform.get("greeting",            ""),
            "rules":                platform.get("rules",               []),
            "prohibited_topics":    platform.get("prohibited_topics",   []),
            "language":             platform.get("language",            "vi"),
            "business_name":        platform.get("business_name",       ""),
            "business_address":     platform.get("business_address",    ""),
            "business_hours":       platform.get("business_hours",      ""),
            "business_phone":       platform.get("business_phone",      ""),
            "business_website":     platform.get("business_website",    ""),
            "business_type":        platform.get("business_type",       ""),
            "target_customers":     platform.get("target_customers",    ""),
            "faq":                  platform.get("faq",                 ""),
            "products":             platform.get("products",            []),
            "slot_scenarios":       platform.get("slot_scenarios",       []),
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT bot_config → %s | page=%s personality=%s",
            topic, platform.get("page_id"), platform.get("personality"),
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT bot_config publish failed: %s", e)
        return False


async def publish_onboarding_data(
    platform: dict,
    qa_pairs: list,
) -> bool:
    """
    Send raw onboarding Q&A pairs to the Worker via MQTT.

    Worker receives this, calls Gemini to normalise the answers into BotConfig
    fields, then merges + persists the resulting config.

    qa_pairs: list of {"question_id": str, "question_text": str, "answer": str}
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "process_onboarding",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "page_id":      platform.get("page_id", ""),
            "platform_id":  platform.get("id", ""),
            "platform_name": platform.get("page_name", ""),
            "qa_pairs":     qa_pairs,
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT process_onboarding → %s | page=%s pairs=%d",
            topic, platform.get("page_id"), len(qa_pairs),
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT process_onboarding publish failed: %s", e)
        return False


async def publish_delivery_event(
    platform: dict,
    customer_id: str,
    mids: list,
    watermark: int,
) -> bool:
    """
    Publish delivery confirmation event tới Worker.
    Worker sẽ dùng mids để gọi FB Graph API lấy nội dung tin nhắn
    rồi lưu vào conversation với sender_type phù hợp.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "fetch_delivered_messages",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "platform_type": "facebook",
            "platform_id": platform["id"],
            "page_id": platform["page_id"],
            "customer_id": customer_id,
            "mids": mids,
            "watermark": watermark,
            "_standalone": {
                "page_access_token": platform.get("page_access_token", ""),
            },
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info(
            "✅ MQTT delivery → %s | customer=%s | mids=%d",
            topic, customer_id, len(mids),
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT delivery publish failed: %s", e)
        return False


async def publish_auto_reply_config(
    platform_id: str,
    page_id: str,
    auto_replies: list[dict],
) -> bool:
    """
    Publish auto-reply config update to Worker via MQTT.

    Sent as a worker event on w/events/{worker_uuid} topic.
    Worker receives this and stores rules in local SQLite.

    Args:
        platform_id: Platform ID (from backend DB)
        page_id:     Facebook Page ID / Zalo OA ID
        auto_replies: List of {trigger: str, response: str}

    Returns:
        True if publish succeeded, False otherwise.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False

    event = {
        "event_type": "auto_reply_config_update",
        "jwt_token": _build_jwt(WORKER_UUID) if JWT_SECRET_KEY else "",
        "data": {
            "platform_id": platform_id,
            "page_id": page_id,
            "auto_replies": auto_replies,
        },
    }

    topic = f"w/events/{WORKER_UUID}"

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, event)
        logger.info(
            "✅ MQTT auto-reply config → %s | platform=%s page=%s rules=%d",
            topic, platform_id, page_id, len(auto_replies),
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT auto-reply publish failed: %s", e)
        return False


async def publish_set_agent_control(
    conversation_id: str,
    page_id: str,
    agent_controlled: bool,
) -> bool:
    """
    Gửi lệnh set_agent_control tới Worker qua MQTT.
    Worker cập nhật is_bot_handled và (nếu agent takeover) hủy slot session đang chạy.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env — không thể publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "set_agent_control",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "conversation_id": conversation_id,
            "page_id": page_id,
            "agent_controlled": agent_controlled,
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        mode = "agent" if agent_controlled else "bot"
        logger.info(
            "✅ MQTT set_agent_control → %s | conv=%s mode=%s",
            topic, conversation_id, mode,
        )
        return True
    except Exception as e:
        logger.error("❌ MQTT set_agent_control failed: %s", e)
        return False


async def publish_delete_page_data(page_id: str, platform_id: str) -> bool:
    """
    Gửi lệnh delete_page_data tới Worker qua MQTT.
    Worker xóa toàn bộ dữ liệu liên quan đến page_id trong SQLite:
    conversations, messages, customers, slot_leads, bot_configs, platform_tokens.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chưa set trong .env")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chưa set trong .env")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "delete_page_data",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "page_id": page_id,
            "platform_id": platform_id,
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info("✅ MQTT delete_page_data → %s | page_id=%s", topic, page_id)
        return True
    except Exception as e:
        logger.error("❌ MQTT delete_page_data failed: %s", e)
        return False


async def publish_set_bot_enabled(page_id: str, enabled: bool) -> bool:
    """
    Gửi lệnh set_bot_enabled tới Worker qua MQTT.
    Worker sẽ bật (enabled=True) hoặc tắt (enabled=False) toàn cục chatbot
    của page đó — khác với kiểm soát agent từng cuộc hội thoại.
    """
    if not WORKER_UUID or not JWT_SECRET_KEY:
        logger.warning("publish_set_bot_enabled: WORKER_UUID or JWT_SECRET_KEY not configured")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "set_bot_enabled",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "page_id": page_id,
            "enabled": enabled,
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        state = "✅ Bật" if enabled else "⏸️  Tắt"
        logger.info("%s chatbot (MQTT) → page_id=%s", state, page_id)
        return True
    except Exception as e:
        logger.error("❌ MQTT set_bot_enabled failed: %s", e)
        return False
