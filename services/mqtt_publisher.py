"""
MQTT Publisher â€” gá»­i Facebook webhook events tá»›i Worker qua MQTT.

Thay tháº¿ HTTP forward (POST /api/v1/webhook/facebook).
Backend publish JWT-signed command lÃªn topic w/tasks/{worker_uuid}.
Worker nháº­n vÃ  xá»­ lÃ½ qua WorkflowExecutor (Ä‘áº§y Ä‘á»§ scenario DAG, SQLite, AI...).

Command format giá»‘ng há»‡t TADAGRAM production:
{
    "command_id": "uuid",
    "type": "new_message",
    "jwt_token": "eyJ...",          â† HS256 signed, worker verify trÆ°á»›c khi xá»­ lÃ½
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

# â”€â”€ Config tá»« .env â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
MQTT_BROKER_URL  = os.getenv("MQTT_BROKER_URL", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_USERNAME    = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD    = os.getenv("MQTT_PASSWORD", "")
JWT_SECRET_KEY   = os.getenv("JWT_SECRET_KEY", "")
WORKER_UUID      = os.getenv("WORKER_UUID", "")


def _build_jwt(worker_uuid: str) -> str:
    """
    Táº¡o JWT token Ä‘á»ƒ Worker verify.
    Payload: worker_uuid + iat + exp (5 phÃºt).
    Algorithm: HS256 â€” pháº£i khá»›p JWT_SECRET_KEY cá»§a Worker.
    """
    payload = {
        "worker_uuid": worker_uuid,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,  # 5 phÃºt
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")


def _publish_sync(topic: str, message: dict) -> None:
    """
    Synchronous MQTT publish dÃ¹ng paho single-publish helper.
    Cháº¡y trong executor thread Ä‘á»ƒ khÃ´ng block async event loop.
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
    Publish má»™t new_message command lÃªn MQTT topic w/tasks/{worker_uuid}.

    Args:
        platform:    Platform dict tá»« DB (cÃ³ id, page_id, ...)
        sender_psid: Page-Scoped User ID cá»§a ngÆ°á»i gá»­i
        event_type:  "message" hoáº·c "postback"
        event_data:  Raw Facebook event object

    Returns:
        True náº¿u publish thÃ nh cÃ´ng, False náº¿u lá»—i.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chÆ°a set trong .env â€” khÃ´ng thá»ƒ publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chÆ°a set trong .env â€” khÃ´ng thá»ƒ kÃ½ JWT")
        return False

    # â”€â”€ TrÃ­ch xuáº¥t ná»™i dung tin nháº¯n â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    content_text = ""
    platform_msg_id = ""
    command_type = "new_message"   # default â€” override for echo below

    if event_type == "message":
        msg_obj = event_data.get("message", {})
        platform_msg_id = msg_obj.get("mid", "")
        # Check for attachments (images, videos, audio, files, stickers)
        attachments = msg_obj.get("attachments", [])
        if attachments:
            att = attachments[0]
            att_type = att.get("type", "image")   # "image" | "video" | "audio" | "file" | "fallback" | "sticker"
            att_url = att.get("payload", {}).get("url", "") or att.get("payload", {}).get("sticker_id", "")
            if att_type == "sticker":
                att_type = "image"
            content_type = att_type if att_type in ("image", "video", "audio", "file") else "image"
            content_url = att_url
        else:
            content_type = "text"
            content_url = ""
            content_text = msg_obj.get("text", "")
    elif event_type == "message_echo":
        msg_obj = event_data.get("message", {})
        platform_msg_id = msg_obj.get("mid", "")
        command_type = "agent_message"
        # Handle attachment echoes (e.g. agent sent image/video from FB inbox)
        attachments = msg_obj.get("attachments", [])
        if attachments:
            att = attachments[0]
            att_type = att.get("type", "image")
            att_url = att.get("payload", {}).get("url", "") or att.get("payload", {}).get("sticker_id", "")
            if att_type == "sticker":
                att_type = "image"
            content_type = att_type if att_type in ("image", "video", "audio", "file") else "image"
            content_url  = att_url
            content_text = ""
        else:
            content_text = msg_obj.get("text", "")
            content_type = "text"
            content_url  = ""
    elif event_type == "postback":
        pb = event_data.get("postback", {})
        content_text = pb.get("title", "") or pb.get("payload", "")
        content_type = "text"
        content_url  = ""

    # â”€â”€ Build command â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                "content_type": content_type,
                "content_text": content_text,
                "content_url": content_url,
            },
            "platform_message_id": platform_msg_id,
            # Standalone extension: worker dÃ¹ng khi khÃ´ng cÃ³ token trong SQLite
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
            "âœ… MQTT published â†’ %s | type=%s | content_type=%s | text=%s",
            topic, event_type, content_type, (content_text or content_url)[:80],
        )
        return True
    except Exception as e:
        logger.error("âŒ MQTT publish failed (%s:%s): %s", MQTT_BROKER_URL, MQTT_BROKER_PORT, e)
        return False


async def publish_save_platform(platform: dict) -> bool:
    """
    Gá»­i lá»‡nh save_platform tá»›i Worker qua MQTT.
    Worker nháº­n vÃ  lÆ°u platform metadata + access token vÃ o SQLite.
    Gá»i sau khi user connect Facebook page thÃ nh cÃ´ng.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chÆ°a set â€” khÃ´ng thá»ƒ publish save_platform")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chÆ°a set â€” khÃ´ng thá»ƒ kÃ½ JWT")
        return False

    command = {
        "command_id": str(uuid.uuid4()),
        "type": "save_platform",
        "jwt_token": _build_jwt(WORKER_UUID),
        "payload": {
            "platform_id":       platform["id"],
            "platform_type":     platform.get("platform_type", "facebook"),
            "page_id":           platform["page_id"],
            "page_access_token": platform.get("page_access_token", ""),
            "page_name":         platform.get("page_name", ""),
            "page_category":     platform.get("page_category", ""),
            "page_picture_url":  platform.get("page_picture_url", ""),
            "fan_count":         platform.get("fan_count", 0),
            "status":            platform.get("status", "active"),
            "created_at":        platform.get("created_at", ""),
        },
    }

    topic = f"w/tasks/{WORKER_UUID}"
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_sync, topic, command)
        logger.info("âœ… MQTT save_platform â†’ %s | page_id=%s", topic, platform["page_id"])
        return True
    except Exception as e:
        logger.error("âŒ MQTT save_platform failed: %s", e)
        return False


async def publish_bot_config(
    platform: dict,
) -> bool:
    """
    Publish bot config (personality, tone, greeting, chatbot_name)
    cho má»™t page cá»¥ thá»ƒ tá»›i Worker qua MQTT.
    Worker lÆ°u config nÃ y vÃ o bá»™ nhá»› vÃ  dÃ¹ng khi xá»­ lÃ½ tin nháº¯n cá»§a page Ä‘Ã³.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chÆ°a set trong .env â€” khÃ´ng thá»ƒ publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chÆ°a set trong .env")
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
            "âœ… MQTT bot_config â†’ %s | page=%s personality=%s",
            topic, platform.get("page_id"), platform.get("personality"),
        )
        return True
    except Exception as e:
        logger.error("âŒ MQTT bot_config publish failed: %s", e)
        return False


async def publish_delivery_event(
    platform: dict,
    customer_id: str,
    mids: list,
    watermark: int,
) -> bool:
    """
    Publish delivery confirmation event tá»›i Worker.
    Worker sáº½ dÃ¹ng mids Ä‘á»ƒ gá»i FB Graph API láº¥y ná»™i dung tin nháº¯n
    rá»“i lÆ°u vÃ o conversation vá»›i sender_type phÃ¹ há»£p.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chÆ°a set trong .env â€” khÃ´ng thá»ƒ publish MQTT")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chÆ°a set trong .env")
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
            "âœ… MQTT delivery â†’ %s | customer=%s | mids=%d",
            topic, customer_id, len(mids),
        )
        return True
    except Exception as e:
        logger.error("âŒ MQTT delivery publish failed: %s", e)
        return False


async def publish_delete_page_data(page_id: str, platform_id: str) -> bool:
    """
    Gá»­i lá»‡nh delete_page_data tá»›i Worker qua MQTT.
    Worker xÃ³a toÃ n bá»™ dá»¯ liá»‡u liÃªn quan Ä‘áº¿n page_id trong SQLite:
    conversations, messages, customers, slot_leads, bot_configs, platform_tokens.
    """
    if not WORKER_UUID:
        logger.error("WORKER_UUID chÆ°a set trong .env")
        return False
    if not JWT_SECRET_KEY:
        logger.error("JWT_SECRET_KEY chÆ°a set trong .env")
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
        logger.info("âœ… MQTT delete_page_data â†’ %s | page_id=%s", topic, page_id)
        return True
    except Exception as e:
        logger.error("\u274c MQTT delete_page_data failed: %s", e)
        return False
