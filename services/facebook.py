"""
Facebook Graph API integration.
Handles OAuth code exchange, page listing, page token, webhook subscription, send API.
"""
import hashlib
import hmac
import httpx
import os
import logging

logger = logging.getLogger(__name__)

FB_GRAPH_URL = "https://graph.facebook.com/v18.0"
FB_APP_ID = os.getenv("FB_APP_ID", "")
FB_APP_SECRET = os.getenv("FB_APP_SECRET", "")
FB_REDIRECT_URI = os.getenv("FB_REDIRECT_URI", "http://localhost:3458/auth/facebook/callback")


async def exchange_code_for_token(code: str) -> dict:
    """
    Step 1: Đổi authorization code → User Access Token.
    Facebook trả về short-lived token (~1-2h).
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{FB_GRAPH_URL}/oauth/access_token",
            params={
                "client_id": FB_APP_ID,
                "client_secret": FB_APP_SECRET,
                "redirect_uri": FB_REDIRECT_URI,
                "code": code,
            },
        )
        data = resp.json()

        if "error" in data:
            logger.error("FB code exchange failed: %s", data["error"])
            raise Exception(data["error"].get("message", "Facebook OAuth error"))

        return {
            "access_token": data["access_token"],
            "token_type": data.get("token_type", "bearer"),
            "expires_in": data.get("expires_in"),
        }


async def get_long_lived_token(short_token: str) -> dict:
    """
    Đổi short-lived token → long-lived token (~60 ngày).
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{FB_GRAPH_URL}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": FB_APP_ID,
                "client_secret": FB_APP_SECRET,
                "fb_exchange_token": short_token,
            },
        )
        data = resp.json()

        if "error" in data:
            logger.error("FB long-lived token exchange failed: %s", data["error"])
            raise Exception(data["error"].get("message", "Token exchange error"))

        return {
            "access_token": data["access_token"],
            "token_type": data.get("token_type", "bearer"),
            "expires_in": data.get("expires_in"),  # ~5184000 (60 days)
        }


async def get_user_pages(user_access_token: str) -> list[dict]:
    """
    Step 2: Dùng User Access Token để lấy danh sách Pages người dùng quản lý.
    Chỉ lấy pages mà user có quyền gửi tin nhắn.
    """
    pages = []
    url = f"{FB_GRAPH_URL}/me/accounts"
    params = {
        "access_token": user_access_token,
        "fields": "id,name,access_token,category,picture{url},fan_count,is_published",
        "limit": 100,
    }

    async with httpx.AsyncClient() as client:
        while url:
            resp = await client.get(url, params=params)
            data = resp.json()

            if "error" in data:
                logger.error("FB get pages failed: %s", data["error"])
                raise Exception(data["error"].get("message", "Failed to get pages"))

            for page in data.get("data", []):
                pages.append({
                    "page_id": page["id"],
                    "name": page["name"],
                    "category": page.get("category", ""),
                    "access_token": page["access_token"],  # Page Access Token (long-lived inherits)
                    "picture_url": page.get("picture", {}).get("data", {}).get("url", ""),
                    "fan_count": page.get("fan_count", 0),
                    "is_published": page.get("is_published", True),
                })

            # Pagination
            url = data.get("paging", {}).get("next")
            params = {}  # next URL already contains params

    return pages


async def subscribe_page_webhook(page_id: str, page_access_token: str) -> bool:
    """
    Step 3: Subscribe page to receive webhook events (messages, messaging_postbacks, etc.)
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{FB_GRAPH_URL}/{page_id}/subscribed_apps",
            params={
                "access_token": page_access_token,
                "subscribed_fields": (
                    "messages,"
                    "message_echoes,"
                    "messaging_postbacks,"
                    "messaging_optins,"
                    "messaging_referrals,"
                    "message_deliveries,"
                    "message_reads,"
                    "messaging_handovers,"
                    "messaging_policy_enforcement,"
                    "messaging_account_linking"
                ),
            },
        )
        data = resp.json()

        if "error" in data:
            logger.error("FB webhook subscription failed for page %s: %s", page_id, data["error"])
            return False

        success = data.get("success", False)
        if success:
            logger.info("✅ Webhook subscribed for page %s", page_id)
        return success


async def get_page_info(page_id: str, page_access_token: str) -> dict:
    """Lấy thông tin chi tiết của page."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{FB_GRAPH_URL}/{page_id}",
            params={
                "access_token": page_access_token,
                "fields": "id,name,category,picture{url},fan_count,is_published,link",
            },
        )
        data = resp.json()

        if "error" in data:
            raise Exception(data["error"].get("message", "Failed to get page info"))

        return data


async def unsubscribe_page_webhook(page_id: str, page_access_token: str) -> bool:
    """
    Hủy kết nối page khỏi app hoàn toàn.
    Gọi DELETE /{page_id}/subscribed_apps → xóa toàn bộ event subscriptions
    của page đối với app này trên Facebook.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{FB_GRAPH_URL}/{page_id}/subscribed_apps",
            params={"access_token": page_access_token},
        )
        data = resp.json()
        if "error" in data:
            logger.error(
                "FB unsubscribe_page_webhook error page_id=%s: %s",
                page_id,
                data["error"],
            )
            return False
        success = data.get("success", False)
        logger.info("FB unsubscribe_page_webhook page_id=%s → success=%s", page_id, success)
        return success


# ── Webhook signature verification ───────────────────────────────────────────

def verify_signature(payload: bytes, signature_header: str) -> bool:
    """
    Xác minh X-Hub-Signature-256 từ Facebook.
    signature_header format: 'sha256=<hex>'
    """
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(
        FB_APP_SECRET.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    received = signature_header[7:]  # strip 'sha256='
    return hmac.compare_digest(expected, received)

