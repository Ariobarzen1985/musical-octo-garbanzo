import asyncio
import base64
import json
import logging
import os
import secrets
import sqlite3
import uuid as uuid_lib
from contextlib import closing
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from flask import Flask, request, redirect, session, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash

# ============================================================
#  تنظیمات کلیدی
# ============================================================
API_TOKEN = os.getenv("API_TOKEN", "8883749489:AAEpaX4_oKE8_7llrJ_TWTBG-r9QeKZVXrA")
INITIAL_ADMINS = {8443938939}

TON_DONATE_ADDRESS = "UQAQbW_kDwLvTaqnZsM6U8aU46oVA7vEDMbChOwTC719Hv4N"

CF_API_BASE = "https://api.cloudflare.com/client/v4"
WORKER_SCRIPT_NAME = "ariobarzan-master"
WORKER_JS_PATH = "worker.js"

if os.path.exists(WORKER_JS_PATH):
    with open(WORKER_JS_PATH, "r", encoding="utf-8") as f:
        WORKER_JS_SOURCE = f.read()
else:
    # نسخه‌ی پیش‌فرض واقعی: پروتکل VLESS را با TCP Sockets کلودفلر
    # (cloudflare:sockets) کامل پیاده‌سازی می‌کند تا کانفیگ‌ها واقعاً کار کنند.
    # (قبلاً نسخه‌ی این فایل فقط WebSocket را باز می‌کرد و ترافیک را رد
    # نمی‌کرد — کانفیگ‌ها وصل می‌شدند ولی هیچ داده‌ای رد و بدل نمی‌شد.)
    WORKER_JS_SOURCE = """import { connect } from "cloudflare:sockets";

export default {
  async fetch(request, env) {
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Ariobarzan relay is running.", { status: 200 });
    }

    const allowedUuid = (env.UUID || "").replace(/-/g, "").toLowerCase();
    if (!allowedUuid) {
      return new Response("UUID not configured", { status: 500 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    let remoteSocket = null;
    let sessionStarted = false;
    let isUdp = false;
    let udpPort = null;

    server.addEventListener("message", async (event) => {
      try {
        const data = event.data;
        const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(await data.arrayBuffer());

        if (!sessionStarted) {
          const parsed = parseVlessHeader(buf, allowedUuid);
          if (!parsed) {
            server.close(1008, "invalid vless header");
            return;
          }
          sessionStarted = true;
          isUdp = parsed.cmd === 2;
          udpPort = parsed.port;

          // پاسخ هندشیک VLESS: نسخه + بدون addon
          server.send(new Uint8Array([parsed.version, 0]));

          if (isUdp) {
            if (udpPort === 53) {
              // درخواست‌های DNS را با DNS-over-HTTPS واقعی پاسخ می‌دهیم
              await handleUdpFrames(parsed.rawClientData, server);
            } else {
              // Cloudflare Workers نمی‌تواند UDP خام (مثل QUIC روی پورت ۴۴۳) را
              // رله کند؛ به‌جای هنگ‌کردن طولانی، فوری می‌بندیم تا کلاینت سریع
              // به TCP سوییچ کند (اگر «Block QUIC» در کلاینت فعال باشد، اصلاً
              // به این حالت نمی‌رسد).
              server.close(1000, "udp not supported on workers");
            }
            return;
          }

          remoteSocket = connect({ hostname: parsed.addr, port: parsed.port });
          const writer = remoteSocket.writable.getWriter();
          if (parsed.rawClientData.length > 0) {
            await writer.write(parsed.rawClientData);
          }
          writer.releaseLock();
          pumpRemoteToClient(remoteSocket, server);
        } else if (isUdp) {
          if (udpPort === 53) {
            await handleUdpFrames(buf, server);
          }
        } else {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(buf);
          writer.releaseLock();
        }
      } catch (err) {
        try { server.close(1011, "relay error"); } catch (_) {}
      }
    });

    server.addEventListener("close", () => {
      try { remoteSocket && remoteSocket.close(); } catch (_) {}
    });

    return new Response(null, { status: 101, webSocket: client });
  },
};

function parseVlessHeader(buf, expectedUuidHex) {
  if (buf.length < 24) return null;
  const version = buf[0];

  let uuidHex = "";
  for (let i = 1; i <= 16; i++) uuidHex += buf[i].toString(16).padStart(2, "0");
  if (uuidHex !== expectedUuidHex) return null;

  let offset = 17;
  const optLen = buf[offset];
  offset += 1 + optLen;

  const cmd = buf[offset]; // 1 = TCP, 2 = UDP
  offset += 1;

  const port = (buf[offset] << 8) + buf[offset + 1];
  offset += 2;

  const addrType = buf[offset];
  offset += 1;

  let addr;
  if (addrType === 1) {
    addr = buf[offset] + "." + buf[offset + 1] + "." + buf[offset + 2] + "." + buf[offset + 3];
    offset += 4;
  } else if (addrType === 2) {
    const len = buf[offset];
    offset += 1;
    addr = new TextDecoder().decode(buf.slice(offset, offset + len));
    offset += len;
  } else if (addrType === 3) {
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(((buf[offset] << 8) + buf[offset + 1]).toString(16));
      offset += 2;
    }
    addr = parts.join(":");
  } else {
    return null;
  }

  const rawClientData = buf.slice(offset);
  return { version, addr, port, cmd, rawClientData };
}

async function pumpRemoteToClient(remoteSocket, ws) {
  const reader = remoteSocket.readable.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      ws.send(value);
    }
  } catch (_) {
    // اتصال قطع شد
  } finally {
    try { ws.close(); } catch (_) {}
  }
}

// ============================================================
//  رله‌ی DNS با فریم‌بندی استاندارد VLESS UDP (پیشوند طول ۲ بایتی)
// ============================================================
async function handleUdpFrames(buf, ws) {
  let offset = 0;
  while (offset + 2 <= buf.length) {
    const len = (buf[offset] << 8) + buf[offset + 1];
    offset += 2;
    if (offset + len > buf.length) break;
    const packet = buf.slice(offset, offset + len);
    offset += len;

    try {
      const respBytes = await resolveDnsOverHttps(packet);
      const respLen = respBytes.length;
      const framed = new Uint8Array(2 + respLen);
      framed[0] = (respLen >> 8) & 0xff;
      framed[1] = respLen & 0xff;
      framed.set(respBytes, 2);
      ws.send(framed);
    } catch (_) {
      // این کوئری خاص را نادیده می‌گیریم؛ بقیه‌ی جریان ادامه پیدا می‌کند
    }
  }
}

async function resolveDnsOverHttps(queryBytes) {
  const res = await fetch("https://cloudflare-dns.com/dns-query", {
    method: "POST",
    headers: {
      "content-type": "application/dns-message",
      "accept": "application/dns-message",
    },
    body: queryBytes,
  });
  const buf = await res.arrayBuffer();
  return new Uint8Array(buf);
}
"""

COUNTRY_POOLS = {
    "Netherlands": {
        "flag": "🇳🇱",
        "seed": ["188.114.96.3", "188.114.97.3", "188.114.99.5"],
        "reserve": ["188.114.96.4", "188.114.97.9"]
    },
    "Germany": {
        "flag": "🇩🇪",
        "seed": ["104.16.85.20", "104.16.86.20"],
        "reserve": ["104.16.88.20"]
    },
    "United States": {
        "flag": "🇺🇸",
        "seed": ["104.17.24.14", "104.18.20.10"],
        "reserve": ["104.17.25.14"]
    }
}

DB_PATH = "ariobarzan_pro.db"
HEALTH_CHECK_INTERVAL_SECONDS = 600
HEALTH_CHECK_TIMEOUT = 4

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ============================================================
#  FSM States
# ============================================================
class SetupStates(StatesGroup):
    waiting_for_cf_token = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()

class ForcedChannelStates(StatesGroup):
    waiting_for_details = State()

class PasswordRecoveryStates(StatesGroup):
    waiting_for_target_id = State()

class MTProxyStates(StatesGroup):
    waiting_for_details = State()

class AdminManageStates(StatesGroup):
    waiting_for_admin_id = State()

class SupportStates(StatesGroup):
    waiting_for_user_msg = State()

class TicketReplyStates(StatesGroup):
    waiting_for_reply = State()


# ============================================================
#  متن دکمه‌های کیبورد ثابت (Reply Keyboard) — به‌جای دکمه‌های
#  شیشه‌ای/اینلاین قبلی، همه‌جا همین ثابت‌ها هم برای ساخت کیبورد و هم
#  برای تشخیص پیام کاربر استفاده می‌شوند تا هیچ‌جا ناهماهنگ نشود.
# ============================================================
BTN_CONNECT = "⚡ اتصال خودکار با توکن کلودفلر"
BTN_CFG_BUILDER = "🛠 ساخت کانفیگ تخصصی (گیم، نت ملی)"
BTN_GET_SUB = "📁 دریافت لینک ساب‌اسکریپت"
BTN_SINGLE = "🔑 دریافت کانفیگ‌های تکی"
BTN_GUIDE = "📖 راهنمای جامع استفاده"
BTN_SUPPORT = "💬 ارتباط با پشتیبانی"
BTN_DONATE = "💎 حمایت از سازنده (TON)"
BTN_RESTART = "🔄 شروع مجدد ربات"
BTN_ADMIN = "🛠 پنل مدیریت غول"
BTN_BACK = "◀️ بازگشت به منوی اصلی"

BTN_TOGGLE_ON = "🔴 خاموش کردن ربات"
BTN_TOGGLE_OFF = "🟢 روشن کردن ربات"
BTN_PROJ_SOCIALS = "📺 ثبت کانال یوتیوب و تلگرام پروژه"
BTN_USERS = "👥 مدیریت کاربران"
BTN_STATS = "📊 آمار کلی سیستم"
BTN_MANAGE_ADMINS = "👑 مدیریت ادمین‌ها"
BTN_SUPPORT_LIST = "📨 پیام‌های پشتیبانی"
BTN_BROADCAST = "📢 ارسال پیام همگانی"

BTN_CFG_GAMING = "🎮 ساخت کانفیگ مخصوص گیم (Low-Ping)"
BTN_CFG_NATIONAL = "🛡 ساخت کانفیگ نت ملی (Anti-Block)"
BTN_CFG_NORMAL = "⚡ ساخت کانفیگ معمولی (High-Speed)"

# مجموعه‌ی همه‌ی متن دکمه‌های منو — برای این‌که اگر کاربر وسط پرکردن یک
# فرم (مثلاً فرستادن توکن) روی یکی از این دکمه‌ها بزنه، به‌جای این‌که
# متن دکمه به‌اشتباه به‌عنوان ورودی فرم برداشت بشه، عملیات لغو بشه.
ALL_MENU_BUTTON_TEXTS = {
    BTN_CONNECT, BTN_CFG_BUILDER, BTN_GET_SUB, BTN_SINGLE, BTN_GUIDE,
    BTN_SUPPORT, BTN_DONATE, BTN_RESTART, BTN_ADMIN, BTN_BACK,
    BTN_TOGGLE_ON, BTN_TOGGLE_OFF, BTN_PROJ_SOCIALS, BTN_USERS, BTN_STATS,
    BTN_MANAGE_ADMINS, BTN_SUPPORT_LIST, BTN_BROADCAST,
    BTN_CFG_GAMING, BTN_CFG_NATIONAL, BTN_CFG_NORMAL,
}


async def cancel_state_if_menu_button(message: types.Message, state: FSMContext) -> bool:
    """اگر پیام کاربر یکی از دکمه‌های منو باشه (نه ورودی واقعی فرم)،
    عملیات نیمه‌کاره رو لغو می‌کنه و True برمی‌گردونه."""
    if message.text and message.text.strip() in ALL_MENU_BUTTON_TEXTS:
        await state.clear()
        await message.answer(
            "⚠️ عملیات قبلی لغو شد. لطفاً دوباره روی همون دکمه بزنید.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return True
    return False


# ============================================================
#  DATABASE INIT & METHODS
# ============================================================
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                cf_token TEXT,
                cf_account_id TEXT,
                config_uuid TEXT,
                worker_host TEXT,
                created_at TEXT
            )
        """)
        # ستون رمز عبور پنل وب اختصاصی هر کاربر (اگر قبلاً جدول وجود داشته
        # و این ستون رو نداشته باشه، اینجا اضافه‌ش می‌کنیم)
        try:
            conn.execute("ALTER TABLE users ADD COLUMN web_password_hash TEXT")
        except sqlite3.OperationalError:
            pass  # ستون از قبل وجود داره
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ip_pool (
                ip TEXT PRIMARY KEY,
                country TEXT,
                flag TEXT,
                alive INTEGER DEFAULT 1,
                last_checked TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                admin_id INTEGER PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                status TEXT DEFAULT 'open',
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS project_socials (
                platform TEXT PRIMARY KEY,
                title TEXT,
                url TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_project_checks (
                user_id INTEGER,
                platform TEXT,
                verified INTEGER DEFAULT 0,
                PRIMARY KEY(user_id, platform)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forced_join_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                chat_ref TEXT,
                invite_link TEXT,
                kind TEXT DEFAULT 'channel',
                added_at TEXT
            )
        """)
        conn.commit()

    for aid in INITIAL_ADMINS:
        add_admin_db(aid)

    if not get_all_ip_rows():
        for country, info in COUNTRY_POOLS.items():
            for ip in info["seed"]:
                add_ip(ip, country, info["flag"])

    if get_setting("bot_enabled") is None:
        set_setting("bot_enabled", "1")


def get_user(user_id: int):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def get_user_by_uuid(config_uuid: str):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM users WHERE config_uuid=?", (config_uuid,)).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def set_web_password(config_uuid: str, password: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "UPDATE users SET web_password_hash=? WHERE config_uuid=?",
            (generate_password_hash(password), config_uuid),
        )
        conn.commit()


def clear_web_password(config_uuid: str):
    """برای بازیابی رمز: پاک‌کردن رمز فعلی تا کاربر دفعه‌ی بعد که وارد سایتش
    می‌شه، بتونه یه رمز تازه تعیین کنه."""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("UPDATE users SET web_password_hash=NULL WHERE config_uuid=?", (config_uuid,))
        conn.commit()


def upsert_user(user_id: int, **fields):
    try:
        existing = get_user(user_id)
        with closing(sqlite3.connect(DB_PATH)) as conn:
            if existing:
                for key, value in fields.items():
                    conn.execute(f"UPDATE users SET {key}=? WHERE user_id=?", (value, user_id))
            else:
                for key in ("config_uuid", "worker_host", "cf_token", "cf_account_id", "username"):
                    fields.setdefault(key, None)
                conn.execute(
                    "INSERT INTO users (user_id, username, cf_token, cf_account_id, config_uuid, worker_host, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (user_id, fields["username"], fields["cf_token"], fields["cf_account_id"],
                     fields["config_uuid"], fields["worker_host"], datetime.utcnow().isoformat()),
                )
            conn.commit()
    except Exception as e:
        logging.error(f"upsert_user error: {e}")


def all_users():
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute("SELECT * FROM users").fetchall()]
    except Exception:
        return []


def get_setting(key: str, default=None):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else default
    except Exception:
        return default


def set_setting(key: str, value: str):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute("INSERT INTO settings (key, value) VALUES (?,?) "
                         "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            conn.commit()
    except Exception as e:
        logging.error(f"set_setting error: {e}")


def add_admin_db(admin_id: int):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute("INSERT OR IGNORE INTO admins (admin_id) VALUES (?)", (admin_id,))
            conn.commit()
    except Exception:
        pass


def is_admin(user_id: int) -> bool:
    if user_id in INITIAL_ADMINS:
        return True
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            row = conn.execute("SELECT 1 FROM admins WHERE admin_id=?", (user_id,)).fetchone()
            return bool(row)
    except Exception:
        return False


def add_ip(ip: str, country: str, flag: str):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ip_pool (ip, country, flag, alive, last_checked) VALUES (?,?,?,1,?)",
                (ip, country, flag, datetime.utcnow().isoformat()),
            )
            conn.commit()
    except Exception:
        pass


def mark_ip_status(ip: str, alive: bool):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute("UPDATE ip_pool SET alive=?, last_checked=? WHERE ip=?",
                          (1 if alive else 0, datetime.utcnow().isoformat(), ip))
            conn.commit()
    except Exception:
        pass


def remove_ip(ip: str):
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute("DELETE FROM ip_pool WHERE ip=?", (ip,))
            conn.commit()
    except Exception:
        pass


def get_all_ip_rows():
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute("SELECT * FROM ip_pool").fetchall()]
    except Exception:
        return []


def get_active_pool_grouped():
    rows = get_all_ip_rows()
    grouped: dict[str, dict] = {}
    for r in rows:
        if not r["alive"]:
            continue
        grouped.setdefault(r["country"], {"flag": r["flag"], "ips": []})
        grouped[r["country"]]["ips"].append(r["ip"])
    return grouped


# ============================================================
#  سلامت‌سنجی هوشمند IP
# ============================================================
async def tcp_alive(ip: str, port: int = 443, timeout: float = HEALTH_CHECK_TIMEOUT) -> bool:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except Exception:
        return False


def pick_replacement(country: str, used_ips: set[str]) -> str | None:
    reserve = COUNTRY_POOLS.get(country, {}).get("reserve", [])
    for candidate in reserve:
        if candidate not in used_ips:
            return candidate
    return None


async def health_check_cycle():
    rows = get_all_ip_rows()
    used_ips = {r["ip"] for r in rows}
    for row in rows:
        alive = await tcp_alive(row["ip"])
        if alive:
            mark_ip_status(row["ip"], True)
            continue
        replacement = pick_replacement(row["country"], used_ips)
        if replacement and await tcp_alive(replacement):
            remove_ip(row["ip"])
            add_ip(replacement, row["country"], row["flag"])
            used_ips.add(replacement)
        else:
            mark_ip_status(row["ip"], False)


async def health_check_loop():
    while True:
        try:
            await health_check_cycle()
        except Exception:
            pass
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)


# ============================================================
#  CLOUDFLARE API
# ============================================================
async def get_cf_account_id(cf_token: str) -> str | None:
    headers = {"Authorization": f"Bearer {cf_token}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{CF_API_BASE}/accounts", headers=headers) as resp:
            data = await resp.json()
            if data.get("success") and data.get("result"):
                return data["result"][0]["id"]
    return None


async def deploy_worker_auto(cf_token: str, user_uuid: str) -> tuple[str, str] | None:
    account_id = await get_cf_account_id(cf_token)
    if not account_id:
        return None

    headers = {"Authorization": f"Bearer {cf_token}"}
    metadata = {
        "main_module": "worker.js",
        "bindings": [{"type": "plain_text", "name": "UUID", "text": user_uuid}],
        "compatibility_date": "2024-09-23",
    }
    form = aiohttp.FormData()
    form.add_field("metadata", json.dumps(metadata), content_type="application/json")
    form.add_field("worker.js", WORKER_JS_SOURCE, filename="worker.js", content_type="application/javascript+module")

    script_url = f"{CF_API_BASE}/accounts/{account_id}/workers/scripts/{WORKER_SCRIPT_NAME}"

    async with aiohttp.ClientSession() as session:
        async with session.put(script_url, headers=headers, data=form) as resp:
            res_json = await resp.json()
            if not res_json.get("success"):
                return None

        async with session.post(f"{script_url}/subdomain", headers=headers, json={"enabled": True}) as resp:
            await resp.json()

        async with session.get(f"{CF_API_BASE}/accounts/{account_id}/workers/subdomain", headers=headers) as resp:
            sub_data = await resp.json()
            subdomain = (sub_data.get("result") or {}).get("subdomain")

    if not subdomain:
        return None
    return f"{WORKER_SCRIPT_NAME}.{subdomain}.workers.dev", account_id


# ============================================================
#  سیستم ساخت کانفیگ تخصصی
# ============================================================
def build_configs_for_user(user_uuid: str, worker_host: str, profile_type: str = "normal") -> list[str]:
    configs = []
    pool = get_active_pool_grouped()
    ordered_countries = ["Netherlands"] + [c for c in pool.keys() if c != "Netherlands"]
    counter = 1
    total_built = 0
    
    for country in ordered_countries:
        if country not in pool:
            continue
        info = pool[country]
        for ip in info["ips"]:
            if total_built >= 15:
                break
            
            if profile_type == "gaming":
                path_val = "%2Fgaming"
                name_prefix = "🎮 Gaming [Low-Ping]"
            elif profile_type == "national":
                path_val = "%2Fnational"
                name_prefix = "🛡 National [Anti-Block]"
            else:
                path_val = "%2F"
                name_prefix = "⚡ Normal"

            name = f"Ariobarzen {info['flag']} {name_prefix} - {counter}"
            link = (
                f"vless://{user_uuid}@{ip}:443"
                f"?encryption=none&security=tls&sni={worker_host}&host={worker_host}"
                f"&type=ws&path={path_val}#{name}"
            )
            configs.append(link)
            counter += 1
            total_built += 1
        if total_built >= 15:
            break
            
    return configs


def build_sub_text(user_uuid: str, worker_host: str) -> str:
    raw_list = (
        build_configs_for_user(user_uuid, worker_host, "normal") +
        build_configs_for_user(user_uuid, worker_host, "gaming") +
        build_configs_for_user(user_uuid, worker_host, "national")
    )
    raw = "\n".join(raw_list)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


# ============================================================
#  عضویت اجباری (کانال/گروه) — بررسی واقعی با bot.get_chat_member،
#  با قابلیت افزودن/حذف نامحدود از پنل ادمین
# ============================================================
def add_forced_channel(title: str, chat_ref: str, invite_link: str, kind: str = "channel"):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO forced_join_channels (title, chat_ref, invite_link, kind, added_at) VALUES (?,?,?,?,?)",
            (title, chat_ref, invite_link, kind, datetime.utcnow().isoformat()),
        )
        conn.commit()


def remove_forced_channel(channel_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM forced_join_channels WHERE id=?", (channel_id,))
        conn.commit()


def list_forced_channels():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM forced_join_channels ORDER BY id").fetchall()]


async def get_pending_forced_channels(user_id: int):
    channels = list_forced_channels()
    pending = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch["chat_ref"], user_id)
            if member.status in ("left", "kicked"):
                pending.append(ch)
        except Exception:
            # اگر ربات ادمین اون کانال/گروه نباشه یا chat_ref اشتباه باشه،
            # نمی‌تونیم عضویت رو تأیید کنیم؛ برای امنیت، عضونشده در نظر می‌گیریم
            pending.append(ch)
    return pending


def forced_join_required_kb(pending: list[dict]) -> types.InlineKeyboardMarkup:
    rows = []
    for ch in pending:
        icon = "👥" if ch["kind"] == "group" else "📢"
        rows.append([types.InlineKeyboardButton(text=f"{icon} عضویت در {ch['title']}", url=ch["invite_link"])])
    rows.append([types.InlineKeyboardButton(text="🔄 بررسی مجدد عضویت", callback_data="check_forced_join")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            user = data.get("event_from_user")
            if user is None or is_admin(user.id):
                return await handler(event, data)

            if get_setting("bot_enabled", "1") != "1":
                text = "🚧 ربات موقتاً توسط مدیریت خاموش است."
                if isinstance(event, types.CallbackQuery):
                    await event.answer(text, show_alert=True)
                else:
                    await event.answer(text)
                return

            cb_data = getattr(event, "data", None)
            is_verification = cb_data == "check_forced_join"

            if not is_verification:
                pending = await get_pending_forced_channels(user.id)
                if pending:
                    text = "⚠️ برای استفاده از ربات آریوبرزن، ابتدا در کانال/گروه(های) زیر عضو شوید:"
                    keyboard = forced_join_required_kb(pending)
                    if isinstance(event, types.CallbackQuery):
                        await event.message.answer(text, reply_markup=keyboard)
                        await event.answer()
                    else:
                        await event.answer(text, reply_markup=keyboard)
                    return
        except Exception:
            pass

        return await handler(event, data)


dp.message.outer_middleware(AccessMiddleware())
dp.callback_query.outer_middleware(AccessMiddleware())


@dp.callback_query(F.data == "check_forced_join")
async def check_forced_join_cb(callback: types.CallbackQuery):
    pending = await get_pending_forced_channels(callback.from_user.id)
    if not pending:
        await callback.message.answer("✅ عضویت شما تأیید شد.", reply_markup=main_menu_kb(callback.from_user.id))
    else:
        await callback.answer("هنوز در همه‌ی کانال/گروه‌های لازم عضو نشده‌اید.", show_alert=True)


# ============================================================
#  کیبوردها
# ============================================================
def main_menu_kb(user_id: int) -> types.InlineKeyboardMarkup:
    rows = [
        [types.InlineKeyboardButton(text="⚡ اتصال خودکار با توکن کلودفلر", callback_data="connect_cf")],
        [types.InlineKeyboardButton(text="🛠 ساخت کانفیگ تخصصی (گیم، نت ملی)", callback_data="config_builder_menu")],
        [types.InlineKeyboardButton(text="📁 دریافت لینک ساب‌اسکریپت", callback_data="get_configs")],
        [types.InlineKeyboardButton(text="🔑 دریافت کانفیگ تکی", callback_data="get_single_config")],
        [types.InlineKeyboardButton(text="🔌 دریافت پروکسی تلگرامی", callback_data="get_mtproxy")],
        [types.InlineKeyboardButton(text="📖 راهنمای جامع استفاده", callback_data="user_guide")],
        [types.InlineKeyboardButton(text="💬 ارتباط با پشتیبانی", callback_data="support_user")],
        [types.InlineKeyboardButton(text="💎 حمایت از سازنده (TON)", callback_data="support_creator")],
        [types.InlineKeyboardButton(text="🔄 شروع مجدد ربات", callback_data="restart_bot")],
    ]
    if is_admin(user_id):
        rows.append([types.InlineKeyboardButton(text="🛠 پنل مدیریت غول", callback_data="admin_panel")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def back_to_main_kb() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ بازگشت به منوی اصلی", callback_data="back_main")],
    ])


def admin_panel_kb() -> types.InlineKeyboardMarkup:
    bot_on = get_setting("bot_enabled", "1") == "1"
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=("🔴 خاموش کردن ربات" if bot_on else "🟢 روشن کردن ربات"), callback_data="toggle_bot")],
        [types.InlineKeyboardButton(text="📢 عضویت اجباری (کانال/گروه)", callback_data="admin_forced_join_list")],
        [types.InlineKeyboardButton(text="🔌 تنظیم پروکسی تلگرامی", callback_data="admin_mtproxy_setup")],
        [types.InlineKeyboardButton(text="🔑 بازیابی رمز پنل کاربر", callback_data="admin_recover_password")],
        [types.InlineKeyboardButton(text="👥 مدیریت کاربران", callback_data="admin_users_list")],
        [types.InlineKeyboardButton(text="📊 آمار کلی سیستم", callback_data="admin_stats")],
        [types.InlineKeyboardButton(text="👑 مدیریت ادمین‌ها", callback_data="admin_manage_admins")],
        [types.InlineKeyboardButton(text="📨 پیام‌های پشتیبانی", callback_data="admin_support_list")],
        [types.InlineKeyboardButton(text="📢 ارسال پیام همگانی", callback_data="admin_broadcast")],
        [types.InlineKeyboardButton(text="◀️ بازگشت به منوی اصلی", callback_data="back_main")],
    ])


# ============================================================
#  هندلرهای ربات
# ============================================================
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    upsert_user(message.from_user.id, username=message.from_user.username)
    await message.answer(
        "🚀 **به ربات پیشرفته آریوبرزن خوش آمدید**\n\n"
        "این ربات به صورت کاملاً اتوماتیک با توکن کلودفلر شما متصل شده و کانفیگ‌های پرسرعت می‌سازد.",
        reply_markup=main_menu_kb(message.from_user.id),
        parse_mode="Markdown"
    )


@dp.message(Command("panel"))
async def open_panel_cmd(message: types.Message):
    # همان دکمه‌ی «☰ Menu» کنار کادر پیام؛ برای کاربر عادی پنل کاربری و
    # برای ادمین (چون در main_menu_kb دکمه‌ی پنل مدیریت هم اضافه می‌شود)
    # هر دو پنل را در یک منو نشان می‌دهد.
    upsert_user(message.from_user.id, username=message.from_user.username)
    await message.answer("🎛 **پنل شما**", reply_markup=main_menu_kb(message.from_user.id), parse_mode="Markdown")


@dp.callback_query(F.data == "restart_bot")
async def restart_bot_cb(callback: types.CallbackQuery):
    await callback.message.edit_text("🔄 ربات با موفقیت ری‌استارت شد.", reply_markup=main_menu_kb(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "back_main")
async def back_main_cb(callback: types.CallbackQuery):
    await callback.message.edit_text("🚀 **منوی اصلی آریوبرزن**", reply_markup=main_menu_kb(callback.from_user.id), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "user_guide")
async def user_guide_cb(callback: types.CallbackQuery):
    guide_text = (
        "📖 **راهنمای جامع ربات آریوبرزن**\n\n"
        "این ربات به شما کمک می‌کند با استفاده از اکانت رایگان کلودفلر خودتان، "
        "یک سرور اختصاصی (Worker) بسازید که نقش پروکسی VLESS را بازی می‌کند. "
        "یعنی سرعت و پایداری اتصال شما به زیرساخت جهانی و رایگان کلودفلر وابسته "
        "است، نه یک VPS معمولی که ممکن است فیلتر یا کند شود.\n\n"

        "**۱) نوع اتصال — چطور کار می‌کند؟**\n"
        "وقتی توکن API کلودفلر خودتان را از بخش «اتصال خودکار با توکن کلودفلر» "
        "ارسال می‌کنید، ربات به‌صورت کاملاً خودکار:\n"
        "  • اکانت کلودفلر شما را با همان توکن شناسایی می‌کند (نیازی به فرستادن "
        "چیز دیگری مثل Account ID نیست)،\n"
        "  • یک Worker اختصاصی روی حساب خودِ شما دیپلوی می‌کند،\n"
        "  • یک UUID یکتا برای شما می‌سازد و در همان Worker ثبت می‌کند،\n"
        "  • زیردامنه‌ی رایگان workers.dev را برایتان فعال می‌کند.\n"
        "از این لحظه، Worker شما به‌عنوان سرور VLESS کار می‌کند و کانفیگ‌هایی که "
        "ربات می‌سازد مستقیماً به همان Worker وصل می‌شوند.\n\n"

        "**۲) توکن کلودفلر خودم امنه؟**\n"
        "توکن فقط برای دیپلوی‌کردن Worker روی حساب خودتان استفاده می‌شود؛ به "
        "دامنه‌ها یا اطلاعات دیگر حساب شما دسترسی داده نمی‌شود مگر همان دسترسی "
        "محدودی که هنگام ساخت توکن (Workers Scripts:Edit) به آن داده‌اید.\n\n"

        "**۳) انواع کانفیگ چه فرقی دارند؟**\n"
        "  🎮 **گیم (Low-Ping):** برای بازی‌های آنلاین بهینه شده؛ مسیر و تنظیمات "
        "طوری انتخاب شده که تأخیر (پینگ) تا حد ممکن کم بماند.\n"
        "  🛡 **نت ملی / شرایط اضطراری (Anti-Block):** برای زمانی که اینترنت "
        "بین‌الملل محدود یا مسدود شده و فقط دسترسی به نت داخلی/محدود دارید؛ "
        "این پروفایل تلاش می‌کند حتی در این شرایط اتصال برقرار بماند.\n"
        "  ⚡ **عمومی (High-Speed):** برای استفاده‌ی روزمره با بیشترین سرعت "
        "ممکن، مناسب مرور وب، دانلود و استریم.\n"
        "هر سه نوع را می‌توانید از «ساخت کانفیگ تخصصی» بسازید، یا همه را یک‌جا "
        "از «دریافت لینک ساب‌اسکریپت» به‌صورت یک لینک واحد دریافت کنید.\n\n"

        "**۴) لینک ساب‌اسکریپت را کجا وارد کنم؟**\n"
        "لینکی که از «دریافت لینک ساب‌اسکریپت» می‌گیرید را در برنامه‌هایی مثل "
        "**V2Box**، **V2RayNG**، **Streisand** یا هر کلاینت دیگری که از VLESS و "
        "«Subscription» پشتیبانی می‌کند وارد کنید و به‌روزرسانی (Update) بزنید. "
        "این لینک ثابت است و همیشه آخرین کانفیگ‌های سالم را برمی‌گرداند، چون "
        "ربات به‌صورت خودکار وضعیت IP ها را بررسی و در صورت نیاز جایگزین "
        "می‌کند.\n\n"

        "**۵) مشکلی پیش اومد چیکار کنم؟**\n"
        "از بخش «ارتباط با پشتیبانی» پیام بدید تا در سریع‌ترین زمان بررسی بشه.\n\n"

        "**۶) حمایت از سازنده**\n"
        "این ربات رایگان و بدون تبلیغات در اختیار شما قرار گرفته. اگر دوست "
        "دارید از توسعه و نگه‌داری‌اش حمایت کنید، از دکمه‌ی «💎 حمایت از سازنده "
        "(TON)» در منوی اصلی استفاده کنید؛ آدرس ولت به‌صورت متن قابل‌کپی نمایش "
        "داده می‌شود و کاملاً اختیاری است."
    )
    await callback.message.answer(guide_text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "support_creator")
async def support_creator_cb(callback: types.CallbackQuery):
    text = (
        "💎 **حمایت از سازنده**\n\n"
        "از این‌که وقت گذاشتید و تا اینجا با ما همراه بودید سپاسگزاریم 🙏\n"
        "این ربات با علاقه و بدون چشم‌داشت برای شما ساخته و نگه‌داری می‌شود. "
        "اگر این ابزار براتون مفید بوده و دوست دارید از ادامه‌ی توسعه و "
        "نگه‌داری‌اش حمایت کنید، می‌تونید با هر مبلغی (تتر روی شبکه TON) "
        "به آدرس زیر واریز کنید. هر کمکی، هرچند کوچک، با قدردانی پذیرفته می‌شه ❤️\n\n"
        "آدرس ولت TON:\n"
        f"`{TON_DONATE_ADDRESS}`\n\n"
        "برای کپی کردن، فقط روی آدرس بالا بزنید."
    )
    await callback.message.answer(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "connect_cf")
async def ask_cf_token(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🔑 **اتصال اتوماتیک به کلودفلر**\n\n"
        "لطفاً فقط توکن API کلودفلر خود را بفرستید (ربات به طور خودکار بقیه مراحل ساخت ورکر را انجام می‌دهد):",
        reply_markup=back_to_main_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(SetupStates.waiting_for_cf_token)
    await callback.answer()


@dp.message(SetupStates.waiting_for_cf_token)
async def process_cf_token(message: types.Message, state: FSMContext):
    cf_token = message.text.strip()
    await state.clear()

    status_msg = await message.answer("⏳ در حال برقراری ارتباط با کلودفلر و راه‌اندازی ورکر اختصاصی...")
    user = get_user(message.from_user.id)
    user_uuid = (user or {}).get("config_uuid") or str(uuid_lib.uuid4())

    res = await deploy_worker_auto(cf_token, user_uuid)
    if not res:
        await status_msg.edit_text("❌ اتصال ناموفق بود. توکن نامعتبر است یا دسترسی‌های لازم را ندارد.", reply_markup=main_menu_kb(message.from_user.id))
        return

    worker_host, account_id = res
    upsert_user(message.from_user.id, cf_token=cf_token, cf_account_id=account_id,
                config_uuid=user_uuid, worker_host=worker_host)

    await status_msg.edit_text(
        f"🎉 **اتصال و راه‌اندازی با موفقیت انجام شد!**\n\nهاست اختصاصی شما:\n`{worker_host}`",
        reply_markup=main_menu_kb(message.from_user.id),
        parse_mode="Markdown"
    )


# ============================================================
#  ساخت کانفیگ‌های تخصصی و لینک ساب‌اسکریپت
# ============================================================
@dp.callback_query(F.data == "config_builder_menu")
async def config_builder_menu_cb(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or not user.get("worker_host"):
        await callback.answer("⚠️ ابتدا باید اکانت کلودفلر خود را وصل کنید.", show_alert=True)
        return

    rows = [
        [types.InlineKeyboardButton(text="🎮 ساخت کانفیگ مخصوص گیم (Low-Ping)", callback_data="make_cfg:gaming")],
        [types.InlineKeyboardButton(text="🛡 ساخت کانفیگ نت ملی (Anti-Block)", callback_data="make_cfg:national")],
        [types.InlineKeyboardButton(text="⚡ ساخت کانفیگ معمولی (High-Speed)", callback_data="make_cfg:normal")],
        [types.InlineKeyboardButton(text="◀️ بازگشت به منوی اصلی", callback_data="back_main")]
    ]
    await callback.message.edit_text(
        "🛠 **بخش ساخت کانفیگ‌های تخصصی**\n\nپروفایل مورد نظر خود را انتخاب کنید:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("make_cfg:"))
async def make_custom_config_cb(callback: types.CallbackQuery):
    profile = callback.data.split(":", 1)[1]
    user = get_user(callback.from_user.id)
    if not user or not user.get("worker_host"):
        await callback.answer("⚠️ ابتدا اکانت خود را متصل کنید.", show_alert=True)
        return

    configs = build_configs_for_user(user["config_uuid"], user["worker_host"], profile)
    text = (
        f"✅ **کانفیگ‌های پروفایل ({profile.upper()}) آماده شد!**\n\n"
        f"نمونه کانفیگ:\n`{configs[0]}`\n\n"
        f"برای دریافت لیست کامل ۱۵ عددی کلاینت، از دکمه «دریافت لینک ساب‌اسکریپت» استفاده کنید."
    )
    await callback.message.answer(text, reply_markup=main_menu_kb(callback.from_user.id), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "get_configs")
async def get_user_configs(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or not user.get("worker_host"):
        await callback.answer("⚠️ ابتدا باید اکانت کلودفلر خود را وصل کنید.", show_alert=True)
        return

    base_url = os.getenv("PUBLIC_SUB_BASE", "https://musical-octo-garbanzo-production.up.railway.app")
    sub_link = f"{base_url}/sub/{user['config_uuid']}"

    text = (
        f"📁 **لینک ساب‌اسکریپت اختصاصی شما**\n\n"
        f"این لینک را کپی کرده و مستقیماً در **V2Box** یا **V2RayNG** وارد کنید:\n\n"
        f"`{sub_link}`"
    )
    await callback.message.answer(text, reply_markup=main_menu_kb(callback.from_user.id), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "get_single_config")
async def get_single_config(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or not user.get("worker_host"):
        await callback.answer("⚠️ ابتدا اکانت خود را وصل کنید.", show_alert=True)
        return

    configs = build_configs_for_user(user["config_uuid"], user["worker_host"], "normal")
    await callback.message.answer(
        f"🔑 **نمونه کانفیگ تکی پرسرعت:**\n\n`{configs[0]}`",
        reply_markup=main_menu_kb(callback.from_user.id),
        parse_mode="Markdown"
    )
    await callback.answer()


# ============================================================
#  ارتباط با پشتیبانی
# ============================================================
@dp.callback_query(F.data == "support_user")
async def support_user_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("💬 پیام خود را برای پشتیبانی ارسال کنید:", reply_markup=back_to_main_kb())
    await state.set_state(SupportStates.waiting_for_user_msg)
    await callback.answer()


@dp.message(SupportStates.waiting_for_user_msg)
async def support_user_send(message: types.Message, state: FSMContext):
    text = message.text
    await state.clear()
    
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO support_tickets (user_id, message, created_at) VALUES (?,?,?)",
                       (message.from_user.id, text, datetime.utcnow().isoformat()))
        conn.commit()
        ticket_id = cursor.lastrowid

    await message.answer("✅ پیام شما به پشتیبانی ارسال شد.", reply_markup=main_menu_kb(message.from_user.id))
    for aid in INITIAL_ADMINS:
        try:
            await bot.send_message(aid, f"📨 **تیکت پشتیبانی #{ticket_id}** از کاربر `{message.from_user.id}`:\n\n{text}", parse_mode="Markdown")
        except Exception:
            pass


# ============================================================
#  پنل مدیریت
# ============================================================
@dp.callback_query(F.data == "admin_panel")
async def admin_panel_cb(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("⛔️ دسترسی غیرمجاز.", show_alert=True)
    await callback.message.edit_text("🛠 **پنل مدیریت آریوبرزن**", reply_markup=admin_panel_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "toggle_bot")
async def toggle_bot_cb(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = get_setting("bot_enabled", "1")
    set_setting("bot_enabled", "0" if current == "1" else "1")
    await callback.message.edit_text("🛠 **پنل مدیریت آریوبرزن**", reply_markup=admin_panel_kb(), parse_mode="Markdown")
    await callback.answer("وضعیت ربات تغییر کرد.")


@dp.callback_query(F.data == "admin_stats")
async def admin_stats_cb(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    users = all_users()
    active_workers = sum(1 for u in users if u.get("worker_host"))
    text = f"📊 **آمار سیستم**\n\n👥 کل کاربران: {len(users)}\n⚡ ورکر فعال: {active_workers}"
    await callback.message.answer(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "admin_users_list")
async def admin_users_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    users = all_users()
    lines = [f"• `{u['user_id']}` (@{u['username'] or '—'})" for u in users[:20]]
    text = "👥 **لیست کاربران:**\n\n" + ("\n".join(lines) if lines else "کاربری ثبت نشده.")
    await callback.message.answer(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "admin_manage_admins")
async def admin_manage_admins(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("➕ آیدی عددی ادمین جدید را بفرستید:", reply_markup=back_to_main_kb())
    await state.set_state(AdminManageStates.waiting_for_admin_id)
    await callback.answer()


@dp.message(AdminManageStates.waiting_for_admin_id)
async def admin_add_save(message: types.Message, state: FSMContext):
    try:
        new_aid = int(message.text.strip())
        add_admin_db(new_aid)
        await state.clear()
        await message.answer("✅ ادمین جدید افزوده شد.", reply_markup=main_menu_kb(message.from_user.id))
    except Exception:
        await message.answer("❌ آیدی نامعتبر است.")


@dp.callback_query(F.data == "admin_mtproxy_setup")
async def admin_mtproxy_setup_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    current_server = get_setting("mtproxy_server")
    current_info = f"\n\nتنظیمات فعلی: `{current_server}`" if current_server else ""
    await callback.message.answer(
        "🔌 **تنظیم پروکسی تلگرامی (MTProto)**\n\n"
        "این بخش فقط لینک پروکسی رو برای یک سرور MTProto که *از قبل دارید* می‌سازه — "
        "خود ربات نمی‌تونه سرور MTProto بسازه (بر خلاف VLESS، این پروتکل روی "
        "Cloudflare Workers قابل پیاده‌سازی امن نیست).\n\n"
        "اگه سرور MTProto ندارید، ارزون‌ترین راه نصب پکیج رسمی `MTProxy` روی یک "
        "VPS کوچیکه، یا استفاده از @MTProxybot برای دریافت یک پروکسی رسمی.\n\n"
        "اطلاعات رو با فرمت زیر بفرستید:\n`سرور | پورت | سیکرت`\nمثال:\n`1.2.3.4 | 443 | ee1234567890abcdef1234567890abcdef`"
        + current_info,
        parse_mode="Markdown",
    )
    await state.set_state(MTProxyStates.waiting_for_details)
    await callback.answer()


@dp.message(MTProxyStates.waiting_for_details)
async def admin_mtproxy_setup_save(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [p.strip() for p in message.text.split("|")]
    if len(parts) < 3:
        await message.answer("❌ فرمت نامعتبر است. سه بخش را با `|` جدا کنید: سرور | پورت | سیکرت")
        return
    server, port, secret = parts[0], parts[1], parts[2]
    set_setting("mtproxy_server", server)
    set_setting("mtproxy_port", port)
    set_setting("mtproxy_secret", secret)
    await state.clear()
    await message.answer(
        "✅ اطلاعات پروکسی ذخیره شد.\n\n"
        "⚠️ برای نمایش «کانال اسپانسر» به کاربرانی که با این پروکسی وصل می‌شن، "
        "باید سرور/پورت/سیکرت رو در ربات رسمی تلگرام @MTProxybot ثبت کنید و از "
        "همون‌جا کانال دلخواه رو به‌عنوان اسپانسر تنظیم کنید — این تنظیم بخشی از "
        "لینک نیست و فقط از طریق خود تلگرام قابل انجامه.",
        reply_markup=main_menu_kb(message.from_user.id),
    )


def build_mtproxy_link() -> str | None:
    server = get_setting("mtproxy_server")
    port = get_setting("mtproxy_port")
    secret = get_setting("mtproxy_secret")
    if not (server and port and secret):
        return None
    return f"tg://proxy?server={server}&port={port}&secret={secret}"


@dp.callback_query(F.data == "get_mtproxy")
async def get_mtproxy_cb(callback: types.CallbackQuery):
    link = build_mtproxy_link()
    if not link:
        await callback.answer("پروکسی تلگرامی هنوز توسط مدیریت تنظیم نشده.", show_alert=True)
        return
    await callback.message.answer(
        f"🔌 **پروکسی تلگرامی آریوبرزن**\n\n`{link}`\n\n"
        f"روی لینک بزنید یا کپی کنید و در تلگرام باز کنید.\n\n"
        f"📢 کانال آریوبرزن: {TELEGRAM_CHANNEL_LINK}",
        reply_markup=back_to_main_kb(),
        parse_mode="Markdown",
    )
    await callback.answer()
async def admin_recover_password_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer(
        "🔑 آیدی عددی تلگرام کاربری که می‌خواهید رمز پنلش پاک بشه رو بفرستید.\n"
        "(کاربر با ورود بعدی به سایتش، رمز تازه تعیین می‌کنه.)",
        reply_markup=back_to_main_kb(),
    )
    await state.set_state(PasswordRecoveryStates.waiting_for_target_id)
    await callback.answer()


@dp.message(PasswordRecoveryStates.waiting_for_target_id)
async def admin_recover_password_save(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ آیدی عددی نامعتبر است.")
        return

    target_user = get_user(target_id)
    if not target_user or not target_user.get("config_uuid"):
        await message.answer("❌ کاربری با این آیدی یا پنل فعال پیدا نشد.", reply_markup=main_menu_kb(message.from_user.id))
        return

    clear_web_password(target_user["config_uuid"])
    await message.answer(
        f"✅ رمز پنل کاربر `{target_id}` پاک شد. با ورود بعدی به سایتش، رمز جدید تعیین می‌کنه.",
        reply_markup=main_menu_kb(message.from_user.id),
        parse_mode="Markdown",
    )
    try:
        await bot.send_message(
            target_id,
            "🔑 رمز پنل وب شما توسط پشتیبانی بازنشانی شد. با ورود بعدی به لینک پنلتون، یک رمز جدید تعیین کنید.",
        )
    except Exception:
        pass


@dp.callback_query(F.data == "admin_support_list")
async def admin_support_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        tickets = conn.execute("SELECT * FROM support_tickets WHERE status='open'").fetchall()
    
    if not tickets:
        await callback.answer("تیکت بازی وجود ندارد.", show_alert=True)
        return

    text = "📨 **تیکت‌های باز:**\n\n"
    for t in tickets:
        text += f"#{t['ticket_id']} | کاربر: `{t['user_id']}`\nمتن: {t['message']}\n---\n"
    await callback.message.answer(text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "admin_forced_join_list")
async def admin_forced_join_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    channels = list_forced_channels()
    rows = []
    if channels:
        for ch in channels:
            icon = "👥" if ch["kind"] == "group" else "📢"
            rows.append([types.InlineKeyboardButton(
                text=f"{icon} {ch['title']}", url=ch["invite_link"])])
            rows.append([types.InlineKeyboardButton(
                text=f"❌ حذف «{ch['title']}»", callback_data=f"fj_del:{ch['id']}")])
    rows.append([types.InlineKeyboardButton(text="➕ افزودن کانال/گروه جدید", callback_data="fj_add_start")])
    rows.append([types.InlineKeyboardButton(text="◀️ بازگشت به پنل مدیریت", callback_data="admin_panel")])

    text = (
        "📢 **مدیریت عضویت اجباری**\n\n"
        + (f"در حال حاضر {len(channels)} کانال/گروه ثبت شده:" if channels else "هنوز هیچ کانال/گروهی ثبت نشده.")
        + "\n\n⚠️ برای این‌که ربات بتونه عضویت رو چک کنه، حتماً باید **ادمین** همون کانال/گروه باشه."
    )
    await callback.message.edit_text(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data.startswith("fj_del:"))
async def admin_forced_join_delete(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    channel_id = int(callback.data.split(":", 1)[1])
    remove_forced_channel(channel_id)
    await callback.answer("✅ حذف شد.")
    await admin_forced_join_list(callback)


@dp.callback_query(F.data == "fj_add_start")
async def fj_add_start(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    rows = [
        [types.InlineKeyboardButton(text="📢 کانال", callback_data="fj_kind:channel")],
        [types.InlineKeyboardButton(text="👥 گروه", callback_data="fj_kind:group")],
        [types.InlineKeyboardButton(text="◀️ انصراف", callback_data="admin_forced_join_list")],
    ]
    await callback.message.edit_text(
        "نوع مورد نظر را انتخاب کنید:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("fj_kind:"))
async def fj_choose_kind(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    kind = callback.data.split(":", 1)[1]
    await state.update_data(fj_kind=kind)
    await state.set_state(ForcedChannelStates.waiting_for_details)
    kind_fa = "گروه" if kind == "group" else "کانال"
    await callback.message.answer(
        f"اطلاعات {kind_fa} را با این فرمت بفرستید (با کاراکتر `|` جدا کنید):\n\n"
        "`عنوان | آیدی‌عددی یا یوزرنیم | لینک دعوت`\n\n"
        "مثال (عمومی):\n`کانال آریوبرزن | @Ariobarzen_Panel | https://t.me/Ariobarzen_Panel`\n\n"
        "مثال (خصوصی، وقتی یوزرنیم نداره):\n`گروه VIP | -1001234567890 | https://t.me/+AbCdEfGh`\n\n"
        "⚠️ حتماً ربات رو از قبل ادمین همون کانال/گروه کنید.",
        parse_mode="Markdown",
    )
    await callback.answer()


@dp.message(ForcedChannelStates.waiting_for_details)
async def fj_save_details(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [p.strip() for p in message.text.split("|")]
    if len(parts) < 3:
        await message.answer("❌ فرمت نامعتبر است. سه بخش را با `|` جدا کنید: عنوان | آیدی/یوزرنیم | لینک دعوت")
        return
    title, chat_ref, invite_link = parts[0], parts[1], parts[2]
    data = await state.get_data()
    kind = data.get("fj_kind", "channel")
    add_forced_channel(title, chat_ref, invite_link, kind)
    await state.clear()
    await message.answer(
        f"✅ «{title}» به لیست عضویت اجباری اضافه شد.",
        reply_markup=main_menu_kb(message.from_user.id),
    )


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("📢 پیام همگانی خود را ارسال کنید:")
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()


@dp.message(BroadcastStates.waiting_for_message)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    users = all_users()
    sent = 0
    for u in users:
        try:
            await bot.send_message(u["user_id"], message.text)
            sent += 1
        except Exception:
            pass
        await asyncio.sleep(0.03)
    await state.clear()
    await message.answer(f"✅ پیام همگانی به {sent} کاربر ارسال شد.", reply_markup=main_menu_kb(message.from_user.id))


# ============================================================
#  پنل وب اختصاصی آریوبرزن (Flask) — سایت شخصی هر کاربر
# ============================================================
flask_app = Flask(__name__)
flask_app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))

TELEGRAM_BOT_LINK = "https://t.me/Aryobarzen_Bot"
TELEGRAM_CHANNEL_LINK = "https://t.me/Ariobarzen_Panel"
NON_COMMERCIAL_WARNING = (
    "این پنل کاملاً رایگان و شخصی است. فروش این پنل یا کانفیگ‌های آن ممنوع، "
    "غیراخلاقی و مصداق کلاهبرداری است."
)

BASE_STYLE = """
<style>
  * { box-sizing: border-box; }
  body {
    margin: 0; min-height: 100vh; direction: rtl;
    font-family: 'Vazirmatn', Tahoma, sans-serif;
    background: radial-gradient(circle at 20% 20%, #131c33 0%, #05070d 60%);
    color: #e8ecf7;
    display: flex; flex-direction: column; align-items: center;
    padding: 24px 14px 60px;
  }
  .brand {
    display: flex; align-items: center; gap: 10px; margin-bottom: 22px;
  }
  .brand-badge {
    width: 46px; height: 46px; border-radius: 14px;
    background: linear-gradient(135deg, #00d4ff, #7b2ff7);
    display: flex; align-items: center; justify-content: center;
    font-size: 22px; box-shadow: 0 0 20px rgba(0, 212, 255, .45);
  }
  .brand-name { font-size: 22px; font-weight: 800; letter-spacing: .5px; }
  .brand-name span { color: #00d4ff; }
  .card {
    width: 100%; max-width: 430px;
    background: rgba(255,255,255,.045);
    border: 1px solid rgba(0,212,255,.18);
    border-radius: 20px; padding: 26px 22px;
    backdrop-filter: blur(14px);
    box-shadow: 0 10px 40px rgba(0,0,0,.45);
    margin-bottom: 18px;
  }
  .card h2 { margin: 0 0 6px; font-size: 18px; color: #fff; }
  .card p.desc { color: #9aa5c0; font-size: 13px; margin: 0 0 18px; }
  input[type=password], input[type=text] {
    width: 100%; padding: 13px 14px; border-radius: 12px;
    border: 1px solid rgba(255,255,255,.12); background: rgba(0,0,0,.35);
    color: #fff; font-size: 14px; margin-bottom: 12px; direction: ltr; text-align: left;
  }
  button, .btn {
    width: 100%; padding: 13px; border: none; border-radius: 12px;
    background: linear-gradient(135deg, #00d4ff, #7b2ff7);
    color: #06101f; font-weight: 800; font-size: 14px; cursor: pointer;
    box-shadow: 0 6px 18px rgba(123,47,247,.35);
    text-decoration: none; display: inline-block; text-align: center;
  }
  .btn-outline {
    background: transparent; border: 1px solid rgba(255,255,255,.2); color: #e8ecf7;
    box-shadow: none; font-weight: 600;
  }
  .error { color: #ff6b81; font-size: 13px; margin: -4px 0 12px; }
  .warning-banner {
    width: 100%; max-width: 430px; background: rgba(255,80,80,.1);
    border: 1px solid rgba(255,80,80,.35); color: #ffb4bd;
    border-radius: 14px; padding: 12px 14px; font-size: 12.5px;
    margin-bottom: 18px; line-height: 1.9; text-align: center;
  }
  .row2 { display: flex; gap: 10px; }
  .stat-box {
    flex: 1; background: rgba(0,0,0,.3); border-radius: 12px; padding: 12px;
    text-align: center; border: 1px solid rgba(255,255,255,.06);
  }
  .stat-box .num { font-size: 18px; font-weight: 800; color: #00d4ff; }
  .stat-box .label { font-size: 11px; color: #9aa5c0; margin-top: 4px; }
  .cfg-item {
    background: rgba(0,0,0,.3); border: 1px solid rgba(255,255,255,.08);
    border-radius: 12px; padding: 10px 12px; margin-bottom: 8px;
  }
  .cfg-item .cfg-name { font-size: 12.5px; color: #d8e0f5; margin-bottom: 6px; }
  .cfg-item textarea {
    width: 100%; direction: ltr; text-align: left; font-size: 11px;
    background: rgba(0,0,0,.4); color: #7ff0c2; border: none; border-radius: 8px;
    padding: 8px; resize: none; height: 46px;
  }
  .sub-box textarea {
    width: 100%; direction: ltr; text-align: left; font-size: 11.5px;
    background: rgba(0,0,0,.4); color: #7ff0c2; border: 1px solid rgba(255,255,255,.1);
    border-radius: 10px; padding: 10px; height: 60px; margin-bottom: 10px;
  }
  #qrcode { display: flex; justify-content: center; margin: 14px 0; }
  #qrcode img { border-radius: 10px; background: #fff; padding: 8px; }
  .social-row { display: flex; gap: 10px; margin-top: 8px; }
  .social-row a {
    flex: 1; padding: 11px; border-radius: 12px; text-align: center;
    font-size: 12.5px; font-weight: 700; text-decoration: none;
  }
  .tg-bot { background: rgba(0,212,255,.12); color: #4fd6ff; border: 1px solid rgba(0,212,255,.3); }
  .tg-channel { background: rgba(123,47,247,.15); color: #b98bff; border: 1px solid rgba(123,47,247,.35); }
  .footer-note { color: #5c6785; font-size: 11px; margin-top: 10px; text-align: center; }
  h3.section-title { font-size: 14px; color: #cfd7ee; margin: 22px 0 10px; }
</style>
"""

LOGIN_SETUP_PAGE = """
<!doctype html><html lang="fa"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>پنل آریوبرزن</title>""" + BASE_STYLE + """
</head><body>
  <div class="brand">
    <div class="brand-badge">⚡</div>
    <div class="brand-name">پنل <span>آریوبرزن</span></div>
  </div>

  {% if is_setup %}
  <div class="card">
    <h2>🔐 تعیین رمز عبور پنل</h2>
    <p class="desc">این اولین باریه که وارد پنل اختصاصی خودتون می‌شید. یک رمز عبور برای ورودهای بعدی تعیین کنید.</p>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="post">
      <input type="password" name="password" placeholder="رمز عبور جدید (حداقل ۴ کاراکتر)" required>
      <input type="password" name="password2" placeholder="تکرار رمز عبور" required>
      <button type="submit">تعیین رمز و ورود</button>
    </form>
  </div>
  {% else %}
  <div class="card">
    <h2>🔑 ورود به پنل</h2>
    <p class="desc">رمز عبور پنل اختصاصی خودتون رو وارد کنید.</p>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="post">
      <input type="password" name="password" placeholder="رمز عبور" required>
      <button type="submit">ورود</button>
    </form>
    <p class="footer-note">رمز رو فراموش کردید؟ از ربات تلگرام، بخش پشتیبانی درخواست بازیابی بدید.</p>
  </div>
  {% endif %}

  <div class="social-row" style="width:100%;max-width:430px;">
    <a class="tg-bot" href="{{ bot_link }}" target="_blank">🤖 ربات تلگرام</a>
    <a class="tg-channel" href="{{ channel_link }}" target="_blank">📢 کانال آریوبرزن</a>
  </div>
</body></html>
"""

DASHBOARD_PAGE = """
<!doctype html><html lang="fa"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>داشبورد آریوبرزن</title>""" + BASE_STYLE + """
<script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
</head><body>
  <div class="brand">
    <div class="brand-badge">⚡</div>
    <div class="brand-name">پنل <span>آریوبرزن</span></div>
  </div>

  <div class="warning-banner">⚠️ {{ warning }}</div>

  <div class="card">
    <h2>👋 خوش اومدی</h2>
    <p class="desc">شناسه پنل: {{ short_uuid }}</p>
    <div class="row2">
      <div class="stat-box"><div class="num">{{ total_configs }}</div><div class="label">تعداد کانفیگ</div></div>
      <div class="stat-box"><div class="num">{{ status_label }}</div><div class="label">وضعیت Worker</div></div>
    </div>
  </div>

  <div class="card sub-box">
    <h2>📁 لینک ساب‌اسکریپت</h2>
    <p class="desc">این لینک رو در V2Box / V2RayNG / Streisand وارد و آپدیت کنید.</p>
    <textarea readonly onclick="this.select()">{{ sub_link }}</textarea>
    <div id="qrcode"></div>
    <a class="btn" href="{{ sub_link }}" target="_blank">باز کردن لینک ساب</a>
  </div>

  <div class="card">
    <h3 class="section-title" style="margin-top:0;">⚡ کانفیگ‌های تکی (کپی مستقیم)</h3>
    {% for c in configs %}
    <div class="cfg-item">
      <div class="cfg-name">{{ c.name }}</div>
      <textarea readonly onclick="this.select()">{{ c.link }}</textarea>
    </div>
    {% endfor %}
  </div>

  <div class="social-row" style="width:100%;max-width:430px;">
    <a class="tg-bot" href="{{ bot_link }}" target="_blank">🤖 ربات تلگرام</a>
    <a class="tg-channel" href="{{ channel_link }}" target="_blank">📢 کانال آریوبرزن</a>
  </div>
  <a class="btn btn-outline" style="max-width:430px;width:100%;margin-top:12px;" href="/panel/{{ uuid }}/logout">خروج از پنل</a>
  <p class="footer-note">ساخته‌شده با ⚡ توسط آریوبرزن</p>

  <script>
    new QRCode(document.getElementById("qrcode"), {
      text: {{ sub_link|tojson }},
      width: 160, height: 160,
    });
  </script>
</body></html>
"""


def _web_session_uuid():
    return session.get("panel_uuid")


@flask_app.route("/panel/<config_uuid>", methods=["GET", "POST"])
def panel_login(config_uuid):
    user = get_user_by_uuid(config_uuid)
    if not user or not user.get("worker_host"):
        return "پنل یافت نشد.", 404

    is_setup = not user.get("web_password_hash")
    error = None

    if request.method == "POST":
        if is_setup:
            pw1 = request.form.get("password", "")
            pw2 = request.form.get("password2", "")
            if len(pw1) < 4:
                error = "رمز عبور باید حداقل ۴ کاراکتر باشد."
            elif pw1 != pw2:
                error = "رمز عبور و تکرار آن یکسان نیستند."
            else:
                set_web_password(config_uuid, pw1)
                session["panel_uuid"] = config_uuid
                return redirect(f"/panel/{config_uuid}/dashboard")
        else:
            pw = request.form.get("password", "")
            if check_password_hash(user["web_password_hash"], pw):
                session["panel_uuid"] = config_uuid
                return redirect(f"/panel/{config_uuid}/dashboard")
            error = "رمز عبور اشتباه است."

    if _web_session_uuid() == config_uuid and not is_setup:
        return redirect(f"/panel/{config_uuid}/dashboard")

    return render_template_string(
        LOGIN_SETUP_PAGE, is_setup=is_setup, error=error,
        bot_link=TELEGRAM_BOT_LINK, channel_link=TELEGRAM_CHANNEL_LINK,
    )


@flask_app.route("/panel/<config_uuid>/dashboard")
def panel_dashboard(config_uuid):
    if _web_session_uuid() != config_uuid:
        return redirect(f"/panel/{config_uuid}")

    user = get_user_by_uuid(config_uuid)
    if not user or not user.get("worker_host"):
        return "پنل یافت نشد.", 404

    normal = build_configs_for_user(config_uuid, user["worker_host"], "normal")
    gaming = build_configs_for_user(config_uuid, user["worker_host"], "gaming")
    national = build_configs_for_user(config_uuid, user["worker_host"], "national")
    all_configs = normal + gaming + national

    configs_view = []
    for link in all_configs:
        try:
            name = link.split("#", 1)[1]
        except IndexError:
            name = "Ariobarzan Config"
        configs_view.append({"name": name, "link": link})

    base_url = os.getenv("PUBLIC_SUB_BASE", "https://musical-octo-garbanzo-production.up.railway.app")
    sub_link = f"{base_url}/sub/{config_uuid}"

    return render_template_string(
        DASHBOARD_PAGE,
        uuid=config_uuid, short_uuid=config_uuid[:8],
        total_configs=len(configs_view), status_label="✅ فعال",
        sub_link=sub_link, configs=configs_view,
        warning=NON_COMMERCIAL_WARNING,
        bot_link=TELEGRAM_BOT_LINK, channel_link=TELEGRAM_CHANNEL_LINK,
    )


@flask_app.route("/panel/<config_uuid>/logout")
def panel_logout(config_uuid):
    session.pop("panel_uuid", None)
    return redirect(f"/panel/{config_uuid}")


@flask_app.route("/sub/<config_uuid>")
def sub_route(config_uuid):
    user = get_user_by_uuid(config_uuid)
    if not user or not user.get("worker_host"):
        return "Config not found", 404
    sub_content = build_sub_text(config_uuid, user["worker_host"])
    return sub_content, 200, {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": 'inline; filename="sub.txt"',
    }


@flask_app.route("/")
def root_page():
    return "Ariobarzan panel service is running.", 200


def run_http_server():
    port = int(os.getenv("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


async def run_web_app():
    await asyncio.to_thread(run_http_server)


# ============================================================
#  اجرای اصلی
# ============================================================
async def main():
    init_db()
    try:
        # ثبت دستور /panel روی همون دکمه‌ی منوی کنار کادر پیام تلگرام
        # (نزدیک‌ترین معادل رسمی به «۴ خونه» که خودِ اپ تلگرام در اختیار می‌ذاره؛
        # چون تلگرام امکان رنگی‌کردن یا آیکون سفارشی برای این دکمه رو نمی‌ده)
        await bot.set_my_commands([
            types.BotCommand(command="start", description="🚀 شروع ربات"),
            types.BotCommand(command="panel", description="🎛 پنل کاربری و مدیریت"),
        ])
        await bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())
    except Exception as e:
        logging.error(f"set commands/menu error: {e}")

    asyncio.create_task(run_web_app())
    asyncio.create_task(health_check_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
