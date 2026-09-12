import asyncio
import base64
import json
import logging
import os
import sqlite3
import uuid as uuid_lib
from contextlib import closing
from datetime import datetime

import aiohttp
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ============================================================
#  تنظیمات کلیدی
# ============================================================
API_TOKEN = os.getenv("API_TOKEN", "8883749489:AAEpaX4_oKE8_7llrJ_TWTBG-r9QeKZVXrA")
INITIAL_ADMINS = {8443938939}

CF_API_BASE = "https://api.cloudflare.com/client/v4"
WORKER_SCRIPT_NAME = "ariobarzan-master"
WORKER_JS_PATH = "worker.js"

if os.path.exists(WORKER_JS_PATH):
    with open(WORKER_JS_PATH, "r", encoding="utf-8") as f:
        WORKER_JS_SOURCE = f.read()
else:
    WORKER_JS_SOURCE = """
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader && upgradeHeader.toLowerCase() === "websocket") {
      return await handleVlessWebSocket(request, env.UUID);
    }
    return new Response("Ariobarzan Proxy Active", { status: 200 });
  }
};

async function handleVlessWebSocket(request, userID) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();
  return new Response(null, {
    status: 101,
    webSocket: client,
  });
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

class ProjectSocialStates(StatesGroup):
    waiting_for_youtube = State()
    waiting_for_telegram = State()

class AdminManageStates(StatesGroup):
    waiting_for_admin_id = State()

class SupportStates(StatesGroup):
    waiting_for_user_msg = State()


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
        # جدول نگهداری لینک‌های پروژه (یوتیوب و تلگرام اختصاصی پروژه)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS project_socials (
                platform TEXT PRIMARY KEY,
                title TEXT,
                url TEXT
            )
        """)
        # وضعیت تأیید عضویت کاربران در کانال‌های پروژه
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_project_checks (
                user_id INTEGER,
                platform TEXT,
                verified INTEGER DEFAULT 0,
                PRIMARY KEY(user_id, platform)
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
#  CLOUDFLARE API (اتصال کاملاً اتوماتیک تنها با توکن کاربر)
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
#  سیستم ساخت کانفیگ تخصصی (گیم، نت ملی، معمولی)
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
    raw = "\n".join(raw_list[:15])
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


# ============================================================
#  MIDDLEWARE & PROJECT SOCIAL GATES (عضویت اجباری پروژه‌ای)
# ============================================================
def get_pending_project_socials(user_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        socials = conn.execute("SELECT * FROM project_socials").fetchall()
        pending = []
        for s in socials:
            v = conn.execute("SELECT verified FROM user_project_checks WHERE user_id=? AND platform=?", (user_id, s["platform"])).fetchone()
            if not v or not v[0]:
                pending.append(dict(s))
        return pending


def project_join_required_kb(social_items: list[dict]) -> types.InlineKeyboardMarkup:
    rows = []
    for s in social_items:
        icon = "📺" if s["platform"] == "youtube" else "📢"
        rows.append([types.InlineKeyboardButton(text=f"{icon} عضویت در {s['title']}", url=s["url"])])
        rows.append([types.InlineKeyboardButton(text=f"✅ تأیید عضویت: {s['title']}", callback_data=f"verify_proj_social:{s['platform']}")])
    rows.append([types.InlineKeyboardButton(text="🔄 بررسی نهایی عضویت‌ها", callback_data="check_proj_membership")])
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

            pending_socials = get_pending_project_socials(user.id)
            cb_data = getattr(event, "data", None)
            is_verification = cb_data == "check_proj_membership" or (cb_data and cb_data.startswith("verify_proj_social:"))

            if not is_verification and pending_socials:
                text = "⚠️ برای استفاده از ربات لطفا در کانال‌های رسمی پروژه زیر عضو شوید:"
                keyboard = project_join_required_kb(pending_socials)
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


@dp.callback_query(F.data.startswith("verify_proj_social:"))
async def verify_proj_social_cb(callback: types.CallbackQuery):
    platform = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("INSERT OR REPLACE INTO user_project_checks (user_id, platform, verified) VALUES (?,?,1)", (user_id, platform))
        conn.commit()
    await callback.answer("✅ تأیید شد! لطفاً دکمه بررسی نهایی را بزنید.", show_alert=True)


@dp.callback_query(F.data == "check_proj_membership")
async def check_proj_membership_cb(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    pending_socials = get_pending_project_socials(user_id)
    if not pending_socials:
        await callback.message.answer("✅ عضویت شما تأیید شد.", reply_markup=main_menu_kb(user_id))
    else:
        await callback.answer("هنوز در تمام کانال‌های پروژه عضو نشده‌اید یا تأیید نکرده‌اید.", show_alert=True)


# ============================================================
#  کیبوردها
# ============================================================
def main_menu_kb(user_id: int) -> types.InlineKeyboardMarkup:
    rows = [
        [types.InlineKeyboardButton(text="⚡ اتصال خودکار با توکن کلودفلر", callback_data="connect_cf")],
        [types.InlineKeyboardButton(text="🛠 ساخت کانفیگ تخصصی (گیم، نت ملی)", callback_data="config_builder_menu")],
        [types.InlineKeyboardButton(text="📁 دریافت لینک ساب‌اسکریپت", callback_data="get_configs")],
        [types.InlineKeyboardButton(text="🔑 دریافت کانفیگ تکی", callback_data="get_single_config")],
        [types.InlineKeyboardButton(text="📖 راهنمای جامع استفاده", callback_data="user_guide")],
        [types.InlineKeyboardButton(text="💬 ارتباط با پشتیبانی", callback_data="support_user")],
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
        [types.InlineKeyboardButton(text="📺 ثبت کانال یوتیوب و تلگرام پروژه", callback_data="admin_project_socials")],
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
        "📖 **راهنمای جامع استفاده از ربات آریوبرزن**\n\n"
        "۱. توکن کلودفلر خود را از بخش «اتصال خودکار با توکن کلودفلر» ارسال کنید.\n"
        "۲. لینک ساب‌اسکریپت خود را در V2Box، V2RayNG یا سایر کلاینت‌ها وارد کنید.\n"
        "۳. از کانفیگ‌های اختصاصی گیم، نت ملی و معمولی لذت ببرید."
    )
    await callback.message.answer(guide_text, reply_markup=back_to_main_kb(), parse_mode="Markdown")
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
#  ساخت کانفیگ‌های تخصصی و لینک ساب‌اسکریپت (رفع مشکل خالی بودن کلاینت‌ها)
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
        f"✅ **کانفیگ‌های پروفایل ({profile.upper}) آماده شد!**\n\n"
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

    base_url = os.getenv("PUBLIC_SUB_BASE", "https://your-domain.up.railway.app")
    sub_link = f"{base_url}/sub/{user['config_uuid']}"

    text = (
        f"📁 **لینک ساب‌اسکریپت اختصاصی شما**\n\n"
        f"این لینک را کپی کرده و مستقیماً در **V2Box** یا **V2RayNG** وارد کنید (گزینه Subscribtion):\n\n"
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
#  پنل مدیریت و بخش ثبت کانال یوتیوب و تلگرام پروژه
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


# ============================================================
#  مدیریت ثبت کانال یوتیوب و کانال تلگرام پروژه برای عضویت اجباری
# ============================================================
@dp.callback_query(F.data == "admin_project_socials")
async def admin_project_socials_cb(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        socials = {r["platform"]: r for r in conn.execute("SELECT * FROM project_socials").fetchall()}

    yt = socials.get("youtube")
    tg = socials.get("telegram")

    text = (
        "📺 **ثبت کردن کانال یوتیوب و کانال تلگرام پروژه**\n\n"
        f"• کانال یوتیوب فعلی: {yt['title'] if yt else 'ثبت نشده'} (`{yt['url'] if yt else '—'}`)\n"
        f"• کانال تلگرام فعلی: {tg['title'] if tg else 'ثبت نشده'} (`{tg['url'] if tg else '—'}`)\n\n"
        "یکی از گزینه‌های زیر را برای ثبت یا تغییر انتخاب کنید:"
    )
    rows = [
        [types.InlineKeyboardButton(text="📺 ثبت/ویرایش کانال یوتیوب پروژه", callback_data="set_proj_yt")],
        [types.InlineKeyboardButton(text="📢 ثبت/ویرایش کانال تلگرام پروژه", callback_data="set_proj_tg")],
        [types.InlineKeyboardButton(text="◀️ بازگشت به پنل مدیریت", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "set_proj_yt")
async def set_proj_yt_cb(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer(
        "ارسال اطلاعات کانال یوتیوب پروژه:\n\n"
        "لطفاً عنوان و لینک را با فرمت زیر بفرستید:\n`عنوان | لینک`\nمثال:\n`کانال یوتیوب آریوبرزن | https://youtube.com/@Saman_Night_Fear`",
        reply_markup=back_to_main_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(ProjectSocialStates.waiting_for_youtube)
    await callback.answer()


@dp.message(ProjectSocialStates.waiting_for_youtube)
async def save_proj_yt(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split("|")
    if len(parts) < 2:
        await message.answer("❌ فرمت نامعتبر است. از کاراکتر `|` استفاده کنید.")
        return
    title, url = parts[0].strip(), parts[1].strip()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("INSERT OR REPLACE INTO project_socials (platform, title, url) VALUES ('youtube', ?, ?)", (title, url))
        conn.commit()
    await state.clear()
    await message.answer("✅ کانال یوتیوب پروژه با موفقیت ثبت شد و به بخش عضویت اجباری اضافه گردید.", reply_markup=main_menu_kb(message.from_user.id))


@dp.callback_query(F.data == "set_proj_tg")
async def set_proj_tg_cb(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer(
        "ارسال اطلاعات کانال تلگرام پروژه:\n\n"
        "لطفاً عنوان و لینک را با فرمت زیر بفرستید:\n`عنوان | لینک`\nمثال:\n`کانال تلگرام پروژه | https://t.me/YourChannel`",
        reply_markup=back_to_main_kb(),
        parse_mode="Markdown"
    )
    await state.set_state(ProjectSocialStates.waiting_for_telegram)
    await callback.answer()


@dp.message(ProjectSocialStates.waiting_for_telegram)
async def save_proj_tg(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split("|")
    if len(parts) < 2:
        await message.answer("❌ فرمت نامعتبر است. از کاراکتر `|` استفاده کنید.")
        return
    title, url = parts[0].strip(), parts[1].strip()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("INSERT OR REPLACE INTO project_socials (platform, title, url) VALUES ('telegram', ?, ?)", (title, url))
        conn.commit()
    await state.clear()
    await message.answer("✅ کانال تلگرام پروژه با موفقیت ثبت شد و به بخش عضویت اجباری اضافه گردید.", reply_markup=main_menu_kb(message.from_user.id))


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
#  وب‌سرور ساب‌اسکریپت (بهینه‌سازی شده برای کلاینت‌های V2Box و V2RayNG)
# ============================================================
async def sub_handler(request: web.Request):
    req_uuid = request.match_info["user_uuid"]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE config_uuid=?", (req_uuid,)).fetchone()
    if not row or not row["worker_host"]:
        return web.Response(status=404, text="Config not found")
    
    sub_content = build_sub_text(req_uuid, row["worker_host"])
    return web.Response(
        text=sub_content,
        content_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": "inline; filename=\"sub.txt\""}
    )


async def run_web_app():
    app = web.Application()
    app.router.add_get("/sub/{user_uuid}", sub_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


# ============================================================
#  اجرای اصلی
# ============================================================
async def main():
    init_db()
    await run_web_app()
    asyncio.create_task(health_check_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
