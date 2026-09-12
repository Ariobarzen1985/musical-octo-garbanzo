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
from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

# ============================================================
#  تنظیمات
# ============================================================
API_TOKEN = os.getenv("API_TOKEN", "8883749489:AAEpaX4_oKE8_7llrJ_TWTBG-r9QeKZVXrA")
ADMIN_IDS = {8443938939}

CF_API_BASE = "https://api.cloudflare.com/client/v4"
WORKER_SCRIPT_NAME = "ariobarzan-relay"
WORKER_JS_PATH = "worker.js"

with open(WORKER_JS_PATH, "r", encoding="utf-8") as f:
    WORKER_JS_SOURCE = f.read()

# لیست پایه (seed) + رزرو برای جایگزینی خودکار. این‌ها IP های عمومی
# Anycast خود کلودفلرن؛ لیبل کشور صرفاً بر پایه‌ی تجربه‌ی عمومیه، نه تضمین.
# توصیه می‌شود دوره‌ای این رزروها را از منابع «Clean IP» به‌روز کنید.
COUNTRY_POOLS = {
    "Netherlands": {"flag": "🇳🇱", "seed": ["188.114.96.3", "188.114.97.3", "188.114.99.5"],
                     "reserve": ["188.114.96.4", "188.114.97.9", "188.114.98.6", "188.114.99.11"]},
    "Germany": {"flag": "🇩🇪", "seed": ["104.16.85.20", "104.16.86.20", "104.16.87.20"],
                "reserve": ["104.16.88.20", "104.16.89.20", "104.16.90.20", "104.16.91.20"]},
    "United States": {"flag": "🇺🇸", "seed": ["104.17.24.14", "104.18.20.10", "104.19.30.5"],
                       "reserve": ["104.17.25.14", "104.18.21.10", "104.19.31.5", "104.20.15.10"]},
    "Turkey": {"flag": "🇹🇷", "seed": ["172.67.10.10", "172.67.20.20"],
               "reserve": ["172.67.30.30", "172.67.40.40"]},
    "France": {"flag": "🇫🇷", "seed": ["104.20.10.10", "104.21.10.10"],
               "reserve": ["104.20.11.10", "104.21.11.10"]},
    "United Kingdom": {"flag": "🇬🇧", "seed": ["172.64.100.5", "172.64.150.5"],
                        "reserve": ["172.64.101.5", "172.64.151.5"]},
    "Canada": {"flag": "🇨🇦", "seed": ["104.24.10.10", "104.25.10.10"],
               "reserve": ["104.24.11.10", "104.25.11.10"]},
}

DB_PATH = "ariobarzan.db"
SUB_HOST = "0.0.0.0"
SUB_PORT = int(os.getenv("PORT", 8080))
PUBLIC_SUB_BASE = os.getenv("PUBLIC_SUB_BASE", "https://your-panel-domain.com")

HEALTH_CHECK_INTERVAL_SECONDS = 600  # هر ۱۰ دقیقه
HEALTH_CHECK_TIMEOUT = 5

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class SetupStates(StatesGroup):
    waiting_for_cf_token = State()
    waiting_for_account_id = State()


class BroadcastStates(StatesGroup):
    waiting_for_message = State()


class ChannelStates(StatesGroup):
    waiting_for_channel = State()


# ============================================================
#  دیتابیس
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
        conn.commit()

    # اگر استخر IP خالی است، با مقادیر seed پر کن
    if not get_all_ip_rows():
        for country, info in COUNTRY_POOLS.items():
            for ip in info["seed"]:
                add_ip(ip, country, info["flag"])

    if get_setting("bot_enabled") is None:
        set_setting("bot_enabled", "1")
    if get_setting("forced_join_enabled") is None:
        set_setting("forced_join_enabled", "0")
    if get_setting("forced_channels") is None:
        set_setting("forced_channels", "[]")


def get_user(user_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None


def upsert_user(user_id: int, **fields):
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


def all_users():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM users").fetchall()]


def get_setting(key: str, default=None):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else default


def set_setting(key: str, value: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("INSERT INTO settings (key, value) VALUES (?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        conn.commit()


def add_ip(ip: str, country: str, flag: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO ip_pool (ip, country, flag, alive, last_checked) VALUES (?,?,?,1,?)",
            (ip, country, flag, datetime.utcnow().isoformat()),
        )
        conn.commit()


def mark_ip_status(ip: str, alive: bool):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("UPDATE ip_pool SET alive=?, last_checked=? WHERE ip=?",
                      (1 if alive else 0, datetime.utcnow().isoformat(), ip))
        conn.commit()


def remove_ip(ip: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM ip_pool WHERE ip=?", (ip,))
        conn.commit()


def get_all_ip_rows():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM ip_pool").fetchall()]


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
#  بررسی خودکار سلامت IP ها + جایگزینی
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

        logging.warning(f"IP فیلتر/غیرقابل‌دسترس شناسایی شد: {row['ip']} ({row['country']})")
        replacement = pick_replacement(row["country"], used_ips)
        if replacement and await tcp_alive(replacement):
            remove_ip(row["ip"])
            add_ip(replacement, row["country"], row["flag"])
            used_ips.add(replacement)
            logging.info(f"جایگزین شد: {row['ip']} → {replacement} ({row['country']})")
        else:
            mark_ip_status(row["ip"], False)  # فعلاً غیرفعال بمونه تا رزرو جدید پیدا بشه


async def health_check_loop():
    while True:
        try:
            await health_check_cycle()
        except Exception as e:
            logging.error(f"health check error: {e}")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)


# ============================================================
#  Cloudflare API — دیپلوی Worker
# ============================================================
async def deploy_worker(cf_token: str, account_id: str, user_uuid: str) -> str | None:
    headers = {"Authorization": f"Bearer {cf_token}"}
    metadata = {
        "main_module": "worker.js",
        "bindings": [{"type": "plain_text", "name": "UUID", "text": user_uuid}],
        "compatibility_date": "2024-09-23",
    }
    form = aiohttp.FormData()
    form.add_field("metadata", json.dumps(metadata), content_type="application/json")
    form.add_field("worker.js", WORKER_JS_SOURCE, filename="worker.js",
                    content_type="application/javascript+module")

    script_url = f"{CF_API_BASE}/accounts/{account_id}/workers/scripts/{WORKER_SCRIPT_NAME}"

    async with aiohttp.ClientSession() as session:
        async with session.put(script_url, headers=headers, data=form) as resp:
            result = await resp.json()
            if not result.get("success"):
                logging.error(f"Worker upload failed: {result.get('errors')}")
                return None

        async with session.post(f"{script_url}/subdomain", headers=headers, json={"enabled": True}) as resp:
            await resp.json()

        async with session.get(f"{CF_API_BASE}/accounts/{account_id}/workers/subdomain", headers=headers) as resp:
            sub_data = await resp.json()
            subdomain = (sub_data.get("result") or {}).get("subdomain")

    if not subdomain:
        return None
    return f"{WORKER_SCRIPT_NAME}.{subdomain}.workers.dev"


async def verify_cf_token(cf_token: str) -> bool:
    headers = {"Authorization": f"Bearer {cf_token}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{CF_API_BASE}/user/tokens/verify", headers=headers) as resp:
            data = await resp.json()
            return bool(data.get("success"))


# ============================================================
#  ساخت کانفیگ‌ها (همیشه از استخر زنده‌ی فعلی می‌خواند)
# ============================================================
def build_configs_for_user(user_uuid: str, worker_host: str) -> list[str]:
    configs = []
    counter = 1
    for country, info in get_active_pool_grouped().items():
        for ip in info["ips"]:
            name = f"Ariobarzan {info['flag']} {country}-{counter}"
            link = (
                f"vless://{user_uuid}@{ip}:443"
                f"?encryption=none&security=tls&sni={worker_host}&host={worker_host}"
                f"&type=ws&path=%2F#{name}"
            )
            configs.append(link)
            counter += 1
    return configs


def build_sub_text(user_uuid: str, worker_host: str) -> str:
    raw = "\n".join(build_configs_for_user(user_uuid, worker_host))
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


# ============================================================
#  عضویت اجباری
# ============================================================
async def is_member_of_all(user_id: int, channels: list[str]) -> bool:
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch, user_id)
            if member.status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True


def join_required_kb(channels: list[str]) -> types.InlineKeyboardMarkup:
    rows = [[types.InlineKeyboardButton(text=f"📢 عضویت در {c}", url=f"https://t.me/{c.lstrip('@')}")]
            for c in channels]
    rows.append([types.InlineKeyboardButton(text="✅ عضو شدم، بررسی کن", callback_data="check_membership")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None or user.id in ADMIN_IDS:
            return await handler(event, data)

        if get_setting("bot_enabled", "1") != "1":
            text = "🚧 ربات موقتاً توسط ادمین خاموش شده است. لطفاً بعداً مراجعه کنید."
            if isinstance(event, types.CallbackQuery):
                await event.answer(text, show_alert=True)
            else:
                await event.answer(text)
            return

        if get_setting("forced_join_enabled", "0") == "1":
            channels = json.loads(get_setting("forced_channels", "[]"))
            cb_data = getattr(event, "data", None)
            if channels and cb_data != "check_membership":
                if not await is_member_of_all(user.id, channels):
                    text = "برای استفاده از ربات ابتدا در کانال(های) زیر عضو شوید:"
                    if isinstance(event, types.CallbackQuery):
                        await event.message.answer(text, reply_markup=join_required_kb(channels))
                        await event.answer()
                    else:
                        await event.answer(text, reply_markup=join_required_kb(channels))
                    return

        return await handler(event, data)


dp.message.outer_middleware(AccessMiddleware())
dp.callback_query.outer_middleware(AccessMiddleware())


@dp.callback_query(F.data == "check_membership")
async def check_membership_cb(callback: types.CallbackQuery):
    channels = json.loads(get_setting("forced_channels", "[]"))
    if await is_member_of_all(callback.from_user.id, channels):
        await callback.message.answer("✅ عضویت تأیید شد.", reply_markup=main_menu_kb(callback.from_user.id))
    else:
        await callback.answer("هنوز عضو همه‌ی کانال‌ها نشده‌اید.", show_alert=True)


# ============================================================
#  کیبوردها
# ============================================================
def main_menu_kb(user_id: int) -> types.InlineKeyboardMarkup:
    rows = [
        [types.InlineKeyboardButton(text="⚡ اتصال به کلودفلر", callback_data="connect_cf")],
        [types.InlineKeyboardButton(text="📁 دریافت لینک ساب", callback_data="get_configs")],
        [types.InlineKeyboardButton(text="🧩 پنل مدیریت من", callback_data="my_panel")],
    ]
    if user_id in ADMIN_IDS:
        rows.append([types.InlineKeyboardButton(text="🛠 پنل ادمین", callback_data="admin_panel")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def user_panel_kb() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔄 ساخت مجدد UUID و ری‌دیپلوی", callback_data="regen_uuid")],
        [types.InlineKeyboardButton(text="🔗 دریافت دوباره لینک ساب", callback_data="get_configs")],
        [types.InlineKeyboardButton(text="⬅️ بازگشت", callback_data="back_main")],
    ])


def admin_panel_kb() -> types.InlineKeyboardMarkup:
    bot_on = get_setting("bot_enabled", "1") == "1"
    forced_on = get_setting("forced_join_enabled", "0") == "1"
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=("🔴 خاموش کردن ربات" if bot_on else "🟢 روشن کردن ربات"),
            callback_data="toggle_bot")],
        [types.InlineKeyboardButton(
            text=f"📢 عضویت اجباری ({'فعال' if forced_on else 'غیرفعال'})",
            callback_data="forced_join_menu")],
        [types.InlineKeyboardButton(text="👥 لیست کاربران", callback_data="admin_users")],
        [types.InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin_stats")],
        [types.InlineKeyboardButton(text="🌐 وضعیت IP ها", callback_data="admin_ip_status")],
        [types.InlineKeyboardButton(text="📢 پیام همگانی", callback_data="admin_broadcast")],
        [types.InlineKeyboardButton(text="⬅️ بازگشت", callback_data="back_main")],
    ])


def forced_join_kb() -> types.InlineKeyboardMarkup:
    forced_on = get_setting("forced_join_enabled", "0") == "1"
    channels = json.loads(get_setting("forced_channels", "[]"))
    rows = [[types.InlineKeyboardButton(
        text=("🔴 غیرفعال کردن" if forced_on else "🟢 فعال کردن"),
        callback_data="toggle_forced_join")]]
    for ch in channels:
        rows.append([types.InlineKeyboardButton(text=f"❌ حذف {ch}", callback_data=f"remove_channel:{ch}")])
    rows.append([types.InlineKeyboardButton(text="➕ افزودن کانال", callback_data="add_channel")])
    rows.append([types.InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


# ============================================================
#  هندلرهای اصلی
# ============================================================
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    upsert_user(message.from_user.id, username=message.from_user.username)
    await message.answer(
        "🚀 به ربات مدیریت آریوبرزن خوش آمدید.\n\n"
        "برای دریافت کانفیگ واقعی، ابتدا اکانت کلودفلر خود را وصل کنید.",
        reply_markup=main_menu_kb(message.from_user.id),
    )


@dp.callback_query(F.data == "back_main")
async def back_main(callback: types.CallbackQuery):
    await callback.message.edit_text("🚀 پنل اصلی آریوبرزن", reply_markup=main_menu_kb(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "connect_cf")
async def ask_for_token(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🔑 مرحله ۱ از ۲\n\nتوکن API کلودفلر خود را بفرستید.\n\n"
        "راهنما: Cloudflare Dashboard → My Profile → API Tokens → Create Token\n"
        "دسترسی لازم: «Workers Scripts:Edit»"
    )
    await state.set_state(SetupStates.waiting_for_cf_token)
    await callback.answer()


@dp.message(SetupStates.waiting_for_cf_token)
async def save_token(message: types.Message, state: FSMContext):
    cf_token = message.text.strip()
    await message.delete()

    valid = await verify_cf_token(cf_token)
    if not valid:
        await message.answer("❌ توکن نامعتبر است یا دسترسی کافی ندارد. دوباره تلاش کنید یا /start بزنید.")
        return

    await state.update_data(cf_token=cf_token)
    await message.answer(
        "✅ توکن تأیید شد.\n\n🔑 مرحله ۲ از ۲\n\nحالا Account ID کلودفلر خود را بفرستید."
    )
    await state.set_state(SetupStates.waiting_for_account_id)


@dp.message(SetupStates.waiting_for_account_id)
async def save_account_and_deploy(message: types.Message, state: FSMContext):
    account_id = message.text.strip()
    data = await state.get_data()
    cf_token = data.get("cf_token")
    await state.clear()

    user = get_user(message.from_user.id)
    user_uuid = (user or {}).get("config_uuid") or str(uuid_lib.uuid4())

    status_msg = await message.answer("⏳ در حال دیپلوی Worker روی اکانت کلودفلر شما...")
    worker_host = await deploy_worker(cf_token, account_id, user_uuid)

    if not worker_host:
        await status_msg.edit_text(
            "❌ دیپلوی ناموفق بود. مطمئن شوید توکن دسترسی «Workers Scripts:Edit» دارد "
            "و Account ID درست است. /start را بزنید و دوباره تلاش کنید."
        )
        return

    upsert_user(message.from_user.id, cf_token=cf_token, cf_account_id=account_id,
                config_uuid=user_uuid, worker_host=worker_host)
    await status_msg.edit_text(f"🎉 Worker با موفقیت فعال شد!\n\nهاست: `{worker_host}`", parse_mode="Markdown")
    await message.answer("منوی اصلی:", reply_markup=main_menu_kb(message.from_user.id))


@dp.callback_query(F.data == "get_configs")
async def generate_user_configs(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or not user.get("worker_host"):
        await callback.answer("⚠️ اول باید اکانت کلودفلر را وصل کنید.", show_alert=True)
        return

    pool = get_active_pool_grouped()
    total = sum(len(v["ips"]) for v in pool.values())
    sub_link = f"{PUBLIC_SUB_BASE}/sub/{user['config_uuid']}"
    text = (
        f"🎉 لینک ساب شما با {total} کانفیگ زنده آماده است!\n\n"
        + "\n".join(f"{v['flag']} {country}: {len(v['ips'])} کانفیگ" for country, v in pool.items())
        + f"\n\n🔗 لینک ساب‌اسکریپت:\n`{sub_link}`\n\n"
        "این لینک ثابت است؛ IP های داخلش به‌صورت خودکار در صورت فیلترشدن به‌روز می‌شوند."
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "my_panel")
async def my_panel(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    status = "✅ متصل و فعال" if (user and user.get("worker_host")) else "❌ هنوز وصل نشده"
    await callback.message.edit_text(f"🧩 پنل مدیریت شما\n\nوضعیت Worker: {status}", reply_markup=user_panel_kb())
    await callback.answer()


@dp.callback_query(F.data == "regen_uuid")
async def regen(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or not user.get("cf_token") or not user.get("cf_account_id"):
        await callback.answer("⚠️ اول باید اکانت کلودفلر را وصل کنید.", show_alert=True)
        return
    new_uuid = str(uuid_lib.uuid4())
    await callback.answer("⏳ در حال ری‌دیپلوی با UUID جدید...")
    worker_host = await deploy_worker(user["cf_token"], user["cf_account_id"], new_uuid)
    if worker_host:
        upsert_user(callback.from_user.id, config_uuid=new_uuid, worker_host=worker_host)
        await callback.message.answer("🔄 UUID جدید اعمال شد؛ لینک قبلی دیگر کار نمی‌کند.")
    else:
        await callback.message.answer("❌ ری‌دیپلوی ناموفق بود.")


# ---------------- پنل ادمین ----------------
@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    await callback.message.edit_text("🛠 پنل ادمین", reply_markup=admin_panel_kb())
    await callback.answer()


@dp.callback_query(F.data == "toggle_bot")
async def toggle_bot(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    current = get_setting("bot_enabled", "1")
    new_val = "0" if current == "1" else "1"
    set_setting("bot_enabled", new_val)
    await callback.answer("🟢 ربات روشن شد." if new_val == "1" else "🔴 ربات خاموش شد.", show_alert=True)
    await callback.message.edit_text("🛠 پنل ادمین", reply_markup=admin_panel_kb())


@dp.callback_query(F.data == "forced_join_menu")
async def forced_join_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    await callback.message.edit_text(
        "📢 مدیریت عضویت اجباری\n\n"
        "⚠️ ربات باید ادمین کانال باشد تا بتواند عضویت را چک کند.",
        reply_markup=forced_join_kb(),
    )
    await callback.answer()


@dp.callback_query(F.data == "toggle_forced_join")
async def toggle_forced_join(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    current = get_setting("forced_join_enabled", "0")
    set_setting("forced_join_enabled", "0" if current == "1" else "1")
    await callback.message.edit_text("📢 مدیریت عضویت اجباری", reply_markup=forced_join_kb())
    await callback.answer()


@dp.callback_query(F.data == "add_channel")
async def add_channel_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    await callback.message.answer("یوزرنیم کانال را با @ بفرستید (مثال: @mychannel).")
    await state.set_state(ChannelStates.waiting_for_channel)
    await callback.answer()


@dp.message(ChannelStates.waiting_for_channel)
async def add_channel_save(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    channel = message.text.strip()
    if not channel.startswith("@"):
        await message.answer("فرمت درست نیست؛ باید با @ شروع شود.")
        return
    channels = json.loads(get_setting("forced_channels", "[]"))
    if channel not in channels:
        channels.append(channel)
        set_setting("forced_channels", json.dumps(channels))
    await state.clear()
    await message.answer("✅ کانال اضافه شد.", reply_markup=forced_join_kb())


@dp.callback_query(F.data.startswith("remove_channel:"))
async def remove_channel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    channel = callback.data.split(":", 1)[1]
    channels = json.loads(get_setting("forced_channels", "[]"))
    channels = [c for c in channels if c != channel]
    set_setting("forced_channels", json.dumps(channels))
    await callback.message.edit_text("📢 مدیریت عضویت اجباری", reply_markup=forced_join_kb())
    await callback.answer("حذف شد.")


@dp.callback_query(F.data == "admin_ip_status")
async def admin_ip_status(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    rows = get_all_ip_rows()
    lines = [f"{'✅' if r['alive'] else '❌'} {r['flag']} {r['country']}: {r['ip']}" for r in rows]
    await callback.message.answer("🌐 وضعیت IP ها:\n" + "\n".join(lines) if lines else "استخر IP خالی است.")
    await callback.answer()


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    users = all_users()
    total = len(users)
    deployed = sum(1 for u in users if u.get("worker_host"))
    await callback.message.answer(f"📊 آمار کلی\n\nکل کاربران: {total}\nWorker فعال: {deployed}")
    await callback.answer()


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    users = all_users()
    if not users:
        await callback.message.answer("هیچ کاربری ثبت نشده.")
    else:
        lines = [f"• {u['user_id']} (@{u['username'] or '—'}) — {'✅' if u['worker_host'] else '❌'}"
                  for u in users[:50]]
        await callback.message.answer("👥 کاربران:\n" + "\n".join(lines))
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("⛔️ دسترسی ندارید.", show_alert=True)
    await callback.message.answer("📢 پیام همگانی را ارسال کنید:")
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()


@dp.message(BroadcastStates.waiting_for_message)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    users = all_users()
    sent, failed = 0, 0
    for u in users:
        try:
            await bot.send_message(u["user_id"], message.text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await message.answer(f"✅ ارسال شد به {sent} کاربر. ناموفق: {failed}")
    await state.clear()


# ============================================================
#  وب‌سرور سرو لینک ساب
# ============================================================
async def sub_handler(request: web.Request):
    req_uuid = request.match_info["user_uuid"]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE config_uuid=?", (req_uuid,)).fetchone()
    if not row or not row["worker_host"]:
        return web.Response(status=404, text="Not found")
    body = build_sub_text(req_uuid, row["worker_host"])
    return web.Response(text=body, content_type="text/plain")


async def run_web_app():
    app = web.Application()
    app.router.add_get("/sub/{user_uuid}", sub_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, SUB_HOST, SUB_PORT)
    await site.start()
    logging.info(f"Sub web server running on {SUB_HOST}:{SUB_PORT}")


# ============================================================
#  اجرا
# ============================================================
async def main():
    init_db()
    await run_web_app()
    asyncio.create_task(health_check_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
