# -*- coding: utf-8 -*-

import logging
import uuid
import asyncio
import os
import base64
import traceback
from threading import Thread
from flask import Flask
from pymongo import MongoClient

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
)
from telegram.error import RetryAfter, TelegramError
from telegram import constants
from datetime import datetime

# ══════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════

BOT_TOKEN      = os.getenv("BOT_TOKEN",      "8065984603:AAGq1wNNrGYe8qaDJ57asp7_eAdpT1uy_VU")
OWNER_ID       = int(os.getenv("OWNER_ID",   "8147851167"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003639742558"))
FILE_CHANNEL   = int(os.getenv("FILE_CHANNEL",   "-1002298993427"))          # ← set to your channel id
MONGO_URI      = os.getenv("MONGO_URI",      "mongodb+srv://LordShadow:LordShadow@cluster0.hby3kq2.mongodb.net/?appName=Cluster0")
DB_NAME        = os.getenv("DB_NAME",        "Maskman")
BOT_USERNAME   = os.getenv("BOT_USERNAME",   "Mask_File2_bot")

# max concurrent sends during broadcast
BROADCAST_CONCURRENCY = 25

# ══════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════
#  MONGODB
# ══════════════════════════════════════════════════════════

mongo            = MongoClient(MONGO_URI)
db               = mongo[DB_NAME]
users_col        = db["users"]
ban_col          = db["banned"]
links_col        = db["links"]
batch_col        = db["batches"]
settings_col     = db["settings"]
fsub_col         = db["fsub_channels"]
fsub_pending_col = db["fsub_pending"]

# ══════════════════════════════════════════════════════════
#  IN-MEMORY STATE
# ══════════════════════════════════════════════════════════

BAN_WAIT      : set  = set()
UNBAN_WAIT    : set  = set()
GENLINK_WAIT  : set  = set()
BATCH_WAIT    : dict = {}
ADD_FSUB_WAIT : set  = set()

# ══════════════════════════════════════════════════════════
#  FLASK  (keep-alive / health-check)
# ══════════════════════════════════════════════════════════

flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "» ʙᴏᴛ ɪs ʀᴜɴɴɪɴɢ", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))

# ══════════════════════════════════════════════════════════
#  KEY ENCODING HELPERS
# ══════════════════════════════════════════════════════════

def encode_key(raw: str) -> str:
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_key(encoded: str) -> str | None:
    try:
        padded = encoded + "=" * (4 - len(encoded) % 4)
        return base64.urlsafe_b64decode(padded).decode()
    except Exception:
        return None

# ══════════════════════════════════════════════════════════
#  CORE HELPERS
# ══════════════════════════════════════════════════════════

def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

def is_banned(uid: int) -> bool:
    return ban_col.find_one({"_id": uid}) is not None

def get_auto_delete_seconds() -> int | None:
    doc = settings_col.find_one({"_id": "auto_delete"})
    return doc["minutes"] * 60 if doc else None

# ══════════════════════════════════════════════════════════
#  TELEGRAM ACTIVITY LOGGER
#
#  FIX: PTB v20+ deprecated disable_web_page_preview.
#       Using LinkPreviewOptions now — this was the reason
#       logs were silently failing to send.
# ══════════════════════════════════════════════════════════

async def tg_log(
    bot,
    level: str,
    action: str,
    user=None,
    error: str = None,
) -> None:
    icon = {"INFO": "»", "WARN": "▲", "ERROR": "✗", "SYSTEM": "⬡"}.get(level, "»")

    user_line = ""
    if user:
        uref = (
            f"<a href='https://t.me/{user.username}'>@{user.username}</a>"
            if user.username
            else f"<b>{user.first_name}</b>"
        )
        user_line = f"\n» ᴜsᴇʀ   : {uref}  [<code>{user.id}</code>]"

    err_line = (
        f"\n» ᴇʀʀᴏʀ  : <code>{error[:350]}</code>"
        if error
        else ""
    )

    text = (
        f"<b>{icon} [{level}]</b>{user_line}\n"
        f"» ᴀᴄᴛɪᴏɴ : {action}{err_line}\n"
        f"» ᴛɪᴍᴇ   : <code>{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</code>"
    )

    try:
        await bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=text,
            parse_mode=constants.ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),  # ← fixed
        )
    except Exception as exc:
        logger.error(f"tg_log failed → {exc}")

# ══════════════════════════════════════════════════════════
#  FILE CHANNEL HELPER
#
#  Copies one or more messages to FILE_CHANNEL.
#  Called after genlink / batch link generation.
#  No-ops silently when FILE_CHANNEL == 0.
# ══════════════════════════════════════════════════════════

async def copy_to_file_channel(bot, from_chat_id: int, message_ids: list) -> None:
    """Copy a list of message IDs from from_chat_id into FILE_CHANNEL."""
    if not FILE_CHANNEL:
        return

    for mid in message_ids:
        try:
            await bot.copy_message(
                chat_id=FILE_CHANNEL,
                from_chat_id=from_chat_id,
                message_id=mid,
            )
            await asyncio.sleep(0.05)          # stay under rate limits
        except RetryAfter as exc:
            logger.warning(f"file-channel RetryAfter {exc.retry_after}s mid={mid}")
            await asyncio.sleep(exc.retry_after)
            try:                               # one retry after the wait
                await bot.copy_message(
                    chat_id=FILE_CHANNEL,
                    from_chat_id=from_chat_id,
                    message_id=mid,
                )
            except TelegramError as exc2:
                logger.error(f"file-channel retry failed mid={mid}: {exc2}")
        except TelegramError as exc:
            logger.error(f"file-channel copy failed mid={mid}: {exc}")

# ══════════════════════════════════════════════════════════
#  AUTO-DELETE JOBS
# ══════════════════════════════════════════════════════════

async def _delete_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    d = context.job.data
    for mid in d.get("msg_ids", []):
        try:
            await context.bot.delete_message(d["chat_id"], mid)
        except Exception as exc:
            logger.warning(f"auto-delete msg {mid} → {exc}")
    if d.get("alert_id"):
        try:
            await context.bot.delete_message(d["chat_id"], d["alert_id"])
        except:
            pass

def _schedule_delete(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    msg_ids: list,
    alert_id: int,
    delay: int,
) -> None:
    context.job_queue.run_once(
        _delete_job,
        delay,
        data={"chat_id": chat_id, "msg_ids": msg_ids, "alert_id": alert_id},
    )

def _auto_delete_notice(minutes: int) -> str:
    return (
        f"<b>» ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɴᴏᴛɪᴄᴇ</b>\n"
        f"<blockquote>» ᴅᴇʟᴇᴛᴇs ɪɴ <b>{minutes} ᴍɪɴ</b> — ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ ᴛᴏ sᴀᴠᴇ.</blockquote>"
    )

# ══════════════════════════════════════════════════════════
#  FORCE-SUB HELPERS
# ══════════════════════════════════════════════════════════

def get_fsub_channels() -> list:
    return list(fsub_col.find({}, {"_id": 0}))

def is_force_sub_enabled() -> bool:
    doc = settings_col.find_one({"_id": "force_sub"})
    if doc is None:
        settings_col.insert_one({"_id": "force_sub", "enabled": True})
        return True
    return doc.get("enabled", True)

def _mode_label(mode: str) -> str:
    if "join_request" in mode:
        return "ᴊᴏɪɴ ʀᴇǫᴜᴇsᴛ"
    if "normal" in mode:
        return "ɴᴏʀᴍᴀʟ"
    return "ᴘᴜʙʟɪᴄ"

def force_sub_keyboard() -> InlineKeyboardMarkup:
    channels = get_fsub_channels()
    rows, row = [], []
    for ch in channels:
        tag = " [ᴊʀ]" if ch.get("mode") == "private_join_request" else ""
        row.append(InlineKeyboardButton(f"{ch['name']}{tag}", url=ch["url"]))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("» ᴄʜᴇᴄᴋ ᴊᴏɪɴ ✓", callback_data="check_fsub")])
    return InlineKeyboardMarkup(rows)

async def is_user_joined(bot, user_id: int) -> bool:
    channels = get_fsub_channels()
    if not channels:
        return True
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch["id"], user_id)
            if member.status in ("left", "kicked"):
                return False
        except TelegramError as exc:
            logger.warning(f"fsub membership check failed ch={ch['id']} → {exc}")
            return False
    return True

async def _send_fsub_wall(message, user) -> None:
    text = (
        f"<blockquote><b>» ʜᴇʏ {user.mention_html()}\n"
        "» ʏᴏᴜʀ ᴄᴏɴᴛᴇɴᴛ ɪs ʀᴇᴀᴅʏ ‼\n"
        "» ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ʙᴇʟᴏᴡ ᴛᴏ ᴜɴʟᴏᴄᴋ ᴀᴄᴄᴇss.\n\n"
        "» ɴᴏᴛᴇ : [ᴊʀ] ᴄʜᴀɴɴᴇʟs ʀᴇǫᴜɪʀᴇ ᴀᴅᴍɪɴ ᴀᴘᴘʀᴏᴠᴀʟ.\n"
        "» ᴀғᴛᴇʀ ᴊᴏɪɴɪɴɢ, ᴛᴀᴘ ᴄʜᴇᴄᴋ ᴊᴏɪɴ.</b></blockquote>"
    )
    await message.reply_text(
        text,
        reply_markup=force_sub_keyboard(),
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════════════════════

def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("» ᴀʙᴏᴜᴛ",   callback_data="about"),
            InlineKeyboardButton("» ɴᴇᴛᴡᴏʀᴋ", url="https://t.me/Shiva_Analyst07"),
        ],
        [InlineKeyboardButton("» ᴄʟᴏsᴇ", callback_data="close_msg")],
    ])

def about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("« ʙᴀᴄᴋ",   callback_data="back_to_start"),
            InlineKeyboardButton("» ᴄʟᴏsᴇ",  callback_data="close_msg"),
        ]
    ])

# ══════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id

    users_col.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)

    if is_banned(uid):
        logger.info(f"banned uid={uid} hit /start")
        return

    encoded_key = context.args[0] if context.args else None

    key = None
    if encoded_key:
        key = decode_key(encoded_key)
        if key is None:
            logger.warning(f"could not decode key '{encoded_key}', trying as raw")
            key = encoded_key

    # ── force-sub gate ──────────────────────────────────────
    if is_force_sub_enabled() and not await is_user_joined(context.bot, uid):
        if encoded_key:
            fsub_pending_col.update_one(
                {"_id": uid}, {"$set": {"key": encoded_key}}, upsert=True
            )
            logger.info(f"fsub blocked uid={uid} encoded_key={encoded_key} saved")
            await tg_log(context.bot, "INFO", f"ғsᴜʙ ɢᴀᴛᴇ — key={key}", update.effective_user)
        await _send_fsub_wall(update.message, update.effective_user)
        return

    # ── batch link ──────────────────────────────────────────
    if key and key.startswith("BATCH_"):
        logger.info(f"batch delivery uid={uid} key={key}")
        await tg_log(context.bot, "INFO", f"ʙᴀᴛᴄʜ ᴅᴇʟɪᴠᴇʀʏ | key={key}", update.effective_user)

        batch = batch_col.find_one({"_id": key})
        if not batch:
            await context.bot.send_message(
                chat_id,
                "<blockquote>✗ ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ʙᴀᴛᴄʜ ʟɪɴᴋ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        sent_ids       : list = []
        consecutive_fails     = 0
        MAX_FAILS             = 15

        for mid in range(batch["from_id"], batch["to_id"] + 1):
            try:
                m = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=batch["chat_id"],
                    message_id=mid,
                    protect_content=True,
                )
                sent_ids.append(m.message_id)
                consecutive_fails = 0
                await asyncio.sleep(0.05)
            except RetryAfter as exc:
                logger.warning(f"RetryAfter {exc.retry_after}s batch uid={uid}")
                await asyncio.sleep(exc.retry_after)
            except TelegramError as exc:
                logger.error(f"batch copy failed mid={mid}: {exc}")
                consecutive_fails += 1
                if consecutive_fails >= MAX_FAILS:
                    logger.error(f"batch aborted — {MAX_FAILS} consecutive failures uid={uid}")
                    await tg_log(context.bot, "ERROR", f"ʙᴀᴛᴄʜ ᴀʙᴏʀᴛᴇᴅ | key={key}", error=str(exc))
                    break

        if not sent_ids:
            await context.bot.send_message(
                chat_id,
                "<blockquote>✗ ɴᴏ ᴍᴇssᴀɢᴇs ᴄᴏᴜʟᴅ ʙᴇ ᴅᴇʟɪᴠᴇʀᴇᴅ.\n"
                "» ᴄʜᴇᴄᴋ ʙᴏᴛ ᴘᴇʀᴍɪssɪᴏɴs ɪɴ ᴛʜᴇ sᴏᴜʀᴄᴇ ᴄʜᴀɴɴᴇʟ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            await tg_log(context.bot, "WARN", f"ʙᴀᴛᴄʜ ᴢᴇʀᴏ ᴅᴇʟɪᴠᴇʀᴇᴅ | key={key}")
            return

        logger.info(f"batch delivered {len(sent_ids)} msgs to uid={uid}")
        d = get_auto_delete_seconds()
        if d:
            alert = await context.bot.send_message(
                chat_id,
                _auto_delete_notice(d // 60),
                parse_mode=constants.ParseMode.HTML,
            )
            _schedule_delete(context, chat_id, sent_ids, alert.message_id, d)
        return

    # ── single message link ─────────────────────────────────
    if key:
        logger.info(f"single link uid={uid} key={key}")
        await tg_log(context.bot, "INFO", f"sɪɴɢʟᴇ ʟɪɴᴋ ᴅᴇʟɪᴠᴇʀʏ | key={key}", update.effective_user)

        doc = links_col.find_one({"_id": key})
        if doc:
            try:
                m = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=doc["chat_id"],
                    message_id=doc["message_id"],
                    protect_content=True,
                )
                d = get_auto_delete_seconds()
                if d:
                    alert = await context.bot.send_message(
                        chat_id,
                        _auto_delete_notice(d // 60),
                        parse_mode=constants.ParseMode.HTML,
                    )
                    _schedule_delete(context, chat_id, [m.message_id], alert.message_id, d)
            except TelegramError as exc:
                logger.error(f"single link copy failed key={key}: {exc}")
                await tg_log(context.bot, "ERROR", f"sɪɴɢʟᴇ ʟɪɴᴋ ᴇʀʀᴏʀ | key={key}", error=str(exc))
        else:
            await context.bot.send_message(
                chat_id,
                "<blockquote>✗ ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ʟɪɴᴋ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
        return

    # ── normal start ────────────────────────────────────────
    logger.info(f"uid={uid} normal start")
    await update.message.reply_text(
        "<blockquote>» ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ᴀᴅᴠᴀɴᴄᴇᴅ ʟɪɴᴋs &amp; ғɪʟᴇ sʜᴀʀɪɴɢ ʙᴏᴛ.\n"
        "» sʜᴀʀᴇ ʟɪɴᴋs ᴀɴᴅ ғɪʟᴇs ᴡʜɪʟᴇ ᴋᴇᴇᴘɪɴɢ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟs sᴀғᴇ.</blockquote>\n\n"
        "<blockquote>» ᴍᴀɪɴᴛᴀɪɴᴇᴅ ʙʏ : "
        "<a href='https://t.me/Real_Mask_Man'>Mask Man ™</a></blockquote>",
        reply_markup=start_keyboard(),
        parse_mode=constants.ParseMode.HTML,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )

# ══════════════════════════════════════════════════════════
#  /genlink
# ══════════════════════════════════════════════════════════

async def genlink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text(
            "<blockquote>✗ ᴏᴡɴᴇʀ ᴏɴʟʏ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return
    if is_banned(uid):
        return

    logger.info("owner used /genlink")
    await tg_log(context.bot, "INFO", "/ɢᴇɴʟɪɴᴋ ɪɴɪᴛɪᴀᴛᴇᴅ", update.effective_user)
    GENLINK_WAIT.add(uid)
    await update.message.reply_text(
        "<blockquote>» sᴇɴᴅ ᴏʀ ғᴏʀᴡᴀʀᴅ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ɪᴛs sʜᴀʀᴇᴀʙʟᴇ ʟɪɴᴋ.</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /batch
# ══════════════════════════════════════════════════════════

async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text(
            "<blockquote>✗ ᴏᴡɴᴇʀ ᴏɴʟʏ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return
    if is_banned(uid):
        return

    logger.info("owner used /batch")
    await tg_log(context.bot, "INFO", "/ʙᴀᴛᴄʜ ɪɴɪᴛɪᴀᴛᴇᴅ", update.effective_user)
    BATCH_WAIT[uid] = {"step": "first"}
    await update.message.reply_text(
        "<blockquote>» ғᴏʀᴡᴀʀᴅ ᴛʜᴇ <b>ғɪʀsᴛ</b> ᴍᴇssᴀɢᴇ ғʀᴏᴍ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟ.\n"
        "» ᴇɴsᴜʀᴇ ᴛʜᴇ ʙᴏᴛ ɪs ᴀɴ <b>ᴀᴅᴍɪɴ</b> ɪɴ ᴛʜᴀᴛ ᴄʜᴀɴɴᴇʟ ʙᴇғᴏʀᴇʜᴀɴᴅ.</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /ban  /unban
# ══════════════════════════════════════════════════════════

async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        return
    BAN_WAIT.add(update.effective_user.id)
    await update.message.reply_text(
        "<blockquote>» sᴇɴᴅ ᴛʜᴇ ᴜsᴇʀ ɪᴅ ᴛᴏ ʙᴀɴ.</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        return
    UNBAN_WAIT.add(update.effective_user.id)
    await update.message.reply_text(
        "<blockquote>» sᴇɴᴅ ᴛʜᴇ ᴜsᴇʀ ɪᴅ ᴛᴏ ᴜɴʙᴀɴ.</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /users
# ══════════════════════════════════════════════════════════

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text(
            "<blockquote>✗ ᴏᴡɴᴇʀ ᴏɴʟʏ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    logger.info("owner used /users")
    await tg_log(context.bot, "INFO", "/ᴜsᴇʀs ᴄʜᴇᴄᴋᴇᴅ", update.effective_user)

    try:
        total  = users_col.count_documents({})
        banned = ban_col.count_documents({})
        active = total - banned

        await update.message.reply_text(
            "<b>» ᴜsᴇʀ sᴛᴀᴛɪsᴛɪᴄs</b>\n\n"
            "<blockquote>"
            f"» ᴛᴏᴛᴀʟ   ᴜsᴇʀs  : <code>{total}</code>\n"
            f"» ᴀᴄᴛɪᴠᴇ  ᴜsᴇʀs  : <code>{active}</code>\n"
            f"» ʙᴀɴɴᴇᴅ  ᴜsᴇʀs  : <code>{banned}</code>\n"
            f"» ᴄʜᴇᴄᴋᴇᴅ ᴀᴛ     : <code>{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</code>"
            "</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
    except Exception as exc:
        logger.error(f"/users error: {exc}")
        await tg_log(context.bot, "ERROR", "/ᴜsᴇʀs ᴅʙ ᴇʀʀᴏʀ", error=str(exc))
        await update.message.reply_text(
            f"<blockquote>✗ ᴅʙ ᴇʀʀᴏʀ : <code>{exc}</code></blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )

# ══════════════════════════════════════════════════════════
#  /broadcast
# ══════════════════════════════════════════════════════════

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "<blockquote>» ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ ɪᴛ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    logger.info("owner initiated /broadcast")
    await tg_log(context.bot, "INFO", "/ʙʀᴏᴀᴅᴄᴀsᴛ sᴛᴀʀᴛᴇᴅ", update.effective_user)

    src       = update.message.reply_to_message
    results   = {"success": 0, "blocked": 0, "deleted": 0, "failed": 0}
    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)

    async def _send(user_id: int) -> None:
        async with semaphore:
            try:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=src.chat.id,
                    message_id=src.message_id,
                )
                results["success"] += 1
            except RetryAfter as exc:
                await asyncio.sleep(exc.retry_after)
                results["failed"] += 1
            except Exception as exc:
                err = str(exc).lower()
                if "blocked" in err or "bot was blocked" in err:
                    results["blocked"] += 1
                elif "deleted" in err or "deactivated" in err:
                    results["deleted"] += 1
                else:
                    results["failed"] += 1

    user_ids = [u["_id"] for u in users_col.find({}, {"_id": 1})]
    total    = len(user_ids)

    progress = await update.message.reply_text(
        f"<blockquote>» ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ᴛᴏ <b>{total}</b> ᴜsᴇʀs...</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

    await asyncio.gather(*[_send(uid) for uid in user_ids])

    report = (
        "<b>» ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇ</b>\n\n"
        "<blockquote>"
        f"» ᴛᴏᴛᴀʟ    : <code>{total}</code>\n"
        f"» sᴇɴᴛ     : <code>{results['success']}</code>\n"
        f"» ʙʟᴏᴄᴋᴇᴅ  : <code>{results['blocked']}</code>\n"
        f"» ᴅᴇʟᴇᴛᴇᴅ  : <code>{results['deleted']}</code>\n"
        f"» ғᴀɪʟᴇᴅ   : <code>{results['failed']}</code>"
        "</blockquote>"
    )

    try:
        await progress.delete()
    except:
        pass

    await update.message.reply_text(report, parse_mode=constants.ParseMode.HTML)
    await tg_log(
        context.bot, "INFO",
        f"/ʙʀᴏᴀᴅᴄᴀsᴛ ᴅᴏɴᴇ | sᴇɴᴛ={results['success']} ʙʟᴏᴄᴋᴇᴅ={results['blocked']} ғᴀɪʟᴇᴅ={results['failed']}",
    )

# ══════════════════════════════════════════════════════════
#  /setdel
# ══════════════════════════════════════════════════════════

async def setdel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(
            "<blockquote>» ᴜsᴀɢᴇ : /setdel &lt;ᴍɪɴᴜᴛᴇs&gt;</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    minutes = int(context.args[0])
    settings_col.update_one(
        {"_id": "auto_delete"}, {"$set": {"minutes": minutes}}, upsert=True
    )
    logger.info(f"auto-delete set to {minutes} min")
    await tg_log(context.bot, "INFO", f"/sᴇᴛᴅᴇʟ → {minutes} ᴍɪɴ", update.effective_user)
    await update.message.reply_text(
        f"<blockquote>✓ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ sᴇᴛ ᴛᴏ <b>{minutes}</b> ᴍɪɴᴜᴛᴇ(s).</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /addfsub
# ══════════════════════════════════════════════════════════

async def addfsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        return

    logger.info("owner used /addfsub")
    await tg_log(context.bot, "INFO", "/ᴀᴅᴅғsᴜʙ ɪɴɪᴛɪᴀᴛᴇᴅ", update.effective_user)
    ADD_FSUB_WAIT.add(uid)
    await update.message.reply_text(
        "<blockquote>"
        "» ᴀᴅᴅ ᴍᴇ ᴀs <b>ᴀᴅᴍɪɴ</b> ɪɴ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟ\n"
        "» ᴛʜᴇɴ ғᴏʀᴡᴀʀᴅ ᴀ ᴍᴇssᴀɢᴇ ғʀᴏᴍ ɪᴛ ʜᴇʀᴇ."
        "</blockquote>",
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /delfsub
# ══════════════════════════════════════════════════════════

async def delfsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        return

    logger.info("owner used /delfsub")
    await tg_log(context.bot, "INFO", "/ᴅᴇʟғsᴜʙ ɪɴɪᴛɪᴀᴛᴇᴅ", update.effective_user)

    channels = get_fsub_channels()
    if not channels:
        await update.message.reply_text(
            "<blockquote>✗ ɴᴏ ғsᴜʙ ᴄʜᴀɴɴᴇʟs ғᴏᴜɴᴅ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    buttons, row = [], []
    for ch in channels:
        tag = " [ᴊʀ]" if ch.get("mode") == "private_join_request" else ""
        row.append(InlineKeyboardButton(f"{ch['name']}{tag}", callback_data=f"fsub_pick_{ch['id']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("✗ ᴄʟᴏsᴇ", callback_data="close_msg")])

    await update.message.reply_text(
        "<blockquote><b>» sᴇʟᴇᴄᴛ ᴀ ᴄʜᴀɴɴᴇʟ ᴛᴏ ʀᴇᴍᴏᴠᴇ :</b></blockquote>",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=constants.ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  /fsub
# ══════════════════════════════════════════════════════════

async def fsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_owner(uid):
        return

    valid = ("on", "off", "status", "list")
    if not context.args or context.args[0].lower() not in valid:
        await update.message.reply_text(
            "<blockquote>"
            "» /fsub on     — ᴇɴᴀʙʟᴇ ғsᴜʙ\n"
            "» /fsub off    — ᴅɪsᴀʙʟᴇ ғsᴜʙ\n"
            "» /fsub status — ᴄᴜʀʀᴇɴᴛ sᴛᴀᴛᴜs\n"
            "» /fsub list   — ʟɪsᴛ ᴄʜᴀɴɴᴇʟs"
            "</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    arg = context.args[0].lower()

    if arg == "status":
        enabled  = is_force_sub_enabled()
        ch_count = len(get_fsub_channels())
        status   = "✓ ᴇɴᴀʙʟᴇᴅ" if enabled else "✗ ᴅɪsᴀʙʟᴇᴅ"
        await update.message.reply_text(
            f"<blockquote>» ғsᴜʙ     : {status}\n» ᴄʜᴀɴɴᴇʟs : <code>{ch_count}</code></blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    if arg == "list":
        channels = get_fsub_channels()
        if not channels:
            await update.message.reply_text(
                "<blockquote>» ɴᴏ ᴄʜᴀɴɴᴇʟs ᴀᴅᴅᴇᴅ ʏᴇᴛ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return
        lines = ["<b>» ғsᴜʙ ᴄʜᴀɴɴᴇʟ ʟɪsᴛ :</b>\n"]
        for i, ch in enumerate(channels, 1):
            ml = _mode_label(ch.get("mode", "public"))
            lines.append(f"{i}. <b>{ch['name']}</b>  [<code>{ch['id']}</code>]  — {ml}")
        await update.message.reply_text(
            "<blockquote>" + "\n".join(lines) + "</blockquote>",
            parse_mode=constants.ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return

    enabled = arg == "on"
    settings_col.update_one(
        {"_id": "force_sub"}, {"$set": {"enabled": enabled}}, upsert=True
    )
    logger.info(f"force-sub toggled → {arg.upper()}")
    await tg_log(context.bot, "INFO", f"/ғsᴜʙ → {arg.upper()}", update.effective_user)
    label = "✓ ғᴏʀᴄᴇ-sᴜʙ ᴇɴᴀʙʟᴇᴅ" if enabled else "✗ ғᴏʀᴄᴇ-sᴜʙ ᴅɪsᴀʙʟᴇᴅ"
    await update.message.reply_text(
        f"<blockquote>{label}</blockquote>", parse_mode=constants.ParseMode.HTML
    )

# ══════════════════════════════════════════════════════════
#  /help
# ══════════════════════════════════════════════════════════

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_banned(update.effective_user.id):
        return

    logger.info(f"uid={update.effective_user.id} used /help")

    fc_line = (
        f"» FILE_CHANNEL : <code>{FILE_CHANNEL}</code> (ᴀᴄᴛɪᴠᴇ)\n"
        if FILE_CHANNEL
        else "» FILE_CHANNEL : ɴᴏᴛ sᴇᴛ\n"
    )

    text = (
        "<b>» ʙᴏᴛ ᴄᴏᴍᴍᴀɴᴅs</b>\n\n"
        "<blockquote expandable>"
        "» /start           — ᴏᴘᴇɴ ᴛʜᴇ ʙᴏᴛ\n"
        "» /help            — sʜᴏᴡ ᴄᴏᴍᴍᴀɴᴅs\n"
        "» /genlink         — ɢᴇɴᴇʀᴀᴛᴇ sʜᴀʀᴇᴀʙʟᴇ ʟɪɴᴋ         [ᴏᴡɴᴇʀ]\n"
        "» /batch           — ᴍᴜʟᴛɪ-ᴍsɢ ʙᴀᴛᴄʜ ʟɪɴᴋ              [ᴏᴡɴᴇʀ]\n"
        "» /users           — ᴛᴏᴛᴀʟ ᴜsᴇʀ sᴛᴀᴛs                  [ᴏᴡɴᴇʀ]\n"
        "» /broadcast       — ʙʀᴏᴀᴅᴄᴀsᴛ ᴛᴏ ᴀʟʟ ᴜsᴇʀs             [ᴏᴡɴᴇʀ]\n"
        "» /setdel          — sᴇᴛ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇʀ              [ᴏᴡɴᴇʀ]\n"
        "» /ban             — ʙᴀɴ ᴀ ᴜsᴇʀ                          [ᴏᴡɴᴇʀ]\n"
        "» /unban           — ᴜɴʙᴀɴ ᴀ ᴜsᴇʀ                        [ᴏᴡɴᴇʀ]\n"
        "» /fsub on|off     — ᴛᴏɢɢʟᴇ ғᴏʀᴄᴇ-sᴜʙ                   [ᴏᴡɴᴇʀ]\n"
        "» /fsub status     — sᴛᴀᴛᴜs + ᴄʜᴀɴɴᴇʟ ᴄᴏᴜɴᴛ             [ᴏᴡɴᴇʀ]\n"
        "» /fsub list       — ʟɪsᴛ ᴀʟʟ ᴄʜᴀɴɴᴇʟs                  [ᴏᴡɴᴇʀ]\n"
        "» /addfsub         — ᴀᴅᴅ ғsᴜʙ ᴄʜᴀɴɴᴇʟ                   [ᴏᴡɴᴇʀ]\n"
        "» /delfsub         — ʀᴇᴍᴏᴠᴇ ғsᴜʙ ᴄʜᴀɴɴᴇʟ                [ᴏᴡɴᴇʀ]\n\n"
        "<b>» ᴀᴄᴛɪᴠᴇ ғᴇᴀᴛᴜʀᴇs :</b>\n"
        "» sʜᴀʀᴇᴀʙʟᴇ ʟɪɴᴋs + ʙᴀᴛᴄʜ ʟɪɴᴋs\n"
        "» ғsᴜʙ — ɴᴏʀᴍᴀʟ &amp; ᴊᴏɪɴ ʀᴇǫᴜᴇsᴛ ᴍᴏᴅᴇs\n"
        "» ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴅᴇʟɪᴠᴇʀᴇᴅ ғɪʟᴇs\n"
        "» ᴄᴏɴᴛᴇɴᴛ ᴘʀᴏᴛᴇᴄᴛɪᴏɴ [ɴᴏ ғᴏʀᴡᴀʀᴅɪɴɢ]\n"
        "» ᴇɴᴄᴏᴅᴇᴅ ᴅᴇᴇᴘ-ʟɪɴᴋs [ʙᴀsᴇ64ᴜʀʟ]\n"
        "» ғᴜʟʟ ᴛᴇʟᴇɢʀᴀᴍ ᴀᴄᴛɪᴠɪᴛʏ ʟᴏɢɢɪɴɢ\n"
        "» ʜɪɢʜ-ᴘᴇʀғᴏʀᴍᴀɴᴄᴇ ʙʀᴏᴀᴅᴄᴀsᴛ [25 ᴄᴏɴᴄᴜʀʀᴇɴᴛ ᴡᴏʀᴋᴇʀs]\n"
        f"» ғɪʟᴇ ᴄʜᴀɴɴᴇʟ ʙᴀᴄᴋᴜᴘ [ɢᴇɴʟɪɴᴋ &amp; ʙᴀᴛᴄʜ]"
        "</blockquote>\n"
        "<blockquote expandable>"
        f"<b>» ᴄᴏɴғɪɢ</b>\n"
        f"{fc_line}"
        "<b>» ᴄʀᴇᴅɪᴛs</b>\n"
        "» ᴅᴇᴠᴇʟᴏᴘᴇᴅ ʙʏ \n"
        "» ʟɪʙ  : python-telegram-bot v22\n"
        "» ᴅʙ   : MongoDB  |  ʜᴏsᴛ : Render"
        "</blockquote>"
    )

    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("» sᴜᴘᴘᴏʀᴛ",    url="https://t.me/Shiva_Analyst07"),
                InlineKeyboardButton("» ᴄʜᴀɴɴᴇʟ",    url="https://t.me/Shiva_Analyst07"),
            ],
            [
                InlineKeyboardButton("» ᴅᴇᴠᴇʟᴏᴘᴇʀ",  url="https://t.me/"),
                InlineKeyboardButton("» ᴄʟᴏsᴇ",      callback_data="close_msg"),
            ],
        ]),
        parse_mode=constants.ParseMode.HTML,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )

# ══════════════════════════════════════════════════════════
#  PRIVATE MESSAGE HANDLER
# ══════════════════════════════════════════════════════════

async def private_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_banned(update.effective_user.id):
        return
    if not update.message:
        return

    uid  = update.effective_user.id
    msg  = update.message
    text = msg.text.strip() if msg.text else None

    if text and text.startswith("/"):
        return

    # ── ADD FSUB ──────────────────────────────────────────
    if uid in ADD_FSUB_WAIT:
        ch = None
        if msg.forward_origin and hasattr(msg.forward_origin, "chat"):
            ch = msg.forward_origin.chat
        elif msg.forward_from_chat:
            ch = msg.forward_from_chat

        if not ch:
            await msg.reply_text(
                "<blockquote>✗ ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ ᴀ ᴍᴇssᴀɢᴇ ғʀᴏᴍ ᴀ ᴄʜᴀɴɴᴇʟ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        if ch.type != "channel":
            await msg.reply_text(
                "<blockquote>✗ ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ ғʀᴏᴍ ᴀ ᴄʜᴀɴɴᴇʟ, ɴᴏᴛ ᴀ ɢʀᴏᴜᴘ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        try:
            me = await context.bot.get_me()
            await context.bot.get_chat_member(ch.id, me.id)
        except TelegramError as exc:
            logger.error(f"addfsub access check failed ch={ch.id}: {exc}")
            await tg_log(context.bot, "ERROR", "ᴀᴅᴅғsᴜʙ — ʙᴏᴛ ᴄᴀɴɴᴏᴛ ᴀᴄᴄᴇss ᴄʜᴀɴɴᴇʟ", error=str(exc))
            await msg.reply_text(
                "<blockquote>✗ ɪ ᴄᴀɴɴᴏᴛ ᴀᴄᴄᴇss ᴛʜᴀᴛ ᴄʜᴀɴɴᴇʟ.\n» ᴀᴅᴅ ᴍᴇ ᴀs ᴀᴅᴍɪɴ ғɪʀsᴛ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        channel_id   = ch.id
        channel_name = ch.title or "ᴄʜᴀɴɴᴇʟ"

        if fsub_col.find_one({"id": channel_id}):
            ADD_FSUB_WAIT.discard(uid)
            await msg.reply_text(
                f"<blockquote>» <b>{channel_name}</b> ɪs ᴀʟʀᴇᴀᴅʏ ɪɴ ғsᴜʙ ʟɪsᴛ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        if getattr(ch, "username", None):
            url = f"https://t.me/{ch.username}"
            fsub_col.insert_one({"id": channel_id, "name": channel_name, "url": url, "mode": "public"})
            ADD_FSUB_WAIT.discard(uid)
            logger.info(f"fsub public channel added: {channel_name} ({channel_id})")
            await tg_log(context.bot, "INFO", f"ᴀᴅᴅғsᴜʙ ✓ [ᴘᴜʙʟɪᴄ] | {channel_name} [{channel_id}]", update.effective_user)
            await msg.reply_text(
                f"<blockquote>✓ ᴀᴅᴅᴇᴅ <b>{channel_name}</b> — ᴍᴏᴅᴇ : ᴘᴜʙʟɪᴄ</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        context.user_data["pending_fsub"] = {"id": channel_id, "name": channel_name}
        ADD_FSUB_WAIT.discard(uid)

        await msg.reply_text(
            f"<blockquote>» <b>{channel_name}</b> ɪs ᴀ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟ.\n\n"
            "» <b>ɴᴏʀᴍᴀʟ</b>         — ᴜsᴇʀs ᴊᴏɪɴ ᴠɪᴀ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ᴅɪʀᴇᴄᴛʟʏ.\n"
            "» <b>ᴊᴏɪɴ ʀᴇǫᴜᴇsᴛ</b>   — ᴜsᴇʀs sᴇɴᴅ ᴀ ʀᴇǫᴜᴇsᴛ; ʏᴏᴜ ᴀᴘᴘʀᴏᴠᴇ ᴍᴀɴᴜᴀʟʟʏ.\n\n"
            "» ᴄʜᴏᴏsᴇ ᴊᴏɪɴ ᴍᴏᴅᴇ :</blockquote>",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("» ɴᴏʀᴍᴀʟ",       callback_data="fsub_mode_normal"),
                    InlineKeyboardButton("» ᴊᴏɪɴ ʀᴇǫᴜᴇsᴛ", callback_data="fsub_mode_jr"),
                ],
                [InlineKeyboardButton("✗ ᴄᴀɴᴄᴇʟ", callback_data="fsub_mode_cancel")],
            ]),
            parse_mode=constants.ParseMode.HTML,
        )
        return

    # ── GENLINK ───────────────────────────────────────────
    if uid in GENLINK_WAIT:
        GENLINK_WAIT.remove(uid)

        raw_key = uuid.uuid4().hex[:12]
        links_col.insert_one({
            "_id":        raw_key,
            "chat_id":    msg.chat.id,
            "message_id": msg.message_id,
        })

        # ── copy to FILE_CHANNEL ──────────────────────────
        # The source of the file for genlink is always the
        # message the owner just sent/forwarded in this chat.
        if FILE_CHANNEL:
            try:
                await context.bot.copy_message(
                    chat_id=FILE_CHANNEL,
                    from_chat_id=msg.chat.id,
                    message_id=msg.message_id,
                )
                logger.info(f"genlink key={raw_key} copied to FILE_CHANNEL={FILE_CHANNEL}")
                await tg_log(
                    context.bot, "INFO",
                    f"ғɪʟᴇ ᴄʜᴀɴɴᴇʟ ʙᴀᴄᴋᴜᴘ ✓ | key={raw_key}",
                    update.effective_user,
                )
            except TelegramError as exc:
                logger.error(f"genlink file-channel copy failed key={raw_key}: {exc}")
                await tg_log(
                    context.bot, "ERROR",
                    f"ғɪʟᴇ ᴄʜᴀɴɴᴇʟ ʙᴀᴄᴋᴜᴘ ✗ | key={raw_key}",
                    error=str(exc),
                )

        # encode the raw key → URL-safe base64 for the deep-link
        encoded = encode_key(raw_key)
        link    = f"https://t.me/{BOT_USERNAME}?start={encoded}"

        logger.info(f"genlink raw_key={raw_key} encoded={encoded}")
        await tg_log(context.bot, "INFO", f"ɢᴇɴʟɪɴᴋ ᴄʀᴇᴀᴛᴇᴅ | key={raw_key}", update.effective_user)
        await msg.reply_text(
            f"» ʏᴏᴜʀ sʜᴀʀᴇᴀʙʟᴇ ʟɪɴᴋ :\n\n<code>{link}</code>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("» sʜᴀʀᴇ", url=f"https://t.me/share/url?url={link}")
            ]]),
            parse_mode=constants.ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return

    # ── BATCH ─────────────────────────────────────────────
    if uid in BATCH_WAIT:
        data = BATCH_WAIT[uid]

        # ── step 1 : receive FIRST message ────────────────
        if data["step"] == "first":
            source_chat_id    = None
            source_message_id = None

            if msg.forward_origin and hasattr(msg.forward_origin, "chat"):
                source_chat_id    = msg.forward_origin.chat.id
                source_message_id = msg.forward_origin.message_id
            elif msg.forward_from_chat:
                source_chat_id    = msg.forward_from_chat.id
                source_message_id = msg.forward_from_message_id
            elif text and "t.me/c/" in text:
                try:
                    parts             = text.split("/")
                    source_chat_id    = int("-100" + parts[-2])
                    source_message_id = int(parts[-1])
                except Exception as exc:
                    logger.error(f"batch first parse error: {exc}")
                    await msg.reply_text(
                        "<blockquote>✗ ɪɴᴠᴀʟɪᴅ ʟɪɴᴋ ғᴏʀᴍᴀᴛ.</blockquote>",
                        parse_mode=constants.ParseMode.HTML,
                    )
                    return
            else:
                await msg.reply_text(
                    "<blockquote>✗ ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ ᴀ ᴍᴇssᴀɢᴇ ғʀᴏᴍ ᴀ ᴄʜᴀɴɴᴇʟ.</blockquote>",
                    parse_mode=constants.ParseMode.HTML,
                )
                return

            # ── verify bot is admin in that channel ────────
            try:
                me         = await context.bot.get_me()
                bot_member = await context.bot.get_chat_member(source_chat_id, me.id)
                if bot_member.status not in ("administrator", "creator"):
                    await msg.reply_text(
                        "<blockquote>"
                        "✗ ʙᴏᴛ ɪs ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ᴄʜᴀɴɴᴇʟ.\n"
                        "» ᴘʟᴇᴀsᴇ ᴀᴅᴅ ᴛʜᴇ ʙᴏᴛ ᴀs ᴀᴅᴍɪɴ ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ."
                        "</blockquote>",
                        parse_mode=constants.ParseMode.HTML,
                    )
                    logger.warning(f"batch rejected — bot not admin in chat={source_chat_id}")
                    await tg_log(
                        context.bot, "WARN",
                        f"ʙᴀᴛᴄʜ ʀᴇᴊᴇᴄᴛᴇᴅ — ʙᴏᴛ ɴᴏᴛ ᴀᴅᴍɪɴ | chat={source_chat_id}",
                        update.effective_user,
                    )
                    return
            except TelegramError as exc:
                logger.error(f"batch admin check failed chat={source_chat_id}: {exc}")
                await msg.reply_text(
                    "<blockquote>"
                    "✗ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴠᴇʀɪғʏ ʙᴏᴛ ᴘᴇʀᴍɪssɪᴏɴs.\n"
                    "» ᴇɴsᴜʀᴇ ᴛʜᴇ ʙᴏᴛ ɪs ᴀᴅᴅᴇᴅ ᴀs ᴀᴅᴍɪɴ ɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ."
                    "</blockquote>",
                    parse_mode=constants.ParseMode.HTML,
                )
                await tg_log(
                    context.bot, "ERROR",
                    f"ʙᴀᴛᴄʜ ᴀᴅᴍɪɴ ᴄʜᴇᴄᴋ ᴇʀʀᴏʀ | chat={source_chat_id}",
                    error=str(exc),
                )
                return

            data["chat_id"] = source_chat_id
            data["from_id"] = source_message_id
            data["step"]    = "last"
            await msg.reply_text(
                "<blockquote>» ɴᴏᴡ ғᴏʀᴡᴀʀᴅ ᴛʜᴇ <b>ʟᴀsᴛ</b> ᴍᴇssᴀɢᴇ ғʀᴏᴍ ᴛʜᴇ sᴀᴍᴇ ᴄʜᴀɴɴᴇʟ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        # ── step 2 : receive LAST message ─────────────────
        if data["step"] == "last":
            if msg.forward_origin and hasattr(msg.forward_origin, "chat"):
                to_id = msg.forward_origin.message_id
            elif msg.forward_from_chat:
                to_id = msg.forward_from_message_id
            elif text and "t.me/c/" in text:
                try:
                    to_id = int(text.split("/")[-1])
                except Exception as exc:
                    logger.error(f"batch last parse error: {exc}")
                    await msg.reply_text(
                        "<blockquote>✗ ɪɴᴠᴀʟɪᴅ ʟɪɴᴋ ғᴏʀᴍᴀᴛ.</blockquote>",
                        parse_mode=constants.ParseMode.HTML,
                    )
                    return
            else:
                await msg.reply_text(
                    "<blockquote>✗ ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ ᴛʜᴇ ʟᴀsᴛ ᴍᴇssᴀɢᴇ ғʀᴏᴍ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ.</blockquote>",
                    parse_mode=constants.ParseMode.HTML,
                )
                return

            if to_id < data["from_id"]:
                await msg.reply_text(
                    "<blockquote>✗ ʟᴀsᴛ ɪᴅ ᴍᴜsᴛ ʙᴇ ɢʀᴇᴀᴛᴇʀ ᴛʜᴀɴ ғɪʀsᴛ ɪᴅ.</blockquote>",
                    parse_mode=constants.ParseMode.HTML,
                )
                return

            count         = to_id - data["from_id"] + 1
            raw_batch_key = f"BATCH_{uuid.uuid4().hex[:12]}"

            batch_col.insert_one({
                "_id":     raw_batch_key,
                "chat_id": data["chat_id"],
                "from_id": data["from_id"],
                "to_id":   to_id,
            })
            del BATCH_WAIT[uid]

            # encode batch key → base64url for the deep-link
            encoded = encode_key(raw_batch_key)
            link    = f"https://t.me/{BOT_USERNAME}?start={encoded}"

            logger.info(f"batch raw_key={raw_batch_key} encoded={encoded} msgs={count}")
            await tg_log(
                context.bot, "INFO",
                f"ʙᴀᴛᴄʜ ᴄʀᴇᴀᴛᴇᴅ | key={raw_batch_key} | msgs={count}",
                update.effective_user,
            )

            await msg.reply_text(
                f"» ʙᴀᴛᴄʜ ʟɪɴᴋ — <b>{count}</b> ᴍᴇssᴀɢᴇs :\n\n<code>{link}</code>",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("» sʜᴀʀᴇ", url=f"https://t.me/share/url?url={link}")
                ]]),
                parse_mode=constants.ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )

            # ── copy batch range to FILE_CHANNEL ─────────
            # Runs after the link reply so the owner isn't
            # kept waiting.  Errors are logged but do not
            # surface to the user.
            if FILE_CHANNEL:
                progress_msg = await msg.reply_text(
                    f"<blockquote>» ᴄᴏᴘʏɪɴɢ <b>{count}</b> ᴍsɢs ᴛᴏ ғɪʟᴇ ᴄʜᴀɴɴᴇʟ…</blockquote>",
                    parse_mode=constants.ParseMode.HTML,
                )

                source_chat  = data["chat_id"]
                message_ids  = list(range(data["from_id"], to_id + 1))
                copied = 0
                failed = 0

                for mid in message_ids:
                    try:
                        await context.bot.copy_message(
                            chat_id=FILE_CHANNEL,
                            from_chat_id=source_chat,
                            message_id=mid,
                        )
                        copied += 1
                        await asyncio.sleep(0.05)
                    except RetryAfter as exc:
                        logger.warning(f"batch FC RetryAfter {exc.retry_after}s mid={mid}")
                        await asyncio.sleep(exc.retry_after)
                        try:
                            await context.bot.copy_message(
                                chat_id=FILE_CHANNEL,
                                from_chat_id=source_chat,
                                message_id=mid,
                            )
                            copied += 1
                        except TelegramError as exc2:
                            logger.error(f"batch FC retry failed mid={mid}: {exc2}")
                            failed += 1
                    except TelegramError as exc:
                        logger.error(f"batch FC copy failed mid={mid}: {exc}")
                        failed += 1

                try:
                    await progress_msg.delete()
                except:
                    pass

                summary = (
                    f"<blockquote>» ғɪʟᴇ ᴄʜᴀɴɴᴇʟ ʙᴀᴄᴋᴜᴘ ᴄᴏᴍᴘʟᴇᴛᴇ\n"
                    f"» ᴄᴏᴘɪᴇᴅ : <b>{copied}</b> / {count}"
                    + (f"\n» ғᴀɪʟᴇᴅ : <b>{failed}</b>" if failed else "")
                    + "</blockquote>"
                )
                await msg.reply_text(summary, parse_mode=constants.ParseMode.HTML)

                logger.info(
                    f"batch FC backup done key={raw_batch_key} "
                    f"copied={copied} failed={failed}"
                )
                await tg_log(
                    context.bot, "INFO",
                    f"ʙᴀᴛᴄʜ ғɪʟᴇ ᴄʜᴀɴɴᴇʟ ʙᴀᴄᴋᴜᴘ | key={raw_batch_key} "
                    f"ᴄᴏᴘɪᴇᴅ={copied} ғᴀɪʟᴇᴅ={failed}",
                    update.effective_user,
                )
            return

    # ── BAN ───────────────────────────────────────────────
    if uid in BAN_WAIT:
        BAN_WAIT.remove(uid)
        if not text or not text.isdigit():
            await msg.reply_text(
                "<blockquote>✗ sᴇɴᴅ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍᴇʀɪᴄ ᴜsᴇʀ ɪᴅ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return
        target = int(text)
        ban_col.update_one({"_id": target}, {"$set": {"_id": target}}, upsert=True)
        logger.info(f"uid={target} banned")
        await tg_log(context.bot, "WARN", f"/ʙᴀɴ — ᴜsᴇʀ <code>{target}</code> ʙᴀɴɴᴇᴅ", update.effective_user)
        await msg.reply_text(
            f"<blockquote>✓ ᴜsᴇʀ <code>{target}</code> ʜᴀs ʙᴇᴇɴ ʙᴀɴɴᴇᴅ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    # ── UNBAN ─────────────────────────────────────────────
    if uid in UNBAN_WAIT:
        UNBAN_WAIT.remove(uid)
        if not text or not text.isdigit():
            await msg.reply_text(
                "<blockquote>✗ sᴇɴᴅ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍᴇʀɪᴄ ᴜsᴇʀ ɪᴅ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return
        target = int(text)
        ban_col.delete_one({"_id": target})
        logger.info(f"uid={target} unbanned")
        await tg_log(context.bot, "WARN", f"/ᴜɴʙᴀɴ — ᴜsᴇʀ <code>{target}</code> ᴜɴʙᴀɴɴᴇᴅ", update.effective_user)
        await msg.reply_text(
            f"<blockquote>✓ ᴜsᴇʀ <code>{target}</code> ʜᴀs ʙᴇᴇɴ ᴜɴʙᴀɴɴᴇᴅ.</blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

# ══════════════════════════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ══════════════════════════════════════════════════════════

async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    uid   = query.from_user.id

    if is_banned(uid):
        await query.answer("you are banned.", show_alert=True)
        return

    await query.answer()

    # ── CHECK FSUB ────────────────────────────────────────
    if query.data == "check_fsub":
        if is_force_sub_enabled() and not await is_user_joined(context.bot, uid):
            await query.answer("✗ ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ғɪʀsᴛ.", show_alert=True)
            return

        await query.answer("✓ ᴠᴇʀɪғɪᴇᴅ — ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ.", show_alert=True)
        logger.info(f"uid={uid} passed fsub check")
        await tg_log(context.bot, "INFO", "ғsᴜʙ ᴄʜᴇᴄᴋ ᴘᴀssᴇᴅ ✓", query.from_user)

        try:
            await query.message.delete()
        except:
            pass

        pending = fsub_pending_col.find_one({"_id": uid})
        if pending:
            encoded_key   = pending["key"]
            fsub_pending_col.delete_one({"_id": uid})
            context.args  = [encoded_key]
            await start(Update(update.update_id, message=query.message), context)
            return

        context.args = []
        await start(Update(update.update_id, message=query.message), context)
        return

    # ── FSUB MODE SELECT ──────────────────────────────────
    if query.data in ("fsub_mode_normal", "fsub_mode_jr", "fsub_mode_cancel"):
        pending = context.user_data.get("pending_fsub")
        if not pending:
            await query.answer("session expired.", show_alert=True)
            return

        if query.data == "fsub_mode_cancel":
            context.user_data.pop("pending_fsub", None)
            await query.edit_message_text(
                "<blockquote>✗ ᴄᴀɴᴄᴇʟʟᴇᴅ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        channel_id   = pending["id"]
        channel_name = pending["name"]

        try:
            if query.data == "fsub_mode_normal":
                invite     = await context.bot.create_chat_invite_link(chat_id=channel_id)
                url        = invite.invite_link
                mode       = "private_normal"
                mode_label = "ɴᴏʀᴍᴀʟ"
            else:
                invite     = await context.bot.create_chat_invite_link(
                    chat_id=channel_id, creates_join_request=True
                )
                url        = invite.invite_link
                mode       = "private_join_request"
                mode_label = "ᴊᴏɪɴ ʀᴇǫᴜᴇsᴛ"

        except TelegramError as exc:
            logger.error(f"create_invite_link failed ch={channel_id}: {exc}")
            await tg_log(context.bot, "ERROR", "ᴀᴅᴅғsᴜʙ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ᴇʀʀᴏʀ", error=str(exc))
            context.user_data.pop("pending_fsub", None)
            await query.edit_message_text(
                "<blockquote>✗ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴄʀᴇᴀᴛᴇ ɪɴᴠɪᴛᴇ ʟɪɴᴋ.\n"
                "» ᴇɴsᴜʀᴇ ʙᴏᴛ ʜᴀs ɪɴᴠɪᴛᴇ ᴘᴇʀᴍɪssɪᴏɴ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        fsub_col.insert_one({"id": channel_id, "name": channel_name, "url": url, "mode": mode})
        context.user_data.pop("pending_fsub", None)

        logger.info(f"fsub private channel added: {channel_name} mode={mode}")
        await tg_log(
            context.bot, "INFO",
            f"ᴀᴅᴅғsᴜʙ ✓ [ᴘʀɪᴠᴀᴛᴇ/{mode_label}] | {channel_name} [{channel_id}]",
            query.from_user,
        )
        await query.edit_message_text(
            f"<blockquote>✓ ᴀᴅᴅᴇᴅ <b>{channel_name}</b>\n» ᴍᴏᴅᴇ : <b>{mode_label}</b></blockquote>",
            parse_mode=constants.ParseMode.HTML,
        )
        return

    # ── FSUB PICK ─────────────────────────────────────────
    if query.data.startswith("fsub_pick_"):
        ch_id = int(query.data.split("_", 2)[2])
        ch    = fsub_col.find_one({"id": ch_id})
        if not ch:
            await query.answer("channel not found.", show_alert=True)
            return

        ml = _mode_label(ch.get("mode", "public"))
        await query.edit_message_text(
            f"<blockquote>"
            f"» ɴᴀᴍᴇ : <b>{ch.get('name', 'ᴜɴᴋɴᴏᴡɴ')}</b>\n"
            f"» ɪᴅ   : <code>{ch_id}</code>\n"
            f"» ᴍᴏᴅᴇ : <b>{ml}</b>\n\n"
            f"» ʀᴇᴍᴏᴠᴇ ᴛʜɪs ᴄʜᴀɴɴᴇʟ ғʀᴏᴍ ғsᴜʙ?"
            f"</blockquote>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✗ ʀᴇᴍᴏᴠᴇ", callback_data=f"fsub_remove_{ch_id}"),
                InlineKeyboardButton("ᴄʟᴏsᴇ",    callback_data="close_msg"),
            ]]),
            parse_mode=constants.ParseMode.HTML,
        )
        return

    # ── FSUB REMOVE ───────────────────────────────────────
    if query.data.startswith("fsub_remove_"):
        ch_id = int(query.data.split("_", 2)[2])
        doc   = fsub_col.find_one({"id": ch_id})
        fsub_col.delete_one({"id": ch_id})

        removed_name = doc.get("name", "ᴄʜᴀɴɴᴇʟ") if doc else "ᴄʜᴀɴɴᴇʟ"
        logger.info(f"fsub channel removed: {ch_id}")
        await tg_log(context.bot, "WARN", f"ᴅᴇʟғsᴜʙ — ʀᴇᴍᴏᴠᴇᴅ {removed_name} [{ch_id}]", query.from_user)

        channels = get_fsub_channels()
        if not channels:
            await query.edit_message_text(
                f"<blockquote>✓ ʀᴇᴍᴏᴠᴇᴅ <b>{removed_name}</b>.\n» ɴᴏ ᴄʜᴀɴɴᴇʟs ʀᴇᴍᴀɪɴɪɴɢ.</blockquote>",
                parse_mode=constants.ParseMode.HTML,
            )
            return

        buttons, row = [], []
        for ch in channels:
            tag = " [ᴊʀ]" if ch.get("mode") == "private_join_request" else ""
            row.append(InlineKeyboardButton(f"{ch['name']}{tag}", callback_data=f"fsub_pick_{ch['id']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("✗ ᴄʟᴏsᴇ", callback_data="close_msg")])

        await query.edit_message_text(
            f"<blockquote>✓ ʀᴇᴍᴏᴠᴇᴅ <b>{removed_name}</b>.\n» sᴇʟᴇᴄᴛ ᴀɴᴏᴛʜᴇʀ ᴄʜᴀɴɴᴇʟ :</blockquote>",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=constants.ParseMode.HTML,
        )
        return

    # ── CLOSE ─────────────────────────────────────────────
    if query.data == "close_msg":
        try:
            await query.message.delete()
        except:
            pass
        return

    # ── ABOUT ─────────────────────────────────────────────
    if query.data == "about":
        await query.edit_message_text(
            "<b>» ʙᴏᴛ ɪɴғᴏʀᴍᴀᴛɪᴏɴ</b>\n\n"
            "<blockquote expandable>"
            "» ɴᴀᴍᴇ       : <a href='https://t.me/Mask_File_bot'>I'ᴍ ᴄᴜᴛɪᴇ</a>\n"
            "» ᴅᴇᴠᴇʟᴏᴘᴇʀ  : @Shiva_Analyst07\n"
            "» ʟɪʙʀᴀʀʏ   : <a href='https://docs.python-telegram-bot.org/'>PTB v22</a>\n"
            "» ʟᴀɴɢᴜᴀɢᴇ  : <a href='https://www.python.org/'>Python 3</a>\n"
            "» ᴅᴀᴛᴀʙᴀsᴇ  : <a href='https://www.mongodb.com/'>MongoDB</a>\n"
            "» ʜᴏsᴛɪɴɢ   : <a href='https://t.me/'>VPS</a>"
            "</blockquote>",
            reply_markup=about_keyboard(),
            parse_mode=constants.ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return

    # ── BACK TO START ─────────────────────────────────────
    if query.data == "back_to_start":
        await query.edit_message_text(
            "<blockquote>» ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ᴀᴅᴠᴀɴᴄᴇᴅ ʟɪɴᴋs &amp; ғɪʟᴇ sʜᴀʀɪɴɢ ʙᴏᴛ.\n"
            "» sʜᴀʀᴇ ʟɪɴᴋs ᴀɴᴅ ғɪʟᴇs ᴡʜɪʟᴇ ᴋᴇᴇᴘɪɴɢ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟs sᴀғᴇ.</blockquote>\n\n"
            "<blockquote>» ᴍᴀɪɴᴛᴀɪɴᴇᴅ ʙʏ : "
            "<a href='https://t.me/Shiva_Analyst07'>Shiva_Analyst</a></blockquote>",
            reply_markup=start_keyboard(),
            parse_mode=constants.ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return

    logger.warning(f"unhandled callback='{query.data}' uid={uid}")

# ══════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLER
# ══════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = str(context.error)
    tb  = "".join(traceback.format_tb(context.error.__traceback__))[:600]
    logger.error(f"unhandled exception: {err}\n{tb}")
    await tg_log(
        context.bot,
        "ERROR",
        f"ᴜɴʜᴀɴᴅʟᴇᴅ ᴇxᴄᴇᴘᴛɪᴏɴ : <code>{err[:300]}</code>",
        error=tb,
    )

# ══════════════════════════════════════════════════════════
#  POST-INIT
# ══════════════════════════════════════════════════════════

async def post_init(application: Application) -> None:
    logger.info("post_init running")

    try:
        users_col.create_index("_id", background=True)
        ban_col.create_index("_id",   background=True)
        links_col.create_index("_id", background=True)
        batch_col.create_index("_id", background=True)
        logger.info("MongoDB indexes ensured")
    except Exception as exc:
        logger.warning(f"index creation warning: {exc}")

    await tg_log(application.bot, "SYSTEM", "ʙᴏᴛ sᴛᴀʀᴛᴇᴅ ✓")

    fc_status = f"ᴀᴄᴛɪᴠᴇ (<code>{FILE_CHANNEL}</code>)" if FILE_CHANNEL else "ɴᴏᴛ sᴇᴛ"

    try:
        total = users_col.count_documents({})
        await application.bot.send_message(
            chat_id=OWNER_ID,
            text=(
                "<b>» ʙᴏᴛ ʀᴇsᴛᴀʀᴛᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ ✓</b>\n\n"
                "<blockquote>"
                "» ɴᴇᴡ ᴅᴇᴘʟᴏʏᴍᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ\n"
                f"» ᴛᴏᴛᴀʟ ᴜsᴇʀs ɪɴ ᴅʙ : <code>{total}</code>\n"
                f"» ғɪʟᴇ ᴄʜᴀɴɴᴇʟ      : {fc_status}\n"
                f"» ᴛɪᴍᴇ : <code>{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</code>"
                "</blockquote>"
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("» sᴜᴘᴘᴏʀᴛ", url="https://t.me/Shiva_Analyst07"),
                InlineKeyboardButton("» ᴄʜᴀɴɴᴇʟ",  url="https://t.me/Shiva_Analyst07"),
            ]]),
            parse_mode=constants.ParseMode.HTML,
        )
    except TelegramError as exc:
        logger.error(f"post_init owner notify failed: {exc}")

# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════

def main() -> None:
    Thread(target=run_flask, daemon=True).start()
    logger.info("Flask keep-alive started")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .post_init(post_init)
        .build()
    )

    application.add_error_handler(error_handler)

    application.add_handler(CommandHandler("start",     start))
    application.add_handler(CommandHandler("help",      help_cmd))
    application.add_handler(CommandHandler("genlink",   genlink_cmd))
    application.add_handler(CommandHandler("batch",     batch_cmd))
    application.add_handler(CommandHandler("users",     users_cmd))
    application.add_handler(CommandHandler("broadcast", broadcast_cmd))
    application.add_handler(CommandHandler("setdel",    setdel_cmd))
    application.add_handler(CommandHandler("ban",       ban_cmd))
    application.add_handler(CommandHandler("unban",     unban_cmd))
    application.add_handler(CommandHandler("fsub",      fsub_cmd))
    application.add_handler(CommandHandler("addfsub",   addfsub_cmd))
    application.add_handler(CommandHandler("delfsub",   delfsub_cmd))

    application.add_handler(CallbackQueryHandler(handle_callbacks))
    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, private_handler)
    )

    logger.info("all handlers registered — starting polling")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
