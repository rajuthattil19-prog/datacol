import os
import time
import asyncio
import threading
from datetime import datetime

from flask import Flask
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError

from telegram import Bot

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN missing")
if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URI missing")

# =========================
# Flask for /health
# =========================
app = Flask(__name__)

@app.get("/")
def home():
    return "OK", 200

@app.get("/health")
def health():
    return "OK", 200

# =========================
# Mongo
# =========================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["telegram_anti_fake"]

messages_col = db["tg_messages"]
userstats_col = db["tg_user_stats"]
meta_col = db["tg_meta"]  # store offset

# Indexes
messages_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING), ("ts", ASCENDING)])
userstats_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)

# Hard dedupe index
messages_col.create_index([("chat_id", ASCENDING), ("msg_id", ASCENDING)], unique=True)

# =========================
# Telegram Bot
# =========================
bot = Bot(token=BOT_TOKEN)

# =========================
# Offset persistence
# =========================
def get_offset():
    doc = meta_col.find_one({"_id": "poll_offset"})
    return doc.get("value") if doc else None

def set_offset(value: int):
    meta_col.update_one(
        {"_id": "poll_offset"},
        {"$set": {"value": value, "updated_at": int(time.time())}},
        upsert=True
    )

# =========================
# Helpers
# =========================
def safe_text(msg) -> str:
    if not msg:
        return ""
    return msg.text or msg.caption or ""

def is_service_message(msg) -> bool:
    return bool(
        msg.new_chat_members or
        msg.left_chat_member or
        msg.pinned_message or
        msg.group_chat_created or
        msg.supergroup_chat_created or
        msg.channel_chat_created
    )

def is_group_chat(chat_type: str) -> bool:
    return chat_type in ("group", "supergroup")

async def save_message_to_mongo(update):
    """Save only group/supergroup messages"""
    try:
        if not update.message:
            return

        chat = update.effective_chat
        user = update.effective_user
        msg = update.message

        if is_service_message(msg):
            return

        # ✅ Skip private messages from logging
        if not is_group_chat(chat.type):
            return

        ts = int(msg.date.timestamp()) if msg.date else int(time.time())
        text = safe_text(msg)

        doc = {
            "chat_id": chat.id,
            "user_id": user.id,
            "username": user.username or "",
            "full_name": (user.full_name or "").strip(),
            "ts": ts,
            "iso": datetime.utcfromtimestamp(ts).isoformat() + "Z",
            "text": text,
            "msg_id": msg.message_id,
            "chat_type": chat.type,
        }

        messages_col.insert_one(doc)

        userstats_col.update_one(
            {"chat_id": chat.id, "user_id": user.id},
            {
                "$setOnInsert": {
                    "chat_id": chat.id,
                    "user_id": user.id,
                    "first_seen": ts,
                },
                "$set": {
                    "username": user.username or "",
                    "full_name": (user.full_name or "").strip(),
                    "last_seen": ts,
                },
                "$inc": {"msg_count": 1},
            },
            upsert=True,
        )

    except DuplicateKeyError:
        return
    except PyMongoError as e:
        print("Mongo error:", e)
    except Exception as e:
        print("Save error:", e)

async def reply_stats(chat_id: int):
    # global only group stats
    total_msgs = messages_col.count_documents({})
    total_chats = len(messages_col.distinct("chat_id"))
    total_users = len(userstats_col.distinct("user_id"))

    # this chat stats
    chat_msgs = messages_col.count_documents({"chat_id": chat_id})
    chat_users = userstats_col.count_documents({"chat_id": chat_id})

    text = (
        "📊 Stats (Group Logs Only)\n\n"
        f"🌍 Group chats tracked: {total_chats}\n"
        f"🌍 Users tracked: {total_users}\n"
        f"🌍 Messages stored: {total_msgs}\n\n"
        f"💬 This chat users: {chat_users}\n"
        f"💬 This chat messages: {chat_msgs}"
    )
    await bot.send_message(chat_id=chat_id, text=text)

# =========================
# Polling Loop
# =========================
polling_started = False

async def poll_loop():
    global polling_started
    if polling_started:
        print("⚠️ Poll loop already running, skipping second start")
        return
    polling_started = True

    print("✅ Manual polling started...")

    offset = get_offset()
    print("✅ Loaded offset from DB:", offset)

    # Remove webhook (safe for polling)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Webhook deleted (safe for polling)")
    except Exception as e:
        print("⚠️ delete_webhook failed:", e)

    while True:
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < 1.2:
            try:
                updates = await asyncio.wait_for(
                    bot.get_updates(offset=offset, timeout=5),
                    timeout=6
                )

                if updates:
                    offset = updates[-1].update_id + 1
                    set_offset(offset)

                    for upd in updates:
                        # ✅ Always allow commands to reply (private + group)
                        if upd.message and upd.message.text:
                            txt = upd.message.text.strip()

                            if txt == "/start":
                                await bot.send_message(
                                    chat_id=upd.effective_chat.id,
                                    text="✅ Logger bot running.\nIt logs ONLY group messages.\nUse /stats"
                                )

                            elif txt == "/stats":
                                await reply_stats(upd.effective_chat.id)

                        # ✅ Save only group messages
                        await save_message_to_mongo(upd)

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print("Polling error:", e)

        await asyncio.sleep(3)

def start_polling_thread():
    asyncio.run(poll_loop())

# =========================
# Start everything
# =========================
if __name__ == "__main__":
    threading.Thread(target=start_polling_thread, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

