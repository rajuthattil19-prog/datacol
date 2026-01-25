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
meta_col = db["tg_meta"]  # store offset + last_update_id

# Indexes
messages_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING), ("ts", ASCENDING)])
userstats_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)

# ✅ HARD dedupe index (prevents duplicate inserts)
# same message_id in same chat should be unique
messages_col.create_index([("chat_id", ASCENDING), ("msg_id", ASCENDING)], unique=True)

# =========================
# Telegram Bot (raw)
# =========================
bot = Bot(token=BOT_TOKEN)

# =========================
# Offset persistence
# =========================
def get_offset() -> int | None:
    doc = meta_col.find_one({"_id": "poll_offset"})
    if not doc:
        return None
    return doc.get("value")

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

async def save_message_to_mongo(update):
    """
    Saves 1 message per document.
    Duplicates are automatically ignored using unique index (chat_id, msg_id).
    """
    try:
        if not update.message:
            return

        chat = update.effective_chat
        user = update.effective_user
        msg = update.message

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

        # insert (will raise DuplicateKeyError if already exists)
        messages_col.insert_one(doc)

        # update user stats
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
        # already saved, ignore
        return
    except PyMongoError as e:
        print("Mongo error:", e)
    except Exception as e:
        print("Save error:", e)

async def reply_stats(chat_id: int):
    total_msgs = messages_col.count_documents({})
    total_chats = len(messages_col.distinct("chat_id"))
    total_users = len(userstats_col.distinct("user_id"))

    chat_msgs = messages_col.count_documents({"chat_id": chat_id})
    chat_users = userstats_col.count_documents({"chat_id": chat_id})

    text = (
        "📊 Stats\n\n"
        f"🌍 Chats tracked: {total_chats}\n"
        f"🌍 Users tracked: {total_users}\n"
        f"🌍 Messages stored: {total_msgs}\n\n"
        f"💬 This chat users: {chat_users}\n"
        f"💬 This chat messages: {chat_msgs}"
    )
    await bot.send_message(chat_id=chat_id, text=text)

# =========================
# Manual Polling Loop (Blink style)
# =========================
polling_started = False

async def poll_loop():
    global polling_started
    if polling_started:
        print("⚠️ Poll loop already running, skipping second start")
        return
    polling_started = True

    print("✅ Manual polling started...")

    # load saved offset so restart won't duplicate
    offset = get_offset()
    print("✅ Loaded offset from DB:", offset)

    # IMPORTANT: remove webhook if exists (otherwise conflict)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Webhook deleted (safe for polling)")
    except Exception as e:
        print("⚠️ delete_webhook failed:", e)

    while True:
        start_time = asyncio.get_event_loop().time()

        # ON window ~1.2s
        while asyncio.get_event_loop().time() - start_time < 1.2:
            try:
                updates = await asyncio.wait_for(
                    bot.get_updates(offset=offset, timeout=5),
                    timeout=6
                )

                if updates:
                    # move offset forward and persist
                    offset = updates[-1].update_id + 1
                    set_offset(offset)

                    for upd in updates:
                        # Save every message (deduped by Mongo unique index)
                        await save_message_to_mongo(upd)

                        # manual command detection (dedupe reply using msg_id)
                        if upd.message and upd.message.text:
                            txt = upd.message.text.strip()

                            if txt == "/stats":
                                await reply_stats(upd.effective_chat.id)

                            elif txt == "/start":
                                await bot.send_message(
                                    chat_id=upd.effective_chat.id,
                                    text="✅ Logger bot running.\nUse /stats"
                                )

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print("Polling error:", e)

        # OFF window
        await asyncio.sleep(3)

def start_polling_thread():
    asyncio.run(poll_loop())

# =========================
# Start everything
# =========================
if __name__ == "__main__":
    threading.Thread(target=start_polling_thread, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)







