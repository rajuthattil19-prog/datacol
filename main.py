import os
import time
import sys
import asyncio
from datetime import datetime

# Windows fix for polling
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI missing")

USE_WEBHOOK = bool(RENDER_EXTERNAL_URL)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["telegram_anti_fake"]

messages_col = db["tg_messages"]
userstats_col = db["tg_user_stats"]

messages_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING), ("ts", ASCENDING)])
userstats_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)

def safe_text(msg) -> str:
    if not msg:
        return ""
    return msg.text or msg.caption or ""

async def collect_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return

        chat = update.effective_chat
        user = update.effective_user
        msg = update.message

        chat_id = chat.id
        user_id = user.id

        username = user.username or ""
        full_name = (user.full_name or "").strip()

        ts = int(msg.date.timestamp()) if msg.date else int(time.time())
        text = safe_text(msg)

        messages_col.insert_one({
            "chat_id": chat_id,
            "user_id": user_id,
            "username": username,
            "full_name": full_name,
            "ts": ts,
            "iso": datetime.utcfromtimestamp(ts).isoformat() + "Z",
            "text": text,
            "msg_id": msg.message_id,
            "chat_type": chat.type,
        })

        userstats_col.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {
                "$setOnInsert": {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "first_seen": ts,
                },
                "$set": {
                    "username": username,
                    "full_name": full_name,
                    "last_seen": ts,
                },
                "$inc": {"msg_count": 1},
            },
            upsert=True,
        )

    except PyMongoError as e:
        print("Mongo error:", e)
    except Exception as e:
        print("Collect error:", e)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Logger bot running. Use /stats")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    total_msgs = messages_col.count_documents({})
    total_users = len(userstats_col.distinct("user_id"))

    chat_msgs = messages_col.count_documents({"chat_id": chat_id})
    chat_users = userstats_col.count_documents({"chat_id": chat_id})

    await update.message.reply_text(
        f"📊 Stats\n\n"
        f"🌍 Total users tracked: {total_users}\n"
        f"🌍 Total messages stored: {total_msgs}\n\n"
        f"💬 This chat users: {chat_users}\n"
        f"💬 This chat messages: {chat_msgs}"
    )

application = Application.builder().token(BOT_TOKEN).build()
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(CommandHandler("stats", stats_cmd))
application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, collect_message))

if __name__ == "__main__":
    if USE_WEBHOOK:
        print("✅ WEBHOOK mode")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path="webhook",
            webhook_url=f"{RENDER_EXTERNAL_URL}/webhook",
            drop_pending_updates=True,
        )
    else:
        print("✅ POLLING mode")
        application.run_polling(drop_pending_updates=True, close_loop=False)




