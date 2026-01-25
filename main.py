import os
import time
from datetime import datetime

from aiohttp import web
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
    raise RuntimeError("❌ BOT_TOKEN missing")
if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URI missing")
if not RENDER_EXTERNAL_URL:
    raise RuntimeError("❌ RENDER_EXTERNAL_URL missing (needed for webhook mode)")

# =========================
# Mongo
# =========================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["telegram_anti_fake"]
messages_col = db["tg_messages"]
userstats_col = db["tg_user_stats"]

messages_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING), ("ts", ASCENDING)])
userstats_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)

# =========================
# Bot Handlers
# =========================
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
        ts = int(msg.date.timestamp()) if msg.date else int(time.time())

        messages_col.insert_one({
            "chat_id": chat.id,
            "user_id": user.id,
            "username": user.username or "",
            "full_name": (user.full_name or "").strip(),
            "ts": ts,
            "iso": datetime.utcfromtimestamp(ts).isoformat() + "Z",
            "text": safe_text(msg),
            "msg_id": msg.message_id,
            "chat_type": chat.type,
        })

        userstats_col.update_one(
            {"chat_id": chat.id, "user_id": user.id},
            {
                "$setOnInsert": {"chat_id": chat.id, "user_id": user.id, "first_seen": ts},
                "$set": {
                    "username": user.username or "",
                    "full_name": (user.full_name or "").strip(),
                    "last_seen": ts,
                },
                "$inc": {"msg_count": 1},
            },
            upsert=True,
        )

    except PyMongoError as e:
        print(f"Mongo error: {e}")
    except Exception as e:
        print(f"Error collecting: {e}")

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Logger bot is running.\nUse /stats")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    total_msgs = messages_col.count_documents({})
    total_users = len(userstats_col.distinct("user_id"))
    chat_msgs = messages_col.count_documents({"chat_id": chat_id})
    chat_users = userstats_col.count_documents({"chat_id": chat_id})

    await update.message.reply_text(
        f"📊 Stats\n\n"
        f"🌍 Total users: {total_users}\n"
        f"🌍 Total msgs: {total_msgs}\n\n"
        f"💬 This chat users: {chat_users}\n"
        f"💬 This chat msgs: {chat_msgs}"
    )

# =========================
# Web Server Handlers
# =========================
async def health_check(request):
    return web.Response(text="OK", status=200)

async def telegram_webhook(request):
    bot_app: Application = request.app["bot_app"]
    try:
        data = await request.json()
        update = Update.de_json(data, bot_app.bot)

        # push update to PTB queue
        await bot_app.update_queue.put(update)

        return web.Response(text="Accepted", status=200)
    except Exception as e:
        print(f"Webhook error: {e}")
        return web.Response(text="Error", status=500)

# =========================
# Startup / Cleanup
# =========================
async def on_startup(app):
    bot_app: Application = app["bot_app"]

    print("🚀 Starting Bot...")
    await bot_app.initialize()
    await bot_app.start()

    # ✅ IMPORTANT: start PTB update processor (consumes update_queue)
    await bot_app.updater.start_polling()

    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
    print(f"🔗 Setting webhook: {webhook_url}")

    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.bot.set_webhook(webhook_url)

async def on_cleanup(app):
    bot_app: Application = app["bot_app"]
    print("🛑 Stopping Bot...")

    try:
        await bot_app.updater.stop()
    except Exception:
        pass

    await bot_app.stop()
    await bot_app.shutdown()

# =========================
# Main
# =========================
if __name__ == "__main__":
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_cmd))
    bot_app.add_handler(CommandHandler("stats", stats_cmd))
    bot_app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, collect_message))

    web_app = web.Application()
    web_app["bot_app"] = bot_app

    web_app.router.add_get("/", health_check)
    web_app.router.add_get("/health", health_check)
    web_app.router.add_post("/webhook", telegram_webhook)

    web_app.on_startup.append(on_startup)
    web_app.on_cleanup.append(on_cleanup)

    print(f"✅ Server running on port {PORT}")
    web.run_app(web_app, port=PORT, host="0.0.0.0")





