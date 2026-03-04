import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot de Alertas SCTR activo.")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))

    print("Bot corriendo...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()