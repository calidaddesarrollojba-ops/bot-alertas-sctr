import os
import json
import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()

TAB_ALERTAS = "ALERTAS_SCTR"
TAB_ACK = "ACK_ALERTAS"
TAB_CONFIG = "CONFIG_ALERTAS"


def get_gspread_client() -> gspread.Client:
    if not GOOGLE_CREDS_JSON_TEXT:
        raise RuntimeError("Falta GOOGLE_CREDS_JSON_TEXT")
    info = json.loads(GOOGLE_CREDS_JSON_TEXT)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot de Alertas SCTR activo.")


async def ping_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not SHEET_ID:
            await update.message.reply_text("❌ Falta SHEET_ID en Railway Variables.")
            return

        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws1 = sh.worksheet(TAB_ALERTAS)
        ws2 = sh.worksheet(TAB_ACK)
        ws3 = sh.worksheet(TAB_CONFIG)

        h1 = ws1.row_values(1)
        h2 = ws2.row_values(1)
        h3 = ws3.row_values(1)

        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await update.message.reply_text(
            "✅ Conexión OK con Google Sheets\n"
            f"- {TAB_ALERTAS}: {len(h1)} columnas\n"
            f"- {TAB_ACK}: {len(h2)} columnas\n"
            f"- {TAB_CONFIG}: {len(h3)} columnas\n"
            f"Hora: {now_s}"
        )
    except Exception as e:
        logging.exception("ping_sheet error")
        await update.message.reply_text(f"❌ Error conectando a Sheets:\n{e}")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping_sheet", ping_sheet))

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
