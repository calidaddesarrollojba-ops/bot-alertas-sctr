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

def _ws(sh, name: str):
    return sh.worksheet(name)

def _headers(ws):
    return [h.strip() for h in ws.row_values(1)]

def _col(headers, name: str) -> int:
    return headers.index(name) + 1  # 1-based

def _find_row_by_value(ws, col_idx: int, value: str):
    vals = ws.col_values(col_idx)
    for i, v in enumerate(vals[1:], start=2):
        if str(v).strip() == str(value).strip():
            return i
    return None

async def crear_tablero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /crear_tablero
    - Ejecutar dentro del grupo destino.
    - Crea el mensaje del tablero y guarda el message_id en CONFIG_ALERTAS.
    """
    try:
        chat = update.effective_chat
        if not chat:
            return
        chat_id = str(chat.id)

        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_cfg = _ws(sh, TAB_CONFIG)

        headers = _headers(ws_cfg)
        for required in ("CHAT_ID_ALERTAS", "TABLERO_MESSAGE_ID", "ULTIMA_ACTUALIZACION"):
            if required not in headers:
                await update.message.reply_text(f"❌ CONFIG_ALERTAS no tiene columna: {required}")
                return

        # 1) Crear mensaje tablero
        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tablero_text = (
            "📌 TABLERO SCTR\n"
            f"Actualizado: {now_s}\n\n"
            "✅ Conexión lista.\n"
            "Siguiente: cargaremos alertas y botones."
        )

        msg = await chat.send_message(tablero_text)

        # 2) Guardar / upsert en CONFIG_ALERTAS por CHAT_ID_ALERTAS
        c_chat = _col(headers, "CHAT_ID_ALERTAS")
        c_mid = _col(headers, "TABLERO_MESSAGE_ID")
        c_upd = _col(headers, "ULTIMA_ACTUALIZACION")

        row = _find_row_by_value(ws_cfg, c_chat, chat_id)
        if row is None:
            new_row = [""] * len(headers)
            new_row[c_chat - 1] = chat_id
            new_row[c_mid - 1] = str(msg.message_id)
            new_row[c_upd - 1] = now_s
            ws_cfg.append_row(new_row, value_input_option="USER_ENTERED")
        else:
            ws_cfg.update_cell(row, c_mid, str(msg.message_id))
            ws_cfg.update_cell(row, c_upd, now_s)

        await update.message.reply_text(
            "✅ Tablero creado y registrado en CONFIG_ALERTAS.\n"
            f"CHAT_ID_ALERTAS={chat_id}\n"
            f"TABLERO_MESSAGE_ID={msg.message_id}\n\n"
            "📌 Ahora ANCLA (PIN) ese mensaje en el grupo."
        )

    except Exception as e:
        logging.exception("crear_tablero error")
        await update.message.reply_text(f"❌ Error creando tablero:\n{e}")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping_sheet", ping_sheet))
    app.add_handler(CommandHandler("crear_tablero", crear_tablero))

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()

