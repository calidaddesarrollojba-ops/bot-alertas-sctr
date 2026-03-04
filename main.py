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

def build_tablero_text_from_alertas(rows: list) -> str:
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # filtrar niveles válidos
    valid = []
    for r in rows:
        nivel = str(r.get("NIVEL", "")).strip().upper()
        if nivel in ("CRITICO", "ALERTA", "PROXIMO"):
            valid.append(r)

    # ordenar por nivel y días
    order = {"CRITICO": 0, "ALERTA": 1, "PROXIMO": 2}
    def key_fn(r):
        nivel = str(r.get("NIVEL", "")).strip().upper()
        dias_s = str(r.get("DIAS_RESTANTES", "999")).strip()
        try:
            dias = int(float(dias_s))
        except Exception:
            dias = 999
        emp = str(r.get("EMPRESA", "")).strip()
        return (order.get(nivel, 9), dias, emp)

    valid.sort(key=key_fn)

    # agrupar
    groups = {"CRITICO": [], "ALERTA": [], "PROXIMO": []}
    for r in valid:
        groups[str(r.get("NIVEL", "")).strip().upper()].append(r)

    title = f"📌 TABLERO SCTR\nActualizado: {now_s}\n\n"
    parts = [title]

    def fmt_line(r):
        emp = str(r.get("EMPRESA", "—")).strip() or "—"
        ffin = str(r.get("FECHA_FIN", "—")).strip() or "—"
        dias = str(r.get("DIAS_RESTANTES", "—")).strip() or "—"
        estado = str(r.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()

        badge = "⬜ Sin confirmar"
        if estado == "RECIBIDO":
            badge = "✅ Recibido"
        elif estado == "EN_PROCESO":
            badge = "🟠 En proceso"
        elif estado == "RENOVADO":
            badge = "✅ Renovado"

        return f"• {emp} — {ffin} — {dias} días — {badge}"

    if groups["CRITICO"]:
        parts.append("🔴 VENCEN 0–3 DÍAS")
        parts += [fmt_line(r) for r in groups["CRITICO"]]
        parts.append("")

    if groups["ALERTA"]:
        parts.append("🟠 PRÓXIMOS 4–7 DÍAS")
        parts += [fmt_line(r) for r in groups["ALERTA"]]
        parts.append("")

    if groups["PROXIMO"]:
        parts.append("🟡 PRÓXIMOS 8–15 DÍAS")
        parts += [fmt_line(r) for r in groups["PROXIMO"]]
        parts.append("")

    if not valid:
        parts.append("No hay alertas activas en este momento.")

    parts.append("\n⚠️ Este tablero se actualiza con /actualizar_tablero.")
    return "\n".join(parts).strip()


async def actualizar_tablero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /actualizar_tablero
    - Lee CONFIG_ALERTAS (chat_id + message_id)
    - Lee ALERTAS_SCTR
    - Edita el mensaje del tablero
    """
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws_cfg = _ws(sh, TAB_CONFIG)
        ws_alert = _ws(sh, TAB_ALERTAS)

        cfg_rows = ws_cfg.get_all_records()
        if not cfg_rows:
            await update.message.reply_text("❌ CONFIG_ALERTAS está vacío. Usa /crear_tablero.")
            return

        # Tomamos la primera fila con CHAT_ID_ALERTAS
        cfg = None
        for r in cfg_rows:
            if str(r.get("CHAT_ID_ALERTAS", "")).strip():
                cfg = r
                break
        if not cfg:
            await update.message.reply_text("❌ No encontré CHAT_ID_ALERTAS en CONFIG_ALERTAS.")
            return

        chat_id = str(cfg.get("CHAT_ID_ALERTAS", "")).strip()
        msg_id = str(cfg.get("TABLERO_MESSAGE_ID", "")).strip()
        if not chat_id or not msg_id:
            await update.message.reply_text("❌ Falta CHAT_ID_ALERTAS o TABLERO_MESSAGE_ID en CONFIG_ALERTAS.")
            return

        rows = ws_alert.get_all_records()
        text = build_tablero_text_from_alertas(rows)

        await context.bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(msg_id),
            text=text
        )

        # actualizar ULTIMA_ACTUALIZACION en CONFIG_ALERTAS (fila del chat)
        headers = _headers(ws_cfg)
        c_chat = _col(headers, "CHAT_ID_ALERTAS")
        c_upd = _col(headers, "ULTIMA_ACTUALIZACION")
        row_i = _find_row_by_value(ws_cfg, c_chat, chat_id)
        if row_i:
            now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ws_cfg.update_cell(row_i, c_upd, now_s)

        await update.message.reply_text("✅ Tablero actualizado.")
    except Exception as e:
        logging.exception("actualizar_tablero error")
        await update.message.reply_text(f"❌ Error actualizando tablero:\n{e}")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping_sheet", ping_sheet))
    app.add_handler(CommandHandler("crear_tablero", crear_tablero))
    app.add_handler(CommandHandler("actualizar_tablero", actualizar_tablero))

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()


