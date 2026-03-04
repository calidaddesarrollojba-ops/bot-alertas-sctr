import os
import json
import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
)

logging.basicConfig(level=logging.INFO)

# ======================
# ENV / CONSTANTES
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()

TAB_ALERTAS = "ALERTAS_SCTR"
TAB_ACK = "ACK_ALERTAS"
TAB_CONFIG = "CONFIG_ALERTAS"
TAB_RESP = "RESPONSABLES_EMPRESA"


# ======================
# GOOGLE SHEETS
# ======================
def get_gspread_client() -> gspread.Client:
    if not GOOGLE_CREDS_JSON_TEXT:
        raise RuntimeError("Falta GOOGLE_CREDS_JSON_TEXT")
    info = json.loads(GOOGLE_CREDS_JSON_TEXT)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ======================
# HELPERS SHEET
# ======================
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


# ======================
# COMANDOS BÁSICOS
# ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot de Alertas SCTR activo.")


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    name = f"@{u.username}" if u.username else (u.full_name or "Usuario")
    await update.message.reply_text(f"👤 {name}\n🆔 USER_ID: {u.id}")


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


# ======================
# TABLERO: CREAR
# ======================
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


# ======================
# TABLERO: CONSTRUIR TEXTO
# ======================
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


# ======================
# TABLERO: ACTUALIZAR (EDITAR MENSAJE)
# ======================
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


# ======================
# DETALLE POR EMPRESA + BOTONES
# ======================
async def detalle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /detalle NOMBRE_EMPRESA")
        return

    empresa_q = " ".join(context.args).strip().lower()

    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_ALERTAS)

    rows = ws.get_all_records()

    alerta = None
    for r in rows:
        emp = str(r.get("EMPRESA", "")).strip().lower()
        if emp and (empresa_q in emp):
            alerta = r
            break

    if not alerta:
        await update.message.reply_text("Empresa no encontrada en ALERTAS_SCTR.")
        return

    empresa_nombre = str(alerta.get("EMPRESA", "")).strip()
    fecha_fin = str(alerta.get("FECHA_FIN", "")).strip()
    dias = str(alerta.get("DIAS_RESTANTES", "")).strip()
    estado = str(alerta.get("ESTADO", "SIN_CONFIRMAR")).strip()
    id_alerta = str(alerta.get("ID_ALERTA", "")).strip()

    text = (
        f"📋 DETALLE SCTR\n\n"
        f"Empresa: {empresa_nombre}\n"
        f"Vence: {fecha_fin}\n"
        f"Días restantes: {dias}\n"
        f"Estado: {estado}\n"
        f"ID_ALERTA: {id_alerta}"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Recibido", callback_data=f"ACK|{id_alerta}|RECIBIDO"),
            InlineKeyboardButton("📝 En proceso", callback_data=f"ACK|{id_alerta}|EN_PROCESO"),
            InlineKeyboardButton("✔ Renovado", callback_data=f"ACK|{id_alerta}|RENOVADO"),
        ]
    ]

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


# ======================
# CALLBACK BOTONES (ACK MÍNIMO)
# ======================
async def on_ack_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()  # desbloquea UI

    data = query.data or ""
    parts = data.split("|")
    if len(parts) != 3 or parts[0] != "ACK":
        await query.edit_message_text("❌ Callback inválido.")
        return

    id_alerta = parts[1].strip()
    accion = parts[2].strip()  # RECIBIDO / EN_PROCESO / RENOVADO

    user = query.from_user
    user_name = f"@{user.username}" if user.username else (user.full_name or "Usuario")
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Seguridad: este botón SOLO debe funcionar si proviene del mensaje detalle correcto
        msg_text = (query.message.text or "")
        if f"ID_ALERTA: {id_alerta}" not in msg_text:
            await query.answer("⚠️ Este botón no corresponde a este detalle.", show_alert=True)
            return

        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_ack = sh.worksheet(TAB_ACK)
        ws_alert = sh.worksheet(TAB_ALERTAS)
        ws_resp = sh.worksheet(TAB_RESP)

        # 1) Encontrar la empresa de este ID_ALERTA
        rows_alert = ws_alert.get_all_records()
        empresa = None
        for r in rows_alert:
            if str(r.get("ID_ALERTA", "")).strip() == id_alerta:
                empresa = str(r.get("EMPRESA", "")).strip()
                break

        if not empresa:
            await query.answer("⚠️ No se encontró la empresa de esta alerta.", show_alert=True)
            return

        # 2) Validar que el usuario esté autorizado en RESPONSABLES_EMPRESA (ACTIVO=1)
        rows_resp = ws_resp.get_all_records()
        autorizado = False
        for rr in rows_resp:
            emp = str(rr.get("EMPRESA", "")).strip().lower()
            uid = str(rr.get("USER_ID", "")).strip()
            activo = str(rr.get("ACTIVO", "1")).strip()

            if emp == empresa.lower() and uid == str(user.id) and activo == "1":
                autorizado = True
                break

        if not autorizado:
            await query.answer(f"⛔ No estás autorizado para responder por {empresa}.", show_alert=True)
            return
        

        # 1) Guardar ACK_ALERTAS por headers
        headers_ack = [h.strip() for h in ws_ack.row_values(1)]
        row_ack = {h: "" for h in headers_ack}
        row_ack["ID_ALERTA"] = id_alerta
        row_ack["ACCION"] = accion
        row_ack["USER_NAME"] = user_name
        row_ack["USER_ID"] = str(user.id)
        row_ack["CHAT_ID"] = str(query.message.chat_id)
        row_ack["TIMESTAMP"] = now_s

        ws_ack.append_row([row_ack.get(h, "") for h in headers_ack], value_input_option="USER_ENTERED")

        # 2) Actualizar ESTADO en ALERTAS_SCTR (por ID_ALERTA)
        headers_alert = [h.strip() for h in ws_alert.row_values(1)]
        if "ID_ALERTA" not in headers_alert or "ESTADO" not in headers_alert:
            await query.edit_message_text("❌ ALERTAS_SCTR debe tener columnas ID_ALERTA y ESTADO.")
            return

        col_id = headers_alert.index("ID_ALERTA") + 1
        col_estado = headers_alert.index("ESTADO") + 1

        col_vals = ws_alert.col_values(col_id)
        row_idx = None
        for i, v in enumerate(col_vals[1:], start=2):
            if str(v).strip() == id_alerta:
                row_idx = i
                break

        if row_idx:
            ws_alert.update_cell(row_idx, col_estado, accion)

        # 3) Confirmación visual en el mensaje detalle
        await query.edit_message_text(
            f"✅ Registrado: {accion}\n"
            f"ID_ALERTA: {id_alerta}\n"
            f"Por: {user_name}\n"
            f"Hora: {now_s}"
        )

    except Exception as e:
        logging.exception("on_ack_callback error")
        await query.edit_message_text(f"❌ Error procesando botón:\n{e}")


# ======================
# MAIN
# ======================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("ping_sheet", ping_sheet))
    app.add_handler(CommandHandler("crear_tablero", crear_tablero))
    app.add_handler(CommandHandler("actualizar_tablero", actualizar_tablero))
    app.add_handler(CommandHandler("detalle", detalle))

    app.add_handler(CallbackQueryHandler(on_ack_callback, pattern=r"^ACK\|"))

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()

