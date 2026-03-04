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
        ws4 = sh.worksheet(TAB_RESP)

        h1 = ws1.row_values(1)
        h2 = ws2.row_values(1)
        h3 = ws3.row_values(1)
        h4 = ws4.row_values(1)

        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await update.message.reply_text(
            "✅ Conexión OK con Google Sheets\n"
            f"- {TAB_ALERTAS}: {len(h1)} columnas\n"
            f"- {TAB_ACK}: {len(h2)} columnas\n"
            f"- {TAB_CONFIG}: {len(h3)} columnas\n"
            f"- {TAB_RESP}: {len(h4)} columnas\n"
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

        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tablero_text = (
            "📌 TABLERO SCTR\n"
            f"Actualizado: {now_s}\n\n"
            "✅ Conexión lista.\n"
            "Siguiente: cargaremos alertas y botones."
        )

        msg = await chat.send_message(tablero_text)

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

    valid = []
    for r in rows:
        nivel = str(r.get("NIVEL", "")).strip().upper()
        if nivel in ("CRITICO", "ALERTA", "PROXIMO"):
            valid.append(r)

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

    parts.append("\n✅ Confirma con: /detalle EMPRESA")
    parts.append("🔄 Actualiza manual: /actualizar_tablero")
    return "\n".join(parts).strip()


# ======================
# TABLERO: REFRESH (FUNC REUTILIZABLE)
# ======================
async def refresh_tablero(context: ContextTypes.DEFAULT_TYPE):
    """
    Regenera y edita el mensaje del tablero configurado en CONFIG_ALERTAS.
    """
    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)

    ws_cfg = sh.worksheet(TAB_CONFIG)
    ws_alert = sh.worksheet(TAB_ALERTAS)

    cfg_rows = ws_cfg.get_all_records()
    if not cfg_rows:
        return

    cfg = None
    for r in cfg_rows:
        if str(r.get("CHAT_ID_ALERTAS", "")).strip():
            cfg = r
            break
    if not cfg:
        return

    chat_id = str(cfg.get("CHAT_ID_ALERTAS", "")).strip()
    msg_id = str(cfg.get("TABLERO_MESSAGE_ID", "")).strip()
    if not chat_id or not msg_id:
        return

    rows = ws_alert.get_all_records()
    text = build_tablero_text_from_alertas(rows)

    await context.bot.edit_message_text(
        chat_id=int(chat_id),
        message_id=int(msg_id),
        text=text
    )


# ======================
# TABLERO: ACTUALIZAR (COMANDO)
# ======================
async def actualizar_tablero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await refresh_tablero(context)

        # update ULTIMA_ACTUALIZACION
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_cfg = sh.worksheet(TAB_CONFIG)
        headers = _headers(ws_cfg)

        if "CHAT_ID_ALERTAS" in headers and "ULTIMA_ACTUALIZACION" in headers:
            cfg_rows = ws_cfg.get_all_records()
            cfg = None
            for r in cfg_rows:
                if str(r.get("CHAT_ID_ALERTAS", "")).strip():
                    cfg = r
                    break
            if cfg:
                chat_id = str(cfg.get("CHAT_ID_ALERTAS", "")).strip()
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
# CALLBACK BOTONES (ACK + AUTORIZACIÓN + REFRESH TABLERO)
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
    accion = parts[2].strip().upper()  # RECIBIDO / EN_PROCESO / RENOVADO

    user = query.from_user
    user_name = f"@{user.username}" if user.username else (user.full_name or "Usuario")
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Seguridad: el botón SOLO debe funcionar si proviene del mensaje detalle correcto
        msg_text = (query.message.text or "")
        if f"ID_ALERTA: {id_alerta}" not in msg_text:
            await query.answer("⚠️ Este botón no corresponde a este detalle.", show_alert=True)
            return

        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_ack = sh.worksheet(TAB_ACK)
        ws_alert = sh.worksheet(TAB_ALERTAS)
        ws_resp = sh.worksheet(TAB_RESP)

        # 1) Obtener EMPRESA del ID_ALERTA
        rows_alert = ws_alert.get_all_records()
        empresa = None
        for r in rows_alert:
            if str(r.get("ID_ALERTA", "")).strip() == id_alerta:
                empresa = str(r.get("EMPRESA", "")).strip()
                break

        if not empresa:
            await query.answer("⚠️ No se encontró la empresa de esta alerta.", show_alert=True)
            return

        # 2) Validar autorización por RESPONSABLES_EMPRESA (ACTIVO=1)
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

        # 3) Guardar ACK_ALERTAS por headers (sin asumir columnas extra)
        headers_ack = [h.strip() for h in ws_ack.row_values(1)]
        row_ack = {h: "" for h in headers_ack}
        # columnas típicas
        if "ID_ALERTA" in headers_ack:
            row_ack["ID_ALERTA"] = id_alerta
        if "EMPRESA" in headers_ack:
            row_ack["EMPRESA"] = empresa
        if "ACCION" in headers_ack:
            row_ack["ACCION"] = accion
        if "USER_NAME" in headers_ack:
            row_ack["USER_NAME"] = user_name
        if "USER_ID" in headers_ack:
            row_ack["USER_ID"] = str(user.id)
        if "CHAT_ID" in headers_ack:
            row_ack["CHAT_ID"] = str(query.message.chat_id)
        if "TIMESTAMP" in headers_ack:
            row_ack["TIMESTAMP"] = now_s

        ws_ack.append_row([row_ack.get(h, "") for h in headers_ack], value_input_option="USER_ENTERED")

        # 4) Actualizar ALERTAS_SCTR: ESTADO, CONFIRMADO_POR, CONFIRMADO_AT, UPDATED_AT
        headers_alert = [h.strip() for h in ws_alert.row_values(1)]
        for must in ("ID_ALERTA", "ESTADO"):
            if must not in headers_alert:
                await query.edit_message_text(f"❌ ALERTAS_SCTR debe tener columna {must}.")
                return

        col_id = headers_alert.index("ID_ALERTA") + 1
        col_vals = ws_alert.col_values(col_id)
        row_idx = None
        for i, v in enumerate(col_vals[1:], start=2):
            if str(v).strip() == id_alerta:
                row_idx = i
                break

        if row_idx:
            def upd_if_exists(colname: str, value: str):
                if colname in headers_alert:
                    ws_alert.update_cell(row_idx, headers_alert.index(colname) + 1, value)

            upd_if_exists("ESTADO", accion)
            upd_if_exists("CONFIRMADO_POR", user_name)
            upd_if_exists("CONFIRMADO_AT", now_s)
            upd_if_exists("UPDATED_AT", now_s)

        # 5) Confirmación visual en el mensaje detalle
        await query.edit_message_text(
            f"✅ Registrado: {accion}\n"
            f"Empresa: {empresa}\n"
            f"ID_ALERTA: {id_alerta}\n"
            f"Por: {user_name}\n"
            f"Hora: {now_s}"
        )

        # 6) Actualizar tablero automáticamente
        await refresh_tablero(context)

    except Exception as e:
        logging.exception("on_ack_callback error")
        try:
            await query.edit_message_text(f"❌ Error procesando botón:\n{e}")
        except Exception:
            pass


# ======================
# JOB RECORDATORIOS
# ======================
def _parse_dt(s: str) -> float:
    """
    Convierte 'YYYY-MM-DD HH:MM:SS' a timestamp (segundos).
    Si falla, retorna 0.
    """
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return 0.0

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Se ejecuta cada 60 min.
    Envía recordatorio SOLO si hay CRITICO + SIN_CONFIRMAR.
    Anti-spam: respeta LAST_REMINDER_AT (>= 60 min) a nivel de alerta.
    Requiere columnas en ALERTAS_SCTR:
      LAST_REMINDER_AT, REMINDER_COUNT
    """
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws_cfg = sh.worksheet(TAB_CONFIG)
        ws_alert = sh.worksheet(TAB_ALERTAS)

        cfg_rows = ws_cfg.get_all_records()
        cfg = None
        for r in cfg_rows:
            if str(r.get("CHAT_ID_ALERTAS", "")).strip():
                cfg = r
                break
        if not cfg:
            return

        chat_id = str(cfg.get("CHAT_ID_ALERTAS", "")).strip()
        msg_id = str(cfg.get("TABLERO_MESSAGE_ID", "")).strip()
        if not chat_id or not msg_id:
            return

        rows = ws_alert.get_all_records()
        criticos = []
        for r in rows:
            nivel = str(r.get("NIVEL", "")).strip().upper()
            estado = str(r.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()
            if nivel == "CRITICO" and estado == "SIN_CONFIRMAR":
                criticos.append(r)

        if not criticos:
            return

        headers = _headers(ws_alert)
        if "LAST_REMINDER_AT" not in headers or "REMINDER_COUNT" not in headers or "ID_ALERTA" not in headers:
            # No hay columnas anti-spam aún
            return

        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        now_ts = datetime.now().timestamp()
        one_hour = 60 * 60

        # enviar solo si al menos 1 crítico no fue recordado en la última hora
        need_send = False
        for r in criticos:
            last = str(r.get("LAST_REMINDER_AT", "")).strip()
            last_ts = _parse_dt(last)
            if (now_ts - last_ts) >= one_hour:
                need_send = True
                break
        if not need_send:
            return

        lines = ["⚠️ *RECORDATORIO SCTR CRÍTICO*", "Empresas sin confirmar:", ""]
        for r in criticos:
            emp = str(r.get("EMPRESA", "—")).strip() or "—"
            ffin = str(r.get("FECHA_FIN", "—")).strip() or "—"
            dias = str(r.get("DIAS_RESTANTES", "—")).strip() or "—"
            lines.append(f"• *{emp}* — {ffin} — *{dias} días*")
        lines.append("\n✅ Confirma con: `/detalle EMPRESA`")

        await context.bot.send_message(
            chat_id=int(chat_id),
            text="\n".join(lines),
            parse_mode="Markdown",
            reply_to_message_id=int(msg_id),
            disable_web_page_preview=True
        )

        # Actualizar LAST_REMINDER_AT y REMINDER_COUNT para todos los críticos (para que no repita)
        col_id = headers.index("ID_ALERTA") + 1
        col_last = headers.index("LAST_REMINDER_AT") + 1
        col_cnt = headers.index("REMINDER_COUNT") + 1

        id_col_vals = ws_alert.col_values(col_id)
        id_to_row = {}
        for i, v in enumerate(id_col_vals[1:], start=2):
            vv = str(v).strip()
            if vv:
                id_to_row[vv] = i

        for r in criticos:
            ida = str(r.get("ID_ALERTA", "")).strip()
            if not ida or ida not in id_to_row:
                continue
            row_i = id_to_row[ida]

            try:
                cur = int(str(r.get("REMINDER_COUNT", "0")).strip() or "0")
            except Exception:
                cur = 0

            ws_alert.update_cell(row_i, col_last, now_s)
            ws_alert.update_cell(row_i, col_cnt, str(cur + 1))

    except Exception:
        logging.exception("reminder_job error")


# ======================
# MAIN
# ======================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN en variables de entorno.")

    app = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("ping_sheet", ping_sheet))
    app.add_handler(CommandHandler("crear_tablero", crear_tablero))
    app.add_handler(CommandHandler("actualizar_tablero", actualizar_tablero))
    app.add_handler(CommandHandler("detalle", detalle))
    app.add_handler(CallbackQueryHandler(on_ack_callback, pattern=r"^ACK\|"))

    # Job: recordatorio cada 60 minutos (primera ejecución a los 60s para prueba)
    app.job_queue.run_repeating(reminder_job, interval=60 * 60, first=60)

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
