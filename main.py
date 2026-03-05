import os
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
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
TAB_SCTR = "SCTR_VIGENTE"        # Paso 16/17
TAB_EVENTOS = "EVENTOS_SCTR"     # Paso 18 (si no existe, se omite)
TAB_DASH = "DASHBOARD_SCTR"      # Paso 22 (si no existe, se omite)

# Defaults anti-spam / escalamiento (si no existen columnas, se omite)
REMINDER_MIN_SECONDS = 60 * 60
SYNC_INTERVAL_SECONDS = 6 * 60 * 60
ESCALATION_CHECK_SECONDS = 30 * 60
ESC_LEVEL1_SECONDS = 6 * 60 * 60
ESC_LEVEL2_SECONDS = 12 * 60 * 60
ESC_LEVEL3_SECONDS = 24 * 60 * 60

DT_FMT = "%Y-%m-%d %H:%M:%S"


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

def _headers(ws) -> List[str]:
    return [h.strip() for h in ws.row_values(1)]

def _col(headers: List[str], name: str) -> int:
    return headers.index(name) + 1  # 1-based

def _find_row_by_value(ws, col_idx: int, value: str) -> Optional[int]:
    vals = ws.col_values(col_idx)
    for i, v in enumerate(vals[1:], start=2):
        if str(v).strip() == str(value).strip():
            return i
    return None


# ======================
# HELPERS FECHA / NIVEL
# ======================
def now_s() -> str:
    return datetime.now().strftime(DT_FMT)

def parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, DT_FMT)
    except Exception:
        return None

def parse_date_text(s: str) -> Optional[datetime]:
    """
    FECHA texto: soporta dd/mm/yyyy, dd-mm-yyyy, yyyy-mm-dd, yyyy/mm/dd
    """
    s = (s or "").strip()
    if not s:
        return None

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def calc_nivel(dias: int) -> Optional[str]:
    if dias <= 3:
        return "CRITICO"
    if 4 <= dias <= 7:
        return "ALERTA"
    if 8 <= dias <= 15:
        return "PROXIMO"
    return None


# ======================
# EVENTOS (Paso 18) - si hoja no existe, se omite
# ======================
def try_get_ws(sh, tab_name: str):
    try:
        return sh.worksheet(tab_name)
    except Exception:
        return None

def log_event(sh, event_type: str, payload: Dict[str, Any]) -> None:
    """
    Registra evento en EVENTOS_SCTR si existe.
    Recomendado headers:
      EVENT_ID, EVENT_TYPE, EMPRESA, ID_ALERTA, USER_ID, USER_NAME, DETAILS, TIMESTAMP
    """
    ws_ev = try_get_ws(sh, TAB_EVENTOS)
    if ws_ev is None:
        return

    headers = _headers(ws_ev)
    row = {h: "" for h in headers}

    row["EVENT_ID"] = str(uuid.uuid4())
    row["EVENT_TYPE"] = event_type
    row["TIMESTAMP"] = now_s()

    for k in ("EMPRESA", "ID_ALERTA", "USER_ID", "USER_NAME", "DETAILS"):
        if k in headers and k in payload:
            row[k] = str(payload.get(k, ""))

    if "DETAILS" in headers and not row.get("DETAILS"):
        extras = {k: v for k, v in payload.items() if k not in ("EMPRESA", "ID_ALERTA", "USER_ID", "USER_NAME")}
        if extras:
            row["DETAILS"] = json.dumps(extras, ensure_ascii=False)

    ws_ev.append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")


# ======================
# TABLERO (estilo "panel" como imagen) - Paso 20 mejorado
# ======================
def _esc_html(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def build_tablero_text_from_alertas(rows: List[Dict[str, Any]]) -> str:
    """
    Devuelve HTML (ParseMode.HTML).
    Estilo tipo panel:
      📌 TABLERO SCTR
      Actualizado: dd/mm/aaaa
      🟥 CRÍTICOS...
      🟧 ALERTA...
      🟨 PRÓXIMOS...
    """
    t = datetime.now()
    actualizado = t.strftime("%d/%m/%Y")

    # Filtrar solo los que tienen NIVEL valido (<=15 días por diseño)
    valid: List[Dict[str, Any]] = []
    for r in rows:
        nivel = str(r.get("NIVEL", "")).strip().upper()
        if nivel in ("CRITICO", "ALERTA", "PROXIMO"):
            valid.append(r)

    order = {"CRITICO": 0, "ALERTA": 1, "PROXIMO": 2}

    def _dias_int(r):
        ds = str(r.get("DIAS_RESTANTES", "999")).strip()
        try:
            return int(float(ds))
        except Exception:
            return 999

    def _key(r):
        nivel = str(r.get("NIVEL", "")).strip().upper()
        emp = str(r.get("EMPRESA", "")).strip()
        return (order.get(nivel, 9), _dias_int(r), emp)

    valid.sort(key=_key)

    groups = {"CRITICO": [], "ALERTA": [], "PROXIMO": []}
    for r in valid:
        groups[str(r.get("NIVEL", "")).strip().upper()].append(r)

    def badge(estado: str) -> Tuple[str, str]:
        e = (estado or "SIN_CONFIRMAR").strip().upper()
        if e == "RECIBIDO":
            return "🟩", "RECIBIDO"
        if e == "EN_PROCESO":
            return "🟨", "EN_PROCESO"
        if e == "RENOVADO":
            return "🟦", "RENOVADO"
        return "⬜", "SIN_CONFIRMAR"

    def line(r):
        emp = _esc_html(str(r.get("EMPRESA", "—")).strip() or "—")
        ffin = _esc_html(str(r.get("FECHA_FIN", "—")).strip() or "—")
        dias = _esc_html(str(r.get("DIAS_RESTANTES", "—")).strip() or "—")
        icon, st = badge(str(r.get("ESTADO", "SIN_CONFIRMAR")))
        # Formato tipo imagen: EMPRESA — FECHA — X días — [estado]
        return f"• <b>{emp}</b> — {ffin} — {dias} días — {icon} <b>{st}</b>"

    parts: List[str] = []
    parts.append("📌 <b>TABLERO SCTR</b>")
    parts.append(f"Actualizado: <b>{actualizado}</b>")
    parts.append("")

    if groups["CRITICO"]:
        parts.append("🟥 <b>CRÍTICOS (0–3 días)</b>")
        parts.extend([line(r) for r in groups["CRITICO"]])
        parts.append("")

    if groups["ALERTA"]:
        parts.append("🟧 <b>ALERTA (4–7 días)</b>")
        parts.extend([line(r) for r in groups["ALERTA"]])
        parts.append("")

    if groups["PROXIMO"]:
        parts.append("🟨 <b>PRÓXIMOS (8–15 días)</b>")
        parts.extend([line(r) for r in groups["PROXIMO"]])
        parts.append("")

    if not valid:
        parts.append("✅ <b>Sin alertas activas (0–15 días).</b>")
        parts.append("")

    parts.append("<i>* Toma acción según prioridad *</i>")
    return "\n".join(parts).strip()


async def refresh_tablero(context: ContextTypes.DEFAULT_TYPE) -> Tuple[Optional[int], Optional[int]]:
    """
    Edita el mensaje del tablero según CONFIG_ALERTAS.
    Retorna (chat_id, msg_id) si existe.
    """
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
        return (None, None)

    chat_id = str(cfg.get("CHAT_ID_ALERTAS", "")).strip()
    msg_id = str(cfg.get("TABLERO_MESSAGE_ID", "")).strip()
    if not chat_id or not msg_id:
        return (None, None)

    rows = ws_alert.get_all_records()
    text = build_tablero_text_from_alertas(rows)

    await context.bot.edit_message_text(
        chat_id=int(chat_id),
        message_id=int(msg_id),
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    return (int(chat_id), int(msg_id))


async def bump_tablero(context: ContextTypes.DEFAULT_TYPE, reason: str = ""):
    """
    PIN + bump por reply (sin duplicar tablero).
    """
    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    ws_cfg = sh.worksheet(TAB_CONFIG)

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

    text = "🔄 Tablero actualizado"
    if reason:
        text = f"🔄 {reason}"

    await context.bot.send_message(
        chat_id=int(chat_id),
        text=text,
        reply_to_message_id=int(msg_id),
        disable_web_page_preview=True
    )


# ======================
# COMANDOS
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

        tabs = [TAB_ALERTAS, TAB_ACK, TAB_CONFIG, TAB_RESP]
        extra = [TAB_SCTR, TAB_EVENTOS, TAB_DASH]

        info_lines = ["✅ Conexión OK con Google Sheets"]
        for t in tabs + extra:
            ws = try_get_ws(sh, t)
            if ws is None:
                info_lines.append(f"- {t}: (no existe)")
            else:
                info_lines.append(f"- {t}: {len(ws.row_values(1))} columnas")

        info_lines.append(f"Hora: {now_s()}")
        await update.message.reply_text("\n".join(info_lines))
    except Exception as e:
        logging.exception("ping_sheet error")
        await update.message.reply_text(f"❌ Error conectando a Sheets:\n{e}")


async def crear_tablero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /crear_tablero
    - Ejecutar dentro del grupo destino.
    - Crea el mensaje del tablero y guarda message_id en CONFIG_ALERTAS.
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

        msg = await chat.send_message("📌 TABLERO SCTR\nCreando tablero...")

        c_chat = _col(headers, "CHAT_ID_ALERTAS")
        c_mid = _col(headers, "TABLERO_MESSAGE_ID")
        c_upd = _col(headers, "ULTIMA_ACTUALIZACION")

        row = _find_row_by_value(ws_cfg, c_chat, chat_id)
        if row is None:
            new_row = [""] * len(headers)
            new_row[c_chat - 1] = chat_id
            new_row[c_mid - 1] = str(msg.message_id)
            new_row[c_upd - 1] = now_s()
            ws_cfg.append_row(new_row, value_input_option="USER_ENTERED")
        else:
            ws_cfg.update_cell(row, c_mid, str(msg.message_id))
            ws_cfg.update_cell(row, c_upd, now_s())

        # Render final
        await refresh_tablero(context)

        await update.message.reply_text(
            "✅ Tablero creado y registrado en CONFIG_ALERTAS.\n"
            f"CHAT_ID_ALERTAS={chat_id}\n"
            f"TABLERO_MESSAGE_ID={msg.message_id}\n\n"
            "📌 Ahora ANCLA (PIN) ese mensaje en el grupo."
        )
    except Exception as e:
        logging.exception("crear_tablero error")
        await update.message.reply_text(f"❌ Error creando tablero:\n{e}")


async def actualizar_tablero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await refresh_tablero(context)
        await bump_tablero(context, "Tablero actualizado (manual)")

        # actualizar ULTIMA_ACTUALIZACION
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
                row_i = _find_row_by_value(ws_cfg, _col(headers, "CHAT_ID_ALERTAS"), chat_id)
                if row_i:
                    ws_cfg.update_cell(row_i, _col(headers, "ULTIMA_ACTUALIZACION"), now_s())

        await update.message.reply_text("✅ Tablero actualizado.")
    except Exception as e:
        logging.exception("actualizar_tablero error")
        await update.message.reply_text(f"❌ Error actualizando tablero:\n{e}")


# ======================
# /detalle + BOTONES
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
# Callback ACK (bloqueo doble respuesta)
# ======================
async def on_ack_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()

    data = query.data or ""
    parts = data.split("|")
    if len(parts) != 3 or parts[0] != "ACK":
        await query.edit_message_text("❌ Callback inválido.")
        return

    id_alerta = parts[1].strip()
    accion = parts[2].strip().upper()

    user = query.from_user
    user_name = f"@{user.username}" if user.username else (user.full_name or "Usuario")
    ts = now_s()

    try:
        msg_text = (query.message.text or "")
        if f"ID_ALERTA: {id_alerta}" not in msg_text:
            await query.answer("⚠️ Este botón no corresponde a este detalle.", show_alert=True)
            return

        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_ack = sh.worksheet(TAB_ACK)
        ws_alert = sh.worksheet(TAB_ALERTAS)
        ws_resp = sh.worksheet(TAB_RESP)

        alerts = ws_alert.get_all_records()
        alert_row = None
        for r in alerts:
            if str(r.get("ID_ALERTA", "")).strip() == id_alerta:
                alert_row = r
                break
        if not alert_row:
            await query.answer("⚠️ No se encontró la alerta.", show_alert=True)
            return

        empresa = str(alert_row.get("EMPRESA", "")).strip()
        estado_actual = str(alert_row.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()

        if estado_actual in ("RECIBIDO", "EN_PROCESO", "RENOVADO") and accion != estado_actual:
            confirmado_por = str(alert_row.get("CONFIRMADO_POR", "")).strip() or "otro usuario"
            await query.answer(f"⚠️ Ya está {estado_actual} ({confirmado_por}).", show_alert=True)
            return

        resp_rows = ws_resp.get_all_records()
        autorizado = False
        for rr in resp_rows:
            emp = str(rr.get("EMPRESA", "")).strip().lower()
            uid = str(rr.get("USER_ID", "")).strip()
            activo = str(rr.get("ACTIVO", "1")).strip()
            if emp == empresa.lower() and uid == str(user.id) and activo == "1":
                autorizado = True
                break
        if not autorizado:
            await query.answer(f"⛔ No estás autorizado para responder por {empresa}.", show_alert=True)
            return

        headers_ack = _headers(ws_ack)
        row_ack = {h: "" for h in headers_ack}
        for k, v in {
            "ID_ALERTA": id_alerta,
            "EMPRESA": empresa,
            "ACCION": accion,
            "USER_NAME": user_name,
            "USER_ID": str(user.id),
            "CHAT_ID": str(query.message.chat_id),
            "TIMESTAMP": ts
        }.items():
            if k in headers_ack:
                row_ack[k] = v
        ws_ack.append_row([row_ack.get(h, "") for h in headers_ack], value_input_option="USER_ENTERED")

        headers_alert = _headers(ws_alert)
        if "ID_ALERTA" not in headers_alert or "ESTADO" not in headers_alert:
            await query.edit_message_text("❌ ALERTAS_SCTR debe tener ID_ALERTA y ESTADO.")
            return

        col_id = headers_alert.index("ID_ALERTA") + 1
        col_vals = ws_alert.col_values(col_id)
        row_idx = None
        for i, v in enumerate(col_vals[1:], start=2):
            if str(v).strip() == id_alerta:
                row_idx = i
                break

        def upd_if_exists(colname: str, value: str):
            if colname in headers_alert and row_idx:
                ws_alert.update_cell(row_idx, headers_alert.index(colname) + 1, value)

        upd_if_exists("ESTADO", accion)
        upd_if_exists("CONFIRMADO_POR", user_name)
        upd_if_exists("CONFIRMADO_AT", ts)
        upd_if_exists("UPDATED_AT", ts)

        log_event(sh, "ACK", {
            "EMPRESA": empresa,
            "ID_ALERTA": id_alerta,
            "USER_ID": str(user.id),
            "USER_NAME": user_name,
            "DETAILS": accion,
        })

        await query.edit_message_text(
            f"✅ Registrado: {accion}\n"
            f"Empresa: {empresa}\n"
            f"ID_ALERTA: {id_alerta}\n"
            f"Por: {user_name}\n"
            f"Hora: {ts}"
        )

        await refresh_tablero(context)
        await bump_tablero(context, f"{empresa}: {accion}")

    except Exception as e:
        logging.exception("on_ack_callback error")
        try:
            await query.edit_message_text(f"❌ Error procesando botón:\n{e}")
        except Exception:
            pass


# ======================
# Paso 16: Sync manual desde SCTR_VIGENTE (fecha texto)
# + cierre automático si cambia FECHA_FIN
# ======================
async def sync_alertas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws_sctr = sh.worksheet(TAB_SCTR)
        ws_alert = sh.worksheet(TAB_ALERTAS)

        sctr_rows = ws_sctr.get_all_records()
        if not sctr_rows:
            await update.message.reply_text("⚠️ SCTR_VIGENTE está vacío.")
            return

        headers_alert = _headers(ws_alert)
        required_alert = ["ID_ALERTA", "EMPRESA", "FECHA_FIN", "DIAS_RESTANTES", "NIVEL", "ESTADO", "CREATED_AT", "UPDATED_AT"]
        for c in required_alert:
            if c not in headers_alert:
                await update.message.reply_text(f"❌ Falta columna en ALERTAS_SCTR: {c}")
                return

        col_emp = headers_alert.index("EMPRESA") + 1
        emp_col_vals = ws_alert.col_values(col_emp)
        emp_to_rowidx: Dict[str, int] = {}
        for i, v in enumerate(emp_col_vals[1:], start=2):
            vv = str(v).strip().lower()
            if vv:
                emp_to_rowidx[vv] = i

        alert_records = ws_alert.get_all_records()
        emp_to_record: Dict[str, Dict[str, Any]] = {}
        max_id = 0
        for r in alert_records:
            emp = str(r.get("EMPRESA", "")).strip().lower()
            if emp:
                emp_to_record[emp] = r
            try:
                max_id = max(max_id, int(str(r.get("ID_ALERTA", "0")).strip() or "0"))
            except Exception:
                pass

        col_ida = headers_alert.index("ID_ALERTA") + 1
        col_ff = headers_alert.index("FECHA_FIN") + 1
        col_dias = headers_alert.index("DIAS_RESTANTES") + 1
        col_nivel = headers_alert.index("NIVEL") + 1
        col_estado = headers_alert.index("ESTADO") + 1
        col_created = headers_alert.index("CREATED_AT") + 1
        col_updated = headers_alert.index("UPDATED_AT") + 1

        col_conf_por = headers_alert.index("CONFIRMADO_POR") + 1 if "CONFIRMADO_POR" in headers_alert else None
        col_conf_at = headers_alert.index("CONFIRMADO_AT") + 1 if "CONFIRMADO_AT" in headers_alert else None

        ts = now_s()
        now_dt = datetime.now()

        created = 0
        updated = 0
        skipped = 0
        auto_renov = 0

        for s in sctr_rows:
            empresa = str(s.get("EMPRESA", "")).strip()
            estado_sctr = str(s.get("ESTADO", "ACTIVO")).strip().upper()
            fin_txt = str(s.get("FECHA_FIN", "")).strip()

            if not empresa or estado_sctr != "ACTIVO":
                continue

            dt_fin = parse_date_text(fin_txt)
            if not dt_fin:
                skipped += 1
                continue

            dias = (dt_fin.date() - now_dt.date()).days
            nivel = calc_nivel(dias)
            if nivel is None:
                continue

            emp_key = empresa.lower()
            if emp_key in emp_to_rowidx:
                row_i = emp_to_rowidx[emp_key]
                prev = emp_to_record.get(emp_key, {})
                prev_fin = str(prev.get("FECHA_FIN", "")).strip()
                prev_estado = str(prev.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()

                if prev_fin and prev_fin != fin_txt and prev_estado != "RENOVADO":
                    ws_alert.update_cell(row_i, col_estado, "RENOVADO")
                    if col_conf_por:
                        ws_alert.update_cell(row_i, col_conf_por, "AUTO")
                    if col_conf_at:
                        ws_alert.update_cell(row_i, col_conf_at, ts)
                    auto_renov += 1
                    log_event(sh, "AUTO_RENOVADO", {
                        "EMPRESA": empresa,
                        "ID_ALERTA": str(prev.get("ID_ALERTA", "")),
                        "DETAILS": f"{prev_fin} -> {fin_txt}"
                    })

                ws_alert.update_cell(row_i, col_ff, fin_txt)
                ws_alert.update_cell(row_i, col_dias, str(dias))
                ws_alert.update_cell(row_i, col_nivel, nivel)
                ws_alert.update_cell(row_i, col_updated, ts)
                updated += 1

                log_event(sh, "ALERTA_ACTUALIZADA", {
                    "EMPRESA": empresa,
                    "ID_ALERTA": str(prev.get("ID_ALERTA", "")),
                    "DETAILS": f"dias={dias}, nivel={nivel}"
                })
            else:
                max_id += 1
                new_row = [""] * len(headers_alert)
                new_row[col_ida - 1] = str(max_id)
                new_row[col_emp - 1] = empresa
                new_row[col_ff - 1] = fin_txt
                new_row[col_dias - 1] = str(dias)
                new_row[col_nivel - 1] = nivel
                new_row[col_estado - 1] = "SIN_CONFIRMAR"
                new_row[col_created - 1] = ts
                new_row[col_updated - 1] = ts
                ws_alert.append_row(new_row, value_input_option="USER_ENTERED")
                created += 1

                log_event(sh, "ALERTA_CREADA", {
                    "EMPRESA": empresa,
                    "ID_ALERTA": str(max_id),
                    "DETAILS": f"vence={fin_txt}, dias={dias}, nivel={nivel}"
                })

        await refresh_tablero(context)
        await bump_tablero(context, f"Sync: +{created} / upd {updated} / auto-ren {auto_renov}")

        await update.message.reply_text(
            "✅ Sync listo.\n"
            f"➕ Nuevas: {created}\n"
            f"♻️ Actualizadas: {updated}\n"
            f"🟦 Auto-renovadas (cambio FECHA_FIN): {auto_renov}\n"
            f"⏭️ Omitidas (fecha inválida): {skipped}"
        )
    except Exception as e:
        logging.exception("sync_alertas error")
        await update.message.reply_text(f"❌ Error en sync_alertas:\n{e}")


# ======================
# /estado + /dashboard
# ======================
def _calc_stats(alert_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len([r for r in alert_rows if str(r.get("EMPRESA", "")).strip()])
    crit = 0
    aler = 0
    prox = 0

    sin = 0
    rec = 0
    pro = 0
    ren = 0

    deltas = []
    for r in alert_rows:
        nivel = str(r.get("NIVEL", "")).strip().upper()
        estado = str(r.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()

        if nivel == "CRITICO":
            crit += 1
        elif nivel == "ALERTA":
            aler += 1
        elif nivel == "PROXIMO":
            prox += 1

        if estado == "RECIBIDO":
            rec += 1
        elif estado == "EN_PROCESO":
            pro += 1
        elif estado == "RENOVADO":
            ren += 1
        else:
            sin += 1

        c = parse_dt(str(r.get("CREATED_AT", "")).strip())
        a = parse_dt(str(r.get("CONFIRMADO_AT", "")).strip())
        if c and a and a >= c:
            deltas.append((a - c).total_seconds())

    avg_confirm = None
    if deltas:
        avg_confirm = sum(deltas) / len(deltas)

    return {
        "total": total,
        "crit": crit,
        "alerta": aler,
        "prox": prox,
        "sin": sin,
        "rec": rec,
        "pro": pro,
        "ren": ren,
        "avg_confirm_seconds": avg_confirm,
    }

def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"

async def estado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_alert = sh.worksheet(TAB_ALERTAS)
        stats = _calc_stats(ws_alert.get_all_records())

        msg = (
            "📊 ESTADO SCTR\n\n"
            f"Empresas: {stats['total']}\n"
            f"🚨 Críticas: {stats['crit']} | ⚠️ Alerta: {stats['alerta']} | 🟡 Próximos: {stats['prox']}\n"
            f"⬜ Sin confirmar: {stats['sin']}\n"
            f"🟩 Recibido: {stats['rec']} | 🟨 En proceso: {stats['pro']} | 🟦 Renovado: {stats['ren']}"
        )
        await update.message.reply_text(msg)
    except Exception as e:
        logging.exception("estado error")
        await update.message.reply_text(f"❌ Error en /estado:\n{e}")

async def dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_alert = sh.worksheet(TAB_ALERTAS)
        alert_rows = ws_alert.get_all_records()
        stats = _calc_stats(alert_rows)

        avg_confirm = "—"
        if stats["avg_confirm_seconds"] is not None:
            avg_confirm = _fmt_duration(stats["avg_confirm_seconds"])

        msg = (
            "📈 DASHBOARD SCTR\n\n"
            f"Empresas monitoreadas: {stats['total']}\n"
            f"Alertas activas (0–15 días): {stats['crit'] + stats['alerta'] + stats['prox']}\n"
            f"🚨 Críticas: {stats['crit']}\n"
            f"⬜ Pendientes (sin confirmar): {stats['sin']}\n"
            f"🟩 Recibido: {stats['rec']} | 🟨 En proceso: {stats['pro']} | 🟦 Renovado: {stats['ren']}\n"
            f"⏱ Promedio confirmación: {avg_confirm}\n"
            f"Actualizado: {now_s()}"
        )
        await update.message.reply_text(msg)

        ws_dash = try_get_ws(sh, TAB_DASH)
        if ws_dash is not None:
            headers = _headers(ws_dash)
            if "KEY" in headers and "VALUE" in headers:
                def upsert(k: str, v: str):
                    c_key = _col(headers, "KEY")
                    c_val = _col(headers, "VALUE")
                    r = _find_row_by_value(ws_dash, c_key, k)
                    if r is None:
                        row = [""] * len(headers)
                        row[c_key - 1] = k
                        row[c_val - 1] = v
                        if "UPDATED_AT" in headers:
                            row[_col(headers, "UPDATED_AT") - 1] = now_s()
                        ws_dash.append_row(row, value_input_option="USER_ENTERED")
                    else:
                        ws_dash.update_cell(r, c_val, v)
                        if "UPDATED_AT" in headers:
                            ws_dash.update_cell(r, _col(headers, "UPDATED_AT"), now_s())

                upsert("empresas", str(stats["total"]))
                upsert("criticas", str(stats["crit"]))
                upsert("alerta", str(stats["alerta"]))
                upsert("proximos", str(stats["prox"]))
                upsert("sin_confirmar", str(stats["sin"]))
                upsert("recibido", str(stats["rec"]))
                upsert("en_proceso", str(stats["pro"]))
                upsert("renovado", str(stats["ren"]))
                upsert("prom_confirmacion", avg_confirm)

    except Exception as e:
        logging.exception("dashboard error")
        await update.message.reply_text(f"❌ Error en /dashboard:\n{e}")


# ======================
# JOBS (Paso 17, 19, recordatorios)
# ======================
async def sync_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        ws_sctr = try_get_ws(sh, TAB_SCTR)
        ws_alert = try_get_ws(sh, TAB_ALERTAS)
        if ws_sctr is None or ws_alert is None:
            return

        sctr_rows = ws_sctr.get_all_records()
        if not sctr_rows:
            return

        headers_alert = _headers(ws_alert)
        required_alert = ["ID_ALERTA", "EMPRESA", "FECHA_FIN", "DIAS_RESTANTES", "NIVEL", "ESTADO", "CREATED_AT", "UPDATED_AT"]
        if any(c not in headers_alert for c in required_alert):
            return

        col_emp = headers_alert.index("EMPRESA") + 1
        emp_col_vals = ws_alert.col_values(col_emp)
        emp_to_rowidx = {}
        for i, v in enumerate(emp_col_vals[1:], start=2):
            vv = str(v).strip().lower()
            if vv:
                emp_to_rowidx[vv] = i

        alert_records = ws_alert.get_all_records()
        emp_to_record = {}
        max_id = 0
        for r in alert_records:
            emp = str(r.get("EMPRESA", "")).strip().lower()
            if emp:
                emp_to_record[emp] = r
            try:
                max_id = max(max_id, int(str(r.get("ID_ALERTA", "0")).strip() or "0"))
            except Exception:
                pass

        col_ida = headers_alert.index("ID_ALERTA") + 1
        col_ff = headers_alert.index("FECHA_FIN") + 1
        col_dias = headers_alert.index("DIAS_RESTANTES") + 1
        col_nivel = headers_alert.index("NIVEL") + 1
        col_estado = headers_alert.index("ESTADO") + 1
        col_created = headers_alert.index("CREATED_AT") + 1
        col_updated = headers_alert.index("UPDATED_AT") + 1

        col_conf_por = headers_alert.index("CONFIRMADO_POR") + 1 if "CONFIRMADO_POR" in headers_alert else None
        col_conf_at = headers_alert.index("CONFIRMADO_AT") + 1 if "CONFIRMADO_AT" in headers_alert else None

        ts = now_s()
        nd = datetime.now()

        created = 0
        updated = 0
        auto_ren = 0

        for s in sctr_rows:
            empresa = str(s.get("EMPRESA", "")).strip()
            estado_sctr = str(s.get("ESTADO", "ACTIVO")).strip().upper()
            fin_txt = str(s.get("FECHA_FIN", "")).strip()
            if not empresa or estado_sctr != "ACTIVO":
                continue

            dt_fin = parse_date_text(fin_txt)
            if not dt_fin:
                continue

            dias = (dt_fin.date() - nd.date()).days
            nivel = calc_nivel(dias)
            if nivel is None:
                continue

            emp_key = empresa.lower()
            if emp_key in emp_to_rowidx:
                row_i = emp_to_rowidx[emp_key]
                prev = emp_to_record.get(emp_key, {})
                prev_fin = str(prev.get("FECHA_FIN", "")).strip()
                prev_estado = str(prev.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()

                if prev_fin and prev_fin != fin_txt and prev_estado != "RENOVADO":
                    ws_alert.update_cell(row_i, col_estado, "RENOVADO")
                    if col_conf_por:
                        ws_alert.update_cell(row_i, col_conf_por, "AUTO")
                    if col_conf_at:
                        ws_alert.update_cell(row_i, col_conf_at, ts)
                    auto_ren += 1
                    log_event(sh, "AUTO_RENOVADO", {
                        "EMPRESA": empresa,
                        "ID_ALERTA": str(prev.get("ID_ALERTA", "")),
                        "DETAILS": f"{prev_fin} -> {fin_txt}"
                    })

                ws_alert.update_cell(row_i, col_ff, fin_txt)
                ws_alert.update_cell(row_i, col_dias, str(dias))
                ws_alert.update_cell(row_i, col_nivel, nivel)
                ws_alert.update_cell(row_i, col_updated, ts)
                updated += 1
            else:
                max_id += 1
                new_row = [""] * len(headers_alert)
                new_row[col_ida - 1] = str(max_id)
                new_row[col_emp - 1] = empresa
                new_row[col_ff - 1] = fin_txt
                new_row[col_dias - 1] = str(dias)
                new_row[col_nivel - 1] = nivel
                new_row[col_estado - 1] = "SIN_CONFIRMAR"
                new_row[col_created - 1] = ts
                new_row[col_updated - 1] = ts
                ws_alert.append_row(new_row, value_input_option="USER_ENTERED")
                created += 1
                log_event(sh, "ALERTA_CREADA", {
                    "EMPRESA": empresa,
                    "ID_ALERTA": str(max_id),
                    "DETAILS": f"vence={fin_txt}, dias={dias}, nivel={nivel}"
                })

        if created or updated or auto_ren:
            await refresh_tablero(context)
            await bump_tablero(context, f"AutoSync: +{created} / upd {updated} / auto-ren {auto_ren}")

    except Exception:
        logging.exception("sync_job error")


async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws_cfg = try_get_ws(sh, TAB_CONFIG)
        ws_alert = try_get_ws(sh, TAB_ALERTAS)
        if ws_cfg is None or ws_alert is None:
            return

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

        headers = _headers(ws_alert)
        if "LAST_REMINDER_AT" not in headers or "REMINDER_COUNT" not in headers or "ID_ALERTA" not in headers:
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

        now_dt = datetime.now()
        now_ts = now_dt.timestamp()

        need_send = False
        for r in criticos:
            last_ts = 0.0
            last = str(r.get("LAST_REMINDER_AT", "")).strip()
            ld = parse_dt(last)
            if ld:
                last_ts = ld.timestamp()
            if (now_ts - last_ts) >= REMINDER_MIN_SECONDS:
                need_send = True
                break
        if not need_send:
            return

        lines = ["⚠️ RECORDATORIO SCTR CRÍTICO", "Empresas sin confirmar:", ""]
        for r in criticos:
            emp = str(r.get("EMPRESA", "—")).strip() or "—"
            ffin = str(r.get("FECHA_FIN", "—")).strip() or "—"
            dias = str(r.get("DIAS_RESTANTES", "—")).strip() or "—"
            lines.append(f"• {emp} — {ffin} — {dias} días — ⬜ SIN_CONFIRMAR")
        lines.append("")
        lines.append("✅ Confirma con: /detalle EMPRESA")

        await context.bot.send_message(
            chat_id=int(chat_id),
            text="\n".join(lines),
            reply_to_message_id=int(msg_id),
            disable_web_page_preview=True
        )

        ts = now_s()
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

            ws_alert.update_cell(row_i, col_last, ts)
            ws_alert.update_cell(row_i, col_cnt, str(cur + 1))

            log_event(sh, "RECORDATORIO", {
                "EMPRESA": str(r.get("EMPRESA", "")).strip(),
                "ID_ALERTA": ida,
                "DETAILS": "CRITICO SIN_CONFIRMAR"
            })

    except Exception:
        logging.exception("reminder_job error")


async def escalation_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)

        ws_cfg = try_get_ws(sh, TAB_CONFIG)
        ws_alert = try_get_ws(sh, TAB_ALERTAS)
        if ws_cfg is None or ws_alert is None:
            return

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

        headers = _headers(ws_alert)
        if "ESCALATION_LEVEL" not in headers or "LAST_ESCALATION_AT" not in headers:
            return
        if "CREATED_AT" not in headers or "ESTADO" not in headers or "NIVEL" not in headers or "ID_ALERTA" not in headers:
            return

        rows = ws_alert.get_all_records()
        now_dt = datetime.now()

        col_id = headers.index("ID_ALERTA") + 1
        col_level = headers.index("ESCALATION_LEVEL") + 1
        col_last = headers.index("LAST_ESCALATION_AT") + 1

        id_col_vals = ws_alert.col_values(col_id)
        id_to_row = {}
        for i, v in enumerate(id_col_vals[1:], start=2):
            vv = str(v).strip()
            if vv:
                id_to_row[vv] = i

        for r in rows:
            nivel = str(r.get("NIVEL", "")).strip().upper()
            estado = str(r.get("ESTADO", "SIN_CONFIRMAR")).strip().upper()
            if nivel != "CRITICO" or estado != "SIN_CONFIRMAR":
                continue

            created = parse_dt(str(r.get("CREATED_AT", "")).strip())
            if not created:
                continue

            ida = str(r.get("ID_ALERTA", "")).strip()
            if not ida or ida not in id_to_row:
                continue

            row_i = id_to_row[ida]
            emp = str(r.get("EMPRESA", "—")).strip() or "—"

            try:
                level = int(str(r.get("ESCALATION_LEVEL", "0")).strip() or "0")
            except Exception:
                level = 0

            last_es = parse_dt(str(r.get("LAST_ESCALATION_AT", "")).strip())
            if last_es and (now_dt - last_es).total_seconds() < ESCALATION_CHECK_SECONDS:
                continue

            age = (now_dt - created).total_seconds()
            new_level = level

            if age >= ESC_LEVEL3_SECONDS:
                new_level = max(new_level, 3)
            elif age >= ESC_LEVEL2_SECONDS:
                new_level = max(new_level, 2)
            elif age >= ESC_LEVEL1_SECONDS:
                new_level = max(new_level, 1)

            if new_level <= level:
                continue

            text = (
                f"🚨 ESCALAMIENTO SCTR (Nivel {new_level})\n\n"
                f"Empresa: {emp}\n"
                f"Estado: ⬜ SIN_CONFIRMAR\n"
                f"Tiempo sin confirmación: {_fmt_duration(age)}\n\n"
                "✅ Confirma con: /detalle EMPRESA"
            )

            await context.bot.send_message(
                chat_id=int(chat_id),
                text=text,
                reply_to_message_id=int(msg_id),
                disable_web_page_preview=True
            )

            ws_alert.update_cell(row_i, col_level, str(new_level))
            ws_alert.update_cell(row_i, col_last, now_s())

            log_event(sh, "ESCALAMIENTO", {
                "EMPRESA": emp,
                "ID_ALERTA": ida,
                "DETAILS": f"nivel={new_level}, age={int(age)}s"
            })

    except Exception:
        logging.exception("escalation_job error")


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

    app.add_handler(CommandHandler("sync_alertas", sync_alertas))

    app.add_handler(CommandHandler("estado", estado))
    app.add_handler(CommandHandler("dashboard", dashboard))

    app.add_handler(CallbackQueryHandler(on_ack_callback, pattern=r"^ACK\|"))

    if app.job_queue is None:
        logging.warning('JobQueue no disponible. Instala: python-telegram-bot[job-queue]==21.6')
    else:
        app.job_queue.run_repeating(sync_job, interval=SYNC_INTERVAL_SECONDS, first=60)
        app.job_queue.run_repeating(reminder_job, interval=60 * 60, first=90)
        app.job_queue.run_repeating(escalation_job, interval=ESCALATION_CHECK_SECONDS, first=120)

    print("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
