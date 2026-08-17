"""
ad-streamer UI — веб-панель на отдельной VPS.

- Авторизация логин/пароль, пользователи в локальной SQLite (PBKDF2).
- Роли: viewer (только просмотр, GET) / operator (управление эфиром) /
  admin (+ пользователи, серверы-агенты).
- Сессии: абсолютный TTL и idle-таймаут (см. SESSION_*), Secure-cookie
  (см. SESSION_COOKIE_SECURE — выключайте только для локального http:// без TLS).
- Раздел «Пользователи»: добавление/удаление (для админов), статистика
  сессий (кто, когда, сколько времени работал) и журнал действий.
- Проксирует /api/* на агента с X-API-Key (ключ в браузер не попадает).

Запуск: AGENT_URL=http://10.0.0.5:8500 API_KEY=secret uvicorn server:app --port 8080
ENV: AGENT_URL, API_KEY, UI_DATA (каталог БД, по умолчанию ./data),
     ADMIN_PASSWORD (пароль первого админа при пустой БД — ОБЯЗАТЕЛЕН,
       без него генерируется случайный и печатается в лог при старте,
       "admin/admin" по умолчанию больше НЕ создаётся),
     SESSION_TTL_HOURS (абсолютный срок сессии, по умолчанию 12),
     SESSION_IDLE_MINUTES (разлогин по бездействию, по умолчанию 120),
     SESSION_COOKIE_SECURE ("0" чтобы выключить флаг Secure — только если
       панель открывается по голому http:// без TLS-терминации на nginx)
"""
import hashlib, logging, os, secrets, sqlite3, time
from contextlib import closing
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("ad-streamer-ui")

AGENT_URL = os.environ.get("AGENT_URL", "http://127.0.0.1:8500").rstrip("/")
API_KEY = os.environ.get("API_KEY", "")
UI_DATA = os.path.abspath(os.environ.get("UI_DATA", "./data"))
os.makedirs(UI_DATA, exist_ok=True)
DB_PATH = os.path.join(UI_DATA, "ui.db")

SESSION_TTL_HOURS = float(os.environ.get("SESSION_TTL_HOURS", "12"))
SESSION_IDLE_MINUTES = float(os.environ.get("SESSION_IDLE_MINUTES", "120"))
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "1") != "0"

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def hash_pw(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 120_000).hex()

ROLES = ("viewer", "operator", "admin")

def init_db():
    with closing(db()) as c, c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL,
          pass_hash TEXT NOT NULL, salt TEXT NOT NULL, is_admin INTEGER DEFAULT 0,
          created TEXT DEFAULT (datetime('now','localtime')));
        CREATE TABLE IF NOT EXISTS sessions(
          sid TEXT PRIMARY KEY, user_id INTEGER NOT NULL,
          login_at TEXT NOT NULL, last_seen TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit(
          id INTEGER PRIMARY KEY, user TEXT, ts TEXT, method TEXT, path TEXT);
        CREATE TABLE IF NOT EXISTS agents(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
          api_key TEXT NOT NULL, hls_url TEXT,
          created TEXT DEFAULT (datetime('now','localtime')));
        """)
        if "hls_url" not in [r["name"] for r in c.execute("PRAGMA table_info(agents)")]:
            c.execute("ALTER TABLE agents ADD COLUMN hls_url TEXT")
        # адрес, по которому ВНЕШНИЙ энкодер (Astra Cesbo и т.п.) пушит нам SRT.
        # Обычно совпадает с хостом из url (панель ходит к агенту по внутреннему
        # адресу — по нему же его видит и энкодер), поэтому по умолчанию пусто и
        # выводится из url. Заполнять только если энкодер должен пушить по
        # ДРУГОМУ адресу, чем тот, по которому агента видит панель (отдельный
        # интерфейс/студийная сеть/NAT).
        if "srt_host" not in [r["name"] for r in c.execute("PRAGMA table_info(agents)")]:
            c.execute("ALTER TABLE agents ADD COLUMN srt_host TEXT")
        # роль вместо голого is_admin: viewer (только чтение) / operator
        # (управление эфиром) / admin (+пользователи/серверы). is_admin
        # остаётся для обратной совместимости (старые проверки в этом файле
        # ещё используют его местами) и всегда синхронизирован с role='admin'.
        if "role" not in [r["name"] for r in c.execute("PRAGMA table_info(users)")]:
            c.execute("ALTER TABLE users ADD COLUMN role TEXT")
            c.execute("UPDATE users SET role='admin' WHERE is_admin=1 AND role IS NULL")
            c.execute("UPDATE users SET role='operator' WHERE is_admin=0 AND role IS NULL")
        if not c.execute("SELECT 1 FROM users LIMIT 1").fetchone():
            salt = secrets.token_hex(16)
            pw = os.environ.get("ADMIN_PASSWORD")
            if not pw:
                # "admin/admin" по умолчанию — известный пароль на панели
                # управления живым эфиром, найдётся первым же сканом. Вместо
                # тихого создания такого пользователя — случайный пароль в
                # лог: сервис стартует (не роняем прод), но им нельзя войти,
                # не прочитав журнал (`journalctl -u ad-ui`).
                pw = secrets.token_urlsafe(12)
                log.warning("=" * 60)
                log.warning("ADMIN_PASSWORD не задан! Сгенерирован временный пароль")
                log.warning("для пользователя 'admin': %s", pw)
                log.warning("Установите ADMIN_PASSWORD в окружении и смените пароль.")
                log.warning("=" * 60)
            c.execute("INSERT INTO users(username,pass_hash,salt,is_admin,role) VALUES(?,?,?,1,'admin')",
                      ("admin", hash_pw(pw, salt), salt))
        # миграция со старой одноагентной схемы: первый агент из ENV
        if not c.execute("SELECT 1 FROM agents LIMIT 1").fetchone() and AGENT_URL and API_KEY:
            c.execute("INSERT INTO agents(name,url,api_key) VALUES(?,?,?)",
                      ("Server 1", AGENT_URL, API_KEY))
init_db()
try:
    os.chmod(DB_PATH, 0o600)  # в БД лежат API-ключи агентов
except OSError:
    pass

def get_agent(request: Request):
    """Выбранный агент: cookie agent_id, иначе первый в списке."""
    aid = request.cookies.get("agent_id", "")
    with closing(db()) as c:
        row = None
        if aid.isdigit():
            row = c.execute("SELECT * FROM agents WHERE id=?", (int(aid),)).fetchone()
        if not row:
            row = c.execute("SELECT * FROM agents ORDER BY id LIMIT 1").fetchone()
    if not row:
        raise HTTPException(503, "не настроен ни один сервер (агент)")
    return dict(row)

app = FastAPI(title="ad-streamer ui")

def now():
    return datetime.now().isoformat(timespec="seconds")

def current_user(request: Request):
    sid = request.cookies.get("adsid", "")
    if not sid:
        raise HTTPException(401, "login required")
    with closing(db()) as c, c:
        row = c.execute("""SELECT s.sid, s.login_at, s.last_seen, u.id uid,
                                  u.username, u.is_admin, u.role FROM sessions s
                           JOIN users u ON u.id=s.user_id WHERE s.sid=?""", (sid,)).fetchone()
        if not row:
            raise HTTPException(401, "login required")
        # TTL/idle: бессрочная сессия на пульте управления живым эфиром —
        # реальный риск (уведённый ноутбук/оставленная вкладка = доступ
        # навсегда). Истёкшую сессию удаляем сразу, чтобы не копить мусор.
        login_at = datetime.fromisoformat(row["login_at"])
        last_seen = datetime.fromisoformat(row["last_seen"])
        age_h = (datetime.now() - login_at).total_seconds() / 3600
        idle_m = (datetime.now() - last_seen).total_seconds() / 60
        if age_h > SESSION_TTL_HOURS or idle_m > SESSION_IDLE_MINUTES:
            c.execute("DELETE FROM sessions WHERE sid=?", (sid,))
            raise HTTPException(401, "session expired")
        c.execute("UPDATE sessions SET last_seen=? WHERE sid=?", (now(), sid))
    d = dict(row)
    d["role"] = d.get("role") or ("admin" if d["is_admin"] else "operator")
    return d

def require_operator(request: Request):
    """viewer — только просмотр (GET), не может управлять эфиром."""
    u = current_user(request)
    if u["role"] == "viewer":
        raise HTTPException(403, "только просмотр — обратитесь к администратору за правами оператора")
    return u

def audit(user, method, path):
    if method == "GET":
        return
    with closing(db()) as c, c:
        c.execute("INSERT INTO audit(user,ts,method,path) VALUES(?,?,?,?)",
                  (user, now(), method, path))
        c.execute("DELETE FROM audit WHERE id NOT IN (SELECT id FROM audit ORDER BY id DESC LIMIT 2000)")

# ---------------- auth
@app.post("/login")
async def login(request: Request, response: Response):
    body = await request.json()
    username, password = body.get("username", ""), body.get("password", "")
    time.sleep(0.3)  # притормозить перебор
    with closing(db()) as c:
        u = c.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not u or hash_pw(password, u["salt"]) != u["pass_hash"]:
        raise HTTPException(403, "неверный логин или пароль")
    sid = secrets.token_hex(32)
    with closing(db()) as c, c:
        c.execute("INSERT INTO sessions(sid,user_id,login_at,last_seen) VALUES(?,?,?,?)",
                  (sid, u["id"], now(), now()))
    audit(username, "LOGIN", "/login")
    response.set_cookie("adsid", sid, httponly=True, samesite="strict",
                        secure=SESSION_COOKIE_SECURE, max_age=int(SESSION_TTL_HOURS * 3600))
    role = u["role"] if "role" in u.keys() and u["role"] else ("admin" if u["is_admin"] else "operator")
    return {"ok": True, "username": u["username"], "is_admin": u["is_admin"], "role": role}

@app.post("/logout")
def logout(request: Request, response: Response):
    sid = request.cookies.get("adsid", "")
    with closing(db()) as c, c:
        c.execute("DELETE FROM sessions WHERE sid=?", (sid,))
    response.delete_cookie("adsid")
    return {"ok": True}

@app.get("/me")
def me(request: Request):
    u = current_user(request)
    return {"username": u["username"], "is_admin": u["is_admin"], "role": u["role"]}

# ---------------- users (admin)
def require_admin(request: Request):
    u = current_user(request)
    if not u["is_admin"]:
        raise HTTPException(403, "только для администратора")
    return u

@app.get("/users")
def users_list(request: Request):
    require_admin(request)
    with closing(db()) as c:
        return [dict(r) for r in c.execute(
            "SELECT id,username,is_admin,role,created FROM users ORDER BY id")]

@app.post("/users")
async def users_add(request: Request):
    admin = require_admin(request)
    body = await request.json()
    username, password = body.get("username", "").strip(), body.get("password", "")
    role = body.get("role") or ("admin" if body.get("is_admin") else "operator")
    if role not in ROLES:
        raise HTTPException(400, f"роль должна быть одной из: {', '.join(ROLES)}")
    if not username or len(password) < 6:
        raise HTTPException(400, "логин обязателен, пароль минимум 6 символов")
    salt = secrets.token_hex(16)
    try:
        with closing(db()) as c, c:
            c.execute("INSERT INTO users(username,pass_hash,salt,is_admin,role) VALUES(?,?,?,?,?)",
                      (username, hash_pw(password, salt), salt, int(role == "admin"), role))
    except sqlite3.IntegrityError:
        raise HTTPException(400, "такой логин уже есть")
    audit(admin["username"], "POST", f"/users(+{username}:{role})")
    return {"ok": True}

@app.post("/users/{uid}/role")
async def users_role(uid: int, request: Request):
    admin = require_admin(request)
    body = await request.json()
    role = body.get("role", "")
    if role not in ROLES:
        raise HTTPException(400, f"роль должна быть одной из: {', '.join(ROLES)}")
    if uid == admin["uid"] and role != "admin":
        raise HTTPException(400, "нельзя понизить роль самому себе")
    with closing(db()) as c, c:
        c.execute("UPDATE users SET role=?, is_admin=? WHERE id=?",
                  (role, int(role == "admin"), uid))
    audit(admin["username"], "POST", f"/users/{uid}/role({role})")
    return {"ok": True}

@app.delete("/users/{uid}")
def users_del(uid: int, request: Request):
    admin = require_admin(request)
    if uid == admin["uid"]:
        raise HTTPException(400, "нельзя удалить самого себя")
    with closing(db()) as c, c:
        c.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        c.execute("DELETE FROM users WHERE id=?", (uid,))
    audit(admin["username"], "DELETE", f"/users/{uid}")
    return {"ok": True}

@app.post("/users/{uid}/password")
async def users_pw(uid: int, request: Request):
    admin = require_admin(request)
    body = await request.json()
    password = body.get("password", "")
    if len(password) < 6:
        raise HTTPException(400, "пароль минимум 6 символов")
    salt = secrets.token_hex(16)
    with closing(db()) as c, c:
        c.execute("UPDATE users SET pass_hash=?, salt=? WHERE id=?",
                  (hash_pw(password, salt), salt, uid))
    audit(admin["username"], "POST", f"/users/{uid}/password")
    return {"ok": True}

@app.get("/users/stats")
def users_stats(request: Request, q: str = ""):
    require_admin(request)
    with closing(db()) as c:
        sessions = [dict(r) for r in c.execute(
            """SELECT u.username, s.login_at, s.last_seen,
                      CAST((julianday(s.last_seen)-julianday(s.login_at))*86400 AS INTEGER) dur
               FROM sessions s JOIN users u ON u.id=s.user_id
               ORDER BY s.login_at DESC LIMIT 100""")]
        totals = [dict(r) for r in c.execute(
            """SELECT u.username, COUNT(*) n,
                      SUM(CAST((julianday(s.last_seen)-julianday(s.login_at))*86400 AS INTEGER)) total
               FROM sessions s JOIN users u ON u.id=s.user_id
               GROUP BY u.username ORDER BY total DESC""")]
        # Поиск идёт по БД, а не по уже отданным строкам: в журнале держится до
        # 2000 записей, и фильтровать только последние 100 — значит не найти
        # ровно то, что обычно и ищут (что делал такой-то на прошлой неделе).
        if q:
            like = f"%{q.strip()}%"
            actions = [dict(r) for r in c.execute(
                """SELECT user,ts,method,path FROM audit
                   WHERE user LIKE ? OR path LIKE ? OR method LIKE ? OR ts LIKE ?
                   ORDER BY id DESC LIMIT 300""", (like, like, like, like))]
        else:
            actions = [dict(r) for r in c.execute(
                "SELECT user,ts,method,path FROM audit ORDER BY id DESC LIMIT 100")]
        total_actions = c.execute("SELECT COUNT(*) n FROM audit").fetchone()["n"]
    return {"sessions": sessions, "totals": totals, "actions": actions,
            "actions_total": total_actions, "q": q or ""}

@app.delete("/audit")
def audit_clear(request: Request):
    """Очистить журнал действий. Само это действие тоже записывается — иначе
    в журнале не осталось бы следа, что его чистили, и кем."""
    admin = require_admin(request)
    with closing(db()) as c, c:
        n = c.execute("SELECT COUNT(*) n FROM audit").fetchone()["n"]
        c.execute("DELETE FROM audit")
    audit(admin["username"], "DELETE", f"/audit (очищено записей: {n})")
    return {"ok": True, "deleted": n}

@app.delete("/sessions")
def sessions_clear(request: Request):
    """Завершить чужие сессии — все, кроме текущей. Свою намеренно не трогаем:
    админ, нажавший кнопку, не должен выкинуть сам себя из панели посреди
    работы. Остальные разлогиниваются сразу."""
    admin = require_admin(request)
    mine = request.cookies.get("adsid", "")
    with closing(db()) as c, c:
        n = c.execute("SELECT COUNT(*) n FROM sessions WHERE sid!=?", (mine,)).fetchone()["n"]
        c.execute("DELETE FROM sessions WHERE sid!=?", (mine,))
    audit(admin["username"], "DELETE", f"/sessions (завершено чужих: {n})")
    return {"ok": True, "deleted": n}

# ---------------- agents (серверы Flussonic)
@app.get("/agents")
def agents_list(request: Request):
    current_user(request)
    with closing(db()) as c:
        return [dict(r) for r in c.execute(
            "SELECT id,name,url,hls_url,srt_host,created FROM agents ORDER BY id")]  # ключ не отдаём

@app.post("/agents")
async def agents_add(request: Request):
    admin = require_admin(request)
    body = await request.json()
    name = body.get("name", "").strip()
    url = body.get("url", "").strip().rstrip("/")
    api_key = body.get("api_key", "").strip()
    hls_url = body.get("hls_url", "").strip().rstrip("/") or None
    srt_host = body.get("srt_host", "").strip() or None
    if not (name and url.startswith("http") and api_key):
        raise HTTPException(400, "нужны: название, URL (http://ip:8500), API-ключ")
    with closing(db()) as c, c:
        c.execute("INSERT INTO agents(name,url,api_key,hls_url,srt_host) VALUES(?,?,?,?,?)",
                  (name, url, api_key, hls_url, srt_host))
    audit(admin["username"], "POST", f"/agents(+{name})")
    return {"ok": True}

@app.put("/agents/{aid}/srthost")
async def agents_set_srt_host(aid: int, request: Request):
    """Переопределить адрес для SRT-push у существующего сервера (пусто = снова
    выводить автоматически из url агента)."""
    admin = require_admin(request)
    body = await request.json()
    srt_host = (body.get("srt_host") or "").strip() or None
    with closing(db()) as c, c:
        if not c.execute("SELECT 1 FROM agents WHERE id=?", (aid,)).fetchone():
            raise HTTPException(404)
        c.execute("UPDATE agents SET srt_host=? WHERE id=?", (srt_host, aid))
    audit(admin["username"], "PUT", f"/agents/{aid}/srthost")
    return {"ok": True}

@app.delete("/agents/{aid}")
def agents_del(aid: int, request: Request):
    admin = require_admin(request)
    with closing(db()) as c, c:
        if c.execute("SELECT COUNT(*) n FROM agents").fetchone()["n"] <= 1:
            raise HTTPException(400, "нельзя удалить последний сервер")
        c.execute("DELETE FROM agents WHERE id=?", (aid,))
    audit(admin["username"], "DELETE", f"/agents/{aid}")
    return {"ok": True}

@app.post("/agents/{aid}/test")
async def agents_test(aid: int, request: Request):
    current_user(request)
    with closing(db()) as c:
        a = c.execute("SELECT * FROM agents WHERE id=?", (aid,)).fetchone()
    if not a:
        raise HTTPException(404)
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"{a['url']}/api/ping",
                                 headers={"X-API-Key": a["api_key"]})
        return {"ok": r.status_code == 200, "status": r.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}

# ---------------- синхронизация потоков между серверами
# Панель — единственное место, которое знает про все агенты сразу (у каждого
# своя база и свой ключ), поэтому копирование потоков живёт здесь, а не в
# агенте.
#
# Потоки на разных серверах сопоставляются ПО ИМЕНИ: id у них независимые и
# совпадать не могут. Одноимённый поток на цели обновляется, отсутствующий —
# создаётся.
#
# Копируется ВСЁ: параметры кадра и кодека, выход во Flussonic, вход,
# логотип с его координатами, буфер и заглушка. Картинки логотипа и заглушки
# переносятся физически (файл скачивается с исходного агента и заливается в
# медиатеку целевого, если там нет одноимённого) — id медиатек у серверов
# независимы, поэтому сопоставляем по имени файла, как и сами потоки.
#
# ЕДИНСТВЕННОЕ исключение — autostart: копия создаётся ОСТАНОВЛЕННОЙ и никогда
# не поднимается сама. Иначе после синхронизации два сервера немедленно начали
# бы публиковать в один и тот же адрес Flussonic и рвали бы эфир друг другу.
# Запускать копию — ручное решение оператора.
#
# Отдельный случай — вход SRT-push (прямой приём) и YouTube. В input_url там
# лежит служебный адрес, собранный из НОМЕРА потока (порт 17000+id, путь ytN),
# и на цели номер будет другой. Копируется сам источник: для YouTube — ссылка
# (yt_url), для прямого SRT — режим (srt_direct), а рабочий адрес агент цели
# собирает под своим номером. Новый адрес для энкодера видно в карточке потока.
SYNC_FIELDS = ("out_w", "out_h", "banner_w", "banner_h", "vcodec", "vbitrate",
               "fps", "mediamtx_enabled", "audio_tracks")

def _agent_by_id(aid):
    with closing(db()) as c:
        row = c.execute("SELECT * FROM agents WHERE id=?", (aid,)).fetchone()
    return dict(row) if row else None

async def _agent_call(client, agent, method, path, **kw):
    # ключ агента подставляем всегда, но вызывающий может добавить свои
    # заголовки (X-UI-*) — их нельзя терять и нельзя дублировать kwarg
    headers = {"X-API-Key": agent["api_key"], **(kw.pop("headers", None) or {})}
    return await client.request(method, f"{agent['url']}/api/{path}",
                                headers=headers, **kw)

def _stream_payload(s):
    """Поля потока для создания/обновления на другом сервере."""
    data = {k: str(s.get(k) if s.get(k) is not None else "") for k in SYNC_FIELDS}
    data["name"] = s["name"]
    data["output_url"] = s.get("output_url") or ""
    # копия всегда создаётся остановленной (см. комментарий к SYNC_FIELDS)
    data["autostart"] = "0"
    yt = (s.get("yt_url") or "").strip()
    src = (s.get("input_url") or "")
    if yt:
        data["yt_url"] = yt
        data["input_url"] = "-"
    elif "mode=listener" in src:
        # порт зависит от id — пусть агент цели назначит свой
        data["srt_direct"] = "1"
        data["input_url"] = "-"
    else:
        data["input_url"] = src
    return data

async def _copy_banner(client, src_agent, dst_agent, bid, src_banners, cache):
    """Перенести картинку из медиатеки исходного агента в медиатеку целевого.
    Возвращает id на цели (или None). Одноимённая картинка не дублируется —
    считаем, что это она и есть."""
    if not bid:
        return None
    if bid in cache:
        return cache[bid]
    src = src_banners.get(int(bid))
    if not src:
        cache[bid] = None
        return None
    name = src.get("name") or src.get("filename") or f"banner{bid}"
    r = await _agent_call(client, dst_agent, "GET", "banners")
    for b in r.json():
        if (b.get("name") or "") == name:
            cache[bid] = b["id"]
            return b["id"]
    fr = await _agent_call(client, src_agent, "GET", f"banners/{bid}/file")
    fr.raise_for_status()
    # расширение важно: агент принимает только .gif/.png и определяет тип по нему
    ext = os.path.splitext(src.get("filename") or "")[1].lower() or ".png"
    up = await _agent_call(client, dst_agent, "POST", "banners",
                           files={"file": ("copy" + ext, fr.content)},
                           data={"name": name})
    up.raise_for_status()
    cache[bid] = up.json()["id"]
    return cache[bid]

async def _copy_extras(client, src_agent, dst_agent, s, tgt_id, src_banners,
                       cache, uh, errors):
    """Логотип (с координатами), буфер и заглушка — они живут не в полях
    потока, а в отдельных ручках агента, поэтому копируются вторым шагом."""
    try:
        logo = await _copy_banner(client, src_agent, dst_agent,
                                  s.get("logo_banner_id"), src_banners, cache)
        if logo:
            d = {"banner_id": str(logo), "x": str(s.get("logo_x") or 20),
                 "y": str(s.get("logo_y") or 20)}
            # w/h заданы только если логотип двигали по превью вручную —
            # иначе агент сам считает размер, и пустые значения ему слать нельзя
            if s.get("logo_w") and s.get("logo_h"):
                d["w"] = str(s["logo_w"]); d["h"] = str(s["logo_h"])
            await _agent_call(client, dst_agent, "POST", f"streams/{tgt_id}/logo",
                              data=d, headers=uh)
        elif s.get("logo_banner_id"):
            errors.append(f"{s['name']}: логотип не перенесён")
    except Exception as e:
        errors.append(f"{s['name']}: логотип — {str(e)[:60]}")
    try:
        slate = await _copy_banner(client, src_agent, dst_agent,
                                   s.get("gst_slate_banner_id"), src_banners, cache)
        if s.get("gst_buffer_enabled") or slate:
            await _agent_call(client, dst_agent, "POST", f"streams/{tgt_id}/gstbuffer",
                              data={"enabled": "1" if s.get("gst_buffer_enabled") else "0",
                                    "buffer_sec": str(s.get("gst_buffer_sec") or 12),
                                    "slate_banner_id": str(slate or 0)}, headers=uh)
    except Exception as e:
        errors.append(f"{s['name']}: заглушка — {str(e)[:60]}")

def _ui_headers(user):
    # агент по этим заголовкам решает, что оператору видно и что он вправе
    # менять: без них панель работала бы от имени «прямого доступа по ключу»,
    # то есть с правами администратора
    return {"X-UI-User": user["username"], "X-UI-Role": user.get("role") or "admin"}

async def sync_streams_to(streams, target_ids, user, src_agent):
    """Скопировать потоки на указанные серверы. Возвращает отчёт по каждому."""
    report = []
    uh = _ui_headers(user)
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            r = await _agent_call(client, src_agent, "GET", "banners")
            src_banners = {b["id"]: b for b in r.json()}
        except Exception:
            src_banners = {}
        for aid in target_ids:
            agent = _agent_by_id(aid)
            if not agent:
                continue
            res = {"agent": agent["name"], "created": 0, "updated": 0, "errors": []}
            # id уже перенесённых картинок: один логотип на десяти потоках
            # заливается один раз, а не десять
            bcache = {}
            try:
                r = await _agent_call(client, agent, "GET", "streams", headers=uh)
                existing = {x["name"]: x for x in r.json()}
            except Exception as e:
                res["errors"].append(f"нет связи: {str(e)[:70]}")
                report.append(res)
                continue
            for s in streams:
                data = _stream_payload(s)
                try:
                    tgt = existing.get(s["name"])
                    if tgt:
                        # у существующего потока yt_url/srt_direct не трогаем:
                        # адрес там уже собран под его собственный номер
                        upd = {k: v for k, v in data.items()
                               if k not in ("yt_url", "srt_direct")}
                        # "-" — заглушка вместо адреса, который агент собирает
                        # сам при СОЗДАНИИ (srt_direct/yt_url). У PUT таких
                        # параметров нет, поэтому вход существующего потока
                        # оставляем как есть — иначе он превратился бы в "-"
                        if upd["input_url"] == "-"                            or "mode=listener" in (tgt.get("input_url") or "")                            or (tgt.get("yt_url") or ""):
                            upd["input_url"] = tgt["input_url"]
                        # автостарт уже работающей копии не сбрасываем: её
                        # однажды запустили осознанно, синхронизация настроек
                        # не повод её глушить
                        upd["autostart"] = str(tgt.get("autostart") or 0)
                        rr = await _agent_call(client, agent, "PUT",
                                               f"streams/{tgt['id']}", data=upd,
                                               headers=uh)
                        if rr.status_code < 300:
                            res["updated"] += 1
                            await _copy_extras(client, src_agent, agent, s, tgt["id"],
                                               src_banners, bcache, uh, res["errors"])
                        else:
                            res["errors"].append(f"{s['name']}: {rr.text[:90]}")
                    else:
                        # владелец на цели ставится по тому, кто синхронизировал
                        # (заголовки X-UI-*). Переносить владельца исходного
                        # потока нельзя — на другом сервере это был бы чужой
                        # логин без ведома администратора
                        rr = await _agent_call(client, agent, "POST", "streams",
                                               data=data, headers=uh)
                        if rr.status_code < 300:
                            res["created"] += 1
                            await _copy_extras(client, src_agent, agent, s,
                                               rr.json()["id"], src_banners,
                                               bcache, uh, res["errors"])
                        else:
                            res["errors"].append(f"{s['name']}: {rr.text[:90]}")
                except Exception as e:
                    res["errors"].append(f"{s['name']}: {str(e)[:70]}")
            report.append(res)
    return report

@app.post("/sync/streams")
async def sync_streams(request: Request):
    """Скопировать потоки на другие серверы (копия создаётся остановленной).
    body: {stream_ids: [..] | all: true, targets: [id..]}"""
    u = require_operator(request)
    body = await request.json()
    targets = [int(t) for t in (body.get("targets") or [])]
    if not targets:
        raise HTTPException(400, "не выбрано ни одного сервера")
    src_agent = get_agent(request)
    targets = [t for t in targets if t != src_agent["id"]]   # себя не копируем
    if not targets:
        raise HTTPException(400, "выбран только текущий сервер — копировать некуда")
    async with httpx.AsyncClient(timeout=60) as client:
        r = await _agent_call(client, src_agent, "GET", "streams",
                              headers=_ui_headers(u))
        allst = r.json()
    ids = body.get("stream_ids")
    streams = allst if body.get("all") else [s for s in allst if s["id"] in set(ids or [])]
    if not streams:
        raise HTTPException(400, "нечего копировать")
    report = await sync_streams_to(streams, targets, u, src_agent)
    audit(u["username"], "POST", f"/sync/streams({len(streams)}→{len(targets)})")
    return {"ok": True, "report": report}

@app.post("/sync/delete")
async def sync_delete(request: Request):
    """Удалить одноимённый поток на других серверах. body: {name, targets:[id..]}"""
    u = require_operator(request)
    body = await request.json()
    name = (body.get("name") or "").strip()
    targets = [int(t) for t in (body.get("targets") or [])]
    if not name or not targets:
        raise HTTPException(400, "нужно имя потока и хотя бы один сервер")
    src = get_agent(request)
    report = []
    async with httpx.AsyncClient(timeout=60) as client:
        for aid in targets:
            if aid == src["id"]:
                continue
            agent = _agent_by_id(aid)
            if not agent:
                continue
            res = {"agent": agent["name"], "deleted": 0, "errors": []}
            try:
                r = await _agent_call(client, agent, "GET", "streams",
                                      headers=_ui_headers(u))
                for s in r.json():
                    if s["name"] == name:
                        rr = await _agent_call(client, agent, "DELETE",
                                               f"streams/{s['id']}", headers=_ui_headers(u))
                        if rr.status_code < 300:
                            res["deleted"] += 1
                        else:
                            res["errors"].append(rr.text[:90])
            except Exception as e:
                res["errors"].append(str(e)[:70])
            report.append(res)
    audit(u["username"], "POST", f"/sync/delete({name})")
    return {"ok": True, "report": report}

# ---------------- proxy to selected agent
@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(path: str, request: Request):
    # viewer: только чтение (GET) — управлять эфиром (стоп/старт/баннеры/
    # настройки) не может; всё остальное (operator/admin) полный доступ.
    u = current_user(request) if request.method == "GET" else require_operator(request)
    agent = get_agent(request)
    audit(u["username"], request.method, f"[{agent['name']}] /api/{path}")
    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.request(
            request.method, f"{agent['url']}/api/{path}",
            params=dict(request.query_params),
            content=await request.body(),
            headers={"X-API-Key": agent["api_key"],
                     "Content-Type": request.headers.get("content-type", ""),
                     # кто из панели инициировал действие — агенту нужно только
                     # для /record/start (метка в БД записей), сервер-сайд
                     # значение из сессии, клиент подделать не может
                     "X-UI-User": u["username"],
                     # роль нужна агенту, чтобы разграничить потоки между
                     # операторами: свои видит и трогает каждый, все — админ.
                     # Значение server-side, из сессии — клиент подделать не может.
                     "X-UI-Role": u.get("role") or ("admin" if u.get("is_admin") else "operator")})
    return Response(r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type"))

# ---------------- HLS-прокси для встроенного плеера
# Панель тянет HLS с Flussonic через себя (same-origin): браузеру не нужны
# ни прямой доступ к Flussonic, ни CORS. Upstream берётся у ВЫБРАННОГО агента
# (поле hls_url — адрес его Flussonic, достижимый с этой VPS); env-переменная
# HLS_UPSTREAM — запасной вариант для агентов без заполненного hls_url.
HLS_UPSTREAM = os.environ.get("HLS_UPSTREAM", "http://10.0.0.10:8080").rstrip("/")

@app.get("/hls/{path:path}")
async def hls_proxy(path: str, request: Request):
    current_user(request)
    agent = get_agent(request)
    upstream = (agent.get("hls_url") or HLS_UPSTREAM).rstrip("/")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{upstream}/{path}",
                             params=dict(request.query_params))
    return Response(r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type"))

# ---------------- HLS-прокси для MediaMTX (компаньон-сервер агента)
# ЗАЧЕМ: MediaMTX отдаёт HLS по обычному HTTP (без TLS), а панель обычно висит
# за HTTPS (домен + сертификат через nginx) — браузер молча режет такой запрос
# как «mixed content» (HTTP-ресурс внутри HTTPS-страницы), плеер зависает без
# единой ошибки в консоли. Проксируем через саму панель (server-to-server —
# ui/server.py достаёт приватный хост агента напрямую, mixed content тут не
# применяется), тогда для браузера всё идёт с того же HTTPS-домена, что и сама
# панель. variant: "ll" (low-latency HLS, порт 8888) | "classic" (обычный HLS
# без сессионных cookie, порт 8898) — те же порты, что в agent/main.py
# MEDIAMTX_INSTANCES. follow_redirects — MediaMTX шлёт 302 (сессионный cookie-
# чек) перед самим плейлистом.
MTX_HLS_PORTS = {"ll": 8888, "classic": 8898}

@app.get("/mtxhls/{variant}/{path:path}")
async def mtxhls_proxy(variant: str, path: str, request: Request):
    current_user(request)
    if variant not in MTX_HLS_PORTS:
        raise HTTPException(404, "unknown MediaMTX variant")
    agent = get_agent(request)
    from urllib.parse import urlparse
    host = urlparse(agent["url"]).hostname
    upstream = f"http://{host}:{MTX_HLS_PORTS[variant]}"
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.get(f"{upstream}/{path}", params=dict(request.query_params))
    return Response(r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type"))

# HLS-проба вкладки «Проверка» — файлы лежат НЕ на Flussonic, а на самом
# агенте (agent/main.py: /api/checkstream/hls/*), поэтому нужен ключ агента —
# добавляем его здесь же (в браузер ключ не попадает, как и для /api/*).
@app.get("/checkhls/{fname}")
async def checkhls_proxy(fname: str, request: Request):
    current_user(request)
    agent = get_agent(request)
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{agent['url']}/api/checkstream/hls/{fname}",
                             headers={"X-API-Key": agent["api_key"]})
    return Response(r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type"))

@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")))
