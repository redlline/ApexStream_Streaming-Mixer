"""
ad-streamer agent — работает на сервере с Flussonic (рядом с GPU).

Хранит баннеры (GIF/PNG) и видеоролики (MP4), запускает и следит за
GStreamer-движком (gst_streamg.py, один процесс на поток — вход провайдера,
встроенный буфер/заглушка и оверлеи баннера/лого/ролика/HTML слоями
compositor'а, единый GPU-энкод), исполняет расписание, собирает логи.

Схема одного потока:
  вход провайдера ─► gst_streamg.py (буфер+compositor, GPU nvh264enc) ─► rtmp во Flussonic
Движок не рестартует при показах баннера/лого/ролика/HTML — все они слои
одного и того же живого конвейера, управляются TCP control-командами.

ffmpeg используется точечно: конвертация/ресайз загруженных баннеров и
роликов, ffprobe-проверки входа, превью вкладки «Проверка» — не для эфирного
микширования (то делает GStreamer).

Запуск:  API_KEY=secret uvicorn main:app --host 0.0.0.0 --port 8500
ENV: API_KEY (обязателен), DATA_DIR (./data), FFMPEG (ffmpeg)
"""
import json, mimetypes, os, re, shutil, signal, sqlite3, subprocess, threading, time, uuid
import urllib.request, urllib.parse
from PIL import Image
from collections import deque, defaultdict
from datetime import datetime, date
from contextlib import closing

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends, Response
from fastapi.responses import FileResponse

API_KEY = os.environ.get("API_KEY", "")
DATA_DIR = os.path.abspath(os.environ.get("DATA_DIR", "./data"))
BANNER_DIR = os.path.join(DATA_DIR, "banners")
VIDEO_DIR = os.path.join(DATA_DIR, "videos")
FFMPEG = os.environ.get("FFMPEG", "ffmpeg")
FFPROBE = os.environ.get("FFPROBE", "ffprobe")
DB_PATH = os.path.join(DATA_DIR, "agent.db")
# порты на поток: banner_udp, ad_udp, zmq — шаг 4 на stream_id
PORT_BASE = 5500
# GStreamer-буфер (gst_streamg.py, режим relay): отдельный диапазон портов,
# далеко от PORT_BASE+sid*4 (feeders) и от 8080/8500/1935 (Flussonic/агент/RTMP)
GST_RELAY_PORT_BASE = 15500
# venv агента без PyGObject (это системный пакет, не pip) — gst_streamg.py
# запускается системным python3, где gi доступен
GST_PYTHON = os.environ.get("GST_PYTHON", "/usr/bin/python3")
GST_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gst_streamg.py")
# компаньон-серверы MediaMTX: агент сам спавнит и следит за ними (как за
# gst_streamg.py — тот же _spawn/_kill_group), НЕ отдельный systemd-сервис —
# так ими можно управлять из панели (Старт/Стоп) без sudo/лишних прав. Движок
# пушит второй/третий RTMP локально в них. ДВА инстанса нужны, потому что
# hlsVariant в MediaMTX — настройка всего процесса, а не пути: low-latency
# HLS (мин. задержка) требует Secure-cookie (только по HTTPS, а у нас HTTP —
# в реальных браузерах зависает), классический HLS работает без ограничений
# по обычному HTTP. "ll" отдаёт ещё и RTSP/SRT/WebRTC, "classic" — только HLS.
# MEDIAMTX_ENABLED=0 в env — полностью выключает оба.
MEDIAMTX_ENABLED = os.environ.get("MEDIAMTX_ENABLED", "1") != "0"
MEDIAMTX_DIR = os.environ.get("MEDIAMTX_DIR", "/opt/ad-streamer/mediamtx")
MEDIAMTX_BIN = os.path.join(MEDIAMTX_DIR, "mediamtx")
MEDIAMTX_INSTANCES = {
    "ll": {"config": os.path.join(MEDIAMTX_DIR, "mediamtx.yml"),
           "rtmp_port": int(os.environ.get("MEDIAMTX_RTMP_PORT", "11935")),
           "hls_port": 8888, "rtsp_port": 8554, "webrtc_port": 8889, "srt_port": 8890,
           "moq_port": 8892,
           "proc": None},
    "classic": {"config": os.path.join(MEDIAMTX_DIR, "mediamtx-classic.yml"),
                "rtmp_port": int(os.environ.get("MEDIAMTX2_RTMP_PORT", "11936")),
                "hls_port": 8898, "proc": None},
}

os.makedirs(BANNER_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)
CHECK_DIR = os.path.join(DATA_DIR, "checkstream")   # HLS-превью для вкладки «Проверка»
os.makedirs(CHECK_DIR, exist_ok=True)
SLATE_CACHE_DIR = os.path.join(DATA_DIR, "slate_cache")   # см. ensure_slate_png()
os.makedirs(SLATE_CACHE_DIR, exist_ok=True)

# ---------------------------------------------------------------- логи
class LogHub:
    """Кольцевые буферы: строки ffmpeg по потокам + журнал событий агента."""
    def __init__(self):
        self.proc = defaultdict(lambda: deque(maxlen=500))   # stream_id -> строки
        self.events = deque(maxlen=800)
        self.lock = threading.Lock()

    def line(self, sid, tag, text):
        with self.lock:
            self.proc[sid].append(f"{datetime.now():%H:%M:%S} [{tag}] {text}")

    def event(self, text, level="info"):
        with self.lock:
            self.events.append({"ts": datetime.now().isoformat(timespec="seconds"),
                                "level": level, "text": text})

LOG = LogHub()

# ---------------------------------------------------------------- database
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    with closing(db()) as c, c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS banners(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL, filename TEXT NOT NULL,
          size INTEGER, created TEXT DEFAULT (datetime('now','localtime')));
        CREATE TABLE IF NOT EXISTS videos(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL, filename TEXT NOT NULL,
          size INTEGER, created TEXT DEFAULT (datetime('now','localtime')));
        CREATE TABLE IF NOT EXISTS streams(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL,
          input_url TEXT NOT NULL, output_url TEXT NOT NULL,
          out_w INTEGER DEFAULT 1920, out_h INTEGER DEFAULT 1080,
          banner_w INTEGER DEFAULT 1920, banner_h INTEGER DEFAULT 150,
          vcodec TEXT DEFAULT 'h264_nvenc', vbitrate TEXT DEFAULT '6000k',
          fps INTEGER DEFAULT 25, autostart INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS queue(
          id INTEGER PRIMARY KEY, banner_id INTEGER NOT NULL REFERENCES banners(id) ON DELETE CASCADE,
          position INTEGER NOT NULL, duration INTEGER DEFAULT 30);
        CREATE TABLE IF NOT EXISTS schedules(
          id INTEGER PRIMARY KEY, name TEXT,
          stream_id INTEGER REFERENCES streams(id) ON DELETE CASCADE, -- NULL = все
          banner_id INTEGER REFERENCES banners(id) ON DELETE CASCADE, -- NULL = очередь
          video_id INTEGER REFERENCES videos(id) ON DELETE CASCADE,   -- если задан — ролик
          template_id INTEGER REFERENCES templates(id) ON DELETE CASCADE, -- если задан — HTML-баннер
          time_start TEXT NOT NULL, every_minutes INTEGER NOT NULL,
          duration INTEGER NOT NULL, fade REAL DEFAULT 1.0,
          date_from TEXT, date_to TEXT, enabled INTEGER DEFAULT 1, last_run TEXT);
        CREATE TABLE IF NOT EXISTS templates(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL, html TEXT NOT NULL,
          created TEXT DEFAULT (datetime('now','localtime')));
        CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS operator_programs(
          id INTEGER PRIMARY KEY, name TEXT NOT NULL,
          stream_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
          start_at TEXT, steps_json TEXT NOT NULL,
          created TEXT DEFAULT (datetime('now','localtime')));
        CREATE TABLE IF NOT EXISTS input_presets(
          id INTEGER PRIMARY KEY,
          stream_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
          name TEXT NOT NULL, url TEXT NOT NULL,
          position INTEGER DEFAULT 0,
          created TEXT DEFAULT (datetime('now','localtime')));
        """)
        # миграции старой схемы
        for t in ("banners", "videos"):
            if "meta" not in [r["name"] for r in c.execute(f"PRAGMA table_info({t})")]:
                c.execute(f"ALTER TABLE {t} ADD COLUMN meta TEXT DEFAULT ''")
        # логотип ПЕР-ПРЕСЕТ: каждый вход (пресет) помнит свою настройку лого
        # (картинка + позиция/размер), чтобы при переключении входов лого
        # восстанавливался под конкретный поток, а не сбрасывался. NULL в
        # logo_set = «у этого пресета лого не запоминалось, не трогать при
        # активации»; logo_set=0 = «явно без лого».
        pcols = [r["name"] for r in c.execute("PRAGMA table_info(input_presets)")]
        if "logo_set" not in pcols:
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_set INTEGER")
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_banner_id INTEGER")
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_x INTEGER")
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_y INTEGER")
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_w INTEGER")
            c.execute("ALTER TABLE input_presets ADD COLUMN logo_h INTEGER")
        if "audio_tracks" not in pcols:
            # Набор аудиодорожек ДЛЯ ЭТОГО ИСТОЧНИКА. У разных провайдеров они
            # лежат в разном порядке (у одного речь первой, у другого — интершум),
            # а настройка до сих пор жила только на уровне потока и в движок
            # попадала при запуске. Из-за этого после переключения входа в эфир
            # уходила тишина: движок искал дорожку с прежним номером, у нового
            # источника её не оказывалось, и вместо неё подставлялась тишина
            # (иначе завис бы весь конвейер, включая видео).
            # NULL = своей настройки у пресета нет, работает общая настройка
            # потока — прежнее поведение для всех уже созданных пресетов.
            c.execute("ALTER TABLE input_presets ADD COLUMN audio_tracks TEXT")
        cols = [r["name"] for r in c.execute("PRAGMA table_info(schedules)")]
        if "video_id" not in cols:
            c.execute("ALTER TABLE schedules ADD COLUMN video_id INTEGER REFERENCES videos(id) ON DELETE CASCADE")
        if "action" not in cols:
            # show | stream_start | stream_stop
            c.execute("ALTER TABLE schedules ADD COLUMN action TEXT DEFAULT 'show'")
        if "template_id" not in cols:
            c.execute("ALTER TABLE schedules ADD COLUMN template_id INTEGER REFERENCES templates(id) ON DELETE CASCADE")
        cols = [r["name"] for r in c.execute("PRAGMA table_info(streams)")]
        if "out_w" not in cols:
            c.execute("ALTER TABLE streams ADD COLUMN out_w INTEGER DEFAULT 1920")
            c.execute("ALTER TABLE streams ADD COLUMN out_h INTEGER DEFAULT 1080")
        if "logo_banner_id" not in cols:
            c.execute("ALTER TABLE streams ADD COLUMN logo_banner_id INTEGER")
            c.execute("ALTER TABLE streams ADD COLUMN logo_x INTEGER DEFAULT 20")
            c.execute("ALTER TABLE streams ADD COLUMN logo_y INTEGER DEFAULT 20")
        if "gst_buffer_enabled" not in cols:
            # GStreamer-буфер перед микшером: развязывает срывы провайдера от
            # зрителя (см. gst_streamg.py). Выключен по умолчанию — существующие
            # потоки продолжают работать байт-в-байт как раньше.
            c.execute("ALTER TABLE streams ADD COLUMN gst_buffer_enabled INTEGER DEFAULT 0")
            c.execute("ALTER TABLE streams ADD COLUMN gst_buffer_sec REAL DEFAULT 12")
            c.execute("ALTER TABLE streams ADD COLUMN gst_slate_banner_id INTEGER")
        if "engine" not in cols:
            # исторический столбец: раньше был выбор ffmpeg|gstreamer, теперь
            # единственный движок — GStreamer (единый GPU-конвейер: баннер/
            # лого/ролик/HTML слоями compositor'а, без CPU-фидеров).
            c.execute("ALTER TABLE streams ADD COLUMN engine TEXT DEFAULT 'gstreamer'")
        if "mediamtx_enabled" not in cols:
            # отдача через MediaMTX (RTSP/HLS/SRT/WebRTC внешним плеерам) — по
            # умолчанию включена (сохраняет уже задеплоенное поведение для
            # существующих потоков). output_url при этом может быть пустым —
            # тогда поток публикуется ТОЛЬКО через MediaMTX, без Flussonic.
            c.execute("ALTER TABLE streams ADD COLUMN mediamtx_enabled INTEGER DEFAULT 1")
        if "audio_track" not in cols:
            # исторический столбец (одна дорожка). Оставлен для совместимости —
            # актуальное поле audio_tracks (ниже) хранит список индексов строкой.
            c.execute("ALTER TABLE streams ADD COLUMN audio_track INTEGER DEFAULT 0")
        if "audio_tracks" not in cols:
            # список индексов аудиодорожек входа через запятую (напр. "0" или
            # "0,1,2,3"). Одна = прежнее одноязычное поведение. Несколько =
            # мультиязык: N×AAC → mpegtsmux → srtsink в MediaMTX, Flussonic
            # тянет все дорожки по SRT (см. gst_streamg.py). По умолчанию "0".
            c.execute("ALTER TABLE streams ADD COLUMN audio_tracks TEXT DEFAULT '0'")
        if "auto_failover" not in cols:
            # автопереключение по кругу на следующий пресет входа (см.
            # input_presets), когда текущий источник молчит дольше порога —
            # см. _failover_loop(). Выключено по умолчанию.
            c.execute("ALTER TABLE streams ADD COLUMN auto_failover INTEGER DEFAULT 0")
        if "logo_w" not in cols:
            # произвольное перетаскивание логотипа на PGM-превью (как у
            # баннера/HTML) — если заданы, logo_x/logo_y читаются как
            # АБСОЛЮТНЫЕ координаты (не знак = угол), logo_w/logo_h — точный
            # размер. NULL = старый режим «угол + отступ» (см. _gst_logo_xy).
            c.execute("ALTER TABLE streams ADD COLUMN logo_w INTEGER")
            c.execute("ALTER TABLE streams ADD COLUMN logo_h INTEGER")
        if "preview_banner_id" not in cols:
            # картинка ТОЛЬКО для плитки мультивью в панели (baner из
            # библиотеки) — В ЭФИР НЕ ИДЁТ, в отличие от logo_banner_id.
            # NULL = плитка использует эфирный логотип, а без него — название.
            c.execute("ALTER TABLE streams ADD COLUMN preview_banner_id INTEGER")
        if "yt_url" not in cols:
            # ссылка на YouTube-трансляцию. YouTube не отдаёт постоянный адрес
            # потока: yt-dlp выдаёт ВРЕМЕННУЮ ссылку (живёт ~6 часов), поэтому
            # держать её в input_url нельзя — эфир упал бы и не поднялся. Здесь
            # хранится исходная ссылка на канал/трансляцию, а input_url при этом
            # указывает на локальный релей (см. yt_relay_start): отдельный
            # процесс переполучает ссылку сам, а поток читает его как обычный
            # источник.
            c.execute("ALTER TABLE streams ADD COLUMN yt_url TEXT")
        vcols = [r["name"] for r in c.execute("PRAGMA table_info(videos)")]
        if "gain_db" not in vcols:
            # авто-нормализация громкости ролика (EBU R128-подобный анализ
            # через ffmpeg loudnorm при загрузке, см. analyze_loudness) —
            # чтобы реклама не орала громче эфира при вставке. NULL = ещё
            # не проанализирован (старые ролики до этой правки).
            c.execute("ALTER TABLE videos ADD COLUMN gain_db REAL")
init_db()

def get_state(key, default=None):
    with closing(db()) as c:
        r = c.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

def set_state(key, value):
    with closing(db()) as c, c:
        c.execute("INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?",
                  (key, str(value), str(value)))

# ---------------------------------------------------------------- монитор ресурсов
SYS = {"cpu": 0.0, "mem": 0.0, "disk_used": 0.0, "disk_free_gb": 0.0,
       "gpu": None, "ts": ""}

def sys_monitor():
    prev_idle = prev_total = 0
    while True:
        try:
            if os.path.exists("/proc/stat"):
                nums = list(map(int, open("/proc/stat").readline().split()[1:]))
                idle, total = nums[3] + nums[4], sum(nums)
                if prev_total and total > prev_total:
                    SYS["cpu"] = round(100 * (1 - (idle - prev_idle) / (total - prev_total)), 1)
                prev_idle, prev_total = idle, total
                mi = {}
                for ln in open("/proc/meminfo"):
                    k, v = ln.split(":", 1)
                    mi[k] = int(v.split()[0])
                SYS["mem"] = round(100 * (1 - mi["MemAvailable"] / mi["MemTotal"]), 1)
            du = shutil.disk_usage(DATA_DIR)
            SYS["disk_used"] = round(100 * du.used / du.total, 1)
            SYS["disk_free_gb"] = round(du.free / 2**30, 1)
            try:
                out = subprocess.run(
                    ["nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,"
                     "memory.total,encoder.stats.sessionCount",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=10)
                if out.returncode == 0 and out.stdout.strip():
                    name, util, mu, mt, enc = [x.strip() for x in
                                               out.stdout.strip().splitlines()[0].split(",")]
                    SYS["gpu"] = {"name": name, "util": float(util),
                                  "mem_used": int(mu), "mem_total": int(mt),
                                  "enc_sessions": int(enc)}
            except Exception:
                SYS["gpu"] = None
            SYS["ts"] = datetime.now().isoformat(timespec="seconds")
        except Exception:
            pass
        time.sleep(5)

threading.Thread(target=sys_monitor, daemon=True).start()

def limits():
    with closing(db()) as c:
        s = {r["key"]: r["value"] for r in c.execute("SELECT * FROM settings")}
    def f(key, dflt):
        try:
            return float(s.get(key, dflt))
        except ValueError:
            return dflt
    return {"cpu": f("lim_cpu", 85), "mem": f("lim_mem", 90),
            "gpu": f("lim_gpu", 85), "gpu_mem": f("lim_gpu_mem", 90),
            "disk_free_gb": f("lim_disk_free", 5)}

def guard(what):
    """Защита продакшна: не даём запускать новую нагрузку при перегрузе.
    Работающие потоки не трогаем — только блокируем НОВЫЕ запуски."""
    lim = limits()
    problems = []
    if SYS["cpu"] > lim["cpu"]:
        problems.append(f"CPU {SYS['cpu']}% > {lim['cpu']}%")
    if SYS["mem"] > lim["mem"]:
        problems.append(f"RAM {SYS['mem']}% > {lim['mem']}%")
    if SYS["disk_free_gb"] and SYS["disk_free_gb"] < lim["disk_free_gb"]:
        problems.append(f"диск: свободно {SYS['disk_free_gb']} ГБ < {lim['disk_free_gb']} ГБ")
    g = SYS.get("gpu")
    if g:
        if g["util"] > lim["gpu"]:
            problems.append(f"GPU {g['util']}% > {lim['gpu']}%")
        if 100 * g["mem_used"] / g["mem_total"] > lim["gpu_mem"]:
            problems.append(f"GPU память {g['mem_used']}/{g['mem_total']} МБ")
    if problems:
        msg = f"защита ресурсов заблокировала '{what}': " + "; ".join(problems)
        LOG.event(msg, "warning")
        raise HTTPException(429, msg)

# ---------------------------------------------------------------- ffprobe / jobs
def probe(path):
    """Возвращает строку вида '1920x150, 12.3с, gif' или '' если ffprobe недоступен."""
    try:
        out = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,codec_name",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1", path],
            capture_output=True, text=True, timeout=20)
        kv = dict(ln.split("=", 1) for ln in out.stdout.splitlines() if "=" in ln)
        parts = []
        if kv.get("width"):
            parts.append(f"{kv['width']}x{kv['height']}")
        if "duration" in kv:
            try:
                parts.append(f"{float(kv['duration']):.1f}с")
            except ValueError:
                pass
        if kv.get("codec_name"):
            parts.append(kv["codec_name"])
        return ", ".join(parts)
    except Exception:
        return ""

LOUDNESS_TARGET_LUFS = -23.0   # вещательный стандарт EBU R128 — тот же уровень,
                               # что и типичный live-эфир, ролик не будет орать
def analyze_loudness(path, timeout=60):
    """Однопроходный анализ громкости ролика через ffmpeg loudnorm (EBU R128-
    подобная интегральная громкость, LUFS) — считаем ОДИН РАЗ при загрузке в
    библиотеку, а не на каждый показ. Возвращает gain_db (сколько добавить/
    убрать, чтобы попасть в LOUDNESS_TARGET_LUFS) или None, если не вышло
    измерить (напр. видео без звука) — тогда используется 0 (без изменений)."""
    try:
        r = subprocess.run(
            [FFMPEG, "-i", path, "-af",
             f"loudnorm=I={LOUDNESS_TARGET_LUFS}:print_format=json",
             "-f", "null", "-"],
            capture_output=True, text=True, timeout=timeout)
        m = re.search(r"\{[^{}]*\"input_i\"[^{}]*\}", r.stderr)
        if not m:
            return None
        data = json.loads(m.group(0))
        input_i = float(data["input_i"])
        if input_i < -70:   # практическая тишина/нет звука — не усиливать в бесконечность
            return None
        gain = LOUDNESS_TARGET_LUFS - input_i
        return max(-20.0, min(20.0, gain))   # разумные пределы, не искажать в хлам
    except Exception as e:
        LOG.event(f"анализ громкости ролика не удался: {e}", "warning")
        return None

def _fetch_text(url, timeout=8):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VLC/3.0.16 LibVLC/3.0.16"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""

def _best_hls_variant(master_url):
    """Провайдер часто отдаёт HLS MASTER-плейлист с несколькими качествами
    (#EXT-X-STREAM-INF) — обычный ffprobe без подсказки выбирает какой-то ОДИН
    вариант сам (не обязательно максимальный, найдено вживую — выход упорно
    оказывался ниже реального максимума провайдера). Явно парсим master и
    берём URL варианта с наибольшим разрешением (не битрейтом — оно надёжнее:
    у некоторых провайдеров битрейт врёт/совпадает у разных качеств). Если
    вариантов нет (это уже media-плейлист или вообще не HLS) — возвращаем
    исходный URL как есть, ffprobe разберётся сам."""
    if not master_url.lower().startswith("http"):
        return master_url
    text = _fetch_text(master_url)
    if "#EXT-X-STREAM-INF" not in text:
        return master_url
    lines = text.splitlines()
    best = None   # ((pixels, bandwidth), url)
    for i, ln in enumerate(lines):
        if not ln.startswith("#EXT-X-STREAM-INF"):
            continue
        m_res = re.search(r"RESOLUTION=(\d+)x(\d+)", ln)
        m_bw = re.search(r"BANDWIDTH=(\d+)", ln)
        pixels = int(m_res.group(1)) * int(m_res.group(2)) if m_res else 0
        bw = int(m_bw.group(1)) if m_bw else 0
        uri = next((s.strip() for s in lines[i + 1:] if s.strip() and not s.strip().startswith("#")), None)
        if not uri:
            continue
        key = (pixels, bw)
        if best is None or key > best[0]:
            best = (key, urllib.parse.urljoin(master_url, uri))
    return best[1] if best else master_url

def probe_input_params(url, timeout=12):
    """Параметры входного потока: разрешение, fps, битрейт — чтобы выход
    микшера автоматически повторял вход (максимальное качество, что реально
    отдаёт провайдер, см. _best_hls_variant — никаких хардкодов). Возвращает
    dict с тем, что удалось определить (битрейт у HLS часто недоступен —
    тогда останется настроенный)."""
    if not shutil.which(FFPROBE):
        return {}
    url = _best_hls_variant(url)
    try:
        out = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,sample_aspect_ratio,avg_frame_rate,bit_rate",
             "-show_entries", "format=bit_rate",
             "-of", "default=noprint_wrappers=1", url],
            capture_output=True, text=True, timeout=timeout)
        kv = {}
        for ln in out.stdout.splitlines():
            if "=" in ln:
                k, v = ln.split("=", 1)
                kv.setdefault(k, v)  # первое вхождение (stream приоритетнее format)
        res = {}
        if kv.get("width", "N/A") != "N/A" and kv.get("height", "N/A") != "N/A":
            w, h = int(kv["width"]), int(kv["height"])
            # SAR (sample aspect ratio) — многие вещательные SD-источники
            # (классический анаморфный PAL) кодируют кадр НЕКВАДРАТНЫМИ
            # пикселями: ffprobe отдаёт width/height закодированного кадра
            # (напр. 720x576), а реальное отображаемое соотношение сторон
            # (напр. 16:9 = «визуально» 1024x576) задаётся отдельно через SAR.
            # Без поправки на SAR наш квадратно-пиксельный canvas сжимает
            # картинку по горизонтали — найдено вживую («выход всегда 720x576»,
            # хотя источник объявлял RESOLUTION=1024x576 в плейлисте).
            # Пересчитываем width под визуальное соотношение, высоту не трогаем.
            sar = kv.get("sample_aspect_ratio", "")
            if ":" in sar and sar not in ("1:1", "0:1", "N/A"):
                try:
                    sn, sd = (int(x) for x in sar.split(":"))
                    if sn > 0 and sd > 0:
                        w = max(16, (round(w * sn / sd) // 2) * 2)
                except (ValueError, ZeroDivisionError):
                    pass
            if 16 <= w <= 3840 and 16 <= h <= 2160:
                res["out_w"], res["out_h"] = w, h
        fr = kv.get("avg_frame_rate", "")
        if "/" in fr:
            num, den = fr.split("/")
            if den != "0":
                fps = round(int(num) / int(den))
                if 10 <= fps <= 60:
                    res["fps"] = fps
        br = kv.get("bit_rate", "N/A")
        if br != "N/A" and br.isdigit():
            k = max(500, min(20000, int(br) // 1000))
            res["vbitrate"] = f"{k}k"
        return res
    except Exception:
        return {}

JOBS: dict[str, dict] = {}   # job_id -> {status, kind, media_id, detail}
JOBS_LOCK = threading.Lock()

def run_job(job_id, cmd, on_done):
    def worker():
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, errors="replace",
                               timeout=1800)
            if p.returncode != 0:
                tail = "\n".join(p.stderr.splitlines()[-5:])
                raise RuntimeError(tail or f"ffmpeg rc={p.returncode}")
            on_done()
            with JOBS_LOCK:
                JOBS[job_id].update(status="done", detail="готово")
        except Exception as e:
            LOG.event(f"конвертация не удалась: {e}", "error")
            with JOBS_LOCK:
                JOBS[job_id].update(status="error", detail=str(e))
    threading.Thread(target=worker, daemon=True).start()

# ---------------------------------------------------------------- ffmpeg / zmq
def _spawn(cmd, sid=None, tag=None, on_progress=None):
    """Запуск ffmpeg; stderr сливается построчно в LogHub.
    on_progress: если задан (только для микшера), stdout читается как поток
    `-progress pipe:1` — колбэк дёргается на каждый блок (~1 раз/сек), это
    сигнал жизни выхода для стелс-детектора."""
    kw = {}
    if os.name == "nt":
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        # новая группа процессов: если у cmd есть СВОИ дочерние процессы
        # (gst_streamg.py спавнит ffmpeg/Chromium), обычный terminate()/kill()
        # бьёт только по этому PID — дети переживают убийство родителя и
        # осиротевают, продолжая жрать CPU незамеченными (было проверено на
        # проде: зависший ffmpeg-конвертер GIF работал 4+ часа после того, как
        # его родитель gst_streamg.py был убит). killpg() ниже убивает всю группу.
        kw["start_new_session"] = True
    stdout = subprocess.PIPE if on_progress else subprocess.DEVNULL
    p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=stdout,
                         stderr=subprocess.PIPE, text=True, errors="replace", **kw)
    if sid is not None:
        def pump():
            for ln in p.stderr:
                ln = ln.rstrip()
                if ln:
                    LOG.line(sid, tag, ln)
            rc = p.wait()
            LOG.line(sid, tag, f"process exited rc={rc}")
        threading.Thread(target=pump, daemon=True).start()
    if on_progress:
        def pump_progress():
            for ln in p.stdout:
                if ln.startswith("progress="):  # маркер конца блока -progress
                    try:
                        on_progress()
                    except Exception:
                        pass
        threading.Thread(target=pump_progress, daemon=True).start()
    return p

def _kill_group(p):
    """Останавливает процесс, спавненный через _spawn() (start_new_session=True),
    ЦЕЛОЙ ГРУППОЙ — если у него были собственные дочерние процессы (gst_streamg.py
    порождает ffmpeg/Chromium), обычный p.terminate()/p.kill() бьёт только по
    этому PID, а дети переживают и осиротевают (проверено на проде: забытый
    ffmpeg-конвертер жрал 497% CPU часами после того, как родителя убили)."""
    if not p or p.poll() is not None:
        return
    try:
        pgid = os.getpgid(p.pid)
        os.killpg(pgid, signal.SIGTERM)
        p.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

# ---------------------------------------------------------------- MediaMTX
# Два компаньон-сервера (см. MEDIAMTX_INSTANCES выше). Агент спавнит и следит
# за ними сам — тем же _spawn/_kill_group, что и за gst_streamg.py — а НЕ
# через systemd, специально чтобы Старт/Стоп из панели не требовал давать
# adstreamer лишних sudo-прав на управление системными сервисами.
MEDIAMTX_LOCK = threading.Lock()
MEDIAMTX_USER_STOPPED = set()   # ключи ("ll"/"classic"), остановленные вручную — watchdog не лезет обратно

def mediamtx_alive(key):
    p = MEDIAMTX_INSTANCES[key]["proc"]
    return p is not None and p.poll() is None

def start_mediamtx(key):
    with MEDIAMTX_LOCK:
        inst = MEDIAMTX_INSTANCES[key]
        if inst["proc"] and inst["proc"].poll() is None:
            return
        if not (os.path.exists(MEDIAMTX_BIN) and os.path.exists(inst["config"])):
            LOG.event(f"MediaMTX ({key}) не найден ({inst['config']}) — соответствующие "
                      f"внешние ссылки будут недоступны", "warning")
            return
        # sid=-1 — служебный, не привязан к конкретному потоку (лог собирается,
        # но не отдаётся через /api/logs?stream_id=, которое принимает только
        # реальные id потоков)
        inst["proc"] = _spawn([MEDIAMTX_BIN, inst["config"]], sid=-1, tag=f"mediamtx-{key}")
        MEDIAMTX_USER_STOPPED.discard(key)
        LOG.event(f"MediaMTX ({key}) запущен")

def stop_mediamtx(key, user_initiated=False):
    with MEDIAMTX_LOCK:
        inst = MEDIAMTX_INSTANCES[key]
        _kill_group(inst["proc"])
        inst["proc"] = None
        if user_initiated:
            MEDIAMTX_USER_STOPPED.add(key)
    LOG.event(f"MediaMTX ({key}) остановлен" + (" (вручную, из панели)" if user_initiated else ""))

def start_all_mediamtx():
    for key in MEDIAMTX_INSTANCES:
        start_mediamtx(key)

def stop_all_mediamtx(user_initiated=False):
    for key in MEDIAMTX_INSTANCES:
        stop_mediamtx(key, user_initiated=user_initiated)

# ---------------------------------------------------------------- YouTube-релей
# YouTube нельзя подставить прямо во вход потока: yt-dlp отдаёт ВРЕМЕННУЮ
# ссылку (~6 часов), и после её протухания эфир упал бы и сам не поднялся.
# Поэтому между YouTube и микшером стоит релей: он переполучает ссылку и гонит
# поток в MediaMTX, а сам поток читает уже локальный путь как обычный источник.
# Протухание при этом чинится перезапуском одного релея — поток видит короткий
# пропал сигнала, показывает заглушку и подхватывает обратно.
#
# Следит за релеем сам агент (тот же приём, что и с MediaMTX: _spawn/_kill_group),
# а НЕ systemd — чтобы Старт/Стоп из панели не требовал давать adstreamer
# sudo-прав на управление системными юнитами.
YT_RELAY_BIN = os.environ.get("YT_RELAY_BIN", "/usr/local/bin/yt-relay.sh")
YT_RELAYS = {}            # sid -> процесс релея
YT_RELAY_LOCK = threading.Lock()

YT_LINK_RE = re.compile(r"^https?://(www\.|m\.)?(youtube\.com|youtu\.be)/", re.I)

def is_yt_link(url):
    return bool(YT_LINK_RE.match((url or "").strip()))

def yt_path(sid):
    """Путь в MediaMTX, куда релей публикует поток этого канала."""
    return f"yt{sid}"

def yt_input_url(sid):
    """Что подставляется во вход потока. Читаем по RTSP, а не обратно по SRT:
    SRT-выход MediaMTX отдаёт не всякий кодек (см. srtpush_info)."""
    return f"rtsp://127.0.0.1:{MEDIAMTX_INSTANCES['ll']['rtsp_port']}/{yt_path(sid)}"

def yt_relay_alive(sid):
    p = YT_RELAYS.get(sid)
    return p is not None and p.poll() is None

def yt_relay_start(sid, url):
    """Поднять релей для потока sid. Идемпотентно: если уже живой — ничего."""
    if not url:
        return
    with YT_RELAY_LOCK:
        if yt_relay_alive(sid):
            return
        if not os.path.exists(YT_RELAY_BIN):
            LOG.event(f"YouTube-релей не найден ({YT_RELAY_BIN}) — поток {sid} "
                      f"не получит сигнал")
            return
        cmd = [YT_RELAY_BIN, url, yt_path(sid),
               str(MEDIAMTX_INSTANCES["ll"]["srt_port"])]
        YT_RELAYS[sid] = _spawn(cmd, sid, "ytrelay")
        LOG.event(f"YouTube-релей запущен для потока {sid}")

def yt_relay_stop(sid):
    with YT_RELAY_LOCK:
        p = YT_RELAYS.pop(sid, None)
    if p:
        _kill_group(p)
        LOG.event(f"YouTube-релей остановлен для потока {sid}")

def yt_ensure_ready(sid, url, timeout=45):
    """Поднять релей и дождаться, пока поток реально появится в MediaMTX.
    Возвращает (ok, detail). Ждать обязательно: резолв ссылки через yt-dlp
    занимает несколько секунд, и до этого пути ещё не существует — обычная
    проверка входа отвалилась бы с 404 и старт был бы отклонён."""
    yt_relay_start(sid, url)
    deadline = time.time() + timeout
    last = "нет сигнала"
    while time.time() < deadline:
        ok, detail = check_input(yt_input_url(sid), timeout=4)
        if ok:
            return True, "поток получен"
        last = detail
        if not yt_relay_alive(sid):
            return False, "релей не запустился (проверьте ссылку и журнал потока)"
        time.sleep(3)
    return False, f"YouTube не отдал поток за {timeout}с — идёт ли трансляция? ({last})"

def yt_relay_watchdog():
    """Скрипт релея переживает протухание ссылки сам (внутренний цикл), но если
    умрёт весь процесс — поднять его снова некому. Поднимаем: релей нужен ровно
    пока живёт сам поток."""
    while True:
        time.sleep(20)
        try:
            with closing(db()) as c:
                rows = c.execute(
                    "SELECT id, yt_url FROM streams WHERE yt_url IS NOT NULL AND yt_url!=''"
                ).fetchall()
            for r in rows:
                sid = r["id"]
                m = MIXERS.get(sid)
                running = bool(m and m.alive())
                if running and not yt_relay_alive(sid):
                    LOG.event(f"YouTube-релей потока {sid} упал — поднимаю")
                    yt_relay_start(sid, r["yt_url"])
                elif not running and yt_relay_alive(sid):
                    yt_relay_stop(sid)      # поток остановлен — релей ни к чему
        except Exception as e:
            print(f"[ytrelay watchdog] {e}", flush=True)

def ensure_slate_png(src_path):
    """Заглушка (slate) может быть любым загруженным баннером — включая
    анимированный GIF. Раньше движок скармливал ЛЮБОЙ такой файл напрямую в
    `decodebin` (см. gst_streamg.py::_out_desc, ветка slate_branch), а тот
    декодирует ВЕСЬ анимированный GIF целиком, хотя дальше стоит imagefreeze
    и используется только первый кадр. На практике это как минимум лишняя
    работа, а на боевом GIF (1080p, ~1000 кадров) — намертво вешало decodebin
    на префролле ЕЩЁ ДО того, как P2 успевал подняться, из-за чего пропадал
    вообще весь эфир, а не только заглушка (воспроизведено и подтверждено
    напрямую через gst-launch). Решение: конвертируем ЛЮБУЮ заглушку В СТАТИЧНЫЙ
    PNG (один кадр, с альфой) ОДИН РАЗ и кэшируем — движок потом decodebin'ит
    только простой одиночный PNG, что просто, быстро и не может так зависнуть."""
    cache_path = os.path.join(SLATE_CACHE_DIR, os.path.splitext(os.path.basename(src_path))[0] + ".png")
    try:
        src_mtime = os.path.getmtime(src_path)
        if os.path.exists(cache_path) and os.path.getmtime(cache_path) >= src_mtime:
            return cache_path
        im = Image.open(src_path)
        im.seek(0)   # первый кадр (для анимаций) — imagefreeze дальше всё равно берёт только его
        im.convert("RGBA").save(cache_path, "PNG")
        return cache_path
    except Exception as e:
        LOG.event(f"заглушка: не удалось сконвертировать {os.path.basename(src_path)} "
                 f"в PNG ({e}) — используется оригинал как есть", "warning")
        return src_path

class Mixer:
    """Один канал: единый GStreamer-движок (gst_streamg.py) — вход провайдера,
    встроенный буфер/заглушка и оверлеи (баннер/лого/ролик/HTML) слоями
    compositor'а в одном GPU-конвейере, без отдельных ffmpeg-фидеров/шин."""
    def __init__(self, row):
        self.cfg = dict(row)
        sid = self.cfg["id"]
        self.p_gst_ctl = GST_RELAY_PORT_BASE + 1000 + sid  # TCP control движка
        self.gst_engine = None    # процесс gst_streamg.py
        self.started_at = None    # unix-время старта потока, для аптайма в UI
        self.mixer_progress_ts = 0.0  # для «завис ли выход» (грейс на прогрев)
        self.playing_until = 0.0  # локальная бухгалтерия показа баннера/HTML (для UI)
        self.ad_active = False   # локальная бухгалтерия «ролик в эфире» (для UI)
        self.mic_on = False      # локальная бухгалтерия «микрофон включён» (для UI, переживает reload страницы)
        self.obs_connected = False   # OBS-канал подключён (звук слышен), но не обязательно в эфире
        self.obs_live = False        # OBS-наложение реально выведено в эфир
        self.out_volume = 1.0    # текущий фейдер выхода (для UI, переживает reload страницы)
        self.lock = threading.Lock()

    # -------- GStreamer-движок: весь микс в одном процессе
    def _resolve_slate_path(self):
        """PNG-заглушка для длинных срывов провайдера — переиспользуем баннер,
        загруженный на вкладке «Баннеры» (gst_slate_banner_id ссылается на banners.id),
        отдельной формы загрузки под заглушку не заводим."""
        bid = self.cfg.get("gst_slate_banner_id")
        if not bid:
            return None
        with closing(db()) as c:
            r = c.execute("SELECT filename FROM banners WHERE id=?", (bid,)).fetchone()
        if not r:
            return None
        path = os.path.join(BANNER_DIR, r["filename"])
        if not os.path.exists(path):
            return None
        return ensure_slate_png(path)

    def gst_engine_cmd(self):
        c = self.cfg
        import sys
        args = [GST_PYTHON, GST_SCRIPT, "--input", c["input_url"], "--mode", "publish",
                "--control-port", str(self.p_gst_ctl)]
        if c.get("output_url"):
            args += ["--output", c["output_url"]]
        args += [
                # уникальное имя intervideo-канала на поток: внутри одного процесса
                # inter-каналы адресуются по имени, и хотя у разных процессов свои
                # реестры, уникальное имя (sg<id>) исключает любую путаницу при
                # диагностике и на будущее — стоит копейки, снимает целый класс
                # «непонятных пересечений» между потоками.
                "--channel", f"sg{c['id']}",
                # HTML-слой движка спавнит capture_html_raw.py под ЭТИМ же python
                # (venv агента: Playwright+Pillow) — gst_streamg.py сам работает под
                # системным python3 (GST_PYTHON, только там есть биндинги gi)
                "--venv-python", sys.executable,
                "--width", str(c["out_w"]), "--height", str(c["out_h"]),
                "--fps", str(c["fps"]),
                "--banner-width", str(c["banner_w"]), "--banner-height", str(c["banner_h"]),
                "--vbitrate", str(int(re.sub(r"\D", "", c.get("vbitrate", "6000k")) or 6000)),
                # буфер: если у потока выключен — раньше здесь всё равно стоял
                # жёсткий пол в 2с (это была задержка на КАЖДЫЙ кадр через
                # intervideosink ts-offset — попадала в publish ВСЕГДА, что в
                # Flussonic, что в прямой SRT/VLC — не имеет отношения к
                # превью-плееру панели). Раз заглушка+SRT и так есть, держим
                # минимум небольшим — только чтобы очереди/GOP не резонировали.
                # Если буфер включён — берём заданную задержку (та же заглушка).
                "--buffer", str(c.get("gst_buffer_sec") or 12
                                if c.get("gst_buffer_enabled") else 0.5)]
        tracks = parse_audio_tracks(c.get("audio_tracks"))
        for t in tracks:
            args += ["--audio-track", str(t)]
        multi = len(tracks) > 1
        if MEDIAMTX_ENABLED and c.get("mediamtx_enabled", 1):
            if multi:
                # мультиязык: FLV несёт одну аудио, поэтому в MediaMTX уходит два
                # push'а — классический инстанс по RTMP (дорожка 0, для браузерного
                # HLS-плеера панели) и ll-инстанс по SRT со ВСЕМИ дорожками
                # (mpegtsmux); Flussonic тянет мультиязык из ll по SRT.
                args += ["--mediamtx-rtmp-port", str(MEDIAMTX_INSTANCES["classic"]["rtmp_port"])]
                args += ["--mediamtx-srt", f"127.0.0.1:{MEDIAMTX_INSTANCES['ll']['srt_port']}"]
            else:
                for inst in MEDIAMTX_INSTANCES.values():
                    args += ["--mediamtx-rtmp-port", str(inst["rtmp_port"])]
            args += ["--mediamtx-path", self.mediamtx_path()]
        slate = self._resolve_slate_path()
        if slate:
            args += ["--slate", slate]
        return args

    def _gst_ctl(self, cmd, timeout=3):
        """Отправить текстовую команду control-сокету движка GStreamer."""
        import socket as _s
        try:
            with _s.create_connection(("127.0.0.1", self.p_gst_ctl), timeout=timeout) as sk:
                sk.sendall(cmd.encode())
                return sk.recv(4096).decode("utf-8", "replace").strip()
        except Exception as e:
            raise RuntimeError(f"движок не ответил: {e}")

    def _gst_logo_xy(self):
        """Абсолютные xpos/ypos/ширина/высота лого для движка.
        Если задан logo_w/logo_h (перетащили на PGM-превью, см. UI) —
        logo_x/logo_y уже АБСОЛЮТНЫЕ координаты, используем как есть.
        Иначе — старый режим «угол + отступ»: знак logo_x/logo_y определяет
        угол, размер — четверть кадра как потолок."""
        c = self.cfg
        if c.get("logo_w") and c.get("logo_h"):
            return int(c["logo_x"]), int(c["logo_y"]), int(c["logo_w"]), int(c["logo_h"])
        lx, ly = c.get("logo_x", 20) or 0, c.get("logo_y", 20) or 0
        lw = min(c["out_w"] // 4, 480); lh = min(c["out_h"] // 4, 270)
        x = (c["out_w"] - lw + lx) if lx < 0 else lx
        y = (c["out_h"] - lh + ly) if ly < 0 else ly
        return x, y, lw, lh

    # -------- управление
    def _kill(self, p):
        _kill_group(p)

    def start(self):
        sid = self.cfg["id"]
        # YouTube-источник: релей должен работать до старта движка. Здесь это
        # подстраховка для путей, идущих мимо API (autostart при загрузке
        # агента, восстановление watchdog'ом) — при обычном старте из панели
        # релей уже поднят в stream_start. Не роняем старт, если сигнала пока
        # нет: движок переживает отсутствие входа сам (заглушка + переподключение).
        _yt = (self.cfg.get("yt_url") or "").strip()
        if _yt and not yt_relay_alive(sid):
            ok, detail = yt_ensure_ready(sid, _yt)
            if not ok:
                LOG.event(f"[{self.cfg['name']}] YouTube: {detail}", "warning")
        # авто-подстройка fps/битрейта под вход — но НЕ разрешения канваса
        # (out_w/out_h): его оператор уже явно задал в редакторе потока, и
        # оно должно соблюдаться (P1 сам впишет вход любого размера в этот
        # канвас через videoscale). Раньше проба тихо переписывала настроенные
        # 1920x1080 на фактическое разрешение источника (напр. 1046x576) —
        # с более точным пробингом (SAR/выбор лучшей рендиции) это стало
        # происходить систематически и путало оператора: «в настройках 1080p,
        # а на выходе всё равно SD».
        params = probe_input_params(self.cfg["input_url"])
        if params:
            params.pop("out_w", None)
            params.pop("out_h", None)
        if params:
            self.cfg.update(params)
            LOG.event(f"[{self.cfg['name']}] параметры входа: fps={self.cfg['fps']} "
                      f"{self.cfg['vbitrate']} — подстроены автоматически "
                      f"(канвас {self.cfg['out_w']}x{self.cfg['out_h']} — из настроек потока)")
        with self.lock:
            if self.gst_engine and self.gst_engine.poll() is None:
                return
            self._kill(self.gst_engine)
            self.gst_engine = _spawn(self.gst_engine_cmd(), sid, "gstengine")
            self.mic_on = False   # новый процесс движка стартует без микрофона/OBS-наложения
            self.obs_connected = False
            self.obs_live = False
            self.out_volume = 1.0
            self.started_at = time.time()
            self.mixer_progress_ts = time.time() + 8   # грейс на прогрев движка
            LOG.event(f"поток '{self.cfg['name']}' запущен")
            if self.cfg.get("logo_banner_id"):
                threading.Timer(6.0, self._apply_logo_gst).start()

    def stop(self):
        with self.lock:
            self._kill(self.gst_engine)
            self.gst_engine = None
            self.playing_until = 0
            self.ad_active = False
            self.mic_on = False
            self.obs_connected = False
            self.obs_live = False
            self.out_volume = 1.0
            self.started_at = None
            self.mixer_progress_ts = 0.0
            LOG.event(f"поток '{self.cfg['name']}' остановлен")
        # релей нужен ровно пока идёт поток — иначе он молча тянул бы трафик с
        # YouTube и грузил MediaMTX впустую (вне self.lock: _kill_group ждёт
        # завершения процесса, а лок нужен только для полей микшера)
        yt_relay_stop(self.cfg["id"])

    def alive(self):
        return self.gst_engine is not None and self.gst_engine.poll() is None

    def _apply_logo_gst(self):
        """Применить логотип на движке командой (после прогрева)."""
        try:
            if self.cfg.get("logo_banner_id"):
                with closing(db()) as c:
                    b = c.execute("SELECT filename FROM banners WHERE id=?",
                                  (self.cfg["logo_banner_id"],)).fetchone()
                if b:
                    x, y, lw, lh = self._gst_logo_xy()
                    # тот же фикс, что и для заглушки (ensure_slate_png):
                    # логотип — ПОСТОЯННЫЙ статичный слой (imagefreeze берёт
                    # только первый кадр), но decodebin ЛЮБОЙ анимированный
                    # GIF всё равно декодирует целиком — на некоторых файлах
                    # (найдено вживую: 330x100 GIF, лого «LIVE») это тихо не
                    # даёт ни одного кадра на выход (без ошибки в логе — пад
                    # строится штатно, GAP закрывает молчание), и лого просто
                    # не появляется. Конвертируем в кэшированный статичный PNG.
                    path = ensure_slate_png(os.path.join(BANNER_DIR, b["filename"]))
                    self._gst_ctl(f"logo {path} {x} {y} {lw} {lh}")
            else:
                self._gst_ctl("logo off")
        except Exception as e:
            LOG.event(f"[{self.cfg['name']}] лого (движок): {e}", "warning")

    def refresh_logo(self, cfg_updates=None):
        with self.lock:
            if cfg_updates:
                self.cfg.update(cfg_updates)
            if self.alive():
                self._apply_logo_gst()

    def _banner_fit(self, img_path):
        """Прямоугольник (x,y,w,h) для баннера ВНУТРИ баннерной зоны с
        сохранением пропорций картинки — как это делал ffmpeg-движок
        (scale=...:force_original_aspect_ratio=decrease + pad прозрачным). Зона
        потока (banner_w×banner_h) может быть выше/шире картинки (напр. зона
        1920×360, а баннер 1920×150) — тогда БЕЗ подгонки движок растягивал
        картинку на всю зону («ишачий»/чернота). Вписываем с сохранением
        пропорций, прижимаем к НИЗУ зоны по центру (эфирная графика — у нижней
        кромки), остальное зоны остаётся прозрачным (виден эфир."""
        c = self.cfg
        # зона баннера не может быть БОЛЬШЕ самого кадра потока — иначе на
        # потоках с нестандартным разрешением (напр. Nickelodeon 896x504 при
        # дефолтной зоне 1920x150) зона вылезает за пределы кадра и картинка
        # растягивается/обрезается («ишачий» вид).
        bw = min(c["banner_w"], c["out_w"])
        bh = min(c["banner_h"], c["out_h"])
        zx, zy = 0, c["out_h"] - bh
        m = re.match(r"(\d+)x(\d+)", probe(img_path) or "")
        if not m:
            return None    # размер не определился — движок заполнит зону сам
        nw, nh = int(m.group(1)), int(m.group(2))
        if nw <= 0 or nh <= 0:
            return None
        # вписать наибольшим размером в зону с сохранением пропорций (как
        # ffmpeg force_original_aspect_ratio=decrease). Чётные размеры — nvenc/
        # yuv420 любят чётность.
        s = min(bw / nw, bh / nh)
        fw = max(2, (int(nw * s) // 2) * 2)
        fh = max(2, (int(nh * s) // 2) * 2)
        fx = zx + (bw - fw) // 2         # по центру по горизонтали
        fy = zy + (bh - fh)              # прижать к низу зоны
        return fx, fy, fw, fh

    def play_banner(self, img_path, duration, fade, label="", rect=None):
        """rect=(x,y,w,h), если задан явно (интерактивное перетаскивание на
        превью, см. Program-плеер) — иначе автоподгонка (_banner_fit), как
        раньше. Клампим ЛЮБОЙ явный rect по канвасу потока — оператор мог
        тащить рамку на превью с другим соотношением экрана/видео, лучше
        обрезать, чем улететь за пределы кадра."""
        c = self.cfg
        if rect:
            x, y, w, h = rect
            x = max(0, min(int(x), c["out_w"]))
            y = max(0, min(int(y), c["out_h"]))
            w = max(2, min(int(w), c["out_w"] - x))
            h = max(2, min(int(h), c["out_h"] - y))
            rect = (x, y, w, h)
        else:
            rect = self._banner_fit(img_path)
        cmd = f"banner {img_path} {duration} {fade}"
        if rect:
            cmd += " " + " ".join(str(v) for v in rect)
        r = self._gst_ctl(cmd)
        if r.startswith("ERR"):
            raise RuntimeError(r)
        self.playing_until = time.time() + float(duration)
        LOG.event(f"[{self.cfg['name']}] показ баннера {label} на {duration}с")

    def play_html(self, html_path, duration, fade, label="", rect=None):
        """rect=(x,y,w,h) — то же самое, что и у play_banner (произвольное
        размещение с превью); без него — прежняя автоподгонка под banner_w/h."""
        c = self.cfg
        if rect:
            x, y, w, h = rect
            x = max(0, min(int(x), c["out_w"]))
            y = max(0, min(int(y), c["out_h"]))
            bw = max(2, min(int(w), c["out_w"] - x))
            bh = max(2, min(int(h), c["out_h"] - y))
        else:
            bw = min(c["banner_w"], c["out_w"])
            bh = min(c["banner_h"], c["out_h"])
            x, y = 0, c["out_h"] - bh
        r = self._gst_ctl(f"html {html_path} {x} {y} {bw} {bh} "
                          f"15 {duration} {fade}")
        if r.startswith("ERR"):
            raise RuntimeError(r)
        self.playing_until = time.time() + float(duration)
        LOG.event(f"[{self.cfg['name']}] показ HTML {label} на {duration}с")

    def play_video(self, path, label="", gain_db=None):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        # gain_db — результат анализа громкости при загрузке (см.
        # analyze_loudness), переводим дБ в линейный множитель для
        # GStreamer'овского volume-элемента (0дБ = 1.0).
        gain_lin = 10 ** (float(gain_db) / 20) if gain_db is not None else 1.0
        r = self._gst_ctl(f"video {path} {gain_lin:.4f}")
        if r.startswith("ERR"):
            raise RuntimeError(r)
        self.ad_active = True
        LOG.event(f"[{self.cfg['name']}] запущен ролик {label}")
        def watch():
            # длительность ролика заранее неизвестна (в отличие от баннера) —
            # опрашиваем сам движок, пока video_busy не снимется
            while self.alive():
                try:
                    r = self._gst_ctl("ping")
                    if r.startswith("OK "):
                        if not json.loads(r[3:]).get("video_busy"):
                            break
                except Exception:
                    break
                time.sleep(1.0)
            self.ad_active = False
            LOG.event(f"[{self.cfg['name']}] ролик {label} завершён")
        threading.Thread(target=watch, daemon=True).start()

    def stop_video(self):
        try:
            self._gst_ctl("stophtml")
        except Exception:
            pass
        try:
            self._gst_ctl("stopvideo")
        except Exception:
            pass
        self.ad_active = False

    # ---------------- громкость выхода/ролика + VU-метры (UI слева от PGM)
    def set_out_volume(self, v):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl(f"outvol {v}")
        if r.startswith("ERR"):
            raise RuntimeError(r)
        self.out_volume = float(v)   # переживает reload страницы (см. status())

    def set_ad_volume(self, v):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl(f"advol {v}")
        if r.startswith("ERR"):
            raise RuntimeError(r)

    def get_levels(self):
        empty = {"out_rms": -100.0, "ad_rms": -100.0, "mic_rms": -100.0, "obs_rms": -100.0}
        if not self.alive():
            return empty
        # НИКОГДА не поднимаем исключение: /levels опрашивается панелью
        # каждые 400мс, и во время прогрева/рестарта движка контрольный порт
        # ещё закрыт (ConnectionRefused) — раньше это выливалось в 500 и
        # всплывашку «Internal Server Error» у оператора на ровном месте.
        try:
            r = self._gst_ctl("levels", timeout=1.5)
            if r.startswith("OK "):
                return json.loads(r[3:])
        except Exception:
            pass
        return empty

    # ---------------- микрофон комментатора (постоянный SRT-вход через MediaMTX)
    def start_mic(self, streamid, gain=None):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        cmd = f"mic {streamid}" + (f" {gain}" if gain is not None else "")
        r = self._gst_ctl(cmd)
        if r.startswith("ERR"):
            # "уже включён" — не настоящая ошибка, просто UI (после reload
            # страницы) не знал о реальном состоянии движка; синхронизируем
            # локальный флаг и не поднимаем исключение, чтобы кнопка не
            # залипала в недостижимом состоянии.
            if "already on" in r:
                self.mic_on = True
                return
            raise RuntimeError(r)
        self.mic_on = True
        LOG.event(f"[{self.cfg['name']}] микрофон включён (streamid={streamid})")

    def stop_mic(self):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl("micoff")
        if r.startswith("ERR"):
            if "not on" in r:
                self.mic_on = False
                return
            raise RuntimeError(r)
        self.mic_on = False
        LOG.event(f"[{self.cfg['name']}] микрофон выключен")

    def set_mic_volume(self, v):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl(f"micvol {v}")
        if r.startswith("ERR"):
            raise RuntimeError(r)

    # ---------------- наложение полного видео+аудио с OBS: двухшаговая схема
    # (подключение — можно услышать/убедиться что канал живой — потом отдельно
    # "в эфир" с fade-in; "стоп" — fade-out и полный разрыв, возврат к обычному
    # эфиру, а не к промежуточному состоянию подключения).
    def start_obs(self, streamid, audio_mode="mute", fade=None):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        cmd = f"obs {streamid} {audio_mode}" + (f" {fade}" if fade is not None else "")
        r = self._gst_ctl(cmd)
        if r.startswith("ERR"):
            if "already on" in r:
                self.obs_connected = True
                return
            raise RuntimeError(r)
        self.obs_connected = True
        LOG.event(f"[{self.cfg['name']}] OBS-канал подключён (streamid={streamid}, audio={audio_mode})")

    def go_live_obs(self):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl("obslive")
        if r.startswith("ERR"):
            if "already live" in r:
                self.obs_live = True
                return
            raise RuntimeError(r)
        self.obs_live = True
        LOG.event(f"[{self.cfg['name']}] OBS-наложение вышло в эфир")

    def stop_obs(self):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl("obsoff")
        if r.startswith("ERR"):
            if "not on" in r:
                self.obs_connected = False
                self.obs_live = False
                return
            raise RuntimeError(r)
        self.obs_connected = False
        self.obs_live = False
        LOG.event(f"[{self.cfg['name']}] OBS-наложение снято, эфир восстановлен")

    def set_obs_volume(self, v):
        if not self.alive():
            raise RuntimeError("mixer is not running")
        r = self._gst_ctl(f"obsvol {v}")
        if r.startswith("ERR"):
            raise RuntimeError(r)

    def set_input(self, url, tracks=None):
        """Смена входного URL на лету, без рестарта процесса и обрыва publish.
        Канвас P2 (компоновщик+энкодер) всегда фиксирован настройками потока
        (out_w/out_h из редактора) и НЕ подстраивается под источник — P1 сам
        впишет вход любого разрешения в этот канвас через videoscale, каким
        бы он ни был. Раньше (когда канвас при старте подстраивался под
        источник) смена на источник другого разрешения давала «мыло» — тогда
        здесь была проверка мисматча с принудительным полным рестартом. С тех
        пор как канвас перестал зависеть от источника (см. Mixer.start()),
        эта проверка не нужна — смена всегда безопасна и всегда бесшовна.

        tracks — набор аудиодорожек нового источника ("1" или "1,0"), если он
        отличается от текущего: у разных провайдеров дорожки лежат в разном
        порядке. Движок откажет, если меняется их КОЛИЧЕСТВО — тогда нужен
        полный рестарт (см. input_preset_activate)."""
        cmd = f"setinput {url}" + (f" {tracks}" if tracks else "")
        r = self._gst_ctl(cmd)
        if r.startswith("ERR"):
            raise RuntimeError(r)
        self.cfg["input_url"] = url   # чтобы автономный реконнект движка и будущий рестарт брали новый URL
        LOG.event(f"[{self.cfg['name']}] вход сменён на лету (без рестарта эфира)")

    def test_cut_input(self, seconds):
        """Тестовый обрыв входа (см. StreamG.test_cut_input) — искусственно
        рвём вход провайдера на N секунд, БЕЗ реального отключения источника,
        чтобы проверить буфер/заглушку и автовосстановление."""
        r = self._gst_ctl(f"testcut {seconds}")
        if r.startswith("ERR"):
            raise RuntimeError(r)
        LOG.event(f"[{self.cfg['name']}] тестовый обрыв входа на {seconds}с (проверка буфера/заглушки)")
        return r

    def mediamtx_path(self):
        """Имя пути в MediaMTX: последний сегмент output_url, если он задан
        (человекочитаемо, совпадает с именем во Flussonic); иначе — без
        publish во Flussonic вообще (только MediaMTX) — берём "sg<id>"."""
        if self.cfg.get("output_url"):
            return self.cfg["output_url"].rstrip("/").rsplit("/", 1)[-1]
        return f"sg{self.cfg['id']}"

    def status(self):
        # возраст последнего пульса выхода: во время прогрева движка
        # mixer_progress_ts выставлен в будущее (см. start()), не показываем
        # отрицательный возраст
        out_age = (max(0, int(time.time() - self.mixer_progress_ts))
                   if self.alive() and self.mixer_progress_ts else None)
        return {"running": self.alive(),
                "playing": time.time() < self.playing_until,
                "playing_left": max(0, int(self.playing_until - time.time())),
                "ad_playing": self.ad_active,
                "mic_on": self.mic_on if self.alive() else False,
                "obs_connected": self.obs_connected if self.alive() else False,
                "obs_live": self.obs_live if self.alive() else False,
                "out_volume": self.out_volume if self.alive() else 1.0,
                "started_at": self.started_at if self.alive() else None,
                "output_age": out_age,
                "gst_buffer_alive": self.alive() if self.cfg.get("gst_buffer_enabled") else None,
                # путь и порты для панели (внешние RTSP/HLS/SRT/WebRTC-ссылки на
                # компаньон-сервер MediaMTX) — только пока поток реально в эфире
                # (MediaMTX создаёт путь динамически при первом RTMP-push, до
                # старта потока путь ещё не существует)
                "mediamtx": ({"path": self.mediamtx_path(),
                             "rtsp_port": MEDIAMTX_INSTANCES["ll"]["rtsp_port"],
                             # hls_port — low-latency (мин. задержка, но нужен
                             # HTTPS для сессионной cookie — годится не везде);
                             # hls_classic_port — обычный HLS, работает по HTTP
                             # без ограничений (используем его во встроенном
                             # плеере панели, см. playerOpen в UI)
                             "hls_port": MEDIAMTX_INSTANCES["ll"]["hls_port"],
                             "hls_classic_port": MEDIAMTX_INSTANCES["classic"]["hls_port"],
                             "webrtc_port": MEDIAMTX_INSTANCES["ll"]["webrtc_port"],
                             "srt_port": MEDIAMTX_INSTANCES["ll"]["srt_port"],
                             "moq_port": MEDIAMTX_INSTANCES["ll"]["moq_port"]}
                            if MEDIAMTX_ENABLED and self.cfg.get("mediamtx_enabled", 1)
                               and self.alive() else None)}

MIXERS: dict[int, Mixer] = {}
MIX_LOCK = threading.Lock()

def get_mixer(stream_id) -> Mixer:
    with MIX_LOCK:
        if stream_id not in MIXERS:
            with closing(db()) as c:
                row = c.execute("SELECT * FROM streams WHERE id=?", (stream_id,)).fetchone()
            if not row:
                raise HTTPException(404, "stream not found")
            MIXERS[stream_id] = Mixer(row)
        return MIXERS[stream_id]

_WATCHDOG_COOLDOWN = {}  # sid -> unix ts, чтобы не спамить лог/попытки при конфликте
# Микшер здоров, если выдаёт -progress ~1 раз/сек. Порог намеренно большой:
# обычный reconnect входа провайдера (delay_max 5с) не должен приниматься за
# зависание. 25с тишины = процесс жив, но выход замер (класс аварий «висит
# в ожидании публикаций» / reconnect-петля) — publish уже мёртв, рестарт
# только восстанавливает, а не создаёт глитч (зритель уже потерял поток).
def watchdog():
    """Поднимает упавшие потоки (autostart). Движок GStreamer — единый процесс
    со своей внутренней устойчивостью (буфер, never-die, реконнект входа);
    watchdog поднимает его целиком при смерти, во внутренности не лезет.
    Интервал 5с."""
    while True:
        time.sleep(5)
        try:
            # MediaMTX упал сам (не по кнопке «Стоп» в панели) — поднимаем.
            # MEDIAMTX_USER_STOPPED защищает от гонки: если админ явно остановил
            # конкретный инстанс из панели, watchdog не должен тут же поднять
            # его обратно.
            if MEDIAMTX_ENABLED:
                for key in MEDIAMTX_INSTANCES:
                    if key not in MEDIAMTX_USER_STOPPED and not mediamtx_alive(key):
                        LOG.event(f"watchdog: MediaMTX ({key}) не отвечает — перезапуск", "warning")
                        start_mediamtx(key)
            # страховка: «Проверка» — ручной инструмент, если админ забыл её
            # закрыть (ушёл со вкладки), не держим ffmpeg вечно
            with CHECK_LOCK:
                if CHECK["proc"] and time.time() - CHECK["started"] > 900:
                    LOG.event("watchdog: авто-остановка забытой «Проверки» (>15 мин)", "warning")
                    _kill_proc(CHECK["proc"])
                    CHECK["proc"] = None
                    CHECK["url"] = None
            with closing(db()) as c:
                rows = c.execute("SELECT * FROM streams").fetchall()
            for row in rows:
                m = MIXERS.get(row["id"])
                # «был запущен, но умер»: m.started_at выставлен при старте и
                # сброшен при stop(); у мёртвого процесса он остаётся, у
                # намеренно остановленного — None.
                died = m is not None and m.started_at is not None and not m.alive()
                if row["autostart"] and (m is None or died):
                    if time.time() < _WATCHDOG_COOLDOWN.get(row["id"], 0):
                        continue
                    # тот же guard, что и в ручном старте: если другой НАШ поток
                    # уже жив на этом же output_url — не лезть (иначе гонка
                    # watchdog против ручного старта убивает один из потоков).
                    # Пустой output_url (только MediaMTX) в конфликт не считаем.
                    if row["output_url"] and any(
                           oid != row["id"] and om.alive()
                           and om.cfg.get("output_url") == row["output_url"]
                           for oid, om in list(MIXERS.items())):
                        _WATCHDOG_COOLDOWN[row["id"]] = time.time() + 60
                        continue
                    foreign = foreign_publisher_pid(row["output_url"])
                    if foreign:
                        LOG.event(f"watchdog: поток '{row['name']}' не запущен — "
                                  f"на {row['output_url']} уже публикует чужой процесс "
                                  f"(PID {foreign}), похоже на старый ручной скрипт", "error")
                        _WATCHDOG_COOLDOWN[row["id"]] = time.time() + 60
                        continue
                    LOG.event(f"watchdog: перезапуск потока '{row['name']}'", "warning")
                    get_mixer(row["id"]).start()
                    continue
        except Exception as e:
            LOG.event(f"watchdog error: {e}", "error")

# ---------------------------------------------------------------- очередь и расписание
def next_from_queue():
    with closing(db()) as c:
        items = c.execute("""SELECT q.*, b.filename, b.name FROM queue q
                             JOIN banners b ON b.id=q.banner_id
                             ORDER BY q.position""").fetchall()
    if not items:
        return None
    pos = int(get_state("queue_ptr", "0")) % len(items)
    set_state("queue_ptr", (pos + 1) % len(items))
    return items[pos]

def fire(s):
    action = s["action"] if "action" in s.keys() else "show"
    if action in ("stream_start", "stream_stop"):
        with closing(db()) as c:
            ids = ([s["stream_id"]] if s["stream_id"] else
                   [r["id"] for r in c.execute("SELECT id FROM streams")])
        for sid in ids:
            try:
                if action == "stream_start":
                    guard(f"расписание: запуск потока {sid}")
                    old = MIXERS.pop(sid, None)
                    if old:
                        old.stop()
                    get_mixer(sid).start()
                else:
                    m = MIXERS.get(sid)
                    if m:
                        m.stop()
            except HTTPException:
                pass  # guard уже записал причину в журнал
            except Exception as e:
                LOG.event(f"расписание: ошибка {action} потока {sid}: {e}", "error")
        return
    targets = [s["stream_id"]] if s["stream_id"] else list(MIXERS)
    if s["video_id"]:
        with closing(db()) as c:
            v = c.execute("SELECT * FROM videos WHERE id=?", (s["video_id"],)).fetchone()
        if not v:
            return
        for sid in targets:
            m = MIXERS.get(sid)
            if m and m.alive() and not m.ad_active:
                try:
                    m.play_video(os.path.join(VIDEO_DIR, v["filename"]), v["name"])
                except RuntimeError:
                    pass
        return
    if s["template_id"]:
        with closing(db()) as c:
            t = c.execute("SELECT * FROM templates WHERE id=?", (s["template_id"],)).fetchone()
        if not t:
            return
        tmp = os.path.join(DATA_DIR, f"tpl_sched_{s['id']}.html")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(t["html"])
        for sid in targets:
            m = MIXERS.get(sid)
            if m and m.alive() and not m.status()["playing"]:
                try:
                    m.play_html(tmp, s["duration"], s["fade"], t["name"])
                except RuntimeError:
                    pass
        return
    if s["banner_id"]:
        with closing(db()) as c:
            b = c.execute("SELECT * FROM banners WHERE id=?", (s["banner_id"],)).fetchone()
        if not b:
            return
        gif, dur, label = os.path.join(BANNER_DIR, b["filename"]), s["duration"], b["name"]
    else:
        item = next_from_queue()
        if not item:
            return
        gif = os.path.join(BANNER_DIR, item["filename"])
        dur, label = s["duration"] or item["duration"], item["name"]
    for sid in targets:
        m = MIXERS.get(sid)
        if m and m.alive() and not m.status()["playing"]:
            try:
                m.play_banner(gif, dur, s["fade"], label)
            except RuntimeError:
                pass

def scheduler():
    while True:
        time.sleep(15)
        try:
            now = datetime.now()
            today = date.today().isoformat()
            with closing(db()) as c:
                rows = c.execute("SELECT * FROM schedules WHERE enabled=1").fetchall()
            for s in rows:
                if s["date_from"] and today < s["date_from"]: continue
                if s["date_to"] and today > s["date_to"]: continue
                hh, mm = map(int, s["time_start"].split(":"))
                start = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if now < start: continue
                elapsed = (now - start).total_seconds()
                period = (s["every_minutes"] or 0) * 60
                slot = start.timestamp() + (period * int(elapsed // period) if period else 0)
                last = datetime.fromisoformat(s["last_run"]).timestamp() if s["last_run"] else 0
                if slot > last and now.timestamp() - slot < 60:
                    with closing(db()) as c, c:
                        c.execute("UPDATE schedules SET last_run=? WHERE id=?",
                                  (now.isoformat(timespec="seconds"), s["id"]))
                    LOG.event(f"расписание '{s['name'] or s['id']}' сработало")
                    fire(s)
        except Exception as e:
            LOG.event(f"scheduler error: {e}", "error")

_FAILOVER_AFTER = 20.0      # с; вход молчит дольше — считаем сбоем (движок сам
                            # уже переподключается, это порог именно на СМЕНУ
                            # пресета, не на обычный кратковременный реконнект)
_FAILOVER_COOLDOWN = 25.0   # мин. пауза между переключениями одного потока —
                            # не долбить пресеты подряд, дать каждому шанс подняться
_FAILOVER_LAST = {}         # sid -> unix ts последнего переключения

def failover_loop():
    """Автопереключение по кругу на следующий пресет входа (галочка
    «автопереключение» в диалоге «Входы»), когда текущий источник молчит
    дольше порога. Обычные кратковременные сбои движок и так переживает сам
    (буфер+заглушка, см. gst_streamg.py) — это ПОВЕРХ того, для случая, когда
    сам провайдер реально лёг и нужен другой источник, а не просто подождать."""
    while True:
        time.sleep(5)
        try:
            with closing(db()) as c:
                rows = c.execute("SELECT * FROM streams WHERE auto_failover=1").fetchall()
            for row in rows:
                sid = row["id"]
                m = MIXERS.get(sid)
                if not m or not m.alive():
                    continue
                with closing(db()) as c:
                    presets = c.execute(
                        "SELECT name,url FROM input_presets WHERE stream_id=? ORDER BY position",
                        (sid,)).fetchall()
                if not presets:
                    continue
                try:
                    r = m._gst_ctl("ping")
                    if not r.startswith("OK "):
                        continue
                    silent = json.loads(r[3:]).get("input_silent_sec", 0)
                except Exception:
                    continue
                if silent < _FAILOVER_AFTER:
                    continue
                if time.time() - _FAILOVER_LAST.get(sid, 0) < _FAILOVER_COOLDOWN:
                    continue
                urls = [p["url"] for p in presets]
                names = [p["name"] for p in presets]
                cur_url = (m.cfg.get("input_url") or "").strip()
                nxt = (urls.index(cur_url) + 1) % len(urls) if cur_url in urls else 0
                _FAILOVER_LAST[sid] = time.time()
                try:
                    m.set_input(urls[nxt])
                    LOG.event(f"[{row['name']}] автопереключение: вход молчит "
                              f"{silent:.0f}с — переключаюсь на пресет «{names[nxt]}»", "warning")
                except Exception as e:
                    LOG.event(f"[{row['name']}] автопереключение не удалось: {e}", "error")
        except Exception as e:
            LOG.event(f"failover_loop: {e}", "error")

threading.Thread(target=watchdog, daemon=True).start()
threading.Thread(target=failover_loop, daemon=True).start()
threading.Thread(target=scheduler, daemon=True).start()
threading.Thread(target=yt_relay_watchdog, daemon=True).start()
if MEDIAMTX_ENABLED:
    start_all_mediamtx()

# ---------------------------------------------------------------- валидация
def vnum(val, lo, hi, what):
    try:
        v = float(val)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{what}: не число")
    if not (lo <= v <= hi):
        raise HTTPException(400, f"{what}: допустимо {lo}..{hi}")
    return v

def vbr(s):
    k = int(re.sub(r"\D", "", str(s)) or 0)
    if not (100 <= k <= 50000):
        raise HTTPException(400, "битрейт: допустимо 100k..50000k")
    return f"{k}k"

def parse_audio_tracks(s):
    """Строку "0" / "0,1,2" → список int-индексов без дублей, минимум [0].
    Пустое/битое → [0] (одна дорожка, прежнее поведение)."""
    out, seen = [], set()
    for part in str(s or "0").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            t = int(part)
        except ValueError:
            continue
        if t >= 0 and t not in seen:
            seen.add(t); out.append(t)
    return out or [0]

def vtracks(s):
    """Валидация поля аудиодорожек из формы. Возвращает нормализованную строку."""
    tracks = parse_audio_tracks(s)
    for t in tracks:
        if t > 63:
            raise HTTPException(400, "аудиодорожка: индекс 0..63")
    if len(tracks) > 16:
        raise HTTPException(400, "слишком много аудиодорожек (макс. 16)")
    return ",".join(str(t) for t in tracks)

def vtime(s):
    if not re.fullmatch(r"\d{2}:\d{2}", s):
        raise HTTPException(400, "время: формат HH:MM")
    hh, mm = map(int, s.split(":"))
    if hh > 23 or mm > 59:
        raise HTTPException(400, "время: часы 00-23, минуты 00-59")
    return s

def foreign_publisher_pid(output_url):
    """Сканирует /proc в поисках ЧУЖОГО процесса (не нашего), который уже публикует
    на тот же самый RTMP output_url — например, старый ручной скрипт вроде
    banner_out_dynamic.sh, оставленный запущенным при переходе на агента.
    Linux-only (/proc), что нормально: агент работает только на сервере с Flussonic."""
    own_pids = {p.pid for m in MIXERS.values()
                for p in (m.gst_engine,) if p is not None}
    try:
        pids = [d for d in os.listdir("/proc") if d.isdigit()]
    except OSError:
        return None
    needle = output_url.encode()
    for pid_s in pids:
        pid = int(pid_s)
        if pid == os.getpid() or pid in own_pids:
            continue
        try:
            with open(f"/proc/{pid_s}/cmdline", "rb") as f:
                raw = f.read()
        except OSError:
            continue
        # /proc/<pid>/cmdline — argv-элементы через NUL, а не пробел
        args = raw.split(b"\x00")
        if needle in args and b"flv" in args:
            return pid
    return None

CHECK = {"proc": None, "started": 0.0, "url": None}   # вкладка «Проверка»: одна проба зараз
CHECK_LOCK = threading.Lock()

def _kill_proc(p):
    _kill_group(p)

def checkstream_cmd(url):
    """Короткая HLS-проба произвольного входа провайдера (для вкладки «Проверка»
    в UI, ДО добавления его как полноценного потока). Перекодируем в компактный
    h264/aac — источник может быть в любом кодеке/контейнере, а браузер должен
    показать что угодно. Разрешение уменьшено (960px) и hls_time=1 — это только
    визуальная проверка «жив ли сигнал», не эфирное качество."""
    if url.startswith(("http://", "https://")):
        in_flags = ["-reconnect", "1", "-reconnect_delay_max", "5"]
    elif url.startswith("rtsp://"):
        in_flags = ["-rtsp_transport", "tcp"]
    else:
        in_flags = []
    return [FFMPEG, "-hide_banner", "-loglevel", "error",
            *in_flags, "-i", url,
            "-vf", "scale=960:-2", "-c:v", "libx264", "-preset", "veryfast",
            "-tune", "zerolatency", "-g", "50", "-c:a", "aac", "-ar", "44100",
            "-f", "hls", "-hls_time", "1", "-hls_list_size", "3",
            "-hls_flags", "delete_segments+append_list+omit_endlist",
            "-start_number", "0", os.path.join(CHECK_DIR, "index.m3u8")]

def check_input(url, timeout=8):
    """Проверка доступности входного потока провайдера через ffprobe.
    Возвращает (ok, detail). Best-effort: сетевые протоколы разные (http/rtmp/udp/srt),
    поэтому опираемся на общий таймаут процесса, а не на протокол-специфичные опции."""
    if not shutil.which(FFPROBE):
        return True, "ffprobe недоступен на сервере — проверка пропущена"
    try:
        p = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_type", "-of", "default=noprint_wrappers=1", url],
            capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"нет ответа за {timeout}с (таймаут)"
    if p.returncode != 0:
        detail = (p.stderr or "").strip().splitlines()
        return False, (detail[-1] if detail else f"ffprobe rc={p.returncode}")
    return True, "поток отвечает"

def vdate(s):
    if not s:
        return None
    try:
        date.fromisoformat(s)
    except ValueError:
        raise HTTPException(400, "дата: формат YYYY-MM-DD")
    return s

# ---------------------------------------------------------------- API
def auth(x_api_key: str = Header(default="")):
    if not API_KEY or x_api_key != API_KEY:
        raise HTTPException(401, "bad api key")

app = FastAPI(title="ad-streamer agent", dependencies=[Depends(auth)])

@app.get("/api/ping")
def ping():
    return {"ok": True, "time": datetime.now().isoformat(timespec="seconds")}

@app.get("/api/sysstat")
def sysstat():
    return {**SYS, "limits": limits()}

# ---- MediaMTX (два компаньон-сервера: low-latency HLS+RTSP+SRT+WebRTC,
# и отдельно классический HLS без ограничений — см. MEDIAMTX_INSTANCES)
@app.get("/api/mediamtx/status")
def mediamtx_status():
    return {"enabled": MEDIAMTX_ENABLED,
            "instances": {key: {"running": mediamtx_alive(key),
                                "user_stopped": key in MEDIAMTX_USER_STOPPED}
                         for key in MEDIAMTX_INSTANCES},
            # для простых клиентов панели: "жив", если оба инстанса живы
            "running": all(mediamtx_alive(key) for key in MEDIAMTX_INSTANCES)}

@app.get("/api/srtpush/info")
def srtpush_info():
    """Параметры приёма SRT-push: внешний энкодер (Astra Cesbo и т.п.) умеет
    только SRT-caller — то есть пушит НАМ, а слушать должны мы. Приёмником уже
    служит MediaMTX (инстанс "ll"): он слушает SRT и заводит путь сам по
    streamid=publish:<имя>, отдельной настройки на каждый источник не нужно.
    Движок потока читает этот путь локально как обычный вход.

    Панель берёт отсюда и порт, и ФОРМАТ строк — чтобы ничего из этого не было
    зашито во фронтенде: на другом сервере порт может отличаться, и тогда
    подсказка оператору осталась бы неверной. {host} подставляет панель (адрес
    этого агента), {id} вводит оператор."""
    ll = MEDIAMTX_INSTANCES["ll"]
    port = ll["srt_port"]
    rtsp = ll["rtsp_port"]
    return {"enabled": MEDIAMTX_ENABLED,
            "running": mediamtx_alive("ll"),
            "srt_port": port,
            "rtsp_port": rtsp,
            # что вбить в энкодер (адрес НАШЕГО сервера + publish)
            "publish_tpl": f"srt://{{host}}:{port}?streamid=publish:{{id}}",
            # Что подставить в input_url потока (движок рядом с MediaMTX →
            # localhost). Читаем по RTSP, а НЕ обратно по SRT: SRT-выход
            # MediaMTX умеет отдавать не всякий кодек, и вещательный MPEG-2
            # (обычное дело для Astra и аппаратных энкодеров) он не отдаёт —
            # путь при этом заводится и числится ready, а чтение падает с
            # Input/output error. По RTSP тот же поток читается нормально,
            # H.264 через него идёт так же хорошо. Проверено вживую на
            # MPEG-2 SD 720x576 с Astra Cesbo.
            "input_tpl": f"rtsp://127.0.0.1:{rtsp}/{{id}}"}

@app.post("/api/mediamtx/start")
def mediamtx_start_api():
    if not MEDIAMTX_ENABLED:
        raise HTTPException(400, "MediaMTX выключен (MEDIAMTX_ENABLED=0 в agent.env)")
    start_all_mediamtx()
    return {"ok": True}

@app.post("/api/mediamtx/stop")
def mediamtx_stop_api():
    stop_all_mediamtx(user_initiated=True)
    return {"ok": True}

# ---- медиа (общая логика для banners / videos)
MEDIA_EXTS = {"banners": [".gif", ".png"], "videos": [".mp4", ".mov", ".mkv", ".ts"]}
MEDIA_MAX_MB = {"banners": 100, "videos": 2048}

def media_list(table):
    with closing(db()) as c:
        return [dict(r) for r in c.execute(f"SELECT * FROM {table} ORDER BY id DESC")]

async def _save_upload(file, path, max_mb):
    """Потоковая запись на диск с лимитом размера (не читаем файл в память целиком)."""
    limit = max_mb * 1024 * 1024
    size = 0
    with open(path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            size += len(chunk)
            if size > limit:
                f.close()
                os.remove(path)
                raise HTTPException(413, f"файл больше лимита {max_mb} МБ")
            f.write(chunk)
    return size

def _validate_media(path, table):
    """Проверка, что содержимое — реальное изображение/видео (ffprobe его понимает)."""
    meta = probe(path)
    if shutil.which(FFPROBE) and not meta:
        os.remove(path)
        raise HTTPException(400, "файл не распознан как изображение/видео")
    return meta

async def media_upload(table, folder, file, name, exts):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in exts:
        raise HTTPException(400, f"допустимые форматы: {', '.join(exts)}")
    fname = uuid.uuid4().hex + ext
    path = os.path.join(folder, fname)
    size = await _save_upload(file, path, MEDIA_MAX_MB[table])
    meta = _validate_media(path, table)
    gain_db = analyze_loudness(path) if table == "videos" else None
    with closing(db()) as c, c:
        if table == "videos":
            cur = c.execute("INSERT INTO videos(name,filename,size,meta,gain_db) VALUES(?,?,?,?,?)",
                            (name or file.filename, fname, size, meta, gain_db))
        else:
            cur = c.execute(f"INSERT INTO {table}(name,filename,size,meta) VALUES(?,?,?,?)",
                            (name or file.filename, fname, size, meta))
    LOG.event(f"загружен файл: {name or file.filename}"
             + (f" (авто-громкость {gain_db:+.1f}дБ)" if gain_db is not None else ""))
    return {"id": cur.lastrowid}

async def media_replace(table, folder, mid, file):
    with closing(db()) as c:
        r = c.execute(f"SELECT * FROM {table} WHERE id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404)
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in MEDIA_EXTS[table]:
        raise HTTPException(400, f"допустимые форматы: {', '.join(MEDIA_EXTS[table])}")
    # пишем во временный файл; фиксируем только после успешной валидации.
    # Имя файла всегда новое, с расширением НОВОГО файла — иначе замена
    # GIF на PNG оставляла PNG-байты под именем *.gif
    tmp = os.path.join(folder, "up_" + uuid.uuid4().hex)
    size = await _save_upload(file, tmp, MEDIA_MAX_MB[table])
    meta = _validate_media(tmp, table)
    new_fname = uuid.uuid4().hex + ext
    dst = os.path.join(folder, new_fname)
    os.replace(tmp, dst)
    with closing(db()) as c, c:
        if table == "videos":
            gain_db = analyze_loudness(dst)
            c.execute("UPDATE videos SET filename=?, size=?, meta=?, gain_db=? WHERE id=?",
                     (new_fname, size, meta, gain_db, mid))
        else:
            c.execute(f"UPDATE {table} SET filename=?, size=?, meta=? WHERE id=?",
                      (new_fname, size, meta, mid))
    if r["filename"] != new_fname:
        try:
            os.remove(os.path.join(folder, r["filename"]))
        except OSError:
            pass
    LOG.event(f"горячая замена файла: {r['name']}")
    return {"ok": True}

def media_delete(table, folder, mid):
    with closing(db()) as c, c:
        r = c.execute(f"SELECT * FROM {table} WHERE id=?", (mid,)).fetchone()
        if not r:
            raise HTTPException(404)
        c.execute(f"DELETE FROM {table} WHERE id=?", (mid,))
    try:
        os.remove(os.path.join(folder, r["filename"]))
    except OSError:
        pass
    return {"ok": True}

def media_file(table, folder, mid, mt=None):
    with closing(db()) as c:
        r = c.execute(f"SELECT * FROM {table} WHERE id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404)
    if mt is None:  # по расширению: баннеры бывают и GIF, и PNG
        mt = mimetypes.guess_type(r["filename"])[0] or "application/octet-stream"
    return FileResponse(os.path.join(folder, r["filename"]), media_type=mt)

@app.get("/api/banners")
def banners(): return media_list("banners")
@app.post("/api/banners")
async def banners_up(file: UploadFile = File(...), name: str = Form("")):
    return await media_upload("banners", BANNER_DIR, file, name, [".gif", ".png"])
@app.post("/api/banners/{mid}/replace")
async def banners_rep(mid: int, file: UploadFile = File(...)):
    return await media_replace("banners", BANNER_DIR, mid, file)
@app.delete("/api/banners/{mid}")
def banners_del(mid: int): return media_delete("banners", BANNER_DIR, mid)
@app.get("/api/banners/{mid}/file")
def banners_file(mid: int): return media_file("banners", BANNER_DIR, mid)

@app.get("/api/videos")
def videos(): return media_list("videos")
@app.post("/api/videos")
async def videos_up(file: UploadFile = File(...), name: str = Form("")):
    return await media_upload("videos", VIDEO_DIR, file, name, [".mp4", ".mov", ".mkv", ".ts"])
@app.post("/api/videos/{mid}/replace")
async def videos_rep(mid: int, file: UploadFile = File(...)):
    return await media_replace("videos", VIDEO_DIR, mid, file)
@app.delete("/api/videos/{mid}")
def videos_del(mid: int): return media_delete("videos", VIDEO_DIR, mid)
@app.get("/api/videos/{mid}/file")
def videos_file(mid: int): return media_file("videos", VIDEO_DIR, mid, "video/mp4")

# ---- конвертер / ресайзер
def _convert_finish(table, folder, mid, tmp_path, new_fname):
    dst = os.path.join(folder, new_fname)
    os.replace(tmp_path, dst)
    with closing(db()) as c, c:
        old = c.execute(f"SELECT filename FROM {table} WHERE id=?", (mid,)).fetchone()
        c.execute(f"UPDATE {table} SET filename=?, size=?, meta=? WHERE id=?",
                  (new_fname, os.path.getsize(dst), probe(dst), mid))
    if old and old["filename"] != new_fname:
        try:
            os.remove(os.path.join(folder, old["filename"]))
        except OSError:
            pass

@app.post("/api/banners/{mid}/convert")
def banner_convert(mid: int, w: int = Form(1920), h: int = Form(150), fps: int = Form(25)):
    """Подгонка GIF: вписать с сохранением пропорций, поля прозрачные, палитра с альфой."""
    vnum(w, 16, 4096, "ширина"); vnum(h, 16, 4096, "высота"); vnum(fps, 1, 60, "fps")
    with closing(db()) as c:
        r = c.execute("SELECT * FROM banners WHERE id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404)
    src = os.path.join(BANNER_DIR, r["filename"])
    new_fname = uuid.uuid4().hex + ".gif"
    tmp = os.path.join(BANNER_DIR, "tmp_" + new_fname)
    vf = (f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
          f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:color=black@0.0,fps={fps},"
          f"split[a][b];[a]palettegen=reserve_transparent=1[p];"
          f"[b][p]paletteuse=alpha_threshold=128")
    cmd = [FFMPEG, "-y", "-hide_banner", "-i", src,
           "-filter_complex", vf, "-loop", "0", "-f", "gif", tmp]
    job_id = uuid.uuid4().hex[:12]
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "running", "kind": "banner", "media_id": mid,
                        "detail": f"GIF → {w}x{h}@{fps}"}
    LOG.event(f"конвертация баннера '{r['name']}' → {w}x{h}@{fps}")
    run_job(job_id, cmd, lambda: _convert_finish("banners", BANNER_DIR, mid, tmp, new_fname))
    return {"job": job_id}

@app.post("/api/videos/{mid}/convert")
def video_convert(mid: int, w: int = Form(1920), h: int = Form(1080), fps: int = Form(25),
                  vbitrate: str = Form("6000k"), vcodec: str = Form("libx264")):
    """Подгонка ролика: вписать с чёрными полями, h264+aac, faststart."""
    vnum(w, 16, 4096, "ширина"); vnum(h, 16, 4096, "высота"); vnum(fps, 1, 60, "fps")
    vbitrate = vbr(vbitrate)
    if vcodec not in ("h264_nvenc", "libx264"):
        raise HTTPException(400, "кодек: h264_nvenc или libx264")
    with closing(db()) as c:
        r = c.execute("SELECT * FROM videos WHERE id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404)
    guard("конвертация ролика")
    src = os.path.join(VIDEO_DIR, r["filename"])
    new_fname = uuid.uuid4().hex + ".mp4"
    tmp = os.path.join(VIDEO_DIR, "tmp_" + new_fname)
    k = int(re.sub(r"\D", "", vbitrate) or 6000)
    vf = (f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
          f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,fps={fps},format=yuv420p")
    enc = (["-c:v", "h264_nvenc", "-preset", "p4", "-rc", "cbr",
            "-b:v", f"{k}k", "-maxrate", f"{k}k", "-bufsize", f"{k*2}k"]
           if vcodec == "h264_nvenc" else
           ["-c:v", "libx264", "-preset", "fast", "-b:v", f"{k}k",
            "-maxrate", f"{k}k", "-bufsize", f"{k*2}k"])
    cmd = [FFMPEG, "-y", "-hide_banner", "-i", src, "-vf", vf, *enc,
           "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
           "-movflags", "+faststart", "-f", "mp4", tmp]
    job_id = uuid.uuid4().hex[:12]
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "running", "kind": "video", "media_id": mid,
                        "detail": f"MP4 → {w}x{h}@{fps} {vbitrate}"}
    LOG.event(f"конвертация ролика '{r['name']}' → {w}x{h}@{fps} {vbitrate} ({vcodec})")
    run_job(job_id, cmd, lambda: _convert_finish("videos", VIDEO_DIR, mid, tmp, new_fname))
    return {"job": job_id}

@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    with JOBS_LOCK:
        j = JOBS.get(job_id)
    if not j:
        raise HTTPException(404)
    return j

# ---- очередь
@app.get("/api/queue")
def queue_list():
    with closing(db()) as c:
        return [dict(r) for r in c.execute(
            """SELECT q.*, b.name AS banner_name FROM queue q
               JOIN banners b ON b.id=q.banner_id ORDER BY q.position""")]

@app.post("/api/queue")
def queue_add(banner_id: int = Form(...), duration: int = Form(30)):
    with closing(db()) as c, c:
        pos = c.execute("SELECT COALESCE(MAX(position),0)+1 p FROM queue").fetchone()["p"]
        c.execute("INSERT INTO queue(banner_id,position,duration) VALUES(?,?,?)",
                  (banner_id, pos, duration))
    return {"ok": True}

@app.delete("/api/queue/{qid}")
def queue_del(qid: int):
    with closing(db()) as c, c:
        c.execute("DELETE FROM queue WHERE id=?", (qid,))
    return {"ok": True}

@app.post("/api/queue/{qid}/move")
def queue_move(qid: int, dir: str = Form(...)):
    with closing(db()) as c, c:
        ids = [r["id"] for r in c.execute("SELECT id FROM queue ORDER BY position")]
        if qid not in ids:
            raise HTTPException(404, "queue item not found")
        i = ids.index(qid)
        j = i - 1 if dir == "up" else i + 1
        if 0 <= j < len(ids):
            ids[i], ids[j] = ids[j], ids[i]
            for pos, iid in enumerate(ids, 1):
                c.execute("UPDATE queue SET position=? WHERE id=?", (pos, iid))
    return {"ok": True}

# ---- потоки
@app.get("/api/streams")
def streams_list():
    with closing(db()) as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM streams ORDER BY id")]
    for r in rows:
        m = MIXERS.get(r["id"])
        r["status"] = m.status() if m else {"running": False, "playing": False, "ad_playing": False}
    return rows

@app.post("/api/streams")
def stream_add(name: str = Form(...), input_url: str = Form(...), output_url: str = Form(""),
               out_w: int = Form(1920), out_h: int = Form(1080),
               banner_w: int = Form(1920), banner_h: int = Form(150),
               vcodec: str = Form("h264_nvenc"), vbitrate: str = Form("6000k"),
               fps: int = Form(25), autostart: int = Form(0),
               mediamtx_enabled: int = Form(1), audio_tracks: str = Form("0"),
               slate_banner_id: int = Form(0), yt_url: str = Form("")):
    for v, what in ((out_w, "кадр W"), (out_h, "кадр H"),
                    (banner_w, "баннер W"), (banner_h, "баннер H")):
        vnum(v, 16, 4096, what)
    vnum(fps, 1, 60, "fps")
    audio_tracks = vtracks(audio_tracks)
    vbitrate = vbr(vbitrate)
    if vcodec not in ("h264_nvenc", "libx264"):
        raise HTTPException(400, "кодек: h264_nvenc или libx264")
    output_url = output_url.strip()
    # YouTube: во входе потока стоит НЕ сама ссылка, а локальный релей — ссылка
    # YouTube временная и в input_url жить не может (см. yt_relay_start).
    # Реальный адрес подставляем сразу после INSERT, когда известен id.
    yt_url = (yt_url or "").strip()
    # ссылку на YouTube вставили в обычное поле входа — распознаём сами. Иначе
    # поток молча создавался бы нерабочим: движок не умеет ходить на youtube.com
    # напрямую, и оператор увидел бы невнятную ошибку только при старте.
    if not yt_url and is_yt_link(input_url):
        yt_url = input_url.strip()
    if yt_url:
        if not re.match(r"^https?://(www\.|m\.)?(youtube\.com|youtu\.be)/", yt_url, re.I):
            raise HTTPException(400, "ссылка должна быть на youtube.com или youtu.be")
        if not (MEDIAMTX_ENABLED and mediamtx_enabled):
            raise HTTPException(400, "источник YouTube требует включённой "
                                     "отдачи через MediaMTX — через неё идёт релей")
        input_url = "-"          # плейсхолдер, заменится ниже на адрес релея
    # выход обязателен, ЕСЛИ поток никуда больше не публикуется — без него и
    # без MediaMTX эфира просто не будет
    if not output_url and not (MEDIAMTX_ENABLED and mediamtx_enabled):
        raise HTTPException(400, "укажите выход (publish во Flussonic) "
                                 "или включите отдачу через MediaMTX")
    # мультиязык уходит только через MediaMTX (SRT) — без него несколько дорожек
    # доставить некуда (FLV/RTMP несёт одну)
    if len(parse_audio_tracks(audio_tracks)) > 1 and not (MEDIAMTX_ENABLED and mediamtx_enabled):
        raise HTTPException(400, "мультиязык (несколько аудиодорожек) требует "
                                 "включённой отдачи через MediaMTX")
    # заглушка на долгий срыв провайдера — раньше её можно было задать только
    # ОТДЕЛЬНЫМ действием после создания потока, и про неё легко было забыть:
    # поток уходил в эфир, а при первом же обрыве зритель видел чёрный экран
    # вместо картинки. Работает независимо от галочки буфера.
    if slate_banner_id:
        with closing(db()) as c:
            if not c.execute("SELECT 1 FROM banners WHERE id=?",
                             (slate_banner_id,)).fetchone():
                raise HTTPException(404, "заглушка: баннер не найден")
    with closing(db()) as c, c:
        cur = c.execute("""INSERT INTO streams(name,input_url,output_url,out_w,out_h,
                           banner_w,banner_h,vcodec,vbitrate,fps,autostart,engine,
                           mediamtx_enabled,audio_tracks,gst_slate_banner_id,yt_url)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,'gstreamer',?,?,?,?)""",
                        (name, input_url, output_url, out_w, out_h,
                         banner_w, banner_h, vcodec, vbitrate, fps, autostart,
                         mediamtx_enabled, audio_tracks, slate_banner_id or None,
                         yt_url or None))
        sid = cur.lastrowid
        if yt_url:
            # адрес релея известен только теперь — путь в MediaMTX строится по id
            c.execute("UPDATE streams SET input_url=? WHERE id=?",
                      (yt_input_url(sid), sid))
    return {"id": sid}

@app.post("/api/probeaudio")
def probe_audio(url: str = Form(...)):
    """Аудиодорожки источника: порядковый номер, язык, название, кодек, каналы.

    Нужен панели, чтобы оператор ВИДЕЛ дорожки и выбирал нужную, а не угадывал
    номера. Частый случай: провайдер отдаёт интершум первой дорожкой, а речь
    второй — по умолчанию в эфир уходит первая, и получается фон вместо
    комментария.

    Номер в ответе — порядковый СРЕДИ АУДИОДОРОЖЕК (0, 1, 2…), ровно в том
    виде, в каком его ждёт audio_tracks у потока, а не абсолютный индекс
    потока в контейнере."""
    url = (url or "").strip()
    if not url:
        raise HTTPException(400, "укажите URL")
    if not shutil.which(FFPROBE):
        raise HTTPException(503, "ffprobe недоступен на сервере")
    try:
        p = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "a",
             "-show_entries", "stream=codec_name,channels:stream_tags=language,title",
             "-of", "json", url],
            capture_output=True, text=True, timeout=25)
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "источник не ответил за 25с")
    if p.returncode != 0:
        detail = (p.stderr or "").strip().splitlines()
        raise HTTPException(400, detail[-1] if detail else "источник недоступен")
    try:
        streams = json.loads(p.stdout or "{}").get("streams", [])
    except Exception:
        raise HTTPException(500, "не разобрал ответ ffprobe")
    tracks = []
    for i, s in enumerate(streams):
        tags = s.get("tags") or {}
        tracks.append({"n": i,                       # то, что идёт в audio_tracks
                       "codec": s.get("codec_name") or "",
                       "channels": s.get("channels"),
                       "language": (tags.get("language") or "").strip(),
                       "title": (tags.get("title") or "").strip()})
    return {"ok": True, "tracks": tracks}

@app.post("/api/checkinput")
def checkinput(url: str = Form(...)):
    """Проверка произвольного URL (используется формой добавления/правки потока)."""
    ok, detail = check_input(url)
    return {"ok": ok, "detail": detail}

# ---- вкладка «Проверка»: живой предпросмотр произвольного URL до создания потока
@app.post("/api/checkstream/start")
def checkstream_start(url: str = Form(...)):
    if not url.strip():
        raise HTTPException(400, "укажите URL")
    with CHECK_LOCK:
        _kill_proc(CHECK["proc"])
        for f in os.listdir(CHECK_DIR):
            try:
                os.remove(os.path.join(CHECK_DIR, f))
            except OSError:
                pass
        CHECK["proc"] = _spawn(checkstream_cmd(url), sid=None, tag="checkstream")
        CHECK["started"] = time.time()
        CHECK["url"] = url
    return {"ok": True}

@app.post("/api/checkstream/stop")
def checkstream_stop():
    with CHECK_LOCK:
        _kill_proc(CHECK["proc"])
        CHECK["proc"] = None
        CHECK["url"] = None
        for f in os.listdir(CHECK_DIR):
            try:
                os.remove(os.path.join(CHECK_DIR, f))
            except OSError:
                pass
    return {"ok": True}

@app.get("/api/checkstream/status")
def checkstream_status():
    alive = bool(CHECK["proc"] and CHECK["proc"].poll() is None)
    return {"running": alive, "age": round(time.time() - CHECK["started"]) if alive else None,
           "url": CHECK["url"] if alive else None}

@app.get("/api/checkstream/hls/{fname}")
def checkstream_hls(fname: str):
    # basename — защита от path traversal (fname приходит из URL плеера)
    safe = os.path.basename(fname)
    path = os.path.join(CHECK_DIR, safe)
    if not os.path.exists(path):
        raise HTTPException(404, "not found")
    media = "application/vnd.apple.mpegurl" if safe.endswith(".m3u8") else "video/mp2t"
    return FileResponse(path, media_type=media, headers={"Cache-Control": "no-cache"})

@app.put("/api/streams/{sid}")
def stream_edit(sid: int, name: str = Form(...), input_url: str = Form(...),
               output_url: str = Form(""), out_w: int = Form(1920), out_h: int = Form(1080),
               banner_w: int = Form(1920), banner_h: int = Form(150),
               vcodec: str = Form("h264_nvenc"), vbitrate: str = Form("6000k"),
               fps: int = Form(25), autostart: int = Form(0),
               mediamtx_enabled: int = Form(1), audio_tracks: str = Form("0"),
               yt_url: str = Form(None)):
    with closing(db()) as c:
        old = c.execute("SELECT * FROM streams WHERE id=?", (sid,)).fetchone()
    if not old:
        raise HTTPException(404, "stream not found")
    # yt_url не передали вовсе (старый клиент) — оставляем как было; передали
    # пустым — источник YouTube снимается, вход снова обычный
    yt_url = old["yt_url"] if yt_url is None else (yt_url or "").strip()
    # ссылку на YouTube вписали в обычное поле входа — распознаём сами
    # (см. такую же защиту в stream_add)
    if not yt_url and is_yt_link(input_url):
        yt_url = input_url.strip()
    if yt_url:
        if not re.match(r"^https?://(www\.|m\.)?(youtube\.com|youtu\.be)/", yt_url, re.I):
            raise HTTPException(400, "ссылка должна быть на youtube.com или youtu.be")
        if not (MEDIAMTX_ENABLED and mediamtx_enabled):
            raise HTTPException(400, "источник YouTube требует включённой "
                                     "отдачи через MediaMTX — через неё идёт релей")
        input_url = yt_input_url(sid)      # вход всегда указывает на релей
    for v, what in ((out_w, "кадр W"), (out_h, "кадр H"),
                    (banner_w, "баннер W"), (banner_h, "баннер H")):
        vnum(v, 16, 4096, what)
    vnum(fps, 1, 60, "fps")
    audio_tracks = vtracks(audio_tracks)
    vbitrate = vbr(vbitrate)
    if vcodec not in ("h264_nvenc", "libx264"):
        raise HTTPException(400, "кодек: h264_nvenc или libx264")
    output_url = output_url.strip()
    if not output_url and not (MEDIAMTX_ENABLED and mediamtx_enabled):
        raise HTTPException(400, "укажите выход (publish во Flussonic) "
                                 "или включите отдачу через MediaMTX")
    if len(parse_audio_tracks(audio_tracks)) > 1 and not (MEDIAMTX_ENABLED and mediamtx_enabled):
        raise HTTPException(400, "мультиязык (несколько аудиодорожек) требует "
                                 "включённой отдачи через MediaMTX")
    with closing(db()) as c, c:
        c.execute("""UPDATE streams SET name=?, input_url=?, output_url=?, out_w=?, out_h=?,
                    banner_w=?, banner_h=?, vcodec=?, vbitrate=?, fps=?, autostart=?, engine='gstreamer',
                    mediamtx_enabled=?, audio_tracks=?, yt_url=?
                    WHERE id=?""",
                 (name, input_url, output_url, out_w, out_h, banner_w, banner_h,
                  vcodec, vbitrate, fps, autostart, mediamtx_enabled, audio_tracks,
                  yt_url or None, sid))
    # Сменили ссылку — старый релей тянет уже не то, гасим. Если поток в эфире,
    # сразу поднимаем новый: путь в MediaMTX тот же (yt<id>), поэтому движку
    # рестарт не нужен — он увидит короткий пропал сигнала и подхватит обратно.
    # Без немедленного подъёма пришлось бы ждать до 20с очередного тика
    # watchdog'а, и всё это время в эфире висела бы заглушка.
    if (old["yt_url"] or "") != (yt_url or ""):
        yt_relay_stop(sid)
        _m = MIXERS.get(sid)
        if yt_url and _m and _m.alive():
            yt_relay_start(sid, yt_url)
    # Бесшовная смена входа: если поток В ЭФИРЕ и изменился ТОЛЬКО input_url
    # (структурные параметры — размер кадра/баннера, fps, кодек, битрейт, выход,
    # MediaMTX-тумблер — зашиты в пайплайн и требуют полного рестарта), меняем
    # вход на лету командой движку. Тогда UI не делает restartStream, publish не
    # рвётся. Если поменялось что-то структурное — seamless=False, UI
    # перезапустит поток как раньше.
    m = MIXERS.get(sid)
    seamless = False
    if m and m.alive():
        structural = (int(old["out_w"]) != out_w or int(old["out_h"]) != out_h or
                      int(old["banner_w"]) != banner_w or int(old["banner_h"]) != banner_h or
                      int(old["fps"]) != fps or old["vcodec"] != vcodec or
                      vbr(old["vbitrate"]) != vbitrate or old["output_url"] != output_url or
                      int(old["mediamtx_enabled"] if old["mediamtx_enabled"] is not None else 1)
                      != mediamtx_enabled or
                      parse_audio_tracks(old["audio_tracks"]) != parse_audio_tracks(audio_tracks))
        if not structural and old["input_url"] != input_url:
            try:
                m.set_input(input_url)
                seamless = True
            except RuntimeError as e:
                LOG.event(f"поток '{name}': бесшовная смена входа не удалась ({e}) — "
                          f"нужен рестарт", "warning")
    LOG.event(f"поток '{name}' изменён"
             + (" (вход сменён на лету)" if seamless else
                " (применится при следующем старте)" if m and m.alive() else ""))
    return {"ok": True, "seamless": seamless}

@app.delete("/api/streams/{sid}")
def stream_del(sid: int):
    m = MIXERS.pop(sid, None)
    if m:
        m.stop()
    with closing(db()) as c, c:
        c.execute("DELETE FROM streams WHERE id=?", (sid,))
    return {"ok": True}

# ---- пресеты входа: сохранённые URL источников для мгновенного переключения
# (пульт оператора, п.1) — используют уже существующий бесшовный set_input,
# просто дают ему быстрый доступ по кнопке вместо ручного ввода URL в форме.
@app.post("/api/streams/{sid}/autofailover")
def stream_autofailover(sid: int, enabled: int = Form(...)):
    with closing(db()) as c, c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (sid,)).fetchone():
            raise HTTPException(404, "stream not found")
        c.execute("UPDATE streams SET auto_failover=? WHERE id=?", (1 if enabled else 0, sid))
    return {"ok": True}

@app.get("/api/streams/{sid}/presets")
def input_presets_list(sid: int):
    with closing(db()) as c:
        rows = c.execute("SELECT id,name,url,position,logo_set,audio_tracks "
                         "FROM input_presets WHERE stream_id=? ORDER BY position,id",
                         (sid,)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/streams/{sid}/presets/{pid}/tracks")
def input_preset_tracks(sid: int, pid: int, audio_tracks: str = Form("")):
    """Запомнить аудиодорожки ДЛЯ ЭТОГО источника. Пусто = своей настройки нет,
    работает общая настройка потока (прежнее поведение).

    Нужно потому, что у разных провайдеров дорожки лежат в разном порядке:
    настроенная под один источник дорожка у другого может отсутствовать, и
    тогда в эфир уходила тишина (движок подставляет её вместо недостающей —
    иначе завис бы весь конвейер, включая видео)."""
    audio_tracks = (audio_tracks or "").strip()
    if audio_tracks:
        # Строгая проверка: parse_audio_tracks (и vtracks поверх неё) намеренно
        # мягкие — битый ввод превращают в "0", чтобы не ронять поток. Здесь так
        # нельзя: оператор задаёт дорожки явно, и опечатка вместо ошибки молча
        # записала бы "0", затерев прежнюю настройку (поймано тестом).
        if not re.fullmatch(r"\d+(\s*,\s*\d+)*", audio_tracks):
            raise HTTPException(400, "аудиодорожки: только числа через запятую, "
                                     "например «1» или «1,0»")
        audio_tracks = vtracks(audio_tracks)      # та же нормализация, что у потока
    with closing(db()) as c, c:
        if not c.execute("SELECT 1 FROM input_presets WHERE id=? AND stream_id=?",
                         (pid, sid)).fetchone():
            raise HTTPException(404, "preset not found")
        c.execute("UPDATE input_presets SET audio_tracks=? WHERE id=?",
                  (audio_tracks or None, pid))
    return {"ok": True, "audio_tracks": audio_tracks or None}

@app.post("/api/streams/{sid}/presets")
def input_preset_add(sid: int, name: str = Form(...), url: str = Form(...)):
    with closing(db()) as c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (sid,)).fetchone():
            raise HTTPException(404, "stream not found")
    name, url = name.strip(), url.strip()
    if not name or not url:
        raise HTTPException(400, "укажите название и URL пресета")
    with closing(db()) as c, c:
        pos = c.execute("SELECT COALESCE(MAX(position),-1)+1 p FROM input_presets "
                        "WHERE stream_id=?", (sid,)).fetchone()["p"]
        cur = c.execute("INSERT INTO input_presets(stream_id,name,url,position) "
                        "VALUES(?,?,?,?)", (sid, name, url, pos))
    return {"id": cur.lastrowid}

@app.delete("/api/streams/{sid}/presets/{pid}")
def input_preset_del(sid: int, pid: int):
    with closing(db()) as c, c:
        c.execute("DELETE FROM input_presets WHERE id=? AND stream_id=?", (pid, sid))
    return {"ok": True}

@app.post("/api/streams/{sid}/presets/{pid}/activate")
def input_preset_activate(sid: int, pid: int):
    with closing(db()) as c:
        preset = c.execute("SELECT * FROM input_presets WHERE id=? AND stream_id=?",
                           (pid, sid)).fetchone()
        row = c.execute("""SELECT name, input_url, logo_banner_id, logo_x, logo_y,
                           logo_w, logo_h, audio_tracks FROM streams WHERE id=?""",
                        (sid,)).fetchone()
    if not preset:
        raise HTTPException(404, "preset not found")
    if not row:
        raise HTTPException(404, "stream not found")
    # автосохранение ТЕКУЩЕГО входа перед переключением — иначе после
    # активации пресета старый (рабочий) вход теряется без следа, и вернуться
    # на него нечем (баг, на который наткнулся пользователь: переключился на
    # пресет — и обратно пути не было). Сохраняем только если текущий вход ещё
    # не сохранён НИ ОДНИМ пресетом (по URL) — не плодим дубли при повторных
    # переключениях между уже сохранёнными пресетами.
    cur_url = (row["input_url"] or "").strip()
    if cur_url and cur_url != preset["url"]:
        with closing(db()) as c:
            already = c.execute("SELECT 1 FROM input_presets WHERE stream_id=? AND url=?",
                                (sid, cur_url)).fetchone()
        if not already:
            # снимаем и текущее лого в «Прежний вход», чтобы возврат на него
            # восстановил вид (logo_set=1 если лого есть, иначе 0)
            has_logo = 1 if row["logo_banner_id"] else 0
            with closing(db()) as c, c:
                pos = c.execute("SELECT COALESCE(MAX(position),-1)+1 p FROM input_presets "
                                "WHERE stream_id=?", (sid,)).fetchone()["p"]
                # вместе с лого снимаем и текущие аудиодорожки: возврат на этот
                # вход должен восстанавливать и звук тоже, иначе он вернётся с
                # дорожками уже другого провайдера
                c.execute("""INSERT INTO input_presets(stream_id,name,url,position,
                             logo_set,logo_banner_id,logo_x,logo_y,logo_w,logo_h,
                             audio_tracks)
                             VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                         (sid, "Прежний вход", cur_url, pos, has_logo,
                          row["logo_banner_id"], row["logo_x"], row["logo_y"],
                          row["logo_w"], row["logo_h"], row["audio_tracks"] or "0"))
    # Аудиодорожки этого источника. У разных провайдеров они лежат в разном
    # порядке, и без этого после переключения в эфир уходила тишина: движок
    # искал дорожку с прежним номером, а у нового источника её не было.
    pk = preset.keys()
    want_tracks = preset["audio_tracks"] if "audio_tracks" in pk else None
    want_tracks = (want_tracks or "").strip() or None
    cur_tracks = (row["audio_tracks"] or "0").strip()
    tracks_changed = bool(want_tracks) and want_tracks != cur_tracks
    # менять КОЛИЧЕСТВО дорожек на лету нельзя — оно определяет структуру
    # выходной части (интерканалы P2, дорожки мультиязычного SRT), а её мы не
    # пересобираем. При другом количестве честно перезапускаем поток.
    need_restart = tracks_changed and (len(parse_audio_tracks(want_tracks))
                                       != len(parse_audio_tracks(cur_tracks)))

    m = MIXERS.get(sid)
    seamless = False
    if m and m.alive() and not need_restart:
        try:
            m.set_input(preset["url"], want_tracks if tracks_changed else None)
            seamless = True
        except RuntimeError as e:
            raise HTTPException(400, f"смена входа не удалась: {e}")
    # персистим URL (и дорожки) в БД в любом случае — чтобы следующий старт
    # (если поток сейчас не в эфире или бесшовно не вышло) взял уже новый пресет
    with closing(db()) as c, c:
        if tracks_changed:
            c.execute("UPDATE streams SET input_url=?, audio_tracks=? WHERE id=?",
                      (preset["url"], want_tracks, sid))
        else:
            c.execute("UPDATE streams SET input_url=? WHERE id=?", (preset["url"], sid))
    if need_restart and m and m.alive():
        # число языков другое — бесшовно нельзя, перезапускаем весь поток
        LOG.event(f"[{row['name']}] дорожки {cur_tracks} → {want_tracks}: "
                  f"нужен перезапуск потока (разное количество)")
        MIXERS.pop(sid, None)
        m.stop()
        get_mixer(sid).start()
    # восстанавливаем лого этого пресета (если запоминалось, logo_set не NULL):
    # переключение входа больше не сбрасывает лого — оно приходит к виду, в
    # котором настроено для этого конкретного потока/входа.
    if "logo_set" in pk and preset["logo_set"] is not None:
        with closing(db()) as c, c:
            if preset["logo_set"]:
                c.execute("""UPDATE streams SET logo_banner_id=?, logo_x=?, logo_y=?,
                             logo_w=?, logo_h=? WHERE id=?""",
                          (preset["logo_banner_id"], preset["logo_x"], preset["logo_y"],
                           preset["logo_w"], preset["logo_h"], sid))
            else:
                c.execute("UPDATE streams SET logo_banner_id=NULL WHERE id=?", (sid,))
        if m:
            if preset["logo_set"]:
                m.refresh_logo({"logo_banner_id": preset["logo_banner_id"],
                                "logo_x": preset["logo_x"], "logo_y": preset["logo_y"],
                                "logo_w": preset["logo_w"], "logo_h": preset["logo_h"]})
            else:
                m.refresh_logo({"logo_banner_id": None})
    LOG.event(f"поток '{row['name']}': вход переключён на пресет «{preset['name']}»"
             + (" (на лету)" if seamless else " (применится при следующем старте)"))
    return {"ok": True, "seamless": seamless, "url": preset["url"]}

@app.post("/api/streams/{sid}/start")
def stream_start(sid: int):
    guard("запуск потока")
    with closing(db()) as c:
        row = c.execute("SELECT input_url, output_url, name, yt_url FROM streams WHERE id=?",
                        (sid,)).fetchone()
    if not row:
        raise HTTPException(404, "stream not found")
    # YouTube: вход указывает на локальный релей, и до его запуска пути ещё нет —
    # обычная проверка ниже отвалилась бы с 404. Поднимаем релей и ждём сигнала
    # ПЕРЕД проверкой, а оператору отдаём внятную причину, если не дождались.
    if (row["yt_url"] or "").strip():
        ok, detail = yt_ensure_ready(sid, row["yt_url"].strip())
        if not ok:
            yt_relay_stop(sid)
            LOG.event(f"старт потока '{row['name']}' отклонён: {detail}", "error")
            raise HTTPException(409, f"YouTube: {detail}")
    ok, detail = check_input(row["input_url"])
    if not ok:
        LOG.event(f"старт потока '{row['name']}' отклонён: вход недоступен ({detail})", "error")
        raise HTTPException(400, f"вход от провайдера недоступен: {detail}")
    # два НАШИХ потока на один и тот же выход — тоже конфликт (у пользователя
    # несколько каналов-профилей целятся в один stream_ad_1: работать должен
    # один). Пустой output_url (поток только через MediaMTX) в конфликт не
    # считаем — два таких потока никак не пересекаются на Flussonic.
    if row["output_url"]:
        for oid, om in list(MIXERS.items()):
            if oid != sid and om.alive() and om.cfg.get("output_url") == row["output_url"]:
                msg = (f"выход {row['output_url']} уже занят потоком '{om.cfg.get('name')}' — "
                       f"остановите его, затем запускайте этот")
                LOG.event(f"старт потока '{row['name']}' отклонён: {msg}", "error")
                raise HTTPException(409, msg)
    old = MIXERS.pop(sid, None)  # перечитать конфиг из БД
    if old:
        old.stop()
    foreign = foreign_publisher_pid(row["output_url"]) if row["output_url"] else None
    if foreign:
        msg = (f"на {row['output_url']} уже публикует другой процесс (PID {foreign}) — "
               f"похоже на старый ручной скрипт (например banner_out_dynamic.sh). "
               f"Остановите его перед запуском из панели, иначе оба будут конфликтовать "
               f"за один и тот же поток во Flussonic.")
        LOG.event(f"старт потока '{row['name']}' отклонён: {msg}", "error")
        raise HTTPException(409, msg)
    get_mixer(sid).start()
    with closing(db()) as c, c:
        # точка отсчёта T=00:00 операторской программы выставляется автоматом
        # в момент реального старта потока — оператор может в эту секунду
        # быть занят в другой вкладке, ждать ручного "T=сейчас" нельзя
        c.execute("UPDATE operator_programs SET start_at=? WHERE stream_id=?",
                  (datetime.now().isoformat(timespec="seconds"), sid))
    return {"ok": True}

@app.post("/api/streams/{sid}/stop")
def stream_stop(sid: int):
    m = MIXERS.get(sid)
    if m:
        m.stop()
    return {"ok": True}

@app.post("/api/streams/{sid}/play")
def stream_play(sid: int, banner_id: int = Form(...), duration: int = Form(30),
                fade: float = Form(1.0),
                x: int = Form(None), y: int = Form(None),
                w: int = Form(None), h: int = Form(None)):
    vnum(duration, 1, 3600, "длительность")
    fade = vnum(fade, 0, 10, "fade")
    with closing(db()) as c:
        b = c.execute("SELECT * FROM banners WHERE id=?", (banner_id,)).fetchone()
    if not b:
        raise HTTPException(404, "banner not found")
    # rect передаётся ЦЕЛИКОМ (все 4 числа) с интерактивного превью — иначе
    # (не задано / задано частично) — прежняя автоподгонка под banner_w/h
    rect = (x, y, w, h) if None not in (x, y, w, h) else None
    try:
        get_mixer(sid).play_banner(os.path.join(BANNER_DIR, b["filename"]),
                                   duration, fade, b["name"], rect=rect)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/playhtml")
def stream_playhtml(sid: int, template_id: int = Form(...), duration: int = Form(30),
                    fade: float = Form(1.0),
                    x: int = Form(None), y: int = Form(None),
                    w: int = Form(None), h: int = Form(None)):
    vnum(duration, 1, 3600, "длительность")
    fade = vnum(fade, 0, 10, "fade")
    with closing(db()) as c:
        t = c.execute("SELECT * FROM templates WHERE id=?", (template_id,)).fetchone()
    if not t:
        raise HTTPException(404, "template not found")
    tmp = os.path.join(DATA_DIR, f"tpl_{sid}.html")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(t["html"])
    rect = (x, y, w, h) if None not in (x, y, w, h) else None
    try:
        get_mixer(sid).play_html(tmp, duration, fade, t["name"], rect=rect)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

def _save_logo_to_active_preset(sid, logo_set, banner_id=None, x=None, y=None, w=None, h=None):
    """Запомнить настройку лого в АКТИВНОМ пресете входа (тот, чей url ==
    текущему input_url потока) — чтобы при возврате на этот вход лого
    восстановился. Если поток сейчас не на сохранённом пресете — тихо
    пропускаем (лого останется только в самом потоке)."""
    with closing(db()) as c, c:
        row = c.execute("SELECT input_url FROM streams WHERE id=?", (sid,)).fetchone()
        if not row or not (row["input_url"] or "").strip():
            return
        c.execute("""UPDATE input_presets SET logo_set=?, logo_banner_id=?,
                     logo_x=?, logo_y=?, logo_w=?, logo_h=? WHERE stream_id=? AND url=?""",
                  (logo_set, banner_id, x, y, w, h, sid, row["input_url"].strip()))

@app.post("/api/streams/{sid}/logo")
def stream_logo_set(sid: int, banner_id: int = Form(...),
                    x: int = Form(20), y: int = Form(20),
                    w: int = Form(None), h: int = Form(None)):
    # отрицательные x/y = отступ от правого/нижнего края — ТОЛЬКО в старом
    # режиме «угол+отступ» (w/h не заданы). Если w/h пришли (перетащили на
    # PGM-превью) — x/y уже абсолютные координаты, знак не участвует.
    vnum(x, -4096, 4096, "X"); vnum(y, -4096, 4096, "Y")
    manual = w is not None and h is not None
    if manual:
        vnum(w, 2, 4096, "W"); vnum(h, 2, 4096, "H")
    with closing(db()) as c:
        if not c.execute("SELECT 1 FROM banners WHERE id=?", (banner_id,)).fetchone():
            raise HTTPException(404, "banner not found")
    lw, lh = (w if manual else None), (h if manual else None)
    with closing(db()) as c, c:
        c.execute("UPDATE streams SET logo_banner_id=?, logo_x=?, logo_y=?, logo_w=?, logo_h=? WHERE id=?",
                  (banner_id, x, y, lw, lh, sid))
    _save_logo_to_active_preset(sid, 1, banner_id, x, y, lw, lh)
    m = MIXERS.get(sid)
    if m:
        m.refresh_logo({"logo_banner_id": banner_id, "logo_x": x, "logo_y": y,
                        "logo_w": lw, "logo_h": lh})
    LOG.event(f"логотип установлен на поток {sid} ({x},{y}" + (f",{w}x{h})" if manual else ")"))
    return {"ok": True}

@app.delete("/api/streams/{sid}/logo")
def stream_logo_del(sid: int):
    with closing(db()) as c, c:
        c.execute("UPDATE streams SET logo_banner_id=NULL WHERE id=?", (sid,))
    _save_logo_to_active_preset(sid, 0)
    m = MIXERS.get(sid)
    if m:
        m.refresh_logo({"logo_banner_id": None})
    LOG.event(f"логотип убран с потока {sid}")
    return {"ok": True}

@app.post("/api/streams/{sid}/previewimg")
def stream_previewimg_set(sid: int, banner_id: int = Form(...)):
    """Картинка для плитки мультивью в панели (0 = сбросить). В ЭФИР НЕ
    ИДЁТ — чисто оформление превью, в отличие от /logo (тот жжётся в
    картинку трансляции). Движок не трогаем — только БД."""
    with closing(db()) as c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (sid,)).fetchone():
            raise HTTPException(404, "stream not found")
        if banner_id and not c.execute("SELECT 1 FROM banners WHERE id=?",
                                       (banner_id,)).fetchone():
            raise HTTPException(404, "banner not found")
    with closing(db()) as c, c:
        c.execute("UPDATE streams SET preview_banner_id=? WHERE id=?",
                  (banner_id or None, sid))
    LOG.event(f"картинка превью потока {sid}: {banner_id or 'сброшена'}")
    return {"ok": True}

@app.post("/api/streams/{sid}/gstbuffer")
def stream_gst_buffer_set(sid: int, enabled: int = Form(...),
                          buffer_sec: float = Form(12), slate_banner_id: int = Form(0)):
    """GStreamer-буфер провайдера (см. agent/gst_streamg.py): держит запас
    в buffer_sec секунд перед микшером — короткие срывы провайдера (короче
    буфера) зритель не замечает; долгие — заглушка (slate_banner_id), пока
    провайдер не вернётся. Применяется со следующего старта/рестарта потока
    (микшер уже собран из фиксированной команды, на лету не переключается)."""
    buffer_sec = vnum(buffer_sec, 3, 60, "буфер, сек")
    with closing(db()) as c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (sid,)).fetchone():
            raise HTTPException(404, "stream not found")
        if slate_banner_id and not c.execute(
                "SELECT 1 FROM banners WHERE id=?", (slate_banner_id,)).fetchone():
            raise HTTPException(404, "banner not found")
    with closing(db()) as c, c:
        c.execute("""UPDATE streams SET gst_buffer_enabled=?, gst_buffer_sec=?,
                    gst_slate_banner_id=? WHERE id=?""",
                 (1 if enabled else 0, buffer_sec, slate_banner_id or None, sid))
    m = MIXERS.get(sid)
    note = " (применится при следующем старте потока)"
    if m and m.alive():
        LOG.event(f"поток {sid}: настройки GStreamer-буфера изменены{note}")
    return {"ok": True, "note": note.strip()}

@app.post("/api/streams/{sid}/testcut")
def stream_test_cut(sid: int, seconds: float = Form(20)):
    """Тестовый обрыв входа провайдера — для проверки буфера/заглушки и
    автовосстановления без реального отключения источника (см. п.3 запроса
    пользователя: убедиться, что при сбое эфир не рвётся и заглушка
    показывается/скрывается корректно)."""
    seconds = vnum(seconds, 1, 300, "длительность обрыва, сек")
    m = MIXERS.get(sid)
    if not m or not m.alive():
        raise HTTPException(400, "поток не в эфире")
    try:
        m.test_cut_input(seconds)
    except RuntimeError as e:
        raise HTTPException(400, f"не удалось оборвать вход: {e}")
    return {"ok": True}

# ---- HTML-шаблоны
@app.get("/api/templates")
def templates_list():
    with closing(db()) as c:
        return [dict(r) for r in c.execute("SELECT * FROM templates ORDER BY id DESC")]

@app.post("/api/templates")
def template_add(name: str = Form(...), html: str = Form(...)):
    with closing(db()) as c, c:
        cur = c.execute("INSERT INTO templates(name,html) VALUES(?,?)", (name, html))
    return {"id": cur.lastrowid}

@app.post("/api/templates/preview")
def template_preview(html: str = Form(...), w: int = Form(1920), h: int = Form(150),
                     clip: int = Form(0)):
    """Рендер HTML-шаблона headless-Chromium — тем же движком, что и эфир,
    но без запуска ffmpeg-конвейера показа. clip=0 — один PNG-кадр (снятый
    на 2-й секунде, чтобы анимация успела «въехать»); clip=1 — 5-секундный
    видеоролик webm: в нём бегущая строка реально бежит, как будет в эфире.
    Синхронный def: FastAPI выполнит его в threadpool, чтобы sync Playwright
    не конфликтовал с asyncio-циклом event loop.
    ВАЖНО: должен быть объявлен ДО @app.post("/api/templates/{tid}") — иначе
    FastAPI матчит маршруты по порядку регистрации, и "{tid}" перехватывает
    буквальное "preview" как значение параметра (проверено: давало 422)."""
    vnum(w, 16, 3840, "ширина"); vnum(h, 16, 2160, "высота")
    if len(html) > 200_000:
        raise HTTPException(400, "HTML слишком большой для превью")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise HTTPException(503, "Playwright не установлен на сервере")
    uid = uuid.uuid4().hex
    tmp = os.path.join(DATA_DIR, f"preview_{uid}.html")
    vid_dir = os.path.join(DATA_DIR, f"preview_vid_{uid}")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    guard_net = lambda route: (route.continue_()
                               if route.request.url.startswith(("file://", "data:"))
                               else route.abort())
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(args=["--no-sandbox", "--disable-gpu"])
            try:
                if clip:
                    # запись видео возможна только на контексте; webm/vp8
                    # играется любым современным браузером в <video>
                    ctx = browser.new_context(
                        viewport={"width": w, "height": h},
                        record_video_dir=vid_dir,
                        record_video_size={"width": w, "height": h})
                    page = ctx.new_page()
                    page.route("**/*", guard_net)
                    page.goto("file://" + tmp.replace("\\", "/"))
                    page.wait_for_timeout(5000)
                    video = page.video
                    ctx.close()          # финализирует запись файла
                    with open(video.path(), "rb") as vf:
                        data = vf.read()
                    media = "video/webm"
                else:
                    page = browser.new_page(viewport={"width": w, "height": h})
                    page.route("**/*", guard_net)
                    page.goto("file://" + tmp.replace("\\", "/"))
                    # 2с: анимации (бегущая строка, fade-in) успевают «въехать»
                    # в кадр — иначе снимок ловил пустое начало анимации
                    page.wait_for_timeout(2000)
                    data = page.screenshot(omit_background=True, timeout=15000)
                    media = "image/png"
            finally:
                browser.close()
    except Exception as e:
        raise HTTPException(500, f"ошибка рендера: {e}")
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
        shutil.rmtree(vid_dir, ignore_errors=True)
    return Response(content=data, media_type=media)

@app.post("/api/templates/{tid}")
def template_update(tid: int, name: str = Form(...), html: str = Form(...)):
    with closing(db()) as c, c:
        c.execute("UPDATE templates SET name=?, html=? WHERE id=?", (name, html, tid))
    return {"ok": True}

@app.delete("/api/templates/{tid}")
def template_del(tid: int):
    with closing(db()) as c, c:
        c.execute("DELETE FROM templates WHERE id=?", (tid,))
    return {"ok": True}

@app.post("/api/streams/{sid}/playvideo")
def stream_playvideo(sid: int, video_id: int = Form(...)):
    with closing(db()) as c:
        v = c.execute("SELECT * FROM videos WHERE id=?", (video_id,)).fetchone()
    if not v:
        raise HTTPException(404, "video not found")
    guard("запуск ролика")
    try:
        get_mixer(sid).play_video(os.path.join(VIDEO_DIR, v["filename"]), v["name"], v["gain_db"])
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/volume")
def stream_volume_set(sid: int, out: float = Form(None), ad: float = Form(None)):
    """Живые фейдеры мастер-выхода и ролика (UI слева от PGM). 1.0 = без
    изменений, 0 = тишина, >1 = усиление. Ролик уже стартует с авто-громкостью
    (см. analyze_loudness) — этот фейдер поверх неё, для ручной подстройки."""
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    try:
        if out is not None:
            vnum(out, 0, 4, "громкость выхода")
            m.set_out_volume(out)
        if ad is not None:
            vnum(ad, 0, 4, "громкость ролика")
            m.set_ad_volume(ad)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.get("/api/streams/{sid}/levels")
def stream_levels(sid: int):
    m = MIXERS.get(sid)
    if not m:
        return {"out_rms": -100.0, "ad_rms": -100.0, "mic_rms": -100.0}
    return m.get_levels()

@app.post("/api/streams/{sid}/mic")
def stream_mic_start(sid: int, streamid: str = Form(...), gain: float = Form(None)):
    """Включить постоянный микрофон комментатора: движок читает
    srt://127.0.0.1:8890?streamid=read:<streamid> (публикуется через
    MediaMTX с клиента, см. настройки потока → SRT), подмешивает в эфир
    с приглушением (ducking) родной дорожки, не полным мьютом."""
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    if gain is not None:
        vnum(gain, 0, 4, "громкость микрофона")
    try:
        m.start_mic(streamid, gain)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/mic/stop")
def stream_mic_stop(sid: int):
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    try:
        m.stop_mic()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/micvol")
def stream_mic_volume(sid: int, v: float = Form(...)):
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    vnum(v, 0, 4, "громкость микрофона")
    try:
        m.set_mic_volume(v)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/obs")
def stream_obs_start(sid: int, streamid: str = Form(...),
                      audio_mode: str = Form("mute"), fade: float = Form(None)):
    """Плавное наложение полноэкранного видео+аудио с OBS (постоянный
    SRT-вход через MediaMTX, тот же принцип что и микрофон, но видео тоже).
    audio_mode: 'mute' — глушить звук эфира (как ролик), 'keep' — оставить
    звук эфира, микшировать со звуком OBS-канала."""
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    if audio_mode not in ("mute", "keep"):
        raise HTTPException(400, "audio_mode должен быть mute или keep")
    if fade is not None:
        vnum(fade, 0.05, 5, "длительность fade")
    try:
        m.start_obs(streamid, audio_mode, fade)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/obs/live")
def stream_obs_live(sid: int):
    """Второй шаг: реально вывести уже подключённый OBS-канал в эфир
    (fade-in). Без предварительного /obs (подключения) вернёт 409."""
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    try:
        m.go_live_obs()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/obs/stop")
def stream_obs_stop(sid: int):
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    try:
        m.stop_obs()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/obsvol")
def stream_obs_volume(sid: int, v: float = Form(...)):
    m = MIXERS.get(sid)
    if not m:
        raise HTTPException(404, "stream not found or not running")
    vnum(v, 0, 4, "громкость OBS-канала")
    try:
        m.set_obs_volume(v)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}

@app.post("/api/streams/{sid}/stopvideo")
def stream_stopvideo(sid: int):
    m = MIXERS.get(sid)
    if m:
        m.stop_video()
    return {"ok": True}

# ---- логи
@app.get("/api/logs")
def logs(stream_id: int = 0, limit: int = 200):
    with LOG.lock:
        proc = list(LOG.proc.get(stream_id, []))[-limit:] if stream_id else []
        events = list(LOG.events)[-limit:]
    return {"process": proc, "events": events}

@app.delete("/api/logs")
def logs_clear(stream_id: int = 0):
    """Очистить журнал. stream_id=0 — события агента и вывод ВСЕХ потоков,
    иначе только вывод указанного потока.

    Зачем: буферы кольцевые (события — 800 строк, вывод потока — 500), и после
    долгой отладки полезное тонет в старом шуме. Живого эфира это не касается —
    буферы лежат в памяти агента и на конвейер никак не влияют."""
    with LOG.lock:
        if stream_id:
            LOG.proc.pop(stream_id, None)
        else:
            LOG.proc.clear()
            LOG.events.clear()
    LOG.event("журнал очищен" + (f" (поток {stream_id})" if stream_id else " (полностью)"))
    return {"ok": True}

# ---- расписания
@app.get("/api/schedules")
def schedules_list():
    with closing(db()) as c:
        return [dict(r) for r in c.execute(
            """SELECT s.*, b.name AS banner_name, v.name AS video_name,
                      tp.name AS template_name, st.name AS stream_name
               FROM schedules s
               LEFT JOIN banners b ON b.id=s.banner_id
               LEFT JOIN videos v ON v.id=s.video_id
               LEFT JOIN templates tp ON tp.id=s.template_id
               LEFT JOIN streams st ON st.id=s.stream_id ORDER BY s.id""")]

@app.post("/api/schedules")
def schedule_add(name: str = Form(""), stream_id: int = Form(0), banner_id: int = Form(0),
                 video_id: int = Form(0), template_id: int = Form(0), action: str = Form("show"),
                 time_start: str = Form(...),
                 every_minutes: int = Form(0), duration: int = Form(30),
                 fade: float = Form(1.0), date_from: str = Form(""), date_to: str = Form(""),
                 enabled: int = Form(1)):
    vtime(time_start)
    if action not in ("show", "stream_start", "stream_stop"):
        raise HTTPException(400, "action: show|stream_start|stream_stop")
    vnum(every_minutes, 0, 1440, "каждые, мин")
    vnum(duration, 1, 3600, "длительность")
    fade = vnum(fade, 0, 10, "fade")
    date_from, date_to = vdate(date_from), vdate(date_to)
    if date_from and date_to and date_from > date_to:
        raise HTTPException(400, "период: дата «с» позже даты «по»")
    with closing(db()) as c, c:
        cur = c.execute("""INSERT INTO schedules(name,stream_id,banner_id,video_id,template_id,
                           action,time_start,every_minutes,duration,fade,date_from,date_to,enabled)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (name, stream_id or None, banner_id or None, video_id or None,
                         template_id or None, action,
                         time_start, every_minutes, duration, fade,
                         date_from or None, date_to or None, enabled))
    return {"id": cur.lastrowid}

@app.post("/api/schedules/{sid}/toggle")
def schedule_toggle(sid: int):
    with closing(db()) as c, c:
        c.execute("UPDATE schedules SET enabled=1-enabled WHERE id=?", (sid,))
    return {"ok": True}

@app.delete("/api/schedules/{sid}")
def schedule_del(sid: int):
    with closing(db()) as c, c:
        c.execute("DELETE FROM schedules WHERE id=?", (sid,))
    return {"ok": True}

# ---- операторские программы
def vsteps(raw):
    try:
        steps = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        raise HTTPException(400, "steps_json: нужен JSON-массив")
    if not isinstance(steps, list) or len(steps) > 200:
        raise HTTPException(400, "steps_json: нужен массив до 200 шагов")
    for i, st in enumerate(steps, 1):
        if not isinstance(st, dict):
            raise HTTPException(400, f"шаг {i}: нужен объект")
        st["t"] = int(vnum(st.get("t", 0), -86400, 86400, f"шаг {i}: T"))
        st["kind"] = str(st.get("kind", "NOTE")).upper()
        if st["kind"] not in ("RESTART", "START_VIDEO", "START_BANNER", "START_HTML", "NOTE"):
            raise HTTPException(400, f"шаг {i}: тип RESTART|START_VIDEO|START_BANNER|START_HTML|NOTE")
        st["title"] = str(st.get("title", ""))[:160]
        st["note"] = str(st.get("note", ""))[:500]
        st["window"] = str(st.get("window", ""))[:80]
    steps.sort(key=lambda x: x["t"])
    return json.dumps(steps, ensure_ascii=False)

@app.get("/api/programs")
def programs_list():
    with closing(db()) as c:
        rows = [dict(r) for r in c.execute(
            """SELECT p.*, s.name AS stream_name FROM operator_programs p
               JOIN streams s ON s.id=p.stream_id ORDER BY p.id DESC""")]
    for r in rows:
        try:
            r["steps"] = json.loads(r.pop("steps_json") or "[]")
        except json.JSONDecodeError:
            r["steps"] = []
    return rows

@app.post("/api/programs")
def program_add(name: str = Form(...), stream_id: int = Form(...),
                start_at: str = Form(""), steps_json: str = Form("[]")):
    steps_json = vsteps(steps_json)
    with closing(db()) as c, c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (stream_id,)).fetchone():
            raise HTTPException(404, "stream not found")
        cur = c.execute("""INSERT INTO operator_programs(name,stream_id,start_at,steps_json)
                           VALUES(?,?,?,?)""",
                        (name.strip() or "Программа", stream_id, start_at.strip(), steps_json))
    return {"id": cur.lastrowid}

@app.put("/api/programs/{pid}")
def program_update(pid: int, name: str = Form(...), stream_id: int = Form(...),
                   start_at: str = Form(""), steps_json: str = Form("[]")):
    steps_json = vsteps(steps_json)
    with closing(db()) as c, c:
        if not c.execute("SELECT 1 FROM streams WHERE id=?", (stream_id,)).fetchone():
            raise HTTPException(404, "stream not found")
        cur = c.execute("""UPDATE operator_programs
                           SET name=?, stream_id=?, start_at=?, steps_json=?
                           WHERE id=?""",
                        (name.strip() or "Программа", stream_id, start_at.strip(), steps_json, pid))
        if cur.rowcount == 0:
            raise HTTPException(404, "program not found")
    return {"ok": True}

@app.delete("/api/programs/{pid}")
def program_del(pid: int):
    with closing(db()) as c, c:
        c.execute("DELETE FROM operator_programs WHERE id=?", (pid,))
    return {"ok": True}

# ---- настройки
@app.get("/api/settings")
def settings_get():
    with closing(db()) as c:
        return {r["key"]: r["value"] for r in c.execute("SELECT * FROM settings")}

@app.post("/api/settings")
async def settings_set(payload: dict):
    with closing(db()) as c, c:
        for k, v in payload.items():
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) "
                      "ON CONFLICT(key) DO UPDATE SET value=?", (k, str(v), str(v)))
    return {"ok": True}
