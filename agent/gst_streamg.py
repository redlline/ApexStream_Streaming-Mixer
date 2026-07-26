#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ПотокG — GStreamer-канал ad-streamer: провайдер → буфер задержки → publish
в Flussonic, с гарантией НЕразрывной публикации при срывах провайдера.

Архитектура — ДВА независимых пайплайна, связанных intervideo/interaudio:

  P1 (вход; может падать и перезапускаться сколько угодно):
     uridecodebin(HLS провайдера) → нормализация (масштаб/fps/формат)
       → очередь-задержка (buffer_sec) → intervideosink (sync, ts-offset=буфер)
     аудио аналогично → interaudiosink

  P2 (выход; стартует один раз и НЕ останавливается до команды stop):
     intervideosrc  (при голодании сам повторяет последний кадр)
       → compositor (слой заглушки-slate; дальше — слои баннера/ролика, Фаза 2)
       → nvh264enc → flvmux → rtmpsink (RTMP publish во Flussonic)
     interaudiosrc (при голодании отдаёт тишину) → AAC → тот же flvmux

Почему так:
 - Публикация не рвётся НИКОГДА: intervideosrc/interaudiosrc продолжают выдавать
   кадры/тишину, даже когда вход мёртв. Зритель не получает ошибку соединения.
 - Задержка (резервуар) живёт в P1: очередь перед intervideosink, который держит
   каждый кадр buffer_sec через ts-offset. На срыве очередь сливается (выход идёт
   реальным контентом), на восстановлении HLS докачивает пропущенные сегменты
   быстрее реального времени и очередь наполняется обратно.
 - Заглушка — слой compositor'а: alpha 0→1 когда вход мёртв дольше порога,
   1→0 при возврате. Никаких склеек таймстампов и рестартов.
 - P1 при ошибке/EOS пересоздаётся супервизором (переподключение к провайдеру),
   P2 этого даже не замечает.

Standalone-запуск (тест без агента/UI):
  python3 gst_streamg.py --input <hls_url> \
     --output rtmp://127.0.0.1:1935/live/stream_ad_3 \
     --buffer 12 --slate /path/slate.png --seconds 90

Режим relay (--mode relay): P2 вместо RTMP-паблиша отдаёт mpegts/UDP на
127.0.0.1 — тем же транспортом, каким в этом проекте уже ходят внутренние
шины баннера/лого (mpegts повторяет PAT/PMT, приёмник может подключиться в
любой момент). Так буфер+never-die+заглушка StreamG можно поставить ПЕРЕД
боевым ffmpeg-микшером: микшеру достаточно поменять input_url на
udp://127.0.0.1:<port> — сам микшер (фильтры/ZMQ/оверлеи) не трогается.
  python3 gst_streamg.py --input <hls_url> --mode relay --relay-port 5900 \
     --buffer 12 --slate /path/slate.png
"""
import gi, argparse, threading, time, os, sys, socket, json, subprocess
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

# Агент собирает лог процесса ТОЛЬКО из stderr (тот же паттерн, что и у ffmpeg
# в agent/main.py _spawn: stdout спавнится в DEVNULL). Все print() в этом файле
# используют flush=True, поэтому просто перенаправляем stdout на stderr здесь —
# без переписывания каждого print(..., file=sys.stderr) по отдельности.
sys.stdout = sys.stderr

VCAPS = "video/x-raw,format=I420,width={w},height={h},framerate={fps}/1"
ACAPS = "audio/x-raw,format=S16LE,rate=48000,channels=2"


class StreamG:
    def __init__(self, input_url, output_rtmp, buffer_sec=12.0, slate=None,
                 w=1920, h=1080, fps=25, vbitrate=6000, abitrate=128,
                 slate_after=None, channel="sg0", mode="publish", relay_port=None,
                 banner_w=1920, banner_h=150, control_port=None,
                 venv_python=None, capture_html_script=None, html_port_base=15900,
                 mediamtx_rtmp_ports=None, mediamtx_path_override=None,
                 audio_tracks=None, mediamtx_srt=None):
        self.input_url = input_url
        self.output_rtmp = output_rtmp   # может быть None/"" — поток тогда идёт ТОЛЬКО в MediaMTX
        self.mode = mode                      # "publish" (RTMP → Flussonic) | "relay" (mpegts/UDP → ffmpeg-микшер)
        self.relay_port = relay_port
        # постоянные доп. RTMP-выходы в локальные инстансы MediaMTX (компаньон-
        # серверы, НЕ Flussonic) — тот же уже закодированный поток из tee, по
        # одной ветке на порт. Два инстанса нужны, потому что hlsVariant в
        # MediaMTX — настройка ВСЕГО процесса, а не пути: low-latency HLS даёт
        # минимальную задержку, но требует Secure-cookie (только по HTTPS,
        # а у нас HTTP), классический HLS работает без ограничений — держим
        # оба варианта, каждый инстанс отвечает за свой порт. Пустой список/
        # None = выключено (обратная совместимость).
        self.mediamtx_rtmp_ports = mediamtx_rtmp_ports or []
        self.mediamtx_path_override = mediamtx_path_override
        # список индексов аудиодорожек входа, которые тащим (0 = первая).
        # Многопрограммные источники (напр. SRT с несколькими комментаторскими
        # дорожками) отдают несколько audio-пэдов из uridecodebin в порядке
        # появления. Одна дорожка ([0] по умолчанию) = прежнее одноязычное
        # поведение (RTMP во Flussonic/MediaMTX, FLV несёт 1 аудио). Несколько
        # дорожек = мультиязык: N×AAC → mpegtsmux → srtsink в MediaMTX (SRT/TS
        # несёт сколько угодно аудио-PID, Flussonic тянет их все), при этом FLV-
        # выходы (Flussonic RTMP, классический MediaMTX для браузерного плеера)
        # продолжают нести только первую (позицию 0). Порядок сохраняем и
        # дедуплицируем; позиция 0 всегда есть.
        tracks = audio_tracks if audio_tracks else [0]
        seen = set(); self.audio_tracks = []
        for t in tracks:
            t = int(t)
            if t not in seen:
                seen.add(t); self.audio_tracks.append(t)
        if not self.audio_tracks:
            self.audio_tracks = [0]
        # SRT-адрес инстанса MediaMTX (host:port) для multi-audio publish; задаёт
        # агент, когда дорожек больше одной. None/одна дорожка → SRT-ветка не строится.
        self.mediamtx_srt = mediamtx_srt
        self.multi_audio = len(self.audio_tracks) > 1
        # языковые теги входных audio-дорожек (index = позиция среди audio-
        # потоков источника, значение = ISO-639 код типа "rus"/"kaz"/"und") —
        # заполняется _probe_languages() в фоне; без него Flussonic/плееры
        # видят все дорожки мультиязыка одинаково подписанными "a1" с разными
        # PID и непонятно, где какой язык (см. _srt_out_build).
        self.track_languages = []
        self.buffer_sec = float(buffer_sec)
        self.slate = slate                    # путь к PNG-заглушке (или None)
        self.w, self.h, self.fps = w, h, fps
        # зона баннера не может быть больше самого кадра — иначе на потоках с
        # нестандартным разрешением (напр. 896x504) зона вылезает за кадр и
        # картинка растягивается/обрезается (см. тот же клемп в main.py).
        self.banner_w = min(banner_w, w)
        self.banner_h = min(banner_h, h)
        self.control_port = control_port      # TCP-порт управления оверлеями (движок); None = выкл
        self.vbitrate, self.abitrate = vbitrate, abitrate
        # заглушку поднимаем СРАЗУ, как только реальная картинка на выходе
        # почернела: буфер-резервуар (buffer_sec) и так сливается САМ ПО СЕБЕ
        # прежде, чем выход начнёт голодать — добавлять его вторично к порогу
        # не нужно (раньше здесь было ДВОЙНОЕ прибавление buffer_sec — экран
        # чернел на ~10с раньше, чем появлялась заглушка). slate_after — это
        # только небольшой запас ПОВЕРХ buffer_sec, чтобы не дёргать заглушку
        # на микро-заиканиях провайдера короче интервала пробы watchdog'а.
        self.slate_after = slate_after if slate_after else 1.5
        self.ch = channel                     # имя intervideo-канала
        self.p_in = None                      # P1
        self.p_out = None                     # P2
        self.srt_built = False                # мультиязычная SRT-ветка поднята (динамически)
        self.loop = GLib.MainLoop()
        self.lock = threading.Lock()
        self.last_feed_ts = 0.0               # когда P1 последний раз дал кадр
        self.in_started_at = 0.0              # когда P1 последний раз пересобран (см. _watchdog)
        self.in_restarts = 0
        self._restart_pending = False         # см. _restart_input/_swap_input/test_cut_input
        self.slate_on = False
        self.banner_busy = False              # идёт показ баннера (защита от наложения)
        self.logo_bin = None
        self.logo_pad = None
        self.video_busy = False               # идёт вставка ролика
        self.video_refs = None
        self.stopping = False
        self.last_error = None
        # ---- звук: громкость выхода/ролика + VU-метры (UI слева от PGM)
        self.last_levels = {"out_rms": -100.0, "ad_rms": -100.0, "mic_rms": -100.0, "obs_rms": -100.0}
        self.ad_volume_default = 1.0   # применяется каждому новому ролику при старте (см. play_video)
        self.ad_volume_el = None       # текущий volume-элемент играющего ролика (для "advol")
        # ---- микрофон комментатора (постоянный SRT-вход от OBS через
        # локальный MediaMTX, см. start_mic/_mic_build) — в отличие от ролика,
        # это ПЕРЕКЛЮЧАТЕЛЬ (вкл/выкл), а не разовый показ с концом по EOS.
        self.mic_bin = None
        self.mic_refs = None           # (tee, links, mains) — как video_refs у ролика
        self.mic_volume_el = None
        self.mic_streamid = None
        self.mic_volume_default = 1.0
        self.mic_duck = 0.2            # громкость эфира, пока микрофон включён (не мьют — ducking)
        # ---- наложение полного видео+аудио с OBS (постоянный SRT-вход,
        # тот же MediaMTX, см. start_obs/_obs_build) — как ролик (полноэкранный
        # zorder-слой), но ПЕРЕКЛЮЧАТЕЛЬ, а не разовый показ, и с плавным
        # fade вместо жёсткого реза; режим звука настраивается (mute/keep).
        self.obs_bin = None
        self.obs_refs = None           # (vpad, links, mains) — заполняется только при go_live_obs()
        self.obs_volume_el = None
        self.obs_streamid = None
        self.obs_audio_mode = "mute"   # "mute" — глушить эфир (как ролик), "keep" — оставить звук эфира
        self.obs_fade = 0.6            # сек, симметрично на вход/выход
        self.obs_video_tee = None      # tee ПОСЛЕ ghost-пэда бина (уровень p_out) — постоянная ветка в fakesink (просмотр) + временная в эфир
        self.obs_audio_tee = None
        self.obs_is_live = False       # True только после go_live_obs(); obs_bin!=None означает лишь "подключён"
        self.obs_stopping = False      # разборка уже идёт (в фоновом потоке) — защита от повторных/гоночных "Стоп"
        # ---- HTML-слой (Chromium/CDP → сырой RGBA по TCP, см. capture_html_raw.py)
        self.venv_python = venv_python or "/opt/ad-streamer/venv/bin/python3"
        self.capture_html_script = capture_html_script or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "capture_html_raw.py")
        self.html_port_base = html_port_base  # порт занимается на время показа, освобождается после
        self.html_port_seq = 0                # см. show_html — порт теперь меняется на КАЖДЫЙ показ
        self.html_busy = False
        self.html_proc = None
        self.html_refs = None                 # (bin, pad, tcp_port)

    def mediamtx_path(self):
        """Имя пути в MediaMTX. Явный --mediamtx-path (агент всегда передаёт его
        сам) имеет приоритет; иначе — последний сегмент output_rtmp (например
        rtmp://127.0.0.1:1935/live/stream_ad_1 -> "stream_ad_1"), если он задан;
        иначе — имя канала (когда потока вообще нет во Flussonic, только
        MediaMTX)."""
        if self.mediamtx_path_override:
            return self.mediamtx_path_override
        if self.output_rtmp:
            return self.output_rtmp.rstrip("/").rsplit("/", 1)[-1]
        return self.ch

    # ---------------- P2: выходной пайплайн (вечный)
    def _out_desc(self):
        vcaps = VCAPS.format(w=self.w, h=self.h, fps=self.fps)
        delay_ns = int(self.buffer_sec * Gst.SECOND)
        margin_ns = int(2 * Gst.SECOND)   # запас сверх buffer_sec на джиттер очередей
        slate_branch = ""
        if self.slate and os.path.exists(self.slate):
            # слой заглушки: невидим (alpha=0), включается свойством пэда.
            # decodebin (а не pngdec) — чтобы годился ЛЮБОЙ формат баннера из
            # библиотеки (PNG/JPG/GIF); imagefreeze берёт первый кадр и держит
            # его как непрерывный видеопоток. У анимированного GIF застынет
            # первый кадр — для статичной заглушки это и нужно.
            slate_branch = (
                f' filesrc location="{self.slate}" ! decodebin ! imagefreeze ! '
                f'videoconvert ! videoscale ! {vcaps} ! comp.sink_1 ')
        if self.mode == "relay":
            # mpegts/UDP на localhost — тот же транспорт, что уже используют
            # внутренние шины баннера/лого в ffmpeg-микшере (PAT/PMT повторяются,
            # приёмник подключается в любой момент, реконнекты не критичны).
            muxsink = (f'mpegtsmux name=mux alignment=7 ! '
                       f'udpsink host=127.0.0.1 port={self.relay_port} sync=false')
        elif self.output_rtmp:
            muxsink = (f'flvmux name=mux streamable=true ! '
                       f'rtmpsink sync=false location="{self.output_rtmp} live=1"')
        else:
            # без выхода во Flussonic (поток только через MediaMTX): mux всё
            # равно нужен как точка входа для аудио-ветки (она ссылается на
            # него как "mux." дальше по строке) — просто гасим результат.
            muxsink = 'flvmux name=mux streamable=true ! fakesink sync=false'
        # свойства пэда заглушки задаём ТОЛЬКО если заглушка есть — иначе
        # sink_1 создастся пустым (без источника) и компоновщик будет вечно
        # ждать на нём данные. Баннер/лого-пэды добавляются на лету (request).
        comp_props = " sink_1::zorder=5 sink_1::alpha=0.0" if slate_branch else ""
        mediamtx_branch = ""
        if self.mediamtx_rtmp_ports and self.mode == "publish":
            # по постоянной ветке push'а того же h264/aac (из tee, повторного
            # кодирования нет) на КАЖДЫЙ инстанс MediaMTX — обычно два: один
            # под low-latency HLS/RTSP/SRT/WebRTC, второй чисто под классический
            # HLS (см. mediamtx_rtmp_ports). vtee./atee. — ссылка на уже
            # объявленные элементы tee ниже по строке (валидный gst-launch
            # синтаксис в рамках одного parse_launch).
            path = self.mediamtx_path()
            parts = []
            for i, port in enumerate(self.mediamtx_rtmp_ports):
                muxname = f"mtxmux{i}"
                parts.append(
                    f' vtee. ! queue ! flvmux name={muxname} streamable=true ! '
                    f'rtmpsink sync=false location="rtmp://127.0.0.1:{port}/{path} live=1" '
                    f' atee. ! queue ! {muxname}. ')
            mediamtx_branch = "".join(parts)
        # ПРИМЕЧАНИЕ: мультиязычная SRT-ветка (mpegtsmux+srtsink+доп.дорожки) в
        # СТАТИЧЕСКИЙ пайплайн НЕ включается — она строится динамически уже на
        # работающем P2 (см. _srt_out_build), потому что srtsink-caller и mux при
        # старте негоциируют кэпсы через общий tee не мгновенно и на префролле
        # роняли живой intervideosrc (not-negotiated, гонка). Динамическая
        # пристройка к уже прокачивающемуся конвейеру гонку убирает — тот же
        # приём, что для баннера/ролика (_banner_build/_video_build).
        return (
            f'intervideosrc channel={self.ch}v ! {vcaps} ! '
            f'queue max-size-time={delay_ns + margin_ns} max-size-bytes=0 max-size-buffers=0 ! '
            # compositor отдаёт AYUV (а НЕ I420!) — иначе он приводит ВСЕ входы к
            # I420 и теряет альфа-канал оверлеев (баннер/лого/HTML): прозрачные
            # зоны схлопываются в непрозрачный ЧЁРНЫЙ (проверено зелёным фоном).
            # На AYUV-холсте компоновщик блендит RGBA-оверлеи по пиксельной
            # альфе поверх эфира, а в I420 для nvenc конвертим уже ПОСЛЕ него.
            f'compositor name=comp{comp_props} ! '
            f'video/x-raw,format=AYUV,width={self.w},height={self.h},framerate={self.fps}/1 ! '
            f'videoconvert ! {vcaps} ! '
            f'nvh264enc name=enc bitrate={self.vbitrate} rc-mode=cbr '
            f'gop-size={self.fps * 2} bframes=0 zerolatency=true ! '
            # config-interval=-1: SPS/PPS повторяются перед КАЖДЫМ IDR-кадром.
            # Без этого поздний подписчик (ffmpeg-микшер после рестарта,
            # ffprobe и т.п.) ловит "non-existing PPS" и не может декодировать,
            # пока случайно не попадёт на первый keyframe жизни энкодера.
            # tee name=vtee: постоянная точка ответвления для доп. RTMP-веток
            # (см. mediamtx_branch) — без активного request-пэда это чистый
            # passthrough, накладных расходов нет.
            # allow-not-linked=true — ОБЯЗАТЕЛЬНО: между request_pad_simple() и
            # .link() при динамической пристройке ветки (SRT-мультиязык/
            # mediamtx) есть окно, когда новый src-пад уже существует, но ещё
            # не слинкован. По умолчанию (allow-not-linked=false) буфер,
            # пришедший в это окно, роняет tee фатальной ошибкой на весь P2 —
            # именно так гасился весь эфир при подъёме SRT-ветки (см.
            # _srt_out_build). С allow-not-linked=true tee просто тихо
            # игнорирует ещё-не-слинкованный пад вместо падения.
            f'h264parse config-interval=-1 ! tee name=vtee allow-not-linked=true ! queue ! {muxsink} '
            + slate_branch +
            # audiomixer amix: эфирный звук на sink_0, звук ролика подмешивается
            # на request-пэд во время вставки (эфирный при этом глушим volume=0).
            # volume name=outvol: постоянный мастер-фейдер выхода (см. UI —
            # панель громкости слева от PGM-плеера), меняется вживую командой
            # "outvol <0..2>". level name=outlevel: RMS/пик для VU-метра,
            # шлёт message на bus каждые interval нс — читаем в _on_out_msg.
            f' interaudiosrc channel={self.ch}a ! {ACAPS} ! '
            f'queue max-size-time={delay_ns + margin_ns} max-size-bytes=0 max-size-buffers=0 ! '
            f'audioconvert ! audiomixer name=amix ! '
            f'volume name=outvol volume=1.0 ! '
            f'level name=outlevel interval=100000000 post-messages=true ! '
            f'audioconvert ! avenc_aac bitrate={self.abitrate * 1000} ! aacparse ! '
            f'tee name=atee allow-not-linked=true ! queue ! mux.'
            + mediamtx_branch
        )

    # ---------------- P1: входной пайплайн (смертный, пересоздаваемый)
    def _in_desc(self):
        vcaps = VCAPS.format(w=self.w, h=self.h, fps=self.fps)
        delay_ns = int(self.buffer_sec * Gst.SECOND)
        # запас +3с на входных очередях: держит именно buffer_sec задержки,
        # не даёт им превращаться в дополнительный неучтённый резервуар
        qargs = (f'max-size-time={delay_ns + 3 * Gst.SECOND} '
                 f'max-size-bytes=0 max-size-buffers=0')
        # identity name=feed — probe на нём меряет «вход живой» (last_feed_ts).
        # intervideosink sync=true ts-offset=delay: каждый кадр держится buffer_sec
        # до выдачи в канал → очередь стоит наполненной = резервуар задержки.
        # ВАЖНО: без "d." — video/audio-пэды uridecodebin появляются асинхронно
        # (сигнал pad-added), а не в момент parse_launch. Раньше "d. !" линковал
        # ПЕРВЫЙ подходящий пэд, который встретится — для многодорожечных
        # источников (SRT с несколькими audio-PID) это всегда была дорожка 0,
        # выбрать другую было нельзя. Теперь vq/aq — самостоятельные, ещё не
        # подключённые к d элементы; линковку делает _build_input() в
        # обработчике pad-added, с учётом self.audio_track.
        # по одной аудио-цепочке на позицию в self.audio_tracks; позиция 0 идёт
        # в канал {ch}a (как раньше — выходной путь его и слушает), позиция i>=1
        # в {ch}a{i}. Линковку конкретного входного audio-пэда к нужной очереди
        # aq{i} делает _build_input по индексу дорожки.
        achains = ""
        for i in range(len(self.audio_tracks)):
            chan = f'{self.ch}a' if i == 0 else f'{self.ch}a{i}'
            achains += (
                f'queue name=aq{i} {qargs} ! audioconvert ! audioresample ! {ACAPS} ! '
                f'queue name=adelay{i} {qargs} ! '
                f'interaudiosink channel={chan} sync=true ts-offset={delay_ns} ')
        return (
            f'uridecodebin uri="{self.input_url}" name=d use-buffering=false '
            # deinterlace mode=auto: пропускает прогрессивное видео насквозь без
            # изменений, а чересстрочное (coded-picture-structure=field — было
            # найдено вживую на источнике Cartoon Network, 1920x1080) конвертирует
            # в прогрессивное. Без него интерлейс-контент ронял негоциацию
            # кэпсов дальше по цепочке (наш VCAPS не объявляет interlace-mode,
            # молча рассчитан только на прогрессив) — эфир падал сразу на
            # старте с "not-negotiated" на intervideosrc в P2.
            f'queue name=vq {qargs} ! videoconvert ! deinterlace mode=auto ! '
            f'videoscale ! videorate ! {vcaps} ! '
            f'identity name=feed ! queue name=vdelay {qargs} ! '
            f'intervideosink channel={self.ch}v sync=true ts-offset={delay_ns} '
            + achains
        )

    # ---------------- события
    def _on_out_msg(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            self.last_error = f"OUT: {err.message}"
            print(f"[streamG] ВЫХОД УМЕР: {err.message}: {dbg}", flush=True)
            self.loop.quit()      # смерть выхода фатальна — пусть агент перезапустит
        elif msg.type == Gst.MessageType.WARNING:
            wrn, dbg = msg.parse_warning()
            src_name = msg.src.get_name() if msg.src else "?"
            print(f"[streamG] WARN [{src_name}]: {wrn.message}: {dbg}", flush=True)
        elif msg.type == Gst.MessageType.ELEMENT:
            st = msg.get_structure()
            if st and st.get_name() == "level":
                src_name = msg.src.get_name() if msg.src else ""
                try:
                    rms = st.get_value("rms")   # GValueArray, один канал или больше
                    avg = sum(rms) / len(rms) if rms else -100.0
                except Exception:
                    avg = -100.0
                key = "out_rms" if src_name == "outlevel" else \
                      "ad_rms" if src_name == "adlevel" else \
                      "mic_rms" if src_name == "miclevel" else \
                      "obs_rms" if src_name == "obslevel" else None
                if key:
                    self.last_levels[key] = avg
        return True

    def _on_in_msg(self, bus, msg):
        if msg.type in (Gst.MessageType.ERROR, Gst.MessageType.EOS):
            what = "EOS" if msg.type == Gst.MessageType.EOS else \
                   msg.parse_error()[0].message
            print(f"[streamG] вход упал ({what}) — переподключение", flush=True)
            # ВАЖНО: НЕ GLib.idle_add — этот обработчик УЖЕ выполняется на
            # GLib-потоке (диспетчер шины крутится в главном контексте), а
            # _restart_input() дальше блокирующе вызывает set_state(NULL).
            # set_state(NULL) иногда сам ждёт, пока GLib-луп разберёт
            # накопившиеся сообщения от того же пайплайна — если вызвать его
            # ПРЯМО НА этом лупе, получается самозастревание (луп ждёт сам
            # себя) — весь процесс встаёт намертво (найдено вживую: control-
            # сервер и HTML зависали следом, потому что _restart_input держит
            # self.lock всё время, пока сам заблокирован). Настоящий отдельный
            # поток снимает эту гонку полностью.
            threading.Thread(target=self._restart_input, daemon=True).start()
        return True

    def _feed_probe(self, pad, info):
        self.last_feed_ts = time.time()
        return Gst.PadProbeReturn.OK

    def _on_decodebin_pad_added(self, _d, pad, state):
        caps = pad.get_current_caps() or pad.query_caps(None)
        if not caps or caps.get_size() == 0:
            return
        kind = caps.get_structure(0).get_name()
        if kind.startswith("video/") and not state["video_linked"]:
            sinkpad = state["vq"].get_static_pad("sink")
            if not sinkpad.is_linked():
                # если decodebin подключил nvh264dec (NVDEC), кадры приходят в
                # CUDA-памяти (caps несут memory-feature "memory:CUDAMemory") —
                # deinterlace/videoscale дальше по цепочке её не понимают.
                # Явно выгружаем в системную память ОТДЕЛЬНЫМ элементом сразу
                # здесь — без этого моста decodebin не может согласовать
                # CUDA-выход с обычным downstream и на практике вообще не
                # выбирает NVDEC (декодер молча остаётся на CPU, проверено
                # вживую: utilization.decoder=1% на боевом потоке при том, что
                # в изолированном gst-launch тесте с тем же URL NVDEC подключался
                # без проблем — единственная разница была именно в отсутствии
                # моста к системной памяти). Софт-декод (без CUDA-памяти в caps)
                # линкуется как раньше, напрямую — нулевой риск регресса.
                if "memory:CUDAMemory" in caps.to_string():
                    dl = Gst.ElementFactory.make("cudadownload")
                    self.p_in.add(dl)
                    dl.sync_state_with_parent()
                    pad.link(dl.get_static_pad("sink"))
                    dl.get_static_pad("src").link(sinkpad)
                    print("[streamG] вход декодируется на GPU (NVDEC)", flush=True)
                else:
                    pad.link(sinkpad)
                state["video_linked"] = True
        elif kind.startswith("audio/"):
            idx = state["audio_seen"]          # порядковый номер входной дорожки
            state["audio_seen"] += 1
            # если эту дорожку тащим — линкуем к очереди её ПОЗИЦИИ в выходе
            if idx in state["aq_by_track"]:
                sinkpad = state["aq_by_track"][idx].get_static_pad("sink")
                if not sinkpad.is_linked():
                    pad.link(sinkpad)
            # незапрошенные audio-дорожки намеренно НЕ линкуются — decodebin
            # держит их пэд без потребителя, это штатно и не мешает пайплайну

    def _on_decodebin_no_more_pads(self, _d, state):
        """decodebin сигналит, что ВСЕ пэды источника уже перечислены (больше
        не появится). Если среди запрошенных audio_tracks есть индекс, которого
        в реальном источнике не оказалось (напр. запросили дорожки 0,1,2, а
        источник только двухязычный — 0 и 1), очередь aq{i} для неё так и
        останется НАВСЕГДА без единого буфера. БЕЗ этого фикса весь P1 зависал
        в префролле НАВЕЧНО (basesink на такой ветке ждёт первый буфер, чтобы
        завершить переход в PLAYING, а он никогда не придёт) — эфир при этом
        полностью пропадал (чёрный экран/тишина), даже видео, хотя источник и
        остальные дорожки были живы. Подставляем в такую очередь тишину
        (audiotestsrc wave=silence) — тогда ветка прероллится, а пайплайн
        целиком нормально стартует; недостающий язык просто будет молчать."""
        p = self.p_in
        for trk, aq in state["aq_by_track"].items():
            sinkpad = aq.get_static_pad("sink")
            if sinkpad.is_linked():
                continue
            print(f"[streamG] аудиодорожка {trk} не найдена в источнике — "
                  f"тишина вместо неё (иначе завис бы весь эфир)", flush=True)
            silence = Gst.ElementFactory.make("audiotestsrc")
            silence.set_property("wave", "silence")
            silence.set_property("is-live", True)
            p.add(silence)
            silence.get_static_pad("src").link(sinkpad)
            silence.sync_state_with_parent()

    # ---------------- управление входом
    def _build_input(self):
        self.in_started_at = time.time()
        self.p_in = Gst.parse_launch(self._in_desc())
        bus = self.p_in.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_in_msg)
        feed = self.p_in.get_by_name("feed")
        feed.get_static_pad("src").add_probe(Gst.PadProbeType.BUFFER, self._feed_probe)
        d = self.p_in.get_by_name("d")
        # карта «индекс входной дорожки → очередь его позиции»: позиция i тащит
        # входную дорожку self.audio_tracks[i] в очередь aq{i}
        aq_by_track = {trk: self.p_in.get_by_name(f"aq{i}")
                       for i, trk in enumerate(self.audio_tracks)}
        state = {"vq": self.p_in.get_by_name("vq"), "aq_by_track": aq_by_track,
                 "video_linked": False, "audio_seen": 0}
        d.connect("pad-added", self._on_decodebin_pad_added, state)
        d.connect("no-more-pads", self._on_decodebin_no_more_pads, state)
        self.p_in.set_state(Gst.State.PLAYING)

    def _kill_pipeline_async(self, p):
        """set_state(NULL) — ВСЕГДА синхронный вызов у GStreamer (в отличие от
        PLAYING/PAUSED — у NULL нет ASYNC), и на живых сетевых источниках
        (souphttpsrc/tcpserversrc и т.п.) он может зависнуть НАВСЕГДА, если
        внутренний поток элемента застрял в блокирующем чтении/accept —
        подтверждено вживую дважды (HTML-слой и вход). Официального таймаута
        у API нет, поэтому просто не ждём: рвём старый pipeline в отдельном
        потоке fire-and-forget — если он там зависнет, это уже никого не
        блокирует, а мы сразу идём собирать новый."""
        if p:
            threading.Thread(target=p.set_state, args=(Gst.State.NULL,), daemon=True).start()

    def _restart_input(self):
        with self.lock:
            if self.stopping:
                return False
            # ВАЖНО (найдено вживую — двойной реконнект дёргал P1 туда-сюда,
            # видно было как «картинка мелькнула — снова чёрный экран»):
            # реальная ошибка от GStreamer (_on_in_msg) и форсированный
            # реконнект по тишине (_watchdog) могут сработать почти
            # одновременно на один и тот же сбой — БЕЗ этого флага второй
            # вызов рвал P1, который первый только что успел пересобрать
            # (или ещё пересобирает), и зритель видел не одно чистое
            # переподключение, а два подряд с чёрным экраном между ними.
            if self._restart_pending:
                return False
            self._restart_pending = True
            self._kill_pipeline_async(self.p_in)
            self.p_in = None
            self.in_restarts += 1
        # пауза перед реконнектом, чтобы не молотить мёртвого провайдера
        threading.Timer(2.0, lambda: GLib.idle_add(self._delayed_build)).start()
        return False

    def _delayed_build(self):
        with self.lock:
            if not self.stopping and self.p_in is None:
                try:
                    self._build_input()
                    print(f"[streamG] вход пересоздан (№{self.in_restarts})", flush=True)
                    self._restart_pending = False
                except Exception as e:
                    print(f"[streamG] пересоздание входа не удалось: {e}", flush=True)
                    threading.Timer(3.0, lambda: GLib.idle_add(self._delayed_build)).start()
            else:
                self._restart_pending = False
        return False

    def set_input(self, url):
        """Бесшовная смена входного URL БЕЗ рестарта процесса. Пересобираем
        ТОЛЬКО P1 (входную часть — она и так одноразовая, тем же механизмом идёт
        реконнект провайдера), а P2 (компоновщик + энкодер + publish во Flussonic)
        продолжает работать. Publish НЕ обрывается, а 12с-буфер в P2 продолжает
        отдавать накопленный запас, пока новый вход подключается — если он
        поднимется быстрее буфера, зритель не заметит смены вообще. Полный
        рестарт процесса (как было) ронял publish и сбрасывал буфер → ~30с фриза."""
        if not url:
            return "ERR empty url"
        with self.lock:
            self.input_url = url
        # НЕ GLib.idle_add — см. подробное объяснение в _on_in_msg: set_state(NULL)
        # внутри _swap_input блокирующий, гонять его на GLib-лупе рискованно
        # (был реальный deadlock через ту же схему в _restart_input).
        threading.Thread(target=self._swap_input, daemon=True).start()
        return "OK setinput"

    def test_cut_input(self, seconds):
        """Тестовый обрыв входа — искусственно рвём P1 на N секунд, чтобы
        проверить буфер/заглушку и автовосстановление БЕЗ реального отключения
        провайдера. Тот же механизм, что и настоящий сбой (P1 в NULL, потом
        обычная пересборка через _delayed_build), только пауза управляемая, а
        не фиксированные 2с — чтобы гарантированно успеть увидеть, как buffer_sec
        сливается и включается заглушка, прежде чем вход вернётся сам."""
        seconds = max(1.0, min(float(seconds), 300.0))
        with self.lock:
            if self.stopping:
                return "ERR stopping"
            self._restart_pending = True   # см. _restart_input — не даём watchdog'у влезть поверх
            self._kill_pipeline_async(self.p_in)
            self.p_in = None
            self.in_restarts += 1
        print(f"[streamG] тестовый обрыв входа на {seconds:.0f}с", flush=True)
        threading.Timer(seconds, lambda: GLib.idle_add(self._delayed_build)).start()
        return f"OK testcut {seconds:.0f}s"

    def _swap_input(self):
        with self.lock:
            if self.stopping:
                return False
            # тот же флаг, что и у _restart_input — осознанная смена входа
            # (пресет/редактирование) не должна пересекаться с форсированным
            # реконнектом watchdog'а по тишине (см. _restart_input) — иначе
            # именно это и давало «картинка мелькнула — опять чёрный экран»
            # при переключении пресетов.
            self._restart_pending = True
            self._kill_pipeline_async(self.p_in)
            self.p_in = None
        try:
            self._build_input()   # _in_desc() уже читает новый self.input_url
            print(f"[streamG] вход сменён на лету → {self.input_url}", flush=True)
            with self.lock:
                self._restart_pending = False
        except Exception as e:
            print(f"[streamG] смена входа не удалась: {e}", flush=True)
            threading.Timer(2.0, lambda: GLib.idle_add(self._delayed_build)).start()
        return False

    # ---------------- слой баннера (движок: оверлеи на лету, без рестарта)
    # Баннер — динамический request-пэд компоновщика: создаём источник и пэд на
    # время показа, плавно поднимаем/опускаем alpha, затем убираем пэд. Это и
    # есть сильная сторона GStreamer против ffmpeg — слой добавляется/снимается
    # без остановки основного конвейера.
    def show_banner(self, path, duration=30.0, fade=1.0, rect=None):
        """rect = (x, y, w, h) — готовый прямоугольник от агента, посчитанный с
        СОХРАНЕНИЕМ пропорций картинки внутри баннерной зоны (агент знает и зону
        потока, и натуральный размер файла). Если не передан — заполняем всю
        зону (старое поведение, растягивает — оставлено как запасной путь)."""
        if not os.path.exists(path):
            return "ERR banner file not found"
        if self.banner_busy:
            return "ERR banner already showing"
        self.banner_busy = True
        # конвертация GIF→mp4 (если нужна) — на ОТДЕЛЬНОМ потоке, не на GLib
        # main loop: main loop также крутит bus (EOS/ошибки/пробы), и
        # блокирующий subprocess.run внутри idle_add стопорит их обработку на
        # всё время конвертации (для 10-секундного GIF — секунды простоя).
        threading.Thread(target=self._banner_prepare,
                         args=(path, float(duration), float(fade), rect), daemon=True).start()
        return "OK banner"

    def _banner_prepare(self, path, duration, fade, rect):
        real_path = self._ensure_loopable(path)
        loop = real_path != path
        GLib.idle_add(self._banner_build, real_path, loop, duration, fade, rect)

    def _ensure_loopable(self, path):
        """Для .gif возвращает путь к предконвертированному h264/mp4 (кэш по
        mtime рядом с исходником). decodebin через GStreamer капризно крутит
        сырой GIF по кругу (autoplugging иногда даёт единственный кадр вместо
        видеопотока) — а зацикленный mp4 через EOS-seek работает надёжно
        (тот же механизм, что уже доказан на видео-ролике). Не-GIF отдаём как есть."""
        if not path.lower().endswith(".gif"):
            return path
        cache = f"{path}.loopcache.mp4"
        try:
            if os.path.exists(cache) and os.path.getmtime(cache) >= os.path.getmtime(path):
                return cache
            # -ignore_loop 1: декодировать GIF ОДИН раз (игнорируя его собственный
            # loop-counter в метаданных). Без этого флага GIF с loop=infinite
            # (типичный случай) заставляет ffmpeg кодировать вечно — конвертация
            # никогда не доходит до EOF, timeout обрезает файл на середине
            # (получаем битый mp4 без moov atom). Цикл на выходе делает уже
            # GStreamer через seek-на-EOS (_loop_seek_probe).
            r = subprocess.run(
                ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                 "-ignore_loop", "1", "-i", path,
                 "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                 "-c:v", "libx264", "-pix_fmt", "yuv420p", "-movflags", "+faststart", cache],
                timeout=20)
            if r.returncode == 0 and os.path.exists(cache):
                return cache
        except Exception as e:
            print(f"[streamG] конвертация GIF→MP4 не удалась: {e}", flush=True)
        return path   # не получилось — покажем как есть (первый кадр через imagefreeze-путь)

    def _make_image_bin(self, path, loop):
        """bin картинки для слоя компоновщика. loop=True (баннер, .mp4 после
        _ensure_loopable) — цикл по кругу, loop=False (лого, произвольный
        PNG/JPG/GIF) — imagefreeze: первый кадр как непрерывный поток.

        ВАЖНО (loop=True): НЕ используем decodebin — для h264/mp4 он автоплагинит
        аппаратный nvh264dec/nvdec, отдающий кадры в video/x-raw(memory:CUDAMemory).
        compositor эту память не понимает и молча ничего не рендерит (буферы при
        этом реально текут — проверено пробником, визуально пусто). Наш mp4 мы
        сами создали (_ensure_loopable, libx264), формат известен точно — берём
        явную ПРОГРАММНУЮ цепочку qtdemux!h264parse!avdec_h264, без автоплага."""
        b = Gst.Bin.new(None)
        conv = Gst.ElementFactory.make("videoconvert")
        # КРИТИЧНО для прозрачности: форсируем АЛЬФА-формат на выходе слоя. Без
        # этого videoconvert при согласовании с compositor часто выбирает I420
        # (без альфа-канала) — и прозрачные пиксели PNG/лого схлопываются в
        # НЕПРОЗРАЧНЫЙ ЧЁРНЫЙ (проверено: под HTML-баром была сплошная чернота
        # вместо эфира). AYUV несёт альфу, compositor блендит её по пикселям.
        acaps = Gst.ElementFactory.make("capsfilter")
        acaps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=RGBA"))
        b.add(conv); b.add(acaps)
        conv.link(acaps)
        if loop:
            b._loop_path = path   # нужен _reload_loop_chain при следующем цикле
            b._conv = conv
            self._build_loop_chain(b, path, conv)
        else:
            filesrc = Gst.ElementFactory.make("filesrc"); filesrc.set_property("location", path)
            dec = Gst.ElementFactory.make("decodebin")
            freeze = Gst.ElementFactory.make("imagefreeze")
            b.add(filesrc); b.add(dec); b.add(freeze)
            filesrc.link(dec)
            dec.connect("pad-added", lambda _d, p:
                p.link(freeze.get_static_pad("sink"))
                if not freeze.get_static_pad("sink").is_linked() else None)
            freeze.link(conv)
        ghost = Gst.GhostPad.new("src", acaps.get_static_pad("src"))
        b.add_pad(ghost)
        if loop:
            ghost.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, self._loop_seek_probe, b)
        return b

    def _build_loop_chain(self, b, path, conv):
        """filesrc→qtdemux→h264parse→avdec_h264 → conv (уже в bin'е b).
        Вынесено отдельно, чтобы пересобирать ЭТУ цепочку целиком на каждом
        цикле баннера (см. _reload_loop_chain) — попытка просто делать
        seek-на-EOS обратно в 0 на живом декодере оказалась ненадёжной: после
        такого seek qtdemux шлёт новый STREAM_START/CAPS, но НЕ шлёт новый
        SEGMENT, downstream (compositor) продолжает мерить running time по
        СТАРОМУ сегменту — буферы приходят «из прошлого» и GstAggregator их
        молча дропает (слой пропадал визуально сразу после первого цикла GIF,
        хотя таймер показа/alpha шёл исправно — проверено вживую). Полная
        пересборка декодирующей части (тот же путь, что и при первом показе,
        уже проверенный) обходит эту проблему целиком, не трогая ни ghost-пэд,
        ни компоновщик."""
        filesrc = Gst.ElementFactory.make("filesrc"); filesrc.set_property("location", path)
        demux = Gst.ElementFactory.make("qtdemux")
        parse = Gst.ElementFactory.make("h264parse")
        avdec = Gst.ElementFactory.make("avdec_h264")
        for e in (filesrc, demux, parse, avdec):
            b.add(e)
        filesrc.link(demux)
        demux.connect("pad-added", lambda _d, p:
            p.link(parse.get_static_pad("sink"))
            if not parse.get_static_pad("sink").is_linked() else None)
        parse.link(avdec)
        avdec.link(conv)
        b._chain = (filesrc, demux, parse, avdec)
        for e in (filesrc, demux, parse, avdec):
            e.sync_state_with_parent()
        ghost = b.get_static_pad("src")
        if ghost:
            rt = self.p_out.get_clock().get_time() - self.p_out.get_base_time()
            ghost.set_offset(rt)

    def _reload_loop_chain(self, b):
        """конец файла → пересобрать декод-цепочку заново (см. _build_loop_chain)
        вместо ненадёжного seek-на-EOS. Отдельный поток — set_state(NULL) может
        подождать (тот же паттерн, что и везде в этом файле для блокирующих
        остановок элементов)."""
        old = getattr(b, "_chain", ())
        for e in old:
            try:
                b.remove(e)
                e.set_state(Gst.State.NULL)
            except Exception as ex:
                print(f"[streamG] loop-chain teardown: {ex}", flush=True)
        try:
            self._build_loop_chain(b, b._loop_path, b._conv)
        except Exception as ex:
            print(f"[streamG] loop-chain rebuild: {ex}", flush=True)

    def _loop_seek_probe(self, pad, info, b):
        ev = info.get_event()
        if ev and ev.type == Gst.EventType.EOS:
            # конец GIF → EOS не пускаем дальше (иначе слой застынет/уйдёт),
            # пересобираем декод-цепочку с нуля в фоновом потоке.
            threading.Thread(target=self._reload_loop_chain, args=(b,), daemon=True).start()
            return Gst.PadProbeReturn.DROP
        return Gst.PadProbeReturn.OK

    def _add_comp_pad(self, b, x, y, w, h, zorder, alpha):
        comp = self.p_out.get_by_name("comp")
        self.p_out.add(b)
        pad = comp.request_pad_simple("sink_%u")
        pad.set_property("xpos", x); pad.set_property("ypos", y)
        if w: pad.set_property("width", w)
        if h: pad.set_property("height", h)
        pad.set_property("zorder", zorder); pad.set_property("alpha", alpha)
        b.get_static_pad("src").link(pad)
        # КРИТИЧНО (тот же фикс, что и для ролика): содержимое bin'а несёт PTS
        # от 0 (из файла), а конвейер уже играет в running-time далеко вперёд.
        # Без сдвига компоновщик молча считает буферы «из прошлого» и не рендерит
        # их (проверено: буферы реально текут, экран остаётся пустым/чёрным).
        rt = self.p_out.get_clock().get_time() - self.p_out.get_base_time()
        b.get_static_pad("src").set_offset(rt)
        b.sync_state_with_parent()
        # КРИТИЧНО: compositor (GstAggregator) ждёт первый буфер С КАЖДОГО
        # подключённого пэда, прежде чем отдать хоть один кадр на выходе —
        # если источник слоя стартует не мгновенно (особенно HTML: запуск
        # Chromium + первый CDP-кадр — секунды), ВЕСЬ эфир замирает/чернеет
        # на это время. GAP-событие говорит агрегатору «на этом пэде пока
        # пусто, не жди» — остальные слои (в т.ч. живой эфир) продолжают
        # рендериться без пауз.
        # ИСТОРИЯ: раньше здесь слали голый GAP без предшествующих sticky-
        # событий — пэд ещё не видел STREAM-START/CAPS/SEGMENT (их пришлёт
        # позже реальный источник бина), поэтому GAP с временны́м форматом
        # молча отбрасывался агрегатором ("GAP event outside segment,
        # dropping") — это и была причина CRITICAL gst_segment_clip. Первая
        # попытка чинить это (слать только SEGMENT) была НЕПРАВИЛЬНОЙ — без
        # STREAM-START/CAPS перед ним GStreamer ругался "Sticky event
        # misordering" и показ обрывался раньше срока. Правильный порядок —
        # прислать ВСЕ три sticky-события (stream-start → caps → segment)
        # placeholder'ами ПЕРЕД GAP; когда позже придут настоящие от реального
        # источника бина — они просто обновят те же sticky-слоты, это штатно.
        # Проверено изолированным тестом (30с, без CRITICAL/warning/обрывов)
        # перед применением к боевому коду.
        pad.send_event(Gst.Event.new_stream_start(f"layer-{id(pad)}"))
        placeholder_caps = Gst.Caps.from_string(
            f"video/x-raw,format=RGBA,width={w or self.w},height={h or self.h},framerate={self.fps}/1")
        pad.send_event(Gst.Event.new_caps(placeholder_caps))
        seg = Gst.Segment()
        seg.init(Gst.Format.TIME)
        pad.send_event(Gst.Event.new_segment(seg))
        pad.send_event(Gst.Event.new_gap(0, Gst.CLOCK_TIME_NONE))
        return pad

    def _remove_comp_pad(self, b, pad):
        """Безопасное снятие динамического слоя с ЖИВОГО compositor'а.
        Официальный паттерн GStreamer для динамического удаления ветки:
        IDLE-проб на паде источника гарантирует, что колбэк сработает В
        МОМЕНТ, когда через этот пад точно ничего не течёт — без гонки со
        streaming-потоком (в отличие от вызова "руками" в произвольный
        момент, как было раньше).

        Внутри проба — ТОЛЬКО быстрые безопасные операции (unlink,
        release_request_pad). b.set_state(NULL) — тяжёлый БЛОКИРУЮЩИЙ вызов,
        и для HTML-слоя (tcpserversrc, см. _html_finish) может зависнуть
        НАВСЕГДА (баг самого элемента при отмене блокирующего accept()) —
        поэтому его СОЗНАТЕЛЬНО уводим в отдельный поток fire-and-forget.
        Вызывать set_state() прямо из streaming-потока (внутри самого
        проба) вообще ЗАПРЕЩЕНО GStreamer'ом — поймано вживую изолированным
        тестом: "Trying to join task ... from its thread would deadlock" —
        именно так была сломана предыдущая попытка фикса.

        Раньше unlink→state_null→remove→release_pad шли одной строкой в
        порядке, где release_pad — ПОСЛЕДНИЙ шаг: пока блокирующий
        set_state(NULL) не завершится (а он может не завершиться никогда),
        compositor всё ещё считал пад «живым» сток-пэдом и мог ждать с него
        данные — реальный источник SRT-флапа Flussonic именно в момент
        снятия баннера/HTML/ролика. Теперь release_request_pad происходит
        СРАЗУ (миллисекунды), а зависающий teardown бина уже никого не ждёт."""
        comp = self.p_out.get_by_name("comp")
        srcpad = b.get_static_pad("src")

        def idle_cb(pad_, info):
            try:
                srcpad.unlink(pad)
                comp.release_request_pad(pad)
            except Exception as e:
                print(f"[streamG] снятие слоя (pad): {e}", flush=True)

            def teardown():
                try:
                    b.set_state(Gst.State.NULL)
                    self.p_out.remove(b)
                except Exception as e:
                    print(f"[streamG] снятие слоя (teardown): {e}", flush=True)
            threading.Thread(target=teardown, daemon=True).start()
            return Gst.PadProbeReturn.REMOVE

        srcpad.add_probe(Gst.PadProbeType.IDLE, idle_cb)
        return False

    def _banner_build(self, path, loop, duration, fade, rect=None):
        try:
            b = self._make_image_bin(path, loop=loop)
            if rect:
                x, y, w, h = rect
            else:
                x, y, w, h = 0, self.h - self.banner_h, self.banner_w, self.banner_h
            pad = self._add_comp_pad(b, x, y, w, h, 3, 0.0)
            threading.Thread(target=self._banner_life,
                             args=(b, pad, duration, fade), daemon=True).start()
        except Exception as e:
            print(f"[streamG] баннер не построился: {e}", flush=True)
            self.banner_busy = False
        return False

    def _banner_life(self, src, pad, duration, fade):
        try:
            steps = max(1, int(fade * self.fps))
            for i in range(steps + 1):                          # fade in
                pad.set_property("alpha", i / steps)
                time.sleep(fade / steps)
            time.sleep(max(0.0, duration - 2 * fade))           # держим
            for i in range(steps + 1):                          # fade out
                pad.set_property("alpha", 1 - i / steps)
                time.sleep(fade / steps)
        finally:
            # НЕ GLib.idle_add — _banner_finish→_remove_comp_pad блокирующе
            # вызывает set_state(NULL) (см. подробное объяснение у _on_in_msg);
            # на GLib-лупе это тот же риск зависания всего движка целиком.
            threading.Thread(target=self._banner_finish, args=(src, pad), daemon=True).start()

    def _banner_finish(self, src, pad):
        self._remove_comp_pad(src, pad)
        self.banner_busy = False
        print("[streamG] баннер снят", flush=True)
        return False

    # ---------------- слой логотипа (постоянный, до clear_logo)
    def show_logo(self, path, x, y, w=0, h=0):
        if not os.path.exists(path):
            return "ERR logo file not found"
        # НЕ GLib.idle_add — _logo_build может звать _remove_comp_pad (замена
        # прежнего лого), а тот блокирующе вызывает set_state(NULL).
        threading.Thread(target=self._logo_build,
                         args=(path, int(x), int(y), int(w), int(h)), daemon=True).start()
        return "OK logo"

    def _logo_build(self, path, x, y, w, h):
        try:
            if self.logo_pad:                                   # заменяем прежний
                self._remove_comp_pad(self.logo_bin, self.logo_pad)
                self.logo_bin = self.logo_pad = None
            b = self._make_image_bin(path, loop=False)          # лого — статичный
            self.logo_pad = self._add_comp_pad(b, x, y, w, h, 4, 1.0)  # zorder 4: над баннером(3), под заглушкой(5)
            self.logo_bin = b
            print(f"[streamG] логотип ({x},{y})", flush=True)
        except Exception as e:
            print(f"[streamG] логотип не построился: {e}", flush=True)
        return False

    def clear_logo(self):
        threading.Thread(target=self._logo_clear, daemon=True).start()
        return "OK logo off"

    def _logo_clear(self):
        if self.logo_pad:
            self._remove_comp_pad(self.logo_bin, self.logo_pad)
            self.logo_bin = self.logo_pad = None
            print("[streamG] логотип убран", flush=True)
        return False

    # ---------------- вставка ролика (полноэкранный MP4 со звуком, возврат в эфир)
    def play_video(self, path, gain=None):
        if not os.path.exists(path):
            return "ERR video file not found"
        if self.video_busy:
            return "ERR video already playing"
        self.video_busy = True
        if gain is not None:
            self.ad_volume_default = max(0.0, min(4.0, float(gain)))
        GLib.idle_add(self._video_build, path)
        return "OK video"

    def _video_build(self, path):
        try:
            comp = self.p_out.get_by_name("comp")
            # ВСЕ микшеры: amix (дорожка 0) + amix1..amix{N-1} (мультиязык). В
            # каждый вливаем ОДИН И ТОТ ЖЕ рекламный звук (общий на все языки) и
            # у каждого глушим эфир (sink_0) на время ролика.
            amixers = [self.p_out.get_by_name(n) for n in
                       ["amix"] + [f"amix{i}" for i in range(1, len(self.audio_tracks))]]
            amixers = [m for m in amixers if m]
            b = Gst.Bin.new("videoad")
            filesrc = Gst.ElementFactory.make("filesrc"); filesrc.set_property("location", path)
            dec = Gst.ElementFactory.make("decodebin")
            vq = Gst.ElementFactory.make("queue"); vconv = Gst.ElementFactory.make("videoconvert")
            aq = Gst.ElementFactory.make("queue"); aconv = Gst.ElementFactory.make("audioconvert")
            ares = Gst.ElementFactory.make("audioresample")
            # advol: громкость ролика — стартует с ad_volume_default (см.
            # loudness-анализ при загрузке в библиотеку, main.py video_upload),
            # живо подстраивается командой "advol <v>" (UI-фейдер слева от
            # PGM). adlevel: тот же VU-метр, что и у выхода (outlevel).
            advol = Gst.ElementFactory.make("volume")
            advol.set_property("volume", self.ad_volume_default)
            adlevel = Gst.ElementFactory.make("level", "adlevel")
            adlevel.set_property("interval", 100000000)
            adlevel.set_property("post-messages", True)
            for e in (filesrc, dec, vq, vconv, aq, aconv, ares, advol, adlevel):
                b.add(e)
            filesrc.link(dec)
            vq.link(vconv); aq.link(aconv); aconv.link(ares)
            ares.link(advol); advol.link(adlevel)
            def on_pad(_d, pad):
                caps = pad.get_current_caps() or pad.query_caps(None)
                s = caps.to_string() if caps else ""
                if s.startswith("video/") and not vq.get_static_pad("sink").is_linked():
                    pad.link(vq.get_static_pad("sink"))
                elif s.startswith("audio/") and not aq.get_static_pad("sink").is_linked():
                    pad.link(aq.get_static_pad("sink"))
            dec.connect("pad-added", on_pad)
            b.add_pad(Gst.GhostPad.new("vsrc", vconv.get_static_pad("src")))
            b.add_pad(Gst.GhostPad.new("asrc", adlevel.get_static_pad("src")))
            self.ad_volume_el = advol
            self.p_out.add(b)
            # видео ролика — полноэкранный пэд поверх всего (zorder 9)
            vpad = comp.request_pad_simple("sink_%u")
            vpad.set_property("xpos", 0); vpad.set_property("ypos", 0)
            vpad.set_property("width", self.w); vpad.set_property("height", self.h)
            vpad.set_property("zorder", 9); vpad.set_property("alpha", 1.0)
            b.get_static_pad("vsrc").link(vpad)
            # звук ролика раздаём во ВСЕ микшеры через tee (общий рекламный звук
            # на всех языках), в каждом глушим эфирную дорожку (sink_0).
            ad_tee = Gst.ElementFactory.make("tee")
            self.p_out.add(ad_tee)
            b.get_static_pad("asrc").link(ad_tee.get_static_pad("sink"))
            # КРИТИЧНО: файл-ролик стартует с PTS 0, а конвейер живёт по running-time
            # (эфир + буфер). Без сдвига компоновщик/amix считают ролик «в прошлом»
            # и выбрасывают его. Сдвигаем таймстампы видео- и аудио-пэдов на «сейчас».
            rt = self.p_out.get_clock().get_time() - self.p_out.get_base_time()
            b.get_static_pad("vsrc").set_offset(rt)
            b.get_static_pad("asrc").set_offset(rt)
            ad_links = []      # (amix, apad, queue, tee_src) — для разборки в _video_finish
            mains = []         # sink_0 каждого микшера — вернуть громкость в конце
            for m in amixers:
                q = Gst.ElementFactory.make("queue")
                self.p_out.add(q); q.sync_state_with_parent()
                tsrc = ad_tee.request_pad_simple("src_%u")
                tsrc.link(q.get_static_pad("sink"))
                apad = m.request_pad_simple("sink_%u")
                q.get_static_pad("src").link(apad)
                ad_links.append((m, apad, q, tsrc))
                main = m.get_static_pad("sink_0")
                if main:
                    main.set_property("volume", 0.0); mains.append(main)
            ad_tee.sync_state_with_parent()
            self.video_refs = (b, vpad, ad_tee, ad_links, mains)
            # конец ролика (EOS видео) → возврат в эфир
            b.get_static_pad("vsrc").add_probe(
                Gst.PadProbeType.EVENT_DOWNSTREAM, self._video_eos_probe)
            b.sync_state_with_parent()
            print("[streamG] ролик запущен", flush=True)
        except Exception as e:
            print(f"[streamG] ролик не построился: {e}", flush=True)
            self.video_busy = False
        return False

    def _video_eos_probe(self, pad, info):
        ev = info.get_event()
        if ev and ev.type == Gst.EventType.EOS:
            # НЕ GLib.idle_add — _video_finish несколько раз блокирующе зовёт
            # set_state(NULL) (см. _on_in_msg).
            threading.Thread(target=self._video_finish, daemon=True).start()
            return Gst.PadProbeReturn.DROP
        return Gst.PadProbeReturn.OK

    def _video_finish(self):
        if not self.video_refs:
            self.video_busy = False
            return False
        try:
            b, vpad, ad_tee, ad_links, mains = self.video_refs
            comp = self.p_out.get_by_name("comp")
            b.set_state(Gst.State.NULL)
            b.get_static_pad("vsrc").unlink(vpad)
            b.get_static_pad("asrc").unlink(ad_tee.get_static_pad("sink"))
            # разобрать раздачу рекламного звука по всем микшерам
            for (m, apad, q, tsrc) in ad_links:
                q.set_state(Gst.State.NULL)
                q.get_static_pad("src").unlink(apad)
                tsrc.unlink(q.get_static_pad("sink"))
                m.release_request_pad(apad)
                ad_tee.release_request_pad(tsrc)
                self.p_out.remove(q)
            ad_tee.set_state(Gst.State.NULL)
            self.p_out.remove(ad_tee)
            self.p_out.remove(b)
            comp.release_request_pad(vpad)
            for main in mains:
                main.set_property("volume", 1.0)      # вернуть эфирный звук
            print("[streamG] ролик завершён, эфир возвращён", flush=True)
        except Exception as e:
            print(f"[streamG] завершение ролика: {e}", flush=True)
        finally:
            self.video_refs = None
            self.video_busy = False
            self.ad_volume_el = None
            self.last_levels["ad_rms"] = -100.0
        return False

    def stop_video(self):
        threading.Thread(target=self._video_finish, daemon=True).start()
        return "OK stopvideo"

    # ---------------- громкость (мастер-выход + ролик) — фейдеры UI слева от PGM
    def set_out_volume(self, v):
        try:
            v = max(0.0, min(4.0, float(v)))
        except ValueError:
            return "ERR bad volume"
        outvol = self.p_out.get_by_name("outvol") if self.p_out else None
        if not outvol:
            return "ERR no outvol"
        outvol.set_property("volume", v)
        return "OK outvol"

    def set_ad_volume(self, v):
        try:
            v = max(0.0, min(4.0, float(v)))
        except ValueError:
            return "ERR bad volume"
        self.ad_volume_default = v   # запоминаем и на случай СЛЕДУЮЩЕГО ролика
        if self.ad_volume_el:
            self.ad_volume_el.set_property("volume", v)
            return "OK advol"
        return "OK advol (сохранено, применится к следующему ролику — сейчас ничего не играет)"

    # ---------------- микрофон комментатора (постоянный SRT-вход)
    def start_mic(self, streamid, gain=None):
        """streamid — имя, под которым OBS/ffmpeg публикует звук в локальный
        MediaMTX (srt://<obs>:8890?streamid=publish:<streamid>) — мы читаем
        его отсюда же как SRT-клиент (srt://127.0.0.1:8890?streamid=read:...).
        Строим на фоновом потоке, а не GLib.idle_add: srtsrc может подвиснуть
        на connect(), если OBS ещё не подключился/уже отвалился — тот же риск
        блокировки GLib-лупа, что и у tcpserversrc для HTML (см. _on_in_msg)."""
        if self.mic_bin:
            return "ERR mic already on"
        if gain is not None:
            try:
                self.mic_volume_default = max(0.0, min(4.0, float(gain)))
            except ValueError:
                pass
        self.mic_streamid = streamid
        threading.Thread(target=self._mic_build, args=(streamid,), daemon=True).start()
        return "OK mic"

    def _mic_build(self, streamid):
        try:
            amixers = [self.p_out.get_by_name(n) for n in
                       ["amix"] + [f"amix{i}" for i in range(1, len(self.audio_tracks))]]
            amixers = [m for m in amixers if m]
            b = Gst.Bin.new("micbin")
            src = Gst.ElementFactory.make("srtsrc")
            src.set_property("uri", f"srt://127.0.0.1:8890?streamid=read:{streamid}")
            # latency — запас на джиттер сети от комментатора (мс); connect
            # у SRT сам по себе может ждать publisher'а, это ОК — просто висит
            # на фоновом потоке, не блокируя остальной движок.
            src.set_property("latency", 200)
            dec = Gst.ElementFactory.make("decodebin")
            aq = Gst.ElementFactory.make("queue")
            aconv = Gst.ElementFactory.make("audioconvert")
            ares = Gst.ElementFactory.make("audioresample")
            micvol = Gst.ElementFactory.make("volume")
            micvol.set_property("volume", self.mic_volume_default)
            miclevel = Gst.ElementFactory.make("level", "miclevel")
            miclevel.set_property("interval", 100000000)
            miclevel.set_property("post-messages", True)
            for e in (src, dec, aq, aconv, ares, micvol, miclevel):
                b.add(e)
            src.link(dec)
            aq.link(aconv); aconv.link(ares); ares.link(micvol); micvol.link(miclevel)

            def on_pad(_d, pad):
                caps = pad.get_current_caps() or pad.query_caps(None)
                s = caps.to_string() if caps else ""
                if s.startswith("audio/") and not aq.get_static_pad("sink").is_linked():
                    pad.link(aq.get_static_pad("sink"))
                else:
                    # ЗВУК только — но если OBS шлёт видео вместе (обычный
                    # стрим-профиль), НЕслинкованный пэд decodebin — не
                    # безобидная простой, а затор: внутренние очереди
                    # decodebin переполняются раз никто их не читает и
                    # backpressure блокирует ВСЕ пэды, включая аудио (проверено
                    # на живом потоке — mic_rms стоял на -100 при реально
                    # идущих 60+ МБ трафика). Сливаем лишнее в fakesink.
                    sink = Gst.ElementFactory.make("fakesink")
                    sink.set_property("sync", False)
                    sink.set_property("async", False)
                    b.add(sink)
                    sink.sync_state_with_parent()
                    pad.link(sink.get_static_pad("sink"))
            dec.connect("pad-added", on_pad)
            b.add_pad(Gst.GhostPad.new("asrc", miclevel.get_static_pad("src")))

            self.p_out.add(b)
            mic_tee = Gst.ElementFactory.make("tee")
            self.p_out.add(mic_tee)
            b.get_static_pad("asrc").link(mic_tee.get_static_pad("sink"))
            # автокалибровка вместо статического +rt — см. _obs_auto_offset:
            # srtsrc уже клоко-выровнен, статический сдвиг уводил метки в
            # будущее (у микрофона это маскировалось тем, что audiomixer
            # просто «догонял» — но с риском тех же зависаний, что у OBS)
            self._obs_auto_offset([b.get_static_pad("asrc")])

            mic_links = []
            mains = []
            for m in amixers:
                q = Gst.ElementFactory.make("queue")
                self.p_out.add(q); q.sync_state_with_parent()
                tsrc = mic_tee.request_pad_simple("src_%u")
                tsrc.link(q.get_static_pad("sink"))
                apad = m.request_pad_simple("sink_%u")
                q.get_static_pad("src").link(apad)
                mic_links.append((m, apad, q, tsrc))
                main = m.get_static_pad("sink_0")
                if main:
                    # DUCKING, не мьют: эфир остаётся слышен приглушённым
                    # (mic_duck), а не пропадает целиком — в отличие от ролика.
                    main.set_property("volume", self.mic_duck)
                    mains.append(main)
            mic_tee.sync_state_with_parent()
            b.sync_state_with_parent()
            self.mic_refs = (mic_tee, mic_links, mains)
            self.mic_bin = b
            self.mic_volume_el = micvol
            print(f"[streamG] микрофон включён (streamid={streamid})", flush=True)
        except Exception as e:
            print(f"[streamG] микрофон не построился: {e}", flush=True)
            self.mic_bin = None
            self.mic_streamid = None

    def stop_mic(self):
        if not self.mic_bin:
            return "ERR mic not on"
        threading.Thread(target=self._mic_finish, daemon=True).start()
        return "OK micoff"

    def _mic_finish(self):
        detached = []
        try:
            b = self.mic_bin
            mic_tee, mic_links, mains = self.mic_refs
            b.get_static_pad("asrc").unlink(mic_tee.get_static_pad("sink"))
            for (m, apad, q, tsrc) in mic_links:
                q.get_static_pad("src").unlink(apad)
                tsrc.unlink(q.get_static_pad("sink"))
                m.release_request_pad(apad)
                mic_tee.release_request_pad(tsrc)
                self.p_out.remove(q)
                detached.append(q)
            self.p_out.remove(mic_tee); detached.append(mic_tee)
            self.p_out.remove(b); detached.append(b)
            for main in mains:
                main.set_property("volume", 1.0)   # вернуть полную громкость эфира
            print("[streamG] микрофон выключен", flush=True)
        except Exception as e:
            print(f"[streamG] выключение микрофона: {e}", flush=True)
        finally:
            self.mic_bin = None
            self.mic_refs = None
            self.mic_volume_el = None
            self.mic_streamid = None
            self.last_levels["mic_rms"] = -100.0
            # set_state(NULL) на srtsrc МОЖЕТ зависнуть НАВСЕГДА — если делать
            # его ДО сброса состояния (как раньше), finally никогда не
            # выполнялся и mic_bin оставался взведённым навечно ("ERR mic
            # already on" на все последующие включения). Поэтому NULL — в
            # отдельном потоке уже ПОСЛЕ отцепления и сброса состояния.
            def _null():
                for e in detached:
                    try:
                        e.set_state(Gst.State.NULL)
                    except Exception as ex:
                        print(f"[streamG] микрофон отложенный NULL: {ex}", flush=True)
            threading.Thread(target=_null, daemon=True).start()

    def set_mic_volume(self, v):
        try:
            v = max(0.0, min(4.0, float(v)))
        except ValueError:
            return "ERR bad volume"
        self.mic_volume_default = v
        if self.mic_volume_el:
            self.mic_volume_el.set_property("volume", v)
        return "OK micvol"

    # ---------------- наложение полного видео+аудио с OBS (постоянный
    # SRT-вход, полноэкранный слой compositor'а как у ролика, но
    # ПЕРЕКЛЮЧАТЕЛЬ и с плавным fade вместо жёсткого реза; звук эфира по
    # выбору либо глушится (как ролик), либо остаётся слышен целиком).
    def start_obs(self, streamid, audio_mode="mute", fade=None):
        """Только ПОДКЛЮЧЕНИЕ к SRT-каналу OBS — звук уже метрится (obs_rms),
        но в эфир НЕ выводится (видео идёт во временный fakesink, звук — во
        временный сток), пока не вызван go_live_obs(). Двухшаговая схема по
        просьбе оператора: сначала видно/слышно что канал живой, потом
        отдельной кнопкой "В эфир" — реальный вывод с fade-in."""
        if self.obs_stopping:
            return "ERR obs busy stopping"
        if self.obs_bin:
            return "ERR obs already on"
        self.obs_audio_mode = audio_mode if audio_mode in ("mute", "keep") else "mute"
        if fade is not None:
            try:
                self.obs_fade = max(0.05, min(5.0, float(fade)))
            except ValueError:
                pass
        self.obs_streamid = streamid
        threading.Thread(target=self._obs_build, args=(streamid,), daemon=True).start()
        return "OK obs"

    def _obs_link(self, src_pad, sink_pad, label):
        """Gst.Pad.link() НЕ бросает исключение при неудаче — возвращает код
        ошибки, который легко забыть проверить (что и произошло при первой
        реализации: OBS-наложение "выходило в эфир" без единой ошибки в
        логах, но видео/звук физически не текли). Явная проверка."""
        r = src_pad.link(sink_pad)
        if r != Gst.PadLinkReturn.OK:
            raise RuntimeError(f"link failed ({label}): {r.value_nick}")

    def _obs_auto_offset(self, ghost_pads, skip=25, n_samples=90, margin=0.25):
        """Выравнивание таймстампов живого SRT-бина: offset = running-time
        минус PTS. Эволюция подхода (все прежние варианты дали брак):
        1) по первому буферу — в него попадал прогрев декодера (~2с), эти
           2с навсегда оседали в задержке эфира;
        2) минимум дельты + фиксированный запас — запас то мал (энкодер OBS
           отдаёт данные пачками по кейфреймам, размах опозданий до ~1-2с,
           звук рывками), то велик (лишняя задержка);
        3) НЕЗАВИСИМЫЙ замер на каждый пэд — видео (декод 1080p60) опаздывает
           сильнее аудио, его оффсет получался больше → звук убегал ВПЕРЁД
           видео ровно на разницу замеров.
        Теперь: пропускаем прогрев, меряем худшее опоздание на КАЖДОМ пэде,
        а применяем ОДИН общий оффсет (максимум по всем пэдам + запас) —
        A/V-синхронизация сохраняется, худший путь покрыт."""
        # интервалы ВРЕМЕННЫЕ, не в буферах: 25 буферов видео = 0.4с, а
        # прогрев декода длится ~2с — замер попадал в хвост прогрева, оффсет
        # завышался, «ранние» видео-буферы забивали очередь перед compositor,
        # и её подпор тормозил весь выходной мукс (лагал даже звук эфира).
        warmup_s, measure_s = 2.5, 2.0
        done = {}   # pad -> его max-дельта
        def make_probe(my_pad):
            state = {"max": None, "t0": None}
            def probe(pad, info):
                buf = info.get_buffer()
                if not buf or buf.pts == Gst.CLOCK_TIME_NONE:
                    return Gst.PadProbeReturn.OK
                try:
                    now = time.time()
                    if state["t0"] is None:
                        state["t0"] = now
                    el = now - state["t0"]
                    if el < warmup_s:
                        return Gst.PadProbeReturn.OK
                    rt = self.p_out.get_clock().get_time() - self.p_out.get_base_time()
                    delta = rt - buf.pts
                    if state["max"] is None or delta > state["max"]:
                        state["max"] = delta
                    if el < warmup_s + measure_s:
                        return Gst.PadProbeReturn.OK
                    done[my_pad] = state["max"]
                    if len(done) == len(ghost_pads):
                        off = max(done.values()) + int(margin * Gst.SECOND)
                        for p in ghost_pads:
                            if abs(off) > int(0.05 * Gst.SECOND):
                                p.set_offset(off)
                        detail = ", ".join(f"{p.get_name()}={d/1e9:+.2f}s" for p, d in done.items())
                        print(f"[streamG] OBS auto-offset (общий): {off/1e9:+.2f}s ({detail})", flush=True)
                except Exception as e:
                    print(f"[streamG] OBS auto-offset: {e}", flush=True)
                return Gst.PadProbeReturn.REMOVE
            return probe
        for p in ghost_pads:
            p.add_probe(Gst.PadProbeType.BUFFER, make_probe(p))

    def _obs_fade_run(self, pad, a_from, a_to):
        steps = max(1, int(self.obs_fade * self.fps))
        for i in range(steps + 1):
            a = a_from + (a_to - a_from) * i / steps
            try:
                pad.set_property("alpha", a)
            except Exception as e:
                print(f"[streamG] OBS fade: alpha не выставился ({a}): {e}", flush=True)
                return
            time.sleep(self.obs_fade / steps)

    def _obs_build(self, streamid):
        try:
            b = Gst.Bin.new("obsbin")
            src = Gst.ElementFactory.make("srtsrc")
            src.set_property("uri", f"srt://127.0.0.1:8890?streamid=read:{streamid}")
            src.set_property("latency", 200)
            dec = Gst.ElementFactory.make("decodebin")
            vq = Gst.ElementFactory.make("queue"); vconv = Gst.ElementFactory.make("videoconvert")
            vscale = Gst.ElementFactory.make("videoscale")
            vcaps = Gst.ElementFactory.make("capsfilter")
            vcaps.set_property("caps", Gst.Caps.from_string(f"video/x-raw,width={self.w},height={self.h}"))
            aq = Gst.ElementFactory.make("queue"); aconv = Gst.ElementFactory.make("audioconvert")
            ares = Gst.ElementFactory.make("audioresample")
            obsvol = Gst.ElementFactory.make("volume")
            obsvol.set_property("volume", 1.0)
            obslevel = Gst.ElementFactory.make("level", "obslevel")
            obslevel.set_property("interval", 100000000)
            obslevel.set_property("post-messages", True)
            for e in (src, dec, vq, vconv, vscale, vcaps, aq, aconv, ares, obsvol, obslevel):
                b.add(e)
            src.link(dec)
            vq.link(vconv); vconv.link(vscale); vscale.link(vcaps)
            aq.link(aconv); aconv.link(ares); ares.link(obsvol); obsvol.link(obslevel)

            def on_pad(_d, pad):
                caps = pad.get_current_caps() or pad.query_caps(None)
                s = caps.to_string() if caps else ""
                if s.startswith("video/") and not vq.get_static_pad("sink").is_linked():
                    pad.link(vq.get_static_pad("sink"))
                elif s.startswith("audio/") and not aq.get_static_pad("sink").is_linked():
                    pad.link(aq.get_static_pad("sink"))
                else:
                    sink = Gst.ElementFactory.make("fakesink")
                    sink.set_property("sync", False); sink.set_property("async", False)
                    b.add(sink); sink.sync_state_with_parent()
                    pad.link(sink.get_static_pad("sink"))
            dec.connect("pad-added", on_pad)
            b.add_pad(Gst.GhostPad.new("vsrc", vcaps.get_static_pad("src")))
            b.add_pad(Gst.GhostPad.new("asrc", obslevel.get_static_pad("src")))

            self.p_out.add(b)
            # КРИТИЧНО: video_tee/audio_tee — ПРЯМЫЕ дети p_out (как и b, и
            # comp/amix) — линкуем на b ТОЛЬКО через его ghost-пэды. Пробовали
            # тянуть внутренний пэд бина (vcaps.src) напрямую наружу к comp/amix
            # без ghost-пэда — .link() не бросает исключение, но данные физически
            # не текут через границу бина (ни видео, ни звук не доходили,
            # эфир не глушился). Со сторонним предпросмотром (fakesink) до
            # "в эфир" — та же схема, что у мика/ролика: неслинкованный
            # пэд декодера стопорит decodebin целиком, поэтому сток обязателен.
            video_tee = Gst.ElementFactory.make("tee")
            audio_tee = Gst.ElementFactory.make("tee")
            vfake_q = Gst.ElementFactory.make("queue")
            vfake = Gst.ElementFactory.make("fakesink")
            vfake.set_property("sync", False); vfake.set_property("async", False)
            afake_q = Gst.ElementFactory.make("queue")
            afake = Gst.ElementFactory.make("fakesink")
            afake.set_property("sync", False); afake.set_property("async", False)
            for e in (video_tee, audio_tee, vfake_q, vfake, afake_q, afake):
                self.p_out.add(e)
            self._obs_link(b.get_static_pad("vsrc"), video_tee.get_static_pad("sink"), "vsrc->video_tee")
            self._obs_link(b.get_static_pad("asrc"), audio_tee.get_static_pad("sink"), "asrc->audio_tee")
            vtsrc = video_tee.request_pad_simple("src_%u")
            self._obs_link(vtsrc, vfake_q.get_static_pad("sink"), "video_tee->vfake_q")
            self._obs_link(vfake_q.get_static_pad("src"), vfake.get_static_pad("sink"), "vfake_q->vfake")
            atsrc = audio_tee.request_pad_simple("src_%u")
            self._obs_link(atsrc, afake_q.get_static_pad("sink"), "audio_tee->afake_q")
            self._obs_link(afake_q.get_static_pad("src"), afake.get_static_pad("sink"), "afake_q->afake")
            # ТАЙМСТАМПЫ: НЕ статический оффсет +rt (как у файла-ролика)!
            # srtsrc — живой источник, его буферы уже приходят со штампами
            # по часам пайплайна (PTS ≈ running-time, проверено пробами);
            # добавка +rt сдвигала их на rt В БУДУЩЕЕ — compositor/amix
            # ждали этих меток, очереди переполнялись и весь бин замерзал
            # («В эфир» нажато, а видео нет). Автокалибровка по первому
            # реальному буферу покрывает оба режима (0-базные и clock-базные).
            self._obs_auto_offset([b.get_static_pad("vsrc"), b.get_static_pad("asrc")])
            for e in (video_tee, audio_tee, vfake_q, vfake, afake_q, afake):
                e.sync_state_with_parent()
            b.sync_state_with_parent()

            self.obs_bin = b
            self.obs_volume_el = obsvol
            self.obs_video_tee = video_tee
            self.obs_audio_tee = audio_tee
            self.obs_is_live = False
            print(f"[streamG] OBS-канал подключён (streamid={streamid}) — жду 'в эфир'", flush=True)
        except Exception as e:
            print(f"[streamG] OBS-канал не подключился: {e}", flush=True)
            self.obs_bin = None
            self.obs_streamid = None

    def go_live_obs(self):
        if not self.obs_bin:
            return "ERR obs not connected"
        if self.obs_is_live:
            return "ERR obs already live"
        # защита оператора: если с OBS реально не идёт сигнал (obslevel не
        # запостил ни одного замера — OBS не публикует/отвалился), выход
        # «в эфир» только заглушил бы звук трансляции, не показав ничего.
        if self.last_levels.get("obs_rms", -100.0) <= -95.0:
            return "ERR нет сигнала с OBS — проверьте, что трансляция в OBS запущена"
        threading.Thread(target=self._obs_go_live, daemon=True).start()
        return "OK obslive"

    def _obs_go_live(self):
        try:
            comp = self.p_out.get_by_name("comp")
            amixers = [self.p_out.get_by_name(n) for n in
                       ["amix"] + [f"amix{i}" for i in range(1, len(self.audio_tracks))]]
            amixers = [m for m in amixers if m]
            # полноэкранный слой сразу НАД базовой картинкой эфира (zorder 2:
            # выше входа(1), но НИЖЕ баннера(3), лого(4), HTML/заглушки(5) и
            # ролика(9)) — OBS-наложение по смыслу ЗАМЕНЯЕТ эфир, поэтому вся
            # графика (баннеры/HTML/лого) продолжает накладываться поверх него
            # точно так же, как поверх обычного эфира. Стартует прозрачным
            # (alpha=0), fade-in ниже.
            vpad = comp.request_pad_simple("sink_%u")
            vpad.set_property("xpos", 0); vpad.set_property("ypos", 0)
            vpad.set_property("width", self.w); vpad.set_property("height", self.h)
            vpad.set_property("zorder", 2); vpad.set_property("alpha", 0.0)
            vq = Gst.ElementFactory.make("queue")
            # запас очереди 2с (дефолт 1с): оффсет + джиттер держат в ней
            # «ранние» кадры — при переполнении подпор останавливал бы весь
            # OBS-бин и через compositor дёргал бы выходной мукс (лаги эфира)
            vq.set_property("max-size-time", 2 * Gst.SECOND)
            vq.set_property("max-size-bytes", 0)
            vq.set_property("max-size-buffers", 0)
            self.p_out.add(vq); vq.sync_state_with_parent()
            vtsrc = self.obs_video_tee.request_pad_simple("src_%u")
            self._obs_link(vtsrc, vq.get_static_pad("sink"), "video_tee->vq")
            self._obs_link(vq.get_static_pad("src"), vpad, "vq->compositor")

            obs_links = []
            mains = []
            # Два режима звука (по изначальной постановке оператора):
            #  mute — звук OBS ЗАМЕНЯЕТ эфирный (эфир глушится, OBS в микс);
            #  keep — остаётся ТОЛЬКО эфирный звук, звук OBS вообще не
            #         подмешивается (видео без его звука) — НЕ смешивание.
            if self.obs_audio_mode == "mute":
                for m in amixers:
                    q = Gst.ElementFactory.make("queue")
                    q.set_property("max-size-time", 2 * Gst.SECOND)
                    q.set_property("max-size-bytes", 0)
                    q.set_property("max-size-buffers", 0)
                    self.p_out.add(q); q.sync_state_with_parent()
                    atsrc = self.obs_audio_tee.request_pad_simple("src_%u")
                    self._obs_link(atsrc, q.get_static_pad("sink"), "audio_tee->q")
                    apad = m.request_pad_simple("sink_%u")
                    self._obs_link(q.get_static_pad("src"), apad, "q->amix")
                    obs_links.append((m, apad, q, atsrc))
                    main = m.get_static_pad("sink_0")
                    if main:
                        main.set_property("volume", 0.0)
                        mains.append(main)
            self.obs_refs = (vpad, vq, vtsrc, obs_links, mains)
            self.obs_is_live = True
            threading.Thread(target=self._obs_fade_run, args=(vpad, 0.0, 1.0), daemon=True).start()
            print(f"[streamG] OBS-наложение вышло в эфир (audio={self.obs_audio_mode})", flush=True)
        except Exception as e:
            print(f"[streamG] OBS: переход в эфир: {e}", flush=True)

    def stop_obs(self):
        if not self.obs_bin:
            return "ERR obs not on"
        if self.obs_stopping:
            return "ERR obs already stopping"
        self.obs_stopping = True
        threading.Thread(target=self._obs_stop, daemon=True).start()
        return "OK obsoff"

    def _obs_reset_state(self):
        self.obs_bin = None
        self.obs_refs = None
        self.obs_volume_el = None
        self.obs_streamid = None
        self.obs_video_tee = None
        self.obs_audio_tee = None
        self.obs_is_live = False
        self.obs_stopping = False
        self.last_levels["obs_rms"] = -100.0

    def _obs_stop(self):
        # ЗАЩИТА ОТ РАССИНХРОНА: восстанавливаем громкость эфира на ВСЕХ
        # амиксерах безусловно, независимо от self.obs_is_live и от того,
        # какая ветка разборки ниже сработает — если реального замьюченного
        # состояния нет, это просто no-op (volume=1.0 и так). Раньше "Стоп"
        # мог попасть в "не был в эфире" ветку (не восстанавливающую звук),
        # если внутренний obs_is_live разошёлся с реальностью — тогда эфир
        # оставался приглушённым до перезапуска потока.
        try:
            amixers = [self.p_out.get_by_name(n) for n in
                       ["amix"] + [f"amix{i}" for i in range(1, len(self.audio_tracks))]]
            for m in amixers:
                if not m:
                    continue
                main = m.get_static_pad("sink_0")
                if main:
                    main.set_property("volume", 1.0)
        except Exception as e:
            print(f"[streamG] OBS: восстановление громкости эфира: {e}", flush=True)

        try:
            b = self.obs_bin
            video_tee = self.obs_video_tee
            audio_tee = self.obs_audio_tee

            def null_detached(elements):
                # set_state(NULL) на srtsrc МОЖЕТ зависнуть НАВСЕГДА (см.
                # _on_in_msg/_mic_finish) — раньше это висело в том же потоке,
                # что и сброс состояния, и obs_stopping/obs_bin оставались
                # взведёнными навечно: все последующие "Подключить"/"В эфир"
                # получали "already on"/"already live" (замаскированные main.py
                # под успех для reload-синхронизации) и НИЧЕГО реально не
                # делали — у оператора "нажимаю В эфир, а видео нет". Поэтому:
                # сначала отцепить от pipeline и сбросить состояние (быстро),
                # а NULL — отдельным потоком, которому не жалко зависнуть.
                def _null():
                    for e in elements:
                        try:
                            e.set_state(Gst.State.NULL)
                        except Exception as ex:
                            print(f"[streamG] OBS отложенный NULL: {ex}", flush=True)
                threading.Thread(target=_null, daemon=True).start()

            def detach_common():
                # быстрые операции: убрать элементы из pipeline (это не блокирует)
                try:
                    self.p_out.remove(video_tee)
                    self.p_out.remove(audio_tee)
                    self.p_out.remove(b)
                except Exception as e:
                    print(f"[streamG] OBS teardown (общее): {e}", flush=True)

            if not self.obs_is_live:
                def teardown_preview():
                    detach_common()
                    print("[streamG] OBS-подключение снято (в эфир не выходило)", flush=True)
                    self._obs_reset_state()
                    null_detached([video_tee, audio_tee, b])
                threading.Thread(target=teardown_preview, daemon=True).start()
                return

            vpad, vq, vtsrc, obs_links, mains = self.obs_refs
            self._obs_fade_run(vpad, vpad.get_property("alpha"), 0.0)   # fade-out перед разборкой
            comp = self.p_out.get_by_name("comp")
            vqsrcpad = vq.get_static_pad("src")

            def idle_cb(pad_, info):
                try:
                    vqsrcpad.unlink(vpad)
                    comp.release_request_pad(vpad)
                except Exception as e:
                    print(f"[streamG] OBS-наложение снятие видео: {e}", flush=True)

                def teardown():
                    try:
                        vtsrc.unlink(vq.get_static_pad("sink"))
                        video_tee.release_request_pad(vtsrc)
                        self.p_out.remove(vq)
                        for (m, apad, q, atsrc) in obs_links:
                            q.get_static_pad("src").unlink(apad)
                            atsrc.unlink(q.get_static_pad("sink"))
                            m.release_request_pad(apad)
                            audio_tee.release_request_pad(atsrc)
                            self.p_out.remove(q)
                        detach_common()
                        for main in mains:
                            main.set_property("volume", 1.0)
                        print("[streamG] OBS-наложение снято, эфир восстановлен", flush=True)
                    except Exception as e:
                        print(f"[streamG] OBS-наложение teardown: {e}", flush=True)
                    finally:
                        self._obs_reset_state()
                        null_detached([vq] + [q for (_, _, q, _) in obs_links]
                                      + [video_tee, audio_tee, b])
                threading.Thread(target=teardown, daemon=True).start()
                return Gst.PadProbeReturn.REMOVE

            vqsrcpad.add_probe(Gst.PadProbeType.IDLE, idle_cb)
        except Exception as e:
            print(f"[streamG] остановка OBS: {e}", flush=True)
            self._obs_reset_state()

    def set_obs_volume(self, v):
        try:
            v = max(0.0, min(4.0, float(v)))
        except ValueError:
            return "ERR bad volume"
        if self.obs_volume_el:
            self.obs_volume_el.set_property("volume", v)
        return "OK obsvol"

    # ---------------- слой HTML (Chromium/CDP-скринкаст → сырой RGBA по TCP)
    def show_html(self, path, x=0, y=0, w=0, h=0, fps=15, duration=30.0, fade=1.0):
        """HTML на GStreamer-движке: без ffmpeg вообще. capture_html_raw.py
        (отдельный процесс под venv-python агента, там же Playwright) рендерит
        страницу headless-Chromium через тот же CDP-скринкаст, что и
        html_feeder.py для ffmpeg-микшера, но вместо кодирования в h264 просто
        шлёт сырые RGBA-байты по TCP — сюда, в tcpserversrc, который ждёт
        подключение локально. rawvideoparse режет непрерывный поток байт на
        кадры по известным width/height/fps, decodebin/автоплаг НЕ участвует —
        значит НЕ рискует наткнуться на аппаратный NVDEC (см. _make_image_bin),
        а RGBA сохраняет альфа-канал нативно (в отличие от h264+alphaextract,
        которым это делает ffmpeg-движок)."""
        if not os.path.exists(path):
            return "ERR html file not found"
        if self.html_busy:
            return "ERR html already showing"
        self.html_busy = True
        w = int(w) or self.w
        h = int(h) or self.h
        # порт МЕНЯЕТСЯ на каждый показ (не завязан на путь шаблона) — раньше
        # один и тот же шаблон всегда получал один и тот же порт, и если
        # предыдущий tcpserversrc не успел (или не смог, см. _html_finish)
        # освободить порт, новый показ мог начать конфликтовать за него со
        # старым, зависшим — визуально это рваные кадры/цветовой мусор.
        self.html_port_seq = (self.html_port_seq + 1) % 100
        port = self.html_port_base + self.html_port_seq
        GLib.idle_add(self._html_build, path, int(x), int(y), w, h,
                      int(fps), float(duration), float(fade), port)
        return "OK html"

    def _html_build(self, path, x, y, w, h, fps, duration, fade, port):
        try:
            # уникальное имя бина — если предыдущий htmlsrc-бин ещё не успел
            # (или вообще не смог) корректно удалиться в фоне (см. _html_finish),
            # одинаковое имя дало бы конфликт при добавлении в тот же p_out.
            b = Gst.Bin.new(f"htmlsrc_{int(time.time()*1000)}")
            src = Gst.ElementFactory.make("tcpserversrc")
            src.set_property("host", "127.0.0.1")
            src.set_property("port", port)
            # do-timestamp=true ОКОНЧАТЕЛЬНО ВЫКЛЮЧЕН. История: сначала подозревал
            # его в GST_CRITICAL "gst_segment_clip" — откатил, CRITICAL остался
            # (тот оказался отдельным багом в _add_comp_pad, GAP без сегмента —
            # он безвреден, просто шумит в логе). Подумал, что do-timestamp
            # чист, вернул его для плавности бегущей строки — и ИЗОЛИРОВАННЫЙ
            # повторный тест (без побочных правок) чётко воспроизвёл падение
            # показа через ровно 5с ОБА раза. do-timestamp — подтверждённая,
            # воспроизводимая причина: вероятно, TIME-сегмент от tcpserversrc
            # конфликтует с внутренним учётом позиции у rawvideoparse (тот
            # парсит byte-stream) настолько, что через несколько секунд рвёт
            # соединение с capture_html_raw.py. Стабильность важнее плавности —
            # оставляем ВЫКЛЮЧЕННЫМ. Если понадобится плавность бегущей
            # строки — нужен другой механизм тайминга, не do-timestamp здесь.
            # src.set_property("do-timestamp", True)
            parse = Gst.ElementFactory.make("rawvideoparse")
            parse.set_property("width", w); parse.set_property("height", h)
            parse.set_property("format", "rgba")
            parse.set_property("framerate", Gst.Fraction(fps, 1))
            conv = Gst.ElementFactory.make("videoconvert")
            # тот же фикс прозрачности, что и для баннера/лого: форсируем AYUV,
            # иначе videoconvert схлопывает RGBA→I420 и прозрачные зоны страницы
            # становятся непрозрачным чёрным (проверено зелёным фоном).
            acaps = Gst.ElementFactory.make("capsfilter")
            acaps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=RGBA"))
            for e in (src, parse, conv, acaps):
                b.add(e)
            src.link(parse); parse.link(conv); conv.link(acaps)
            ghost = Gst.GhostPad.new("src", acaps.get_static_pad("src"))
            b.add_pad(ghost)
            pad = self._add_comp_pad(b, x, y, w, h, 5, 0.0)   # zorder 5: над лого(4), под заглушкой
            # tcpserversrc уже слушает после sync_state_with_parent() внутри
            # _add_comp_pad — запускаем клиента (venv python) ПОСЛЕ этого
            proc = subprocess.Popen(
                [self.venv_python, self.capture_html_script, path,
                 str(w), str(h), str(fps), "127.0.0.1", str(port), str(duration)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True, start_new_session=True)
            self.html_refs = (b, pad, port)
            self.html_proc = proc
            threading.Thread(target=self._html_life,
                             args=(b, pad, proc, duration, fade), daemon=True).start()
            print(f"[streamG] HTML показан ({path}, порт {port})", flush=True)
        except Exception as e:
            print(f"[streamG] HTML не построился: {e}", flush=True)
            self.html_busy = False
        return False

    def _html_life(self, b, pad, proc, duration, fade):
        try:
            steps = max(1, int(fade * self.fps))
            for i in range(steps + 1):                          # fade in
                pad.set_property("alpha", i / steps)
                time.sleep(fade / steps)
            # держим, пока capture_html_raw.py жив (духом равно duration) —
            # ждём именно завершения процесса, а не просто sleep(duration):
            # если Chromium упал раньше, слой снимаем сразу, не показывая пустоту
            t_end = time.monotonic() + duration
            while proc.poll() is None and time.monotonic() < t_end:
                time.sleep(0.2)
            for i in range(steps + 1):                          # fade out
                pad.set_property("alpha", 1 - i / steps)
                time.sleep(fade / steps)
        finally:
            # НЕ GLib.idle_add — _html_finish→_remove_comp_pad блокирующе
            # зовёт set_state(NULL); это и была настоящая причина "ERR html
            # already showing" НАВСЕГДА — если set_state(NULL) подвисал на
            # GLib-лупе (напр. tcpserversrc ждёт закрытия сокета от уже
            # мёртвого capture_html_raw.py), _html_finish НИКОГДА не
            # завершался → html_busy оставался True навечно, и заодно
            # блокировался весь остальной GLib-луп (см. _on_in_msg).
            threading.Thread(target=self._html_finish, args=(b, pad, proc), daemon=True).start()

    def _html_finish(self, b, pad, proc):
        # ВАЖНО (найдено вживую, стресс-тестом): set_state(NULL) у tcpserversrc
        # внутри _remove_comp_pad может зависнуть НАВСЕГДА даже если Python-
        # клиент (capture_html_raw.py) уже гарантированно убит — судя по всему,
        # это ограничение/баг самого элемента GStreamer при отмене блокирующего
        # accept(), а не проблема очерёдности. set_state(NULL) в GStreamer
        # ВСЕГДА синхронный (в отличие от PLAYING/PAUSED, у него нет ASYNC) —
        # надёжного таймаута на уровне API нет. Поэтому НЕ ждём его вообще:
        # сбрасываем html_busy/html_refs СРАЗУ (можно сразу показывать
        # следующий HTML), а фактическую уборку старого GStreamer-бина
        # запускаем в собственном фоновом потоке fire-and-forget — если он там
        # зависнет, это уже не блокирует ни html_busy, ни новый показ.
        try:
            if proc.poll() is None:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, 15)
                proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        self.html_refs = None
        self.html_proc = None
        self.html_busy = False
        print("[streamG] HTML снят", flush=True)
        threading.Thread(target=self._remove_comp_pad, args=(b, pad), daemon=True).start()
        return False

    def stop_html(self):
        if self.html_refs:
            b, pad, _port = self.html_refs
            proc = self.html_proc
            threading.Thread(target=self._html_finish, args=(b, pad, proc), daemon=True).start()
        return "OK stophtml"


    # ---------------- control-сокет (движок): команды от агента
    def _control_server(self):
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", self.control_port))
            srv.listen(8)
            srv.settimeout(1.0)
        except Exception as e:
            print(f"[streamG] control-сокет не поднялся: {e}", flush=True)
            return
        print(f"[streamG] control на 127.0.0.1:{self.control_port}", flush=True)
        while not self.stopping:
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                data = conn.recv(8192).decode("utf-8", "replace").strip()
                conn.sendall((self._dispatch(data) + "\n").encode())
            except Exception as e:
                try:
                    conn.sendall(f"ERR {e}\n".encode())
                except Exception:
                    pass
            finally:
                conn.close()
        srv.close()

    def _dispatch(self, line):
        p = line.split()
        if not p:
            return "ERR empty"
        if p[0] == "banner" and len(p) >= 2:
            # banner <path> <dur> <fade> [x y w h]
            rect = None
            if len(p) >= 8:
                rect = (int(p[4]), int(p[5]), int(p[6]), int(p[7]))
            return self.show_banner(p[1], p[2] if len(p) > 2 else 30,
                                    p[3] if len(p) > 3 else 1, rect)
        if p[0] == "logo" and len(p) >= 2:
            if p[1] == "off":
                return self.clear_logo()
            # logo <path> <x> <y> [w] [h]
            return self.show_logo(p[1], p[2] if len(p) > 2 else 20,
                                  p[3] if len(p) > 3 else 20,
                                  p[4] if len(p) > 4 else 0, p[5] if len(p) > 5 else 0)
        if p[0] == "video" and len(p) >= 2:
            # video <path> [gain]
            return self.play_video(p[1], p[2] if len(p) > 2 else None)
        if p[0] == "stopvideo":
            return self.stop_video()
        if p[0] == "outvol" and len(p) >= 2:
            return self.set_out_volume(p[1])
        if p[0] == "advol" and len(p) >= 2:
            return self.set_ad_volume(p[1])
        if p[0] == "levels":
            return "OK " + json.dumps(self.last_levels)
        if p[0] == "mic" and len(p) >= 2:
            return self.start_mic(p[1], p[2] if len(p) > 2 else None)
        if p[0] == "micoff":
            return self.stop_mic()
        if p[0] == "micvol" and len(p) >= 2:
            return self.set_mic_volume(p[1])
        if p[0] == "obs" and len(p) >= 2:
            # obs <streamid> [mute|keep] [fade]
            return self.start_obs(p[1], p[2] if len(p) > 2 else "mute",
                                  p[3] if len(p) > 3 else None)
        if p[0] == "obslive":
            return self.go_live_obs()
        if p[0] == "obsoff":
            return self.stop_obs()
        if p[0] == "obsvol" and len(p) >= 2:
            return self.set_obs_volume(p[1])
        if p[0] == "html" and len(p) >= 2:
            # html <path> [x] [y] [w] [h] [fps] [duration] [fade]
            defaults = ["0", "0", "0", "0", "15", "30", "1"]
            rest = (p[2:] + defaults)[:7]
            return self.show_html(p[1], rest[0], rest[1], rest[2], rest[3],
                                  rest[4], rest[5], rest[6])
        if p[0] == "stophtml":
            return self.stop_html()
        if p[0] == "setinput" and len(p) >= 2:
            return self.set_input(p[1])
        if p[0] == "testcut":
            return self.test_cut_input(p[1] if len(p) > 1 else 20)
        if p[0] == "ping":
            return "OK " + json.dumps(self.status())
        return f"ERR unknown cmd {p[0]}"

    # ---------------- заглушка
    def _set_slate(self, on):
        if self.slate_on == on:
            return
        comp = self.p_out.get_by_name("comp")
        if not comp:
            return
        pad = comp.get_static_pad("sink_1")
        if not pad:
            return
        self.slate_on = on   # сразу — защита от повторного вызова, пока идёт fade
        print(f"[streamG] заглушка {'ВКЛ' if on else 'выкл'}", flush=True)
        # плавный переход (0.4с), а не жёсткий скачок alpha 0↔1 — резкий скачок
        # выглядит как «мигание» на стыке с реальной картинкой (см. жалобу
        # пользователя на рывки при переключении заглушки).
        threading.Thread(target=self._slate_fade, args=(pad, on), daemon=True).start()

    def _slate_fade(self, pad, on):
        steps, dur = 10, 0.4
        for i in range(steps + 1):
            a = i / steps if on else 1 - i / steps
            try:
                pad.set_property("alpha", a)
            except Exception:
                return
            time.sleep(dur / steps)

    def _watchdog(self):
        """Вход молчит дольше порога → заглушка; вернулся → эфир.

        ФОРСИРОВАННЫЙ РЕКОННЕКТ ПО ТИШИНЕ (найдено вживую на Nickelodeon):
        обычный реконнект (_on_in_msg → _restart_input) срабатывает ТОЛЬКО на
        явные GStreamer ERROR/EOS от uridecodebin — а провайдер может просто
        молча ЗАВИСНУТЬ (соединение висит, ни ошибки, ни данных), тогда
        _restart_input никогда не вызывается, и эфир не приходит НАВСЕГДА, хотя
        сам источник давно снова доступен. Поэтому здесь же, по одной лишь
        длительной тишине (не дожидаясь ошибки), сами дёргаем restart_input —
        с кулдауном, чтобы не заспамить реконнектами при реально мёртвом входе."""
        FORCE_RECONNECT_AFTER = 30.0    # с; заметно дольше slate_after+buffer_sec
        RECONNECT_COOLDOWN = 15.0
        last_forced = 0.0
        while not self.stopping:
            time.sleep(1)
            now = time.time()
            silent = now - self.last_feed_ts if self.last_feed_ts else (now - self.in_started_at)
            if self.slate and self.p_out:
                if silent > self.buffer_sec + self.slate_after:
                    # буфер (buffer_sec) уже слился, экран на выходе почернел
                    # + небольшой запас (slate_after) — показываем заглушку
                    GLib.idle_add(lambda: self._set_slate(True) or False)
                elif (silent < 2.0 and self.slate_on
                        and now - self.in_started_at > self.buffer_sec):
                    # ВАЖНО: новый кадр на ВХОДЕ (silent<2.0) ещё не значит, что
                    # он уже дошёл до ВЫХОДА — каждый кадр держится buffer_sec
                    # в резервуаре-очереди (intervideosink ts-offset=buffer_sec,
                    # см. _in_desc). Снимать заглушку раньше, чем это время
                    # пройдёт с момента пересборки входа (in_started_at),
                    # означало показать зрителю ЧЁРНЫЙ кадр из-под заглушки —
                    # найдено вживую («заглушка выключилась — опять чёрный
                    # экран — потом эфир»). Ждём полный буфер после реконнекта.
                    GLib.idle_add(lambda: self._set_slate(False) or False)
            if (silent > FORCE_RECONNECT_AFTER and not self._restart_pending
                    and now - last_forced > RECONNECT_COOLDOWN):
                last_forced = now
                print(f"[streamG] вход молчит {silent:.0f}с без явной ошибки — "
                      f"форсированный реконнект", flush=True)
                # НЕ GLib.idle_add — см. _on_in_msg: set_state(NULL) в
                # _restart_input блокирующий, на GLib-лупе рискует застрять.
                threading.Thread(target=self._restart_input, daemon=True).start()

    # ---------------- публичный API
    def _mk_leaky_queue(self):
        q = Gst.ElementFactory.make("queue")
        q.set_property("leaky", 2)               # 2 = downstream: дропаем старое
        q.set_property("max-size-buffers", 400)
        q.set_property("max-size-bytes", 0)
        q.set_property("max-size-time", 0)
        return q

    def _probe_languages(self):
        """ffprobe входа (в фоновом потоке, чтобы не блокировать GLib-луп) —
        достаём ISO-639 языковые теги audio-дорожек ИСТОЧНИКА (если провайдер
        их вообще передаёт — не все это делают). self.track_languages[i] —
        язык i-й по счёту audio-дорожки источника (тот же порядок, что и
        audio_seen в _on_decodebin_pad_added/audio_tracks). Используется
        _srt_out_build, чтобы промаркировать каждую дорожку в PMT мультиязыка —
        иначе Flussonic/плееры видят все дорожки одинаково подписанными "a1"
        с разными PID, и непонятно, где какой язык."""
        try:
            p = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", "a",
                 "-show_entries", "stream_tags=language",
                 "-of", "csv=p=0", self.input_url],
                capture_output=True, text=True, timeout=8)
            self.track_languages = [ln.strip() for ln in p.stdout.strip().splitlines()]
        except Exception as e:
            print(f"[streamG] ffprobe языков не удался: {e}", flush=True)

    def _lang_for_track(self, track_idx):
        """ISO-639 код языка для i-й входной audio-дорожки, или None если
        источник его не передал (тогда taginject просто не ставим — лучше
        никак, чем угадать неверно).

        ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ (проверено вживую): сам тег корректно долетает
        до PMT в исходном TS (taginject → mpegtsmux пишет ISO-639 language
        descriptor правильно — проверено прямым дампом байт ДО MediaMTX), НО
        MediaMTX при ретрансляции по SRT эти дескрипторы теряет — на стороне
        читателя (Flussonic и т.п.) языковой тег уже не виден, дорожки снова
        неотличимы кроме как по PID. Это ограничение MediaMTX, не нашего кода;
        чинить/обходить его — отдельная задача (например, писать язык другим
        способом, который MediaMTX не режет, или обновить/патчить MediaMTX)."""
        if 0 <= track_idx < len(self.track_languages):
            code = self.track_languages[track_idx]
            if code and code.lower() != "und":
                return code
        return None

    def _srt_out_build(self):
        """Динамически пристроить мультиязычную SRT-ветку к УЖЕ работающему P2:
        mpegtsmux (video из vtee + N×AAC) → srtsink publish в MediaMTX. Строим на
        ходу (а не в статическом пайплайне), чтобы не ловить гонку негоциации на
        префролле — тем же приёмом, что и mediamtx_branch тапает vtee/atee вживую.

        ВАЖНО (реальная гонка, была найдена здесь): раньше tsmux/srt синкались
        В КОНЦЕ, уже ПОСЛЕ того как тапы с vtee/atee запускали в них буферы —
        элементы-потребители (tsmux) в этот момент ещё в NULL и не готовы
        принимать данные, что и подвешивало сборку. Правильный порядок — весь
        бин (mux+sink+все внутренние цепочки) поднимается в PLAYING ЦЕЛИКОМ,
        и только ПОТОМ к нему цепляется живой tee — тогда к моменту прихода
        первого буфера вся цепочка уже готова."""
        if self.stopping or not self.p_out or self.srt_built:
            return False
        try:
            host, _, sport = self.mediamtx_srt.partition(":")
            path = self.mediamtx_path()
            p = self.p_out
            vtee = p.get_by_name("vtee"); atee = p.get_by_name("atee")
            mk = Gst.ElementFactory.make

            b = Gst.Bin.new("srtbin")
            tsmux = mk("mpegtsmux"); tsmux.set_property("alignment", 7)
            srt = mk("srtsink")
            srt.set_property("uri", f"srt://{host}:{sport}")
            srt.set_property("streamid", f"publish:{path}")   # mode по умолч. caller
            srt.set_property("sync", False)
            srt.set_property("wait-for-connection", False)
            srt.set_property("async", False)
            b.add(tsmux); b.add(srt); tsmux.link(srt)

            # видео и дорожка 0 (готовая AAC с рекламой): ghost-пэды наружу бина —
            # к ним подключим request-пэды vtee/atee СНАРУЖИ, уже после того как
            # весь бин будет PLAYING. ВАЖНО: между tee и mpegtsmux нужен
            # ПЕРЕПАРСЕР — h264 из vtee негоциирован в stream-format=avc (под
            # flvmux основного выхода), а mpegtsmux требует byte-stream; aac из
            # atee — raw (под flvmux), mpegtsmux хочет adts. h264parse/aacparse
            # переформатируют без перекодирования (иначе GST_PAD_LINK_NOFORMAT).
            vq = self._mk_leaky_queue(); vpe = mk("h264parse")
            b.add(vq); b.add(vpe); vq.link(vpe)
            vpe.get_static_pad("src").link(tsmux.request_pad_simple("sink_%d"))
            b.add_pad(Gst.GhostPad.new("vsink", vq.get_static_pad("sink")))

            aq0 = self._mk_leaky_queue(); ape0 = mk("aacparse")
            b.add(aq0); b.add(ape0); aq0.link(ape0)
            out0 = ape0
            lang0 = self._lang_for_track(self.audio_tracks[0])
            if lang0:
                tag0 = mk("taginject")
                tag0.set_property("tags", f"language-code=(string){lang0}")
                b.add(tag0); ape0.link(tag0); out0 = tag0
            out0.get_static_pad("src").link(tsmux.request_pad_simple("sink_%d"))
            b.add_pad(Gst.GhostPad.new("asink0", aq0.get_static_pad("sink")))

            # дорожки 1..N-1: свой inter-канал, целиком внутри бина (не тап с
            # живого tee — отдельный источник, гонки с ним нет).
            for i in range(1, len(self.audio_tracks)):
                src = mk("interaudiosrc"); src.set_property("channel", f"{self.ch}a{i}")
                cf = mk("capsfilter"); cf.set_property("caps", Gst.Caps.from_string(ACAPS))
                q1 = self._mk_leaky_queue()
                aconv = mk("audioconvert")
                aac = mk("avenc_aac"); aac.set_property("bitrate", self.abitrate * 1000)
                ap = mk("aacparse")
                lang = self._lang_for_track(self.audio_tracks[i])
                tag = mk("taginject") if lang else None
                els = (src, cf, q1, aconv, aac, ap) + ((tag,) if tag else ())
                for e in els:
                    b.add(e)
                src.link(cf); cf.link(q1); q1.link(aconv)
                aconv.link(aac); aac.link(ap)
                out = ap
                if tag:
                    tag.set_property("tags", f"language-code=(string){lang}")
                    ap.link(tag); out = tag
                out.get_static_pad("src").link(tsmux.request_pad_simple("sink_%d"))

            p.add(b)
            b.sync_state_with_parent()   # весь бин PLAYING ДО подключения к живому tee

            vtpad = vtee.request_pad_simple("src_%u")
            atpad = atee.request_pad_simple("src_%u")
            vtpad.link(b.get_static_pad("vsink"))
            atpad.link(b.get_static_pad("asink0"))

            self.srt_built = True
            print(f"[streamG] SRT-мультиязык поднят → srt://{host}:{sport} "
                  f"publish:{path} ({len(self.audio_tracks)} дорожек)", flush=True)
        except Exception as e:
            print(f"[streamG] SRT-мультиязык не построился: {e}", flush=True)
        return False        # одноразово (timeout_add снимаем возвратом False)

    def start(self):
        self.p_out = Gst.parse_launch(self._out_desc())
        bus = self.p_out.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_out_msg)
        self.p_out.set_state(Gst.State.PLAYING)
        self._build_input()
        # мультиязык: SRT-ветку пристраиваем через несколько секунд, когда P2 уже
        # прокачивается (см. _srt_out_build — почему не в статическом пайплайне).
        # Языки пробуем ffprobe'ом ПАРАЛЛЕЛЬНО в фоновом потоке — к моменту
        # сборки SRT-ветки (4с) они обычно уже готовы (см. _probe_languages).
        if self.multi_audio and self.mediamtx_srt and self.mode == "publish":
            threading.Thread(target=self._probe_languages, daemon=True).start()
            # 8с (не 4, как раньше) — ffprobe языковых тегов на реальных HLS-
            # источниках сам по себе занимает несколько секунд (сетевой round-
            # trip до провайдера); при 4с проба часто не успевала до сборки
            # SRT-ветки, и language-теги молча пропадали (не баг — просто race,
            # tag_for_track тихо возвращал None). SRT-ветке лишние 4с на общем
            # 12с-буфере эфира не заметны.
            GLib.timeout_add_seconds(8, self._srt_out_build)
        threading.Thread(target=self._watchdog, daemon=True).start()
        if self.control_port:      # движок: приём команд оверлеев от агента
            threading.Thread(target=self._control_server, daemon=True).start()
        threading.Thread(target=self.loop.run, daemon=True).start()

    def stop(self):
        self.stopping = True
        # html_proc (Chromium под venv-python) — своя process-group
        # (start_new_session=True в _html_build), убийство gst_streamg.py её не
        # затрагивает — без явного killpg здесь Chromium осиротеет и продолжит
        # жрать CPU (тот же класс бага, что был найден и исправлен в _kill_group
        # агента: os.killpg() нужен именно потому что дочерний процесс не в той
        # же группе, что родитель).
        if self.html_proc and self.html_proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.html_proc.pid), 15)
            except Exception:
                try:
                    self.html_proc.kill()
                except Exception:
                    pass
        for p in (self.p_in, self.p_out):
            if p:
                p.set_state(Gst.State.NULL)
        self.loop.quit()

    def status(self):
        silent = round(time.time() - self.last_feed_ts, 1) if self.last_feed_ts else None
        return {"input_silent_sec": silent, "input_restarts": self.in_restarts,
                "slate_on": self.slate_on, "banner_busy": self.banner_busy,
                "logo_on": self.logo_pad is not None, "video_busy": self.video_busy,
                "html_busy": self.html_busy, "mic_on": self.mic_bin is not None,
                "obs_connected": self.obs_bin is not None,
                "obs_live": self.obs_is_live,
                "error": self.last_error}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default=None, help="RTMP-адрес (режим publish)")
    ap.add_argument("--mode", choices=["publish", "relay"], default="publish")
    ap.add_argument("--relay-port", type=int, default=None,
                    help="UDP-порт локального выхода (режим relay)")
    ap.add_argument("--buffer", type=float, default=12)
    ap.add_argument("--slate", default=None)
    ap.add_argument("--width", type=int, default=1920)
    ap.add_argument("--height", type=int, default=1080)
    ap.add_argument("--fps", type=int, default=25)
    ap.add_argument("--vbitrate", type=int, default=6000)
    ap.add_argument("--banner-width", type=int, default=1920)
    ap.add_argument("--banner-height", type=int, default=150)
    ap.add_argument("--control-port", type=int, default=None,
                    help="TCP-порт управления оверлеями (движок; publish-режим)")
    ap.add_argument("--venv-python", default=None,
                    help="python агента (Playwright+Pillow) для HTML-слоя, по умолчанию /opt/ad-streamer/venv/bin/python3")
    ap.add_argument("--capture-html-script", default=None)
    ap.add_argument("--channel", default="sg0",
                    help="имя intervideo/interaudio-канала внутри процесса; "
                         "агент задаёт уникальное на поток (sg<id>)")
    ap.add_argument("--mediamtx-rtmp-port", type=int, action="append", dest="mediamtx_rtmp_ports",
                    help="постоянный доп. push в локальный инстанс MediaMTX "
                         "(RTSP/HLS/SRT/WebRTC для внешних плееров); можно "
                         "указать несколько раз — по одному на инстанс "
                         "(напр. low-latency HLS и классический HLS отдельно)")
    ap.add_argument("--mediamtx-path", default=None,
                    help="имя пути в MediaMTX (агент считает его сам из имени "
                         "потока/output_url); без него — вывод из --output")
    ap.add_argument("--audio-track", type=int, action="append", dest="audio_tracks",
                    help="индекс аудиодорожки входа (0 = первая). Можно указать "
                         "несколько раз — тогда мультиязык: все дорожки уходят "
                         "N×AAC → mpegtsmux → srtsink в MediaMTX (см. --mediamtx-srt); "
                         "FLV-выходы (Flussonic RTMP, классический HLS) несут первую")
    ap.add_argument("--mediamtx-srt", default=None,
                    help="host:port SRT-инстанса MediaMTX для multi-audio publish "
                         "(нужен только при >1 --audio-track)")
    ap.add_argument("--seconds", type=int, default=0)
    a = ap.parse_args()
    if a.mode == "relay" and not a.relay_port:
        ap.error("--mode relay требует --relay-port")
    # publish без --output разрешён, ЕСЛИ есть хоть один --mediamtx-rtmp-port
    # (поток идёт только в MediaMTX, во Flussonic не публикуется вообще) —
    # иначе выхода нет вообще никакого, это ошибка конфигурации.
    if a.mode == "publish" and not a.output and not a.mediamtx_rtmp_ports:
        ap.error("--mode publish требует --output или --mediamtx-rtmp-port")
    s = StreamG(a.input, a.output, buffer_sec=a.buffer, slate=a.slate,
                w=a.width, h=a.height, fps=a.fps, vbitrate=a.vbitrate,
                mode=a.mode, relay_port=a.relay_port, channel=a.channel,
                banner_w=a.banner_width, banner_h=a.banner_height,
                control_port=a.control_port, venv_python=a.venv_python,
                capture_html_script=a.capture_html_script,
                mediamtx_rtmp_ports=a.mediamtx_rtmp_ports,
                mediamtx_path_override=a.mediamtx_path,
                audio_tracks=a.audio_tracks, mediamtx_srt=a.mediamtx_srt)
    s.start()
    t0 = time.time()
    try:
        while s.last_error is None:
            time.sleep(2)
            print(f"[streamG] t={int(time.time()-t0)}s {s.status()}", flush=True)
            if a.seconds and time.time() - t0 > a.seconds:
                break
    except KeyboardInterrupt:
        pass
    finally:
        s.stop()
        print(f"[streamG] остановлен: {s.status()}", flush=True)


if __name__ == "__main__":
    main()
