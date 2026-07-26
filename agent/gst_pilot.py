#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GStreamer-пилот для ad-streamer: вход провайдера (HLS) → буфер задержки (~12с)
→ NVENC (H.264) → publish в Flussonic по RTMP.

Зачем: развязать срывы провайдера от зрителя. Очередь держит запас, и на
коротком (≤ размера буфера) провале провайдера выход продолжает идти ИЗ буфера,
а не рвётся у зрителя. Дополнительно GStreamer переживает добавление оверлеев
(баннер/ролик) без рестарта — это Фаза 2.

Ключевое отличие от «сырого» gst-launch: мы НЕ ставим пайплайн на паузу по
buffering-сообщениям. Именно из-за этого gst-launch на обрыве делал
Buffering → PAUSED (пауза вместо слива). Для live мы буферизацию только логируем,
а очередь на провале сливается сама.

Резервуар задержки при HLS-входе набирается из «забега» hlsdemux по плейлисту
(он докачивает сегменты вперёд быстрее реального времени, очередь наполняется).
Глубину реального резервуара показывает тест обрыва — сколько секунд выход
переживает пропажу провайдера.

Standalone-запуск (для теста, без агента/UI):
  python3 gst_pilot.py --input <hls_url> \
      --output rtmp://127.0.0.1:1935/live/stream_ad_3 --buffer 12 --seconds 60
"""
import gi, argparse, threading, time
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)


class GstPilot:
    """Управляемый пайплайн: вход → буфер → NVENC → RTMP-publish."""

    def __init__(self, input_url, output_rtmp, buffer_sec=12,
                 vbitrate=6000, abitrate=128, gop=50, with_audio=True):
        self.input_url = input_url
        self.output_rtmp = output_rtmp
        self.buffer_ns = int(buffer_sec * Gst.SECOND)
        self.vbitrate = vbitrate            # kbit/s (nvh264enc)
        self.abitrate = abitrate            # kbit/s (avenc_aac хочет bit/s)
        self.gop = gop
        self.with_audio = with_audio
        self.pipeline = None
        self.loop = None
        self._thread = None
        self.last_error = None
        self.last_buffering = None
        self.running = False

    def _desc(self):
        # rtmpsink: 'live=1' в location; sync=false — публикуем по мере готовности,
        # задержку даёт очередь (queue), а не sink.
        # nvh264enc: bframes=0 + zerolatency=true — без переупорядочивания, как
        # в нашем ffmpeg (-tune ll), чтобы транскодер Flussonic ниже не давился.
        audio = ""
        if self.with_audio:
            audio = (f' dec. ! queue name=abuf max-size-time={self.buffer_ns} '
                     f'max-size-bytes=0 max-size-buffers=0 ! audioconvert ! '
                     f'audioresample ! avenc_aac bitrate={self.abitrate * 1000} ! mux.')
        return (
            f'uridecodebin uri="{self.input_url}" name=dec '
            f'buffer-duration={self.buffer_ns} '
            f'dec. ! queue name=vbuf max-size-time={self.buffer_ns} '
            f'max-size-bytes=0 max-size-buffers=0 ! videoconvert ! '
            f'nvh264enc name=enc bitrate={self.vbitrate} rc-mode=cbr '
            f'gop-size={self.gop} bframes=0 zerolatency=true ! '
            f'h264parse ! flvmux name=mux streamable=true ! '
            f'rtmpsink name=pub sync=false location="{self.output_rtmp} live=1"'
            + audio
        )

    def _on_msg(self, bus, msg):
        t = msg.type
        if t == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            self.last_error = f"{err.message}: {dbg}"
            print(f"[pilot] ERROR: {self.last_error}", flush=True)
            if self.loop:
                self.loop.quit()
        elif t == Gst.MessageType.EOS:
            print("[pilot] EOS (источник закончился)", flush=True)
            if self.loop:
                self.loop.quit()
        elif t == Gst.MessageType.WARNING:
            w, dbg = msg.parse_warning()
            print(f"[pilot] WARN: {w.message}: {dbg}", flush=True)
        elif t == Gst.MessageType.BUFFERING:
            # КЛЮЧЕВОЕ: для live НЕ меняем состояние пайплайна по буферизации,
            # только логируем. Иначе получаем «пауза вместо слива» как gst-launch.
            self.last_buffering = msg.parse_buffering()
        return True

    def start(self):
        self.pipeline = Gst.parse_launch(self._desc())
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_msg)
        self.loop = GLib.MainLoop()
        self.pipeline.set_state(Gst.State.PLAYING)
        self.running = True
        self._thread = threading.Thread(target=self.loop.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
        if self.loop:
            self.loop.quit()

    def output_pos(self):
        """Позиция по выходу (сек) — грубый индикатор, что конвейер продвигается."""
        if not self.pipeline:
            return None
        ok, pos = self.pipeline.query_position(Gst.Format.TIME)
        return round(pos / Gst.SECOND, 1) if ok and pos >= 0 else None

    def alive(self):
        return self.running and self.last_error is None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--buffer", type=float, default=12)
    ap.add_argument("--vbitrate", type=int, default=6000)
    ap.add_argument("--no-audio", action="store_true")
    ap.add_argument("--seconds", type=int, default=0,
                    help="автостоп через N сек (0 = бесконечно)")
    a = ap.parse_args()
    p = GstPilot(a.input, a.output, buffer_sec=a.buffer, vbitrate=a.vbitrate,
                 with_audio=not a.no_audio)
    print("[pilot] pipeline:", p._desc(), flush=True)
    p.start()
    t0 = time.time()
    try:
        while p.running:
            time.sleep(2)
            print(f"[pilot] t={int(time.time() - t0)}s out_pos={p.output_pos()} "
                  f"buffering={p.last_buffering}%", flush=True)
            if a.seconds and time.time() - t0 > a.seconds:
                break
    except KeyboardInterrupt:
        pass
    finally:
        p.stop()
        print(f"[pilot] остановлен (последняя ошибка: {p.last_error})", flush=True)


if __name__ == "__main__":
    main()
