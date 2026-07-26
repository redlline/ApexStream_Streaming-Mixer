#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML → сырые RGBA-кадры по TCP, для движка GStreamer (gst_streamg.py).

Не использует ffmpeg вообще: рендерит HTML в headless Chromium (тот же
проверенный CDP-скринкаст, что и html_feeder.py для ffmpeg-микшера), но вместо
кодирования в h264 просто декодирует каждый PNG-кадр через Pillow и шлёт сырые
RGBA-байты подряд по TCP-соединению. Приёмник (gst_streamg.py, движок) читает
их через `tcpclientsrc ! rawvideoparse format=rgba` — GStreamer сам режет
непрерывный поток байт на кадры по известным width/height/fps, декодирования
не нужно (raw video не сжат) — значит не проходит через decodebin/автоплаг и
не рискует наткнуться на аппаратный NVDEC, как это было с баннером-mp4.

Почему TCP, а не UDP: один кадр (например 1920×150 RGBA) — это ~1.1 МБ, что
намного больше предельного размера UDP-датаграммы (65507 байт) — пришлось бы
вручную дробить и склеивать. TCP — обычный непрерывный поток байт, без этой
головной боли, и надёжен (в отличие от UDP не теряет пакеты).

Почему процесс отдельный (не часть gst_streamg.py): Playwright всегда живёт в
venv агента (там же Chromium), а gst_streamg.py — под системным python3 (там
GStreamer-биндинги gi). Эти два окружения не пересекаются, поэтому HTML
рендерится отдельным процессом под venv, который просто отправляет байты по
сети — gst_streamg.py на другой стороне ничего не знает про Playwright.

Использование:
  python capture_html_raw.py <html_path> <w> <h> <fps> <host> <port> <duration>
"""
import socket, sys, threading, time, queue


def transparent_rgba(w, h):
    return bytes(w * h * 4)   # всё по нулям = чёрный, alpha=0 (прозрачный)


def main():
    html_path, w, h, fps, host, port, duration = sys.argv[1:8]
    w, h, fps = int(w), int(h), int(fps)
    port = int(port)
    duration = float(duration)
    frame_size = w * h * 4   # RGBA, 4 байта на пиксель

    # подключаемся как TCP-клиент к уже слушающему gst_streamg (tcpserversrc
    # поднимается ДО спавна этого процесса — но на случай гонки пробуем чуть
    # подождать переподключением)
    sock = None
    for _ in range(50):
        try:
            sock = socket.create_connection((host, port), timeout=2)
            break
        except OSError:
            time.sleep(0.1)
    if sock is None:
        print(f"[capture_html] не удалось подключиться к {host}:{port}", flush=True)
        sys.exit(1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    last_frame = [transparent_rgba(w, h)]
    last_write = [0.0]
    stop = threading.Event()

    # ВАЖНО: отправка по сети — В ОТДЕЛЬНОМ потоке, никогда не в потоке захвата
    # кадра. Раньше sock.sendall() вызывался прямо из цикла screenshot() —
    # любая заминка на TCP-записи (даже на loopback, если gst_streamg.py на
    # той стороне на миг отстал с чтением) съедала время следующего кадра.
    # Для статичного баннера незаметно, а бегущую строку/анимацию видимо
    # подёргивало — именно потому что там важна РОВНАЯ частота кадров.
    # Очередь maxsize=2: если писатель вдруг отстал — не копим старые кадры
    # (эфир важнее полноты), а перезаписываем последний ожидающий.
    frame_q = queue.Queue(maxsize=2)

    def writer():
        while not stop.is_set():
            try:
                raw = frame_q.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                sock.sendall(raw)
                last_write[0] = time.monotonic()
            except OSError:
                stop.set()

    def write_frame(raw):
        try:
            frame_q.put_nowait(raw)
        except queue.Full:
            try:
                frame_q.get_nowait()   # выбросить самый старый ожидающий
            except queue.Empty:
                pass
            try:
                frame_q.put_nowait(raw)
            except queue.Full:
                pass

    def keepalive():
        """Преролл + пульс для статичных страниц (CDP шлёт кадры только на
        изменения) — повторяем последний кадр, если новых не было >150мс."""
        while not stop.is_set():
            if time.monotonic() - last_write[0] > 0.15:
                write_frame(last_frame[0])
            time.sleep(0.05)

    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=keepalive, daemon=True).start()
    t_end = time.monotonic() + duration

    from playwright.sync_api import sync_playwright
    from PIL import Image
    import io

    # ВАЖНО: захват через page.screenshot(omit_background=True), а НЕ через
    # Page.startScreencast. Скринкаст снимает вывод компоновщика Chromium с
    # НЕПРОЗРАЧНЫМ фоном — прозрачные/пустые области страницы приходят ЧЁРНЫМИ
    # (проверено вживую: под баром лоуэр-третей была сплошная чернота вместо
    # эфира). screenshot(omit_background=True) даёт настоящий PNG с альфой —
    # пустые области прозрачны, под баннером виден эфир. Немного медленнее
    # скринкаста (round-trip на кадр), но для эфирной графики 15fps достаточно.
    frame_interval = 1.0 / max(1, fps)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(args=["--no-sandbox", "--disable-gpu",
                                               "--hide-scrollbars"])
            # transparent viewport: omit_background работает только когда у самой
            # страницы фон прозрачен — Playwright гасит дефолтный белый фон
            page = browser.new_page(viewport={"width": w, "height": h})
            page.route("**/*", lambda route: route.continue_()
                       if route.request.url.startswith(("file://", "data:"))
                       else route.abort())
            page.goto("file://" + html_path.replace("\\", "/"))
            # ПРИЖИМАЕМ контент к НИЗУ зоны. Шаблоны — это div фиксированной
            # высоты (150/120/160px) в нормальном потоке, вверху документа. Если
            # зона баннера выше (напр. 360px), контент оказывался вверху зоны =
            # середина экрана. Делаем body flex-колонкой с выравниванием вниз —
            # плашка прижимается к нижней кромке зоны (= низ экрана), а над ней
            # прозрачно (виден эфир). Для absolute-позиционированных элементов
            # flex не мешает — они и так лежат по своим координатам.
            try:
                page.add_style_tag(content=(
                    "html,body{margin:0!important;padding:0!important;"
                    "width:100%!important;height:100%!important;}"
                    "body{display:flex!important;flex-direction:column!important;"
                    "justify-content:flex-end!important;}"))
            except Exception:
                pass
            clip = {"x": 0, "y": 0, "width": w, "height": h}
            while not stop.is_set() and time.monotonic() < t_end:
                t0 = time.monotonic()
                try:
                    png = page.screenshot(omit_background=True, type="png",
                                          clip=clip, timeout=5000)
                    im = Image.open(io.BytesIO(png)).convert("RGBA")
                    if im.size != (w, h):
                        im = im.resize((w, h))
                    raw = im.tobytes()
                    if len(raw) == frame_size:
                        last_frame[0] = raw
                        write_frame(raw)
                except Exception as e:
                    print(f"[capture_html] кадр пропущен: {e}", flush=True)
                # держим целевой fps: спим остаток интервала (но не копим долг)
                dt = time.monotonic() - t0
                if dt < frame_interval:
                    time.sleep(frame_interval - dt)
            browser.close()
    finally:
        stop.set()
        try:
            sock.close()
        except OSError:
            pass


if __name__ == "__main__":
    main()
