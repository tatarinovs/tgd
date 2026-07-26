import os
import json
import time
import queue
import threading
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse
from html import escape


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TGD WebUI</title>
<style>
  :root {
    --bg: #0f172a;
    --card-bg: #1e293b;
    --text: #f8fafc;
    --text-muted: #94a3b8;
    --border: #334155;
    --primary: #38bdf8;
    --primary-hover: #0284c7;
    --danger: #ef4444;
    --danger-hover: #dc2626;
    --success: #22c55e;
    --warning: #eab308;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background-color: var(--bg);
    color: var(--text);
    padding: 24px 16px;
    max-width: 900px;
    margin: 0 auto;
    line-height: 1.5;
  }
  h1 { font-size: 1.75rem; margin-bottom: 20px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 10px; }
  .badge-daemon { font-size: 0.75rem; background: rgba(56, 189, 248, 0.15); color: var(--primary); padding: 4px 8px; border-radius: 6px; border: 1px solid rgba(56, 189, 248, 0.3); }
  .card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
  }
  .card h3 { font-size: 1.1rem; margin-bottom: 14px; color: var(--text); font-weight: 600; }
  form { display: flex; flex-wrap: wrap; gap: 10px; }
  input[type="text"] {
    flex: 1;
    min-width: 220px;
    padding: 10px 14px;
    background: #0f172a;
    border: 1px solid var(--border);
    border-radius: 8px;
    color: #fff;
    font-size: 0.95rem;
    outline: none;
    transition: border-color 0.2s;
  }
  input[type="text"]:focus { border-color: var(--primary); }
  button {
    padding: 10px 18px;
    background: var(--primary);
    color: #0f172a;
    font-weight: 600;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-size: 0.95rem;
    transition: background 0.2s, transform 0.1s;
  }
  button:hover { background: var(--primary-hover); color: #fff; }
  button:active { transform: scale(0.98); }
  .btn-danger { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.3); }
  .btn-danger:hover { background: var(--danger); color: #fff; }
  .btn-warning { background: rgba(234, 179, 8, 0.15); color: var(--warning); border: 1px solid rgba(234, 179, 8, 0.3); }
  .btn-warning:hover { background: var(--warning); color: #0f172a; }

  .group-item {
    border-bottom: 1px solid var(--border);
    padding: 16px 0;
  }
  .group-item:last-child { border-bottom: none; }
  .group-header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 8px; }
  .group-title { font-weight: 600; font-size: 1.05rem; }
  .group-meta { font-size: 0.85rem; color: var(--text-muted); }
  .status-badge {
    font-size: 0.8rem;
    padding: 3px 8px;
    border-radius: 6px;
    font-weight: 600;
    display: inline-block;
  }
  .status-downloading { background: rgba(234, 179, 8, 0.15); color: var(--warning); border: 1px solid rgba(234, 179, 8, 0.3); }
  .status-done { background: rgba(34, 197, 94, 0.15); color: var(--success); border: 1px solid rgba(34, 197, 94, 0.3); }
  .status-error { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.3); }

  .stats-text { font-size: 0.88rem; color: #38bdf8; margin-top: 4px; font-weight: 500; }
  .error-text { font-size: 0.88rem; color: var(--danger); margin-top: 4px; }
  .last-run { font-size: 0.78rem; color: var(--text-muted); margin-top: 4px; }
  .hidden { display: none !important; }
</style>
</head>
<body>

<h1>TGD — Загрузчик Telegram <span class="badge-daemon">Daemon mode</span></h1>

<div class="card">
  <h3>Добавить группу или канал</h3>
  <form action="/add" method="POST">
    <input type="text" name="group" placeholder="ID группы (-100...), @username или ссылка https://t.me/..." required>
    <input type="text" name="topic" placeholder="ID темы или Название (опционально)">
    <button type="submit">Добавить</button>
  </form>
</div>

<div class="card">
  <h3>Сохранённые группы</h3>
  <div id="group-list">
    {GROUPS_HTML}
  </div>
</div>

<script>
if (!!window.EventSource) {
  var source = new EventSource('/events');
  source.onmessage = function(e) {
    var data = JSON.parse(e.data);
    if (data.type === "task") {
      var idStr = "group-" + data.id + "-" + (data.topic || "");
      var groupDiv = document.getElementById(idStr);
      if (groupDiv) {
        var statusBadge = groupDiv.querySelector(".status-badge");
        if (data.status) {
          statusBadge.classList.remove("hidden", "status-downloading", "status-done", "status-error");
          statusBadge.innerText = data.status;
          if (data.status.indexOf("Скачивание") !== -1 || data.status.indexOf("Остановка") !== -1) statusBadge.classList.add("status-downloading");
          else if (data.status.indexOf("Завершено") !== -1) statusBadge.classList.add("status-done");
          else if (data.status.indexOf("Ошибка") !== -1 || data.status.indexOf("Отменено") !== -1 || data.status.indexOf("Прервано") !== -1) statusBadge.classList.add("status-error");
        } else {
          statusBadge.classList.add("hidden");
        }

        var actionForm = groupDiv.querySelector(".action-form");
        if (actionForm) {
          var actionBtn = actionForm.querySelector("button");
          if (data.status && (data.status.indexOf("Скачивание") !== -1 || data.status.indexOf("Остановка") !== -1)) {
            actionForm.action = "/stop";
            actionBtn.innerText = "Стоп";
            actionBtn.className = "btn-warning";
          } else {
            actionForm.action = "/download";
            actionBtn.innerText = "Старт";
            actionBtn.className = "";
          }
        }

        var statsWrap = groupDiv.querySelector(".stats-text");
        if (data.stats) {
          statsWrap.classList.remove("hidden");
          statsWrap.innerText = data.stats;
        } else {
          statsWrap.classList.add("hidden");
        }

        var errorWrap = groupDiv.querySelector(".error-text");
        if (data.error) {
          errorWrap.classList.remove("hidden");
          errorWrap.innerText = "Ошибка: " + data.error;
        } else {
          errorWrap.classList.add("hidden");
        }

        var runWrap = groupDiv.querySelector(".last-run");
        if (data.last_run) {
          runWrap.classList.remove("hidden");
          runWrap.innerText = "Последний запуск: " + data.last_run;
        }
      }
    }
  };
}
</script>

</body>
</html>
"""


class SSEBroadcaster:
    def __init__(self):
        self._listeners = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q = queue.Queue(maxsize=100)
        with self._lock:
            self._listeners.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._lock:
            if q in self._listeners:
                self._listeners.remove(q)

    def broadcast(self, data: dict):
        with self._lock:
            if not self._listeners:
                return
            listeners = list(self._listeners)

        msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
        for q in listeners:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def make_request_handler(store, broadcaster, loop, daemon_callbacks):
    class WebUIHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            # Suppress default request logging to avoid stdout clutter
            pass

        def _send_redirect(self, path='/'):
            self.send_response(303)
            self.send_header('Location', path)
            self.end_headers()

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == '/':
                self.handle_index()
            elif parsed.path == '/events':
                self.handle_sse()
            else:
                self.send_error(404, "Not Found")

        def do_POST(self):
            parsed = urlparse(self.path)
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length).decode('utf-8') if length > 0 else ''
            form_data = parse_qs(body)

            if parsed.path == '/add':
                self.handle_add(form_data)
            elif parsed.path == '/remove':
                self.handle_remove(form_data)
            elif parsed.path == '/download':
                self.handle_download(form_data)
            elif parsed.path == '/stop':
                self.handle_stop(form_data)
            else:
                self.send_error(404, "Not Found")

        def handle_index(self):
            groups = store.list()
            groups_html = ""
            if not groups:
                groups_html = "<p style='color: var(--text-muted);'>Нет добавленных групп.</p>"
            else:
                for g in groups:
                    gid = escape(str(g.get('id', '')))
                    title = escape(str(g.get('title', gid)))
                    topic = escape(str(g.get('topic', '')))
                    status = escape(str(g.get('status', '')))
                    stats = escape(str(g.get('stats', '')))
                    error = escape(str(g.get('last_error', '')))
                    last_run = escape(str(g.get('last_run', '')))

                    id_attr = f"group-{gid}-{topic}"
                    topic_meta = f", тема: {topic}" if topic else ""
                    
                    status_class = "status-badge"
                    if "Скачивание" in status or "Остановка" in status:
                        status_class += " status-downloading"
                    elif "Завершено" in status:
                        status_class += " status-done"
                    elif "Ошибка" in status or "Отменено" in status or "Прервано" in status:
                        status_class += " status-error"
                    else:
                        status_class += " hidden"

                    is_running = ("Скачивание" in status or "Остановка" in status)
                    action_url = "/stop" if is_running else "/download"
                    btn_class = "btn-warning" if is_running else ""
                    btn_text = "Стоп" if is_running else "Старт"

                    groups_html += f"""
                    <div class="group-item" id="{id_attr}">
                      <div class="group-header">
                        <div>
                          <span class="group-title">{title}</span>
                          <span class="group-meta">({gid}{topic_meta})</span>
                        </div>
                        <div>
                          <form action="{action_url}" method="POST" style="display:inline;" class="action-form">
                            <input type="hidden" name="id" value="{gid}">
                            <input type="hidden" name="topic" value="{topic}">
                            <button type="submit" class="{btn_class}">{btn_text}</button>
                          </form>
                          <form action="/remove" method="POST" style="display:inline;">
                            <input type="hidden" name="id" value="{gid}">
                            <input type="hidden" name="topic" value="{topic}">
                            <button type="submit" class="btn-danger">Удалить</button>
                          </form>
                        </div>
                      </div>
                      <div>
                        <span class="{status_class}">{status}</span>
                        <div class="stats-text {'hidden' if not stats else ''}">{stats}</div>
                        <div class="error-text {'hidden' if not error else ''}">Ошибка: {error}</div>
                        <div class="last-run {'hidden' if not last_run else ''}">Последний запуск: {last_run}</div>
                      </div>
                    </div>
                    """

            html = HTML_TEMPLATE.replace("{GROUPS_HTML}", groups_html)
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))

        def handle_add(self, form_data):
            group_input = form_data.get('group', [''])[0].strip()
            topic_input = form_data.get('topic', [''])[0].strip()

            if group_input:
                if 'resolve_and_add' in daemon_callbacks:
                    fut = asyncio.run_coroutine_threadsafe(
                        daemon_callbacks['resolve_and_add'](group_input, topic_input),
                        loop
                    )
                    try:
                        fut.result(timeout=15)
                    except Exception as e:
                        print(f"[WebUI] Ошибка разрешения группы: {e}")
                else:
                    store.add(group_input, group_input, topic_input)

            self._send_redirect('/')

        def handle_remove(self, form_data):
            group_id = form_data.get('id', [''])[0].strip()
            topic = form_data.get('topic', [''])[0].strip()
            if group_id:
                store.remove(group_id, topic)
            self._send_redirect('/')

        def handle_download(self, form_data):
            group_id = form_data.get('id', [''])[0].strip()
            topic = form_data.get('topic', [''])[0].strip()

            if group_id and 'trigger_download' in daemon_callbacks:
                asyncio.run_coroutine_threadsafe(
                    daemon_callbacks['trigger_download'](group_id, topic),
                    loop
                )

            self._send_redirect('/')

        def handle_stop(self, form_data):
            group_id = form_data.get('id', [''])[0].strip()
            topic = form_data.get('topic', [''])[0].strip()

            if group_id and 'cancel_download' in daemon_callbacks:
                fut = asyncio.run_coroutine_threadsafe(
                    daemon_callbacks['cancel_download'](group_id, topic),
                    loop
                )
                try:
                    fut.result(timeout=5)
                except Exception as e:
                    print(f"[WebUI] Ошибка остановки задачи: {e}")

            self._send_redirect('/')

        def handle_sse(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.close_connection = True

            q = broadcaster.subscribe()
            try:
                while True:
                    try:
                        msg = q.get(timeout=15.0)
                    except queue.Empty:
                        msg = ": heartbeat\n\n"
                    self.wfile.write(msg.encode('utf-8'))
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            except Exception:
                pass
            finally:
                broadcaster.unsubscribe(q)

    return WebUIHandler


class WebUIServer:
    def __init__(self, host: str, port: int, store, loop, daemon_callbacks: dict):
        self.host = host
        self.port = port
        self.store = store
        self.loop = loop
        self.broadcaster = SSEBroadcaster()
        self.daemon_callbacks = daemon_callbacks
        
        handler_cls = make_request_handler(self.store, self.broadcaster, self.loop, self.daemon_callbacks)
        self.httpd = ThreadedHTTPServer((self.host, self.port), handler_cls)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        print(f"[WebUI] Веб-интерфейс запущен на http://{self.host}:{self.port}")

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def broadcast_task_update(self, group_id: str, topic: str, status: str, stats: str = "", error: str = "", last_run: str = ""):
        self.broadcaster.broadcast({
            "type": "task",
            "id": str(group_id),
            "topic": str(topic or ""),
            "status": status,
            "stats": stats,
            "error": error,
            "last_run": last_run
        })
