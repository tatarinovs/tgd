import os
import sys
import json
import hmac
import base64
import queue
import threading
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse
from html import escape

# Тело POST у веб-морды — это две короткие формы; всё крупнее читать незачем
MAX_BODY_BYTES = 64 * 1024
# Каждое SSE-соединение держит поток, поэтому их количество ограничено
MAX_SSE_LISTENERS = 32
# Пауза между heartbeat: на ней же детектируется отвалившийся браузер
SSE_HEARTBEAT_SEC = 15.0


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
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
  .status-downloading, .status-stopping { background: rgba(234, 179, 8, 0.15); color: var(--warning); border: 1px solid rgba(234, 179, 8, 0.3); }
  .status-done { background: rgba(34, 197, 94, 0.15); color: var(--success); border: 1px solid rgba(34, 197, 94, 0.3); }
  .status-error, .status-done_errors, .status-cancelled { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.3); }

  .stats-text { font-size: 0.88rem; color: #38bdf8; margin-top: 4px; font-weight: 500; }
  .error-text { font-size: 0.88rem; color: var(--danger); margin-top: 4px; }
  .last-run { font-size: 0.78rem; color: var(--text-muted); margin-top: 4px; }
  .hidden { display: none !important; }
</style>
<script>
  const I18N = {
    ru: {
      title: "TGD — Загрузчик Telegram",
      add_title: "Добавить группу или канал",
      input_group: "ID группы (-100...), @username или ссылка https://t.me/...",
      input_topic: "ID темы или Название (опционально)",
      btn_add: "Добавить",
      saved_title: "Сохранённые группы",
      no_groups: "Нет добавленных групп.",
      btn_start: "Старт",
      btn_stop: "Стоп",
      btn_delete: "Удалить",
      topic_lbl: ", тема:",
      err_lbl: "Ошибка:",
      last_run_lbl: "Последний запуск:",
      
      status_downloading: "Скачивание...",
      status_stopping: "Остановка...",
      status_done: "Завершено",
      status_done_errors: "Завершено с ошибками",
      status_error: "Ошибка",
      status_cancelled: "Отменено",

      stats_preparing: "Подготовка...",
      stats_waiting_stop: "Ждём завершения текущих загрузок... (Новых: {new}, Было: {exists}){speed_txt}",
      stats_downloading: "Новых: {new}, Было: {exists}, Пропущено: {skipped}, Ошибок: {error}{speed_txt}",
      stats_stopped: "Новых: {new}, Существует: {exists}, Пропущено: {skipped}, Ошибок: {error} (Остановлено)",
      stats_finished: "Новых: {new}, Существует: {exists}, Пропущено: {skipped}, Ошибок: {error}",

      resources_title: "Ресурсы демона",
      resource_wait: "Сбор метрик...",
      resource_line: "CPU: {cpu} · Память: {rss} МБ · Потоков: {threads} · Активных задач: {jobs} · Аптайм: {uptime}",
      btn_restart: "Перезапустить демон",
      confirm_restart: "Перезапустить демон сейчас? Активные загрузки будут прерваны и продолжатся при следующем запуске."
    },
    en: {
      title: "TGD — Telegram Downloader",
      add_title: "Add Group or Channel",
      input_group: "Group ID (-100...), @username or link https://t.me/...",
      input_topic: "Topic ID or Name (optional)",
      btn_add: "Add",
      saved_title: "Saved Groups",
      no_groups: "No saved groups.",
      btn_start: "Start",
      btn_stop: "Stop",
      btn_delete: "Delete",
      topic_lbl: ", topic:",
      err_lbl: "Error:",
      last_run_lbl: "Last run:",

      status_downloading: "Downloading...",
      status_stopping: "Stopping...",
      status_done: "Done",
      status_done_errors: "Done with errors",
      status_error: "Error",
      status_cancelled: "Cancelled",

      stats_preparing: "Preparing...",
      stats_waiting_stop: "Waiting for current downloads to finish... (New: {new}, Exists: {exists}){speed_txt}",
      stats_downloading: "New: {new}, Exists: {exists}, Skipped: {skipped}, Errors: {error}{speed_txt}",
      stats_stopped: "New: {new}, Exists: {exists}, Skipped: {skipped}, Errors: {error} (Stopped)",
      stats_finished: "New: {new}, Exists: {exists}, Skipped: {skipped}, Errors: {error}",

      resources_title: "Daemon resources",
      resource_wait: "Collecting metrics...",
      resource_line: "CPU: {cpu} · RAM: {rss} MB · Threads: {threads} · Active jobs: {jobs} · Uptime: {uptime}",
      btn_restart: "Restart daemon",
      confirm_restart: "Restart the daemon now? Active downloads will be interrupted and resume on next run."
    }
  };

  let currentLang = 'en';
  if (navigator.language && navigator.language.toLowerCase().startsWith('ru')) {
    currentLang = 'ru';
  }

  function _t(key) {
    return I18N[currentLang][key] || key;
  }

  // Заменяет ВСЕ вхождения каждого плейсхолдера
  function fillAll(tpl, values) {
    for (var placeholder in values) {
      tpl = tpl.split(placeholder).join(values[placeholder]);
    }
    return tpl;
  }

  function parseStats(statsRaw, status) {
    if (!statsRaw) return "";
    try {
      let data = JSON.parse(statsRaw);
      let textKey = data.text_key;
      if (!textKey) {
        if (status === "cancelled") textKey = "stats_stopped";
        else if (status === "downloading") textKey = "stats_downloading";
        else if (status === "stopping") textKey = "stats_waiting_stop";
        else textKey = "stats_finished";
      } else {
        textKey = "stats_" + textKey;
      }
      
      let speed_txt = "";
      if (data.speed > 0.05) {
        speed_txt = " [" + data.speed.toFixed(1) + " MB/s]";
      }
      // Глобальная замена через split/join: replace() со строковым аргументом
      // менял только первое вхождение, и повторный плейсхолдер тихо оставался
      // бы в тексте (регэксп здесь не используем — шаблон лежит в Python-строке).
      let tpl = _t(textKey);
      return fillAll(tpl, {
        "{new}": data.new || 0,
        "{exists}": data.exists || 0,
        "{skipped}": data.skipped || 0,
        "{error}": data.error || 0,
        "{speed_txt}": speed_txt
      });
    } catch (e) {
      return statsRaw; // fallback to raw string (old groups.json)
    }
  }
  
  function parseStatus(statusRaw) {
    let key = "status_" + statusRaw;
    if (I18N[currentLang][key]) return _t(key);
    return statusRaw;
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.documentElement.lang = currentLang;
    document.querySelectorAll("[data-i18n]").forEach(el => {
      let key = el.getAttribute("data-i18n");
      if (key) el.innerText = _t(key);
    });
    document.querySelectorAll("[data-i18n-placeholder]").forEach(el => {
      let key = el.getAttribute("data-i18n-placeholder");
      if (key) el.placeholder = _t(key);
    });

    // Translate statically rendered items
    document.querySelectorAll(".status-badge").forEach(el => {
      let raw = el.getAttribute("data-raw-status");
      if (raw) el.innerText = parseStatus(raw);
    });
    document.querySelectorAll(".stats-text").forEach(el => {
      let raw = el.getAttribute("data-raw-stats");
      let status = el.getAttribute("data-raw-status");
      if (raw) el.innerText = parseStats(raw, status);
    });
    document.querySelectorAll(".topic-meta").forEach(el => {
      let topic = el.getAttribute("data-topic");
      if (topic) el.innerText = "(" + el.getAttribute("data-gid") + _t("topic_lbl") + " " + topic + ")";
    });
    document.querySelectorAll(".err-prefix").forEach(el => el.innerText = _t("err_lbl") + " ");
    document.querySelectorAll(".run-prefix").forEach(el => el.innerText = _t("last_run_lbl") + " ");
    document.querySelectorAll(".btn-start").forEach(el => el.innerText = _t("btn_start"));
    document.querySelectorAll(".btn-stop").forEach(el => el.innerText = _t("btn_stop"));
    document.querySelectorAll(".btn-delete").forEach(el => el.innerText = _t("btn_delete"));
  });
</script>
</head>
<body>

<h1><span data-i18n="title">TGD</span> <span class="badge-daemon">Daemon mode</span></h1>

<div class="card">
  <h3 data-i18n="add_title">Добавить группу или канал</h3>
  <form action="/add" method="POST">
    <input type="text" name="group" data-i18n-placeholder="input_group" required>
    <input type="text" name="topic" data-i18n-placeholder="input_topic">
    <button type="submit" data-i18n="btn_add">Добавить</button>
  </form>
</div>

<div class="card">
  <h3 data-i18n="saved_title">Сохранённые группы</h3>
  <div id="group-list">
    {GROUPS_HTML}
  </div>
</div>

<div class="card">
  <div class="group-header">
    <h3 data-i18n="resources_title" style="margin-bottom:0;">Ресурсы демона</h3>
    <form action="/restart" method="POST" style="display:inline;" onsubmit="return confirm(_t('confirm_restart'));">
      <button type="submit" class="btn-danger" data-i18n="btn_restart">Перезапустить демон</button>
    </form>
  </div>
  <div class="stats-text" id="resource-line" data-i18n="resource_wait">Сбор метрик...</div>
</div>

<script>
if (!!window.EventSource) {
  var source = new EventSource('/events');
  source.onmessage = function(e) {
    var data = JSON.parse(e.data);
    if (data.type === "resource") {
      var line = document.getElementById("resource-line");
      if (line) {
        var h = Math.floor(data.uptime / 3600);
        var m = Math.floor((data.uptime % 3600) / 60);
        var s = data.uptime % 60;
        var uptimeStr = (h > 0 ? h + "h " : "") + m + "m " + s + "s";
        
        var cpuStr = (data.cpu === null || data.cpu === undefined) ? "N/A" : data.cpu.toFixed(1) + "%";
        var rssStr = (data.rss_mb === null || data.rss_mb === undefined) ? "N/A" : data.rss_mb.toFixed(1);

        line.innerText = fillAll(_t("resource_line"), {
          "{cpu}": cpuStr,
          "{rss}": rssStr,
          "{threads}": data.threads,
          "{jobs}": data.active_jobs,
          "{uptime}": uptimeStr
        });
      }
      return;
    }
    if (data.type === "task") {
      // Ищем по data-key, а не по id элемента: id склеивался из group_id и темы
      // и разъезжался с серверным вариантом на любых спецсимволах в названии.
      var key = data.id + "|" + (data.topic || "");
      var groupDiv = null;
      var items = document.querySelectorAll(".group-item");
      for (var i = 0; i < items.length; i++) {
        if (items[i].getAttribute("data-key") === key) { groupDiv = items[i]; break; }
      }
      if (groupDiv) {
        var statusBadge = groupDiv.querySelector(".status-badge");
        if (data.status) {
          statusBadge.classList.remove("hidden", "status-downloading", "status-done", "status-error", "status-stopping", "status-cancelled", "status-done_errors");
          
          let translatedStatus = parseStatus(data.status);
          statusBadge.innerText = translatedStatus;
          
          let sClass = "status-" + data.status;
          if (data.status === "downloading" || data.status === "stopping") statusBadge.classList.add(sClass);
          else if (data.status === "done") statusBadge.classList.add("status-done");
          else if (data.status === "error" || data.status === "cancelled" || data.status === "done_errors") statusBadge.classList.add(sClass);
          else {
              // fallback for old Russian statuses
              if (data.status.indexOf("Скачивание") !== -1 || data.status.indexOf("Остановка") !== -1) statusBadge.classList.add("status-downloading");
              else if (data.status.indexOf("Завершено") !== -1) statusBadge.classList.add("status-done");
              else if (data.status.indexOf("Ошибка") !== -1 || data.status.indexOf("Отменено") !== -1 || data.status.indexOf("Прервано") !== -1) statusBadge.classList.add("status-error");
          }
        } else {
          statusBadge.classList.add("hidden");
        }

        var actionForm = groupDiv.querySelector(".action-form");
        if (actionForm) {
          var actionBtn = actionForm.querySelector("button");
          let isRunning = (data.status === "downloading" || data.status === "stopping" || data.status.indexOf("Скачивание") !== -1 || data.status.indexOf("Остановка") !== -1);
          if (isRunning) {
            actionForm.action = "/stop";
            actionBtn.innerText = _t("btn_stop");
            actionBtn.className = "btn-warning";
          } else {
            actionForm.action = "/download";
            actionBtn.innerText = _t("btn_start");
            actionBtn.className = "";
          }
        }

        var statsWrap = groupDiv.querySelector(".stats-text");
        if (data.stats) {
          statsWrap.classList.remove("hidden");
          statsWrap.innerText = parseStats(data.stats, data.status);
        } else {
          statsWrap.classList.add("hidden");
        }

        var errorWrap = groupDiv.querySelector(".error-text");
        if (data.error) {
          errorWrap.classList.remove("hidden");
          errorWrap.querySelector('.err-msg').innerText = data.error;
        } else {
          errorWrap.classList.add("hidden");
        }

        var runWrap = groupDiv.querySelector(".last-run");
        if (data.last_run) {
          runWrap.classList.remove("hidden");
          runWrap.querySelector('.run-msg').innerText = data.last_run;
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

    def subscribe(self, max_listeners: int = 0):
        """Возвращает очередь событий или None, если лимит слушателей исчерпан."""
        q = queue.Queue(maxsize=100)
        with self._lock:
            if max_listeners and len(self._listeners) >= max_listeners:
                return None
            self._listeners.append(q)
        return q

    def has_listeners(self) -> bool:
        with self._lock:
            return bool(self._listeners)

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

    def handle_error(self, request, client_address):
        """Браузер, закрывший вкладку (или оборванный SSE), — это норма, а не
        повод сыпать трейсбеком в лог демона."""
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


def make_request_handler(store, broadcaster, loop, daemon_callbacks, auth_token=None):
    class WebUIHandler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, format, *args):
            # Suppress default request logging to avoid stdout clutter
            pass

        def _send_redirect(self, path='/'):
            self.send_response(303)
            self.send_header('Location', path)
            self.send_header('Content-Length', '0')
            self.end_headers()

        def _send_plain(self, code, text=''):
            body = text.encode('utf-8')
            if code >= 400:
                # Тело запроса мы не дочитали — переиспользовать соединение нельзя
                self.close_connection = True
            self.send_response(code)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        # ── доступ ───────────────────────────────────────────────────
        def _authorized(self) -> bool:
            """HTTP Basic, если в .env заданы WEBUI_USER/WEBUI_PASS."""
            if not auth_token:
                return True
            header = self.headers.get('Authorization', '')
            if not header.startswith('Basic '):
                return False
            return hmac.compare_digest(header[6:].strip(), auth_token)

        def _demand_auth(self):
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="TGD"')
            self.send_header('Content-Length', '0')
            self.end_headers()

        def _same_origin(self) -> bool:
            """Защита от CSRF: сторонняя страница не должна дёргать /download,
            /remove и /restart из браузера пользователя."""
            host = (self.headers.get('Host') or '').strip()
            for header in ('Origin', 'Referer'):
                value = self.headers.get(header)
                if not value:
                    continue
                netloc = urlparse(value).netloc
                return bool(netloc) and netloc == host
            # Ни Origin, ни Referer — это не браузерная форма (curl, скрипт)
            return True

        def do_GET(self):
            if not self._authorized():
                return self._demand_auth()
            parsed = urlparse(self.path)
            if parsed.path == '/':
                self.handle_index()
            elif parsed.path == '/events':
                self.handle_sse()
            else:
                self.send_error(404, "Not Found")

        def do_POST(self):
            if not self._authorized():
                return self._demand_auth()
            if not self._same_origin():
                return self._send_plain(403, "Cross-origin request rejected")

            parsed = urlparse(self.path)
            try:
                length = int(self.headers.get('Content-Length', 0) or 0)
            except (TypeError, ValueError):
                return self._send_plain(400, "Bad Content-Length")
            if length < 0 or length > MAX_BODY_BYTES:
                return self._send_plain(413, "Payload too large")

            body = self.rfile.read(length).decode('utf-8', 'replace') if length > 0 else ''
            form_data = parse_qs(body)

            if parsed.path == '/add':
                self.handle_add(form_data)
            elif parsed.path == '/remove':
                self.handle_remove(form_data)
            elif parsed.path == '/download':
                self.handle_download(form_data)
            elif parsed.path == '/stop':
                self.handle_stop(form_data)
            elif parsed.path == '/restart':
                self.handle_restart(form_data)
            else:
                self.send_error(404, "Not Found")

        def handle_index(self):
            groups = store.list()
            groups_html = ""
            if not groups:
                groups_html = "<p style='color: var(--text-muted);' data-i18n=\"no_groups\">Нет добавленных групп.</p>"
            else:
                for g in groups:
                    gid = escape(str(g.get('id', '')))
                    title = escape(str(g.get('title', gid)))
                    topic = escape(str(g.get('topic', '')))
                    status = escape(str(g.get('status', '')))
                    stats = escape(str(g.get('stats', '')))
                    error = escape(str(g.get('last_error', '')))
                    last_run = escape(str(g.get('last_run', '')))

                    key_attr = escape(f"{g.get('id', '')}|{g.get('topic', '')}")
                    topic_meta_attr = f' data-topic="{topic}" data-gid="{gid}"' if topic else ''
                    topic_meta = f", тема: {topic}" if topic else ""
                    
                    status_class = "status-badge"
                    if status in ("downloading", "stopping") or "Скачивание" in status or "Остановка" in status:
                        status_class += " status-downloading"
                    elif status == "done" or "Завершено" in status:
                        status_class += " status-done"
                    elif status in ("error", "cancelled", "done_errors") or "Ошибка" in status or "Отменено" in status or "Прервано" in status:
                        status_class += " status-error"
                    elif not status:
                        status_class += " hidden"

                    is_running = status in ("downloading", "stopping") or "Скачивание" in status or "Остановка" in status
                    action_url = "/stop" if is_running else "/download"
                    btn_class = "btn-warning" if is_running else ""
                    btn_text = "Стоп" if is_running else "Старт"
                    btn_action_class = "btn-stop" if is_running else "btn-start"

                    groups_html += f"""
                    <div class="group-item" data-key="{key_attr}">
                      <div class="group-header">
                        <div>
                          <span class="group-title">{title}</span>
                          <span class="group-meta">({gid}<span class="topic-meta"{topic_meta_attr}>{topic_meta}</span>)</span>
                        </div>
                        <div>
                          <form action="{action_url}" method="POST" style="display:inline;" class="action-form">
                            <input type="hidden" name="id" value="{gid}">
                            <input type="hidden" name="topic" value="{topic}">
                            <button type="submit" class="{btn_class} {btn_action_class}">{btn_text}</button>
                          </form>
                          <form action="/remove" method="POST" style="display:inline;">
                            <input type="hidden" name="id" value="{gid}">
                            <input type="hidden" name="topic" value="{topic}">
                            <button type="submit" class="btn-danger btn-delete">Удалить</button>
                          </form>
                        </div>
                      </div>
                      <div>
                        <span class="{status_class}" data-raw-status="{status}">{status}</span>
                        <div class="stats-text {'hidden' if not stats else ''}" data-raw-stats="{stats}" data-raw-status="{status}">{stats}</div>
                        <div class="error-text {'hidden' if not error else ''}"><span class="err-prefix">Ошибка: </span><span class="err-msg">{error}</span></div>
                        <div class="last-run {'hidden' if not last_run else ''}"><span class="run-prefix">Последний запуск: </span><span class="run-msg">{last_run}</span></div>
                      </div>
                    </div>
                    """

            html = HTML_TEMPLATE.replace("{GROUPS_HTML}", groups_html).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(html)))
            self.end_headers()
            self.wfile.write(html)

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

        def handle_restart(self, form_data):
            if 'restart' in daemon_callbacks:
                asyncio.run_coroutine_threadsafe(daemon_callbacks['restart'](), loop)
            self._send_redirect('/')

        def handle_sse(self):
            q = broadcaster.subscribe(MAX_SSE_LISTENERS)
            if q is None:
                # Каждое соединение держит поток — лучше отказать, чем копить их
                return self._send_plain(503, "Too many event listeners")

            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            # Раньше здесь стоял Access-Control-Allow-Origin: *, и любой сайт мог
            # читать поток событий демона из браузера пользователя.
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True

            try:
                while True:
                    try:
                        msg = q.get(timeout=SSE_HEARTBEAT_SEC)
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

        user = os.getenv('WEBUI_USER', '').strip()
        password = os.getenv('WEBUI_PASS', '')
        self.auth_enabled = bool(user and password)
        auth_token = (
            base64.b64encode(f"{user}:{password}".encode('utf-8')).decode('ascii')
            if self.auth_enabled else None
        )

        handler_cls = make_request_handler(
            self.store, self.broadcaster, self.loop, self.daemon_callbacks, auth_token
        )
        # Может бросить OSError (порт занят) — вызывающий код обязан это поймать
        self.httpd = ThreadedHTTPServer((self.host, self.port), handler_cls)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self._stopped = False

    def has_listeners(self) -> bool:
        return self.broadcaster.has_listeners()

    def start(self):
        self.thread.start()
        auth_note = "" if self.auth_enabled else " (no auth)"
        print(f"[WebUI] http://{self.host}:{self.port}{auth_note}")

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        try:
            self.httpd.shutdown()
        finally:
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

    def broadcast_resource(self, data: dict):
        payload = dict(data)
        payload["type"] = "resource"
        self.broadcaster.broadcast(payload)
