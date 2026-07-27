import locale
import sys
import os

_LANG = "en"

def get_system_lang():
    lang = ''
    try:
        if hasattr(locale, 'getlocale'):
            loc = locale.getlocale()
            if loc and loc[0]:
                lang = loc[0]
        if not lang and hasattr(locale, 'getdefaultlocale'):
            loc = locale.getdefaultlocale()
            if loc and loc[0]:
                lang = loc[0]
    except Exception:
        pass
        
    if not lang and sys.platform == 'win32':
        try:
            import ctypes
            windll = ctypes.windll.kernel32
            lang = locale.windows_locale.get(windll.GetUserDefaultUILanguage(), '')
        except Exception:
            pass
            
    if not lang:
        # LC_ALL и LANGUAGE приоритетнее LANG — их проверяли не все ветки выше
        for var in ('LC_ALL', 'LC_MESSAGES', 'LANGUAGE', 'LANG'):
            value = os.environ.get(var, '')
            if value:
                lang = value.split(':')[0]
                break

    if lang and lang.lower().startswith('ru'):
        return 'ru'
    return 'en'

def init_lang(lang=None):
    global _LANG
    if lang:
        _LANG = lang
    else:
        _LANG = get_system_lang()

MESSAGES = {
    "en": {
        "warn_fast_not_found": "FastTelethonhelper not found — heavy files will be downloaded via standard method",
        "err_incomplete_download": "Incomplete download: {}/{} bytes",
        "warn_attempt_failed": "[{}] Attempt {}/{} failed: {}",
        "err_unhandled_worker": "Unhandled worker error: {}",
        "prompt_code": "Confirmation code: ",
        "prompt_password": "2FA Password: ",
        "info_mtproto_faketls": "Using MTProto FakeTLS proxy: {}:{}",
        "warn_ee_secret_no_faketls": "Secret starts with 'ee' (FakeTLS), but TelethonFakeTLS module is not loaded. Connection may fail.",
        "info_mtproto_proxy": "Using MTProto proxy: {}:{}",
        "info_socks_proxy": "Using {} proxy: {}:{}",
        "info_start": "Start: {}",
        "info_filter_topic": "Filtering by topic ID: {}",
        "info_search_topic": "Searching for topic with name: '{}'...",
        "info_found_topic": "Found topic '{}' with ID: {}",
        "err_topic_not_found": "Topic '{}' not found!",
        "err_topic_list_fail": "Failed to get topic list: {}",
        "err_env_missing": "Error: APP_API_ID, APP_API_HASH or PHONE_NUMBER are not set in .env",
        "info_fill_env": "Fill the file {} and run the program again.",
        "prompt_exit": "\nPress Enter to exit...",
        "ui_interactive_title": "\n=== TGD Interactive Mode ===",
        "prompt_group": "Enter group/channel ID, @username or link: ",
        "err_no_group": "No group specified. Exiting.",
        "prompt_output_dir": "Output folder [{}]: ",
        "err_args_missing": "Error: group_id or output_dir not specified. Use -d for Web UI.",
        "warn_proxy_parse": "Proxy parsing error: {}",
        "info_daemon_started": "TGD Daemon started at http://{}:{}",
        "info_env_created": "Configuration template created: {}",
        "warn_env_create_fail": "Failed to create file {}: {}",
        "err_critical": "Critical error: {}",
        "webui_err_resolve": "[WebUI] Group resolve error: {}",
        "webui_err_stop": "[WebUI] Task stop error: {}",
        "webui_added_group": "[WebUI] Group added: {} ({})",
        "webui_err_add_group": "[WebUI] Error adding group {}: {}",
        "webui_task_running": "[WebUI] Task {}|{} is already running.",
        "webui_task_cancel": "[WebUI] Canceling task {}|{}...",
        "webui_err_download": "[WebUI] Download error {}: {}",
        "err_errors_count": "Errors: {}",
        "warn_psutil_not_found": "psutil not found — using built-in /proc-based resource sampler instead (Linux only; no effect on functionality)",
        "info_restart_requested": "Restart requested from Web UI, restarting the process...",

        # --- argparse ---
        "app_description": "TGD: Telegram media downloader",
        "arg_group_id": "Group ID or link (@username, https://t.me/...)",
        "arg_output_dir": "Output folder",
        "arg_daemon": "Run in server/daemon mode",
        "arg_addr": "Web server address (only with -d)",
        "arg_data": "Data folder (session, groups.json)",
        "arg_env": "Path to .env",
        "arg_timeout": "Per-file timeout (overrides .env TIMEOUT)",
        "arg_retries": "Retries on failure (overrides .env RETRIES)",
        "arg_workers": "Workers for light files (overrides .env WORKERS)",
        "arg_heavy_workers": "Workers for heavy files (overrides .env HEAVY_WORKERS)",
        "arg_heavy_threshold": "'Heavy' file threshold in MB (overrides .env HEAVY_THRESHOLD)",
        "arg_queue_size": "Queue size (overrides .env QUEUE_SIZE)",
        "arg_proxy": "SOCKS5/HTTP/MTProto proxy (e.g.: tg://proxy?server=...)",
        "arg_topic": "Topic (thread) name or ID to download from only",

        # --- runtime ---
        "summary_done": "Finished! New: {}, Existing: {}, Skipped: {}, Errors: {}",
        "stop_graceful": "[STOP] Finishing current downloads... (press Ctrl+C again to abort immediately)",
        "stop_forced": "[FORCED] Aborting immediately...",
        "warn_flood_wait": "[{}] Telegram rate limit (FloodWait): waiting {} s",
        "err_port_busy": "Failed to bind web server to {}:{} — {}",
        "warn_webui_public_no_auth": "Web UI is listening on {} without authentication. Set WEBUI_USER and WEBUI_PASS in .env, or bind to 127.0.0.1.",
        "info_incremental": "Incremental sync: only messages newer than ID {}",
        "info_signal_shutdown": "Signal {} received, shutting down gracefully...",
        "warn_store_corrupt": "{} is corrupted, a backup was saved as {} and an empty list is used",
        "warn_path_too_long": "Output path is very long, file names will be truncated to {} chars",
        "err_auth_no_tty": "Not authorized and there is no terminal to enter the code. Run TGD once interactively to create the session file.",
        "err_proxy_required": "Proxy is configured but could not be used — aborting instead of connecting directly (that would expose your real IP).",
        "warn_chmod_failed": "Failed to restrict permissions on {}: {}",
    },
    "ru": {
        "warn_fast_not_found": "FastTelethonhelper не найден — тяжёлые файлы будут качаться стандартным методом",
        "err_incomplete_download": "Неполная загрузка: {}/{} байт",
        "warn_attempt_failed": "[{}] Попытка {}/{} не удалась: {}",
        "err_unhandled_worker": "Необработанная ошибка воркера: {}",
        "prompt_code": "Код подтверждения: ",
        "prompt_password": "Пароль 2FA: ",
        "info_mtproto_faketls": "Используется MTProto FakeTLS прокси: {}:{}",
        "warn_ee_secret_no_faketls": "Секрет начинается с 'ee' (FakeTLS), но модуль TelethonFakeTLS не загружен. Подключение может не удаться.",
        "info_mtproto_proxy": "Используется MTProto прокси: {}:{}",
        "info_socks_proxy": "Используется {} прокси: {}:{}",
        "info_start": "Старт: {}",
        "info_filter_topic": "Фильтрация по ID темы: {}",
        "info_search_topic": "Поиск темы с названием: '{}'...",
        "info_found_topic": "Найдена тема '{}' с ID: {}",
        "err_topic_not_found": "Тема '{}' не найдена!",
        "err_topic_list_fail": "Не удалось получить список тем: {}",
        "err_env_missing": "Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env",
        "info_fill_env": "Заполните файл {} и запустите программу снова.",
        "prompt_exit": "\nНажмите Enter для выхода...",
        "ui_interactive_title": "\n=== Интерактивный режим TGD ===",
        "prompt_group": "Введите ID группы/канала, @username или ссылку: ",
        "err_no_group": "Группа не указана. Завершение работы.",
        "prompt_output_dir": "Папка сохранения [{}]: ",
        "err_args_missing": "Ошибка: не указаны group_id или output_dir. Используйте -d для веб-интерфейса.",
        "warn_proxy_parse": "Ошибка парсинга прокси: {}",
        "info_daemon_started": "TGD Daemon запущен на http://{}:{}",
        "info_env_created": "Создан шаблон конфигурации: {}",
        "warn_env_create_fail": "Не удалось создать файл {}: {}",
        "err_critical": "Критическая ошибка: {}",
        "webui_err_resolve": "[WebUI] Ошибка разрешения группы: {}",
        "webui_err_stop": "[WebUI] Ошибка остановки задачи: {}",
        "webui_added_group": "[WebUI] Добавлена группа: {} ({})",
        "webui_err_add_group": "[WebUI] Ошибка добавления группы {}: {}",
        "webui_task_running": "[WebUI] Задача {}|{} уже выполняется.",
        "webui_task_cancel": "[WebUI] Отмена задачи {}|{}...",
        "webui_err_download": "[WebUI] Ошибка скачивания {}: {}",
        "err_errors_count": "Ошибок: {}",
        "warn_psutil_not_found": "psutil не найден — используется встроенный сэмплер ресурсов через /proc (только Linux; на работу демона не влияет)",
        "info_restart_requested": "Запрошен перезапуск из веб-морды, перезапускаем процесс...",

        # --- argparse ---
        "app_description": "TGD: Загрузчик медиа из Telegram",
        "arg_group_id": "ID группы или ссылка (@username, https://t.me/...)",
        "arg_output_dir": "Папка сохранения",
        "arg_daemon": "Запустить в режиме сервера/демона",
        "arg_addr": "Адрес веб-сервера (только для -d)",
        "arg_data": "Папка данных (сессия, groups.json)",
        "arg_env": "Путь до .env",
        "arg_timeout": "Таймаут на файл (переопределяет .env TIMEOUT)",
        "arg_retries": "Попыток при сбое (переопределяет .env RETRIES)",
        "arg_workers": "Количество воркеров для лёгких файлов (переопределяет .env WORKERS)",
        "arg_heavy_workers": "Количество воркеров для тяжёлых файлов (переопределяет .env HEAVY_WORKERS)",
        "arg_heavy_threshold": "Порог 'тяжёлого' файла в МБ (переопределяет .env HEAVY_THRESHOLD)",
        "arg_queue_size": "Размер очереди (переопределяет .env QUEUE_SIZE)",
        "arg_proxy": "SOCKS5/HTTP/MTProto прокси (например: tg://proxy?server=...)",
        "arg_topic": "Название темы (раздела) или ID темы для скачивания только из неё",

        # --- runtime ---
        "summary_done": "Завершено! Новых: {}, Существует: {}, Пропущено: {}, Ошибок: {}",
        "stop_graceful": "[СТОП] Завершаем текущие загрузки... (повторный Ctrl+C — прервать немедленно)",
        "stop_forced": "[ПРИНУДИТЕЛЬНО] Прерываем немедленно...",
        "warn_flood_wait": "[{}] Ограничение Telegram (FloodWait): ждём {} с",
        "err_port_busy": "Не удалось занять порт для веб-сервера {}:{} — {}",
        "warn_webui_public_no_auth": "Веб-интерфейс слушает на {} без авторизации. Задайте WEBUI_USER и WEBUI_PASS в .env либо привяжите его к 127.0.0.1.",
        "info_incremental": "Инкрементальная синхронизация: только сообщения новее ID {}",
        "info_signal_shutdown": "Получен сигнал {}, завершаемся корректно...",
        "warn_store_corrupt": "Файл {} повреждён, сохранена копия {}, используется пустой список",
        "warn_path_too_long": "Путь сохранения очень длинный, имена файлов будут урезаны до {} символов",
        "err_auth_no_tty": "Сессия не авторизована, а ввести код некуда — нет терминала. Запустите TGD один раз интерактивно, чтобы создать файл сессии.",
        "err_proxy_required": "Прокси задан, но использовать его не удалось — прерываем запуск вместо прямого подключения (иначе засветится реальный IP).",
        "warn_chmod_failed": "Не удалось ограничить права на {}: {}",
    }
}

def _(key, *args):
    msg = MESSAGES.get(_LANG, MESSAGES["en"]).get(key, MESSAGES["en"].get(key, key))
    if args:
        return msg.format(*args)
    return msg
