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
        lang = os.environ.get('LANG', '')

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
    }
}

def _(key, *args):
    msg = MESSAGES.get(_LANG, MESSAGES["en"]).get(key, MESSAGES["en"].get(key, key))
    if args:
        return msg.format(*args)
    return msg
