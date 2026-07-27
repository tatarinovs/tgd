import os
import argparse
import logging
import asyncio
import sys
import threading
import glob
import time
import json
import signal
import itertools
from collections import Counter
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo
)
from tqdm import tqdm
from utils import setup_tqdm_logger, ProcessResourceSampler, sanitize_filename, name_budget
from i18n import _, init_lang
from store import GroupStore
from webui import WebUIServer

# Язык нужно определить ДО первого вызова _(): часть сообщений (например,
# предупреждение об отсутствии FastTelethonhelper) печатается ещё на импорте.
init_lang()


# ── Monkey-patch: ускорение AES-CTR для MTProxy ──────────────────────
# Telethon использует чистый Python (pyaes) для AES-CTR в обфускации
# MTProxy, что ограничивает скорость ~0.5 MB/s.
# Заменяем на cryptography (OpenSSL/C) → ~2300 MB/s.
try:
    from cryptography.hazmat.primitives.ciphers import (
        Cipher as _Cipher, algorithms as _alg, modes as _modes
    )

    class _FastAESModeCTR:
        __slots__ = ('_enc', '_dec')

        def __init__(self, key, iv):
            if not isinstance(key, bytes):
                raise TypeError(f"AES key must be bytes, got {type(key)}")
            if not isinstance(iv, bytes) or len(iv) != 16:
                raise ValueError(f"AES iv must be 16 bytes, got {len(iv) if isinstance(iv, bytes) else type(iv)}")
            cipher = _Cipher(_alg.AES(key), _modes.CTR(iv))
            self._enc = cipher.encryptor()
            self._dec = cipher.decryptor()

        def encrypt(self, data):
            return self._enc.update(data)

        def decrypt(self, data):
            return self._dec.update(data)

    # Патчим все модули, которые хранят ссылку на AESModeCTR
    import telethon.crypto.aesctr as _aesctr_mod
    _aesctr_mod.AESModeCTR = _FastAESModeCTR
    import telethon.crypto as _crypto_mod
    _crypto_mod.AESModeCTR = _FastAESModeCTR
    # from-импорты копируют ссылку — нужно патчить и конечные модули
    import telethon.network.connection.tcpmtproxy as _mtproxy_mod
    _mtproxy_mod.AESModeCTR = _FastAESModeCTR
    import telethon.network.connection.tcpobfuscated as _obfs_mod
    _obfs_mod.AESModeCTR = _FastAESModeCTR
except ImportError:
    pass
# ─────────────────────────────────────────────────────────────────────

try:
    from FastTelethonhelper import download_file as fast_download
    HAS_FAST = True
except ImportError:
    HAS_FAST = False

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False




# TqdmStream импортируется из utils.py


logger = setup_tqdm_logger(__name__)

# Убираем назойливые INFO логи от библиотек
logging.getLogger('FastTelethonhelper').setLevel(logging.WARNING)
logging.getLogger('telethon').setLevel(logging.ERROR)

if not HAS_FAST:
    logger.warning(_("warn_fast_not_found"))


def _get_app_dir() -> str:
    """Возвращает директорию .exe при запуске как frozen, иначе директорию проекта."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    base = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(base) == 'src':
        return os.path.dirname(base)
    return base


def parse_args():
    parser = argparse.ArgumentParser(description=_("app_description"))
    parser.add_argument('group_id', type=str, nargs='?', default=None, help=_("arg_group_id"))
    parser.add_argument('output_dir', type=str, nargs='?', default=None, help=_("arg_output_dir"))
    parser.add_argument('-d', '--daemon', action='store_true', help=_("arg_daemon"))
    parser.add_argument('--addr', type=str, default="127.0.0.1:8080", help=_("arg_addr"))
    parser.add_argument('--data', type=str, default=None, help=_("arg_data"))
    parser.add_argument('--env', type=str, default='.env', help=_("arg_env"))
    parser.add_argument('--timeout', type=int, default=None, help=_("arg_timeout"))
    parser.add_argument('--retries', type=int, default=None, help=_("arg_retries"))
    parser.add_argument('--workers', type=int, default=None, help=_("arg_workers"))
    parser.add_argument('--heavy-workers', type=int, default=None, help=_("arg_heavy_workers"))
    parser.add_argument('--heavy-threshold', type=int, default=None, help=_("arg_heavy_threshold"))
    parser.add_argument('--queue-size', type=int, default=None, help=_("arg_queue_size"))
    parser.add_argument('--proxy', type=str, default=None, help=_("arg_proxy"))
    parser.add_argument('--topic', type=str, default=None, help=_("arg_topic"))
    return parser.parse_args()


def _env_int(name: str, default: int) -> int:
    """int из .env с защитой от мусора вроде WORKERS=six."""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == '':
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        logger.warning(f"{name}={raw!r} is not a number, using {default}")
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == '':
        return default
    return str(raw).strip().lower() in ('1', 'true', 'yes', 'on', 'да')


def apply_config(args):
    """Единая точка разбора настроек для CLI и демона (раньше два одинаковых блока)."""
    args.timeout = args.timeout if args.timeout is not None else _env_int('TIMEOUT', 3600)
    args.retries = args.retries if args.retries is not None else _env_int('RETRIES', 3)
    args.workers = args.workers if args.workers is not None else _env_int('WORKERS', 6)
    args.heavy_workers = args.heavy_workers if args.heavy_workers is not None else _env_int('HEAVY_WORKERS', 1)
    args.heavy_threshold = args.heavy_threshold if args.heavy_threshold is not None else _env_int('HEAVY_THRESHOLD', 100)
    args.queue_size = args.queue_size if args.queue_size is not None else _env_int('QUEUE_SIZE', 50)
    # Нижние границы: retries=0 раньше приводил к тому, что download_task
    # не выполнял ни одной попытки и возвращал None.
    args.retries = max(1, args.retries)
    args.workers = max(1, args.workers)
    args.heavy_workers = max(1, args.heavy_workers)
    args.queue_size = max(1, args.queue_size)
    args.timeout = max(30, args.timeout)
    # Верхняя граница таймаута считается от размера файла: единый TIMEOUT
    # одинаково душил и 200 КБ, и 2 ГБ.
    args.timeout_per_mb = max(0, _env_int('TIMEOUT_PER_MB', 6))
    return args



def resolve_file_name(message, heavy_threshold_bytes: int, max_name_len: int = 200) -> tuple:
    """
    Возвращает (file_name, is_heavy) или (None, False) если медиа не поддерживается.
    Вынесено отдельно для читаемости и тестируемости.
    """
    media = message.media
    # Префикс "{id}_" тоже занимает место в бюджете имени
    max_name_len = max(8, max_name_len - len(str(message.id)) - 1)

    if isinstance(media, MessageMediaPhoto):
        return f"{message.id}_photo.jpg", False

    if isinstance(media, MessageMediaDocument):
        doc = media.document

        is_heavy = bool(getattr(doc, 'size', 0) >= heavy_threshold_bytes)

        attrs = doc.attributes or []
        is_round = any(
            isinstance(a, DocumentAttributeVideo) and a.round_message
            for a in attrs
        )
        if is_round:
            return f"{message.id}_round.mp4", is_heavy

        orig_name = next(
            (a.file_name for a in attrs if hasattr(a, 'file_name')),
            None
        )
        if orig_name:
            # guard_reserved не нужен: префикс "{id}_" уже уводит имя из
            # пространства имён устройств MS-DOS
            safe = sanitize_filename(orig_name, max_name_len, guard_reserved=False)
            return f"{message.id}_{safe}", is_heavy

        mime = doc.mime_type or ""
        ext = mime.split('/')[-1].split(';')[0]
        ext_map = {
            'quicktime': 'mov',
            'x-matroska': 'mkv',
            'mpeg': 'mp3',
            'ogg': 'ogg',
            'mp4': 'mp4',
            'webm': 'webm',
            'x-msvideo': 'avi',
        }
        ext = ext_map.get(ext, ext) or 'bin'
        prefix = (
            "video" if mime.startswith('video/') else
            "audio" if mime.startswith('audio/') else "file"
        )
        return f"{message.id}_{prefix}.{ext}", is_heavy

    return None, False


# Временные файлы, которые прямо сейчас пишет ЭТОТ процесс. Стартовая уборка
# "*.part" смотрит сюда, чтобы не снести файл параллельной задачи, пишущей в ту
# же папку (две темы одной группы, две группы с одинаковым названием).
_ACTIVE_TMP = set()
_ACTIVE_TMP_LOCK = threading.Lock()
_TMP_SEQ = itertools.count()
_TMP_TAG = f"{os.getpid():x}"

# Крупный буфер: Telethon отдаёт данные кусками по 512 КБ, и на сетевых дисках
# (NAS) каждая запись стоит дорого.
_WRITE_BUFFER = 1024 * 1024
# Порог накопления байт перед обновлением общей статистики: раньше лок брался
# на каждый чанк прогресса, то есть тысячи раз в секунду.
_STATS_FLUSH_BYTES = 1024 * 1024
_MAX_FLOOD_WAITS = 5


def _effective_timeout(base_timeout: int, per_mb: int, size_bytes) -> int:
    """Таймаут пропорционально размеру: единый TIMEOUT одинаково душил и 200 КБ, и 2 ГБ."""
    if not size_bytes or per_mb <= 0:
        return base_timeout
    return max(base_timeout, int(size_bytes / (1024 * 1024) * per_mb) + 60)


async def _sleep_cancellable(seconds: float, cancel) -> bool:
    """Спит, просыпаясь раз в секунду. False — если во время сна попросили остановиться."""
    end = time.monotonic() + seconds
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            return True
        if cancel.is_set():
            return False
        await asyncio.sleep(min(1.0, remaining))


def _remove_quiet(path: str):
    try:
        os.remove(path)
    except OSError:
        pass


async def download_task(client, message, file_name, is_heavy, output_dir, timeout, retries,
                        cancel, show_progress=True, stats=None, stats_lock=None,
                        timeout_per_mb=0):
    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return "exists"

    if cancel.is_set():
        return "skipped"

    # Уникальный суффикс: общий "<имя>.part" позволял двум задачам писать в один
    # и тот же временный файл, а стартовой уборке — удалять чужую загрузку.
    tmp_path = f"{file_path}.{_TMP_TAG}{next(_TMP_SEQ):x}.part"
    expected_size = getattr(getattr(message, 'file', None), 'size', None)
    eff_timeout = _effective_timeout(timeout, timeout_per_mb, expected_size)

    with _ACTIVE_TMP_LOCK:
        _ACTIVE_TMP.add(tmp_path)
    try:
        attempt = 0
        flood_waits = 0
        while attempt < retries:
            try:
                pbar = None
                counted = [0]     # сколько байт уже учтено в общей статистике
                reported = [0]    # сколько байт отдал последний колбэк прогресса

                def flush_bytes(force=False):
                    inc = reported[0] - counted[0]
                    if inc <= 0 or (not force and inc < _STATS_FLUSH_BYTES):
                        return
                    if stats is not None:
                        if stats_lock is not None:
                            with stats_lock:
                                stats["bytes"] += inc
                        else:
                            stats["bytes"] += inc
                    counted[0] = reported[0]

                def on_progress(current, total):
                    if pbar is not None:
                        if total and not pbar.total:
                            pbar.total = total
                        pbar.update(current - pbar.n)
                    reported[0] = current
                    flush_bytes()

                if show_progress:
                    pbar = tqdm(unit='B', unit_scale=True, desc=file_name[:25], leave=False)

                try:
                    if is_heavy and HAS_FAST:
                        # FastTelethonhelper: многопоточная докачка одного файла
                        with open(tmp_path, 'wb', buffering=_WRITE_BUFFER) as f:
                            await asyncio.wait_for(
                                fast_download(
                                    client,
                                    message.media.document,
                                    f,
                                    progress_callback=on_progress
                                ),
                                timeout=eff_timeout
                            )
                    else:
                        # Стандартный Telethon: потоковая запись без расхода ОЗУ
                        with open(tmp_path, 'wb', buffering=_WRITE_BUFFER) as f:
                            await asyncio.wait_for(
                                client.download_media(message, file=f, progress_callback=on_progress),
                                timeout=eff_timeout
                            )
                finally:
                    flush_bytes(force=True)
                    if pbar is not None:
                        pbar.close()

                try:
                    actual_size = os.path.getsize(tmp_path)
                except OSError:
                    actual_size = 0
                if not actual_size or (expected_size and actual_size < expected_size * 0.99):
                    raise ValueError(_("err_incomplete_download", actual_size, expected_size))

                os.replace(tmp_path, file_path)
                return "done"

            except FloodWaitError as e:
                # Ограничение со стороны Telegram: ждать нужно столько, сколько
                # сказал сервер, и не тратить на это попытку — иначе три
                # мгновенных ретрая просто сжигают файл в "error".
                _remove_quiet(tmp_path)
                flood_waits += 1
                if flood_waits > _MAX_FLOOD_WAITS:
                    logger.error(_("warn_attempt_failed", file_name, attempt + 1, retries, e))
                    return "error"
                wait_for = min(int(getattr(e, 'seconds', 60)) + 2, 3600)
                logger.warning(_("warn_flood_wait", file_name, wait_for))
                if not await _sleep_cancellable(wait_for, cancel):
                    return "skipped"

            except asyncio.CancelledError:
                _remove_quiet(tmp_path)
                raise

            except Exception as e:
                attempt += 1
                logger.warning(_("warn_attempt_failed", file_name, attempt, retries, e))
                _remove_quiet(tmp_path)
                if attempt >= retries:
                    return "error"
                if not await _sleep_cancellable(2 ** (attempt - 1), cancel):
                    return "skipped"

        return "error"
    finally:
        with _ACTIVE_TMP_LOCK:
            _ACTIVE_TMP.discard(tmp_path)


async def worker(queue, client, output_dir, timeout, retries, stats, stats_lock, cancel,
                 show_progress=True, timeout_per_mb=0):
    """Воркер из пула — тянет задачи (message, file_name, is_heavy) из очереди до sentinel (None)."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        message, file_name, is_heavy = item

        if cancel.is_set():
            # Задача была уже в очереди, когда нажали «Стоп». Её нужно учесть:
            # иначе веб-морда рапортует «Пропущено: 0» при десятках брошенных файлов.
            with stats_lock:
                stats["skipped"] += 1
            queue.task_done()
            del item, message
            continue

        try:
            result = await download_task(
                client, message, file_name, is_heavy, output_dir, timeout, retries, cancel,
                show_progress=show_progress, stats=stats, stats_lock=stats_lock,
                timeout_per_mb=timeout_per_mb,
            )
            if result != "skipped":
                with stats_lock:
                    stats[result] += 1
        except Exception as e:
            logger.error(_("err_unhandled_worker", e))
            with stats_lock:
                stats["error"] += 1
        finally:
            del item, message
            queue.task_done()



async def authorize(client, phone):
    """
    Авторизация: поддерживает 2FA (пароль) и не блокирует event loop на вводе.
    """
    loop = asyncio.get_running_loop()

    if not (sys.stdin and sys.stdin.isatty()):
        # Под systemd/NAS вводить код некому: раньше демон молча висел на input()
        raise RuntimeError(_("err_auth_no_tty"))

    await client.send_code_request(phone)
    code = await loop.run_in_executor(None, lambda: input(_("prompt_code")).strip())

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        # Включён двухфакторный пароль (2FA)
        password = await loop.run_in_executor(None, lambda: input(_("prompt_password")).strip())
        await client.sign_in(password=password)


def parse_proxy(p_url, client_kwargs):
    """Парсит строку прокси и заполняет client_kwargs."""
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(p_url)

    is_tg_proxy = (parsed.scheme == 'tg' and parsed.netloc == 'proxy')
    is_mtproxy_scheme = parsed.scheme in ('mtproxy', 'mtproto')

    if is_tg_proxy or is_mtproxy_scheme:
        qs = parse_qs(parsed.query)

        if is_tg_proxy:
            server = qs.get('server', [''])[0]
            port = int(qs.get('port', ['0'])[0])
            secret = qs.get('secret', [''])[0]
        else:
            # mtproxy://server:port/SECRET или mtproxy://server:port?secret=SECRET
            server = parsed.hostname
            port = parsed.port
            # Секрет может быть в пути (/SECRET) или в query string (?secret=...)
            path_secret = parsed.path.lstrip('/')
            secret = path_secret if path_secret else qs.get('secret', [''])[0]

        client_kwargs['proxy'] = (server, port, secret)

        if secret.lower().startswith('ee'):
            try:
                from TelethonFakeTLS import ConnectionTcpMTProxyFakeTLS
                client_kwargs['connection'] = ConnectionTcpMTProxyFakeTLS
                logger.info(_("info_mtproto_faketls", server, port))
            except ImportError:
                logger.warning(_("warn_ee_secret_no_faketls"))
                from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
                client_kwargs['connection'] = ConnectionTcpMTProxyRandomizedIntermediate
                logger.info(_("info_mtproto_proxy", server, port))
        else:
            from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
            client_kwargs['connection'] = ConnectionTcpMTProxyRandomizedIntermediate
            logger.info(_("info_mtproto_proxy", server, port))
    else:
        import python_socks
        ptype = python_socks.ProxyType.SOCKS5 if parsed.scheme.startswith('socks') else python_socks.ProxyType.HTTP
        proxy_dict = {'proxy_type': ptype, 'addr': parsed.hostname, 'port': parsed.port}
        if parsed.username:
            proxy_dict['username'] = parsed.username
            proxy_dict['password'] = parsed.password
        client_kwargs['proxy'] = proxy_dict
        logger.info(_("info_socks_proxy", parsed.scheme, parsed.hostname, parsed.port))


def build_client_kwargs(args):
    """Собирает параметры TelegramClient. Возвращает None, если прокси задан,
    но непригоден: раньше в этом случае клиент молча шёл напрямую — для того,
    кто включил прокси ради обхода блокировок, это утечка реального IP."""
    client_kwargs = {'receive_updates': False}
    p_url = args.proxy or os.getenv('PROXY')
    if p_url:
        try:
            parse_proxy(p_url, client_kwargs)
        except Exception as e:
            logger.warning(_("warn_proxy_parse", e))
            logger.error(_("err_proxy_required"))
            return None
        if not client_kwargs.get('proxy'):
            logger.error(_("err_proxy_required"))
            return None
    return client_kwargs


def harden_permissions(path: str):
    """chmod 600 на файлы с секретами (сессия, .env). На Windows no-op."""
    if os.name == 'nt' or not os.path.exists(path):
        return
    try:
        os.chmod(path, 0o600)
    except OSError as e:
        logger.warning(_("warn_chmod_failed", path, e))


async def resolve_topic(client, entity, topic_input):
    """Возвращает (topic_id, topic_title) по ID темы или части её названия.
    (None, "") — тема не найдена. Общая точка для CLI, демона и веб-морды:
    раньше демон сохранял тему строкой '123 (Название)', которую загрузчик
    потом пытался разобрать как название и всегда падал в 'тема не найдена'."""
    from telethon.tl.functions.channels import GetForumTopicsRequest

    raw = str(topic_input).strip()
    if raw.isdigit():
        logger.info(_("info_filter_topic", int(raw)))
        return int(raw), ""

    logger.info(_("info_search_topic", raw))
    try:
        result = await client(GetForumTopicsRequest(
            channel=entity,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
    except Exception as e:
        logger.error(_("err_topic_list_fail", e))
        return None, ""

    needle = raw.lower()
    for t in (getattr(result, 'topics', None) or []):
        if needle in (getattr(t, 'title', '') or '').lower():
            logger.info(_("info_found_topic", t.title, t.id))
            return t.id, t.title

    logger.error(_("err_topic_not_found", raw))
    return None, ""


async def resolve_topic_id(client, entity, topic_input):
    topic_id, _topic_title = await resolve_topic(client, entity, topic_input)
    return topic_id


async def download_group_messages(client, group_id, topic_input, output_dir, args, stats,
                                  stats_lock, cancel, min_id: int = 0) -> int:
    """Качает медиа группы в output_dir. Возвращает максимальный ID обработанного
    сообщения — его можно сохранить как чекпоинт для инкрементальной синхронизации."""
    heavy_threshold_bytes = args.heavy_threshold * 1024 * 1024
    timeout_per_mb = getattr(args, 'timeout_per_mb', 0)
    os.makedirs(output_dir, exist_ok=True)

    # Уборка огрызков от прошлых запусков. Файлы, которые прямо сейчас пишет
    # другая задача этого же процесса, трогать нельзя: на Linux удаление
    # открытого файла молча ломает чужую загрузку.
    with _ACTIVE_TMP_LOCK:
        busy = set(_ACTIVE_TMP)
    for tmp in glob.iglob(os.path.join(glob.escape(output_dir), "*.part")):
        if tmp in busy:
            continue
        try:
            if time.time() - os.path.getmtime(tmp) > 60:
                os.remove(tmp)
        except OSError:
            pass

    # Один listdir вместо os.path.exists на каждое сообщение: на сетевых дисках
    # это была самая дорогая операция в цикле обхода истории.
    try:
        existing_names = {e.name for e in os.scandir(output_dir) if e.is_file()}
    except OSError:
        existing_names = set()

    max_name_len = name_budget(output_dir)
    if max_name_len < 60:
        logger.warning(_("warn_path_too_long", max_name_len))

    heavy_queue = asyncio.Queue(maxsize=args.queue_size)
    light_queue = asyncio.Queue(maxsize=args.queue_size)

    group_input = group_id
    if str(group_input).lstrip('-').isdigit():
        group_input = int(group_input)

    entity = await client.get_entity(group_input)
    logger.info(_("info_start", getattr(entity, 'title', str(group_input))))

    topic_id = None
    if topic_input:
        topic_id = await resolve_topic_id(client, entity, topic_input)
        if topic_id is None:
            return 0

    show_progress = not getattr(args, 'daemon', False)

    worker_kwargs = dict(
        client=client,
        output_dir=output_dir,
        timeout=args.timeout,
        retries=args.retries,
        stats=stats,
        stats_lock=stats_lock,
        cancel=cancel,
        show_progress=show_progress,
        timeout_per_mb=timeout_per_mb,
    )

    heavy_workers = [
        asyncio.create_task(worker(heavy_queue, **worker_kwargs))
        for _worker in range(args.heavy_workers)
    ]
    light_workers = [
        asyncio.create_task(worker(light_queue, **worker_kwargs))
        for _worker in range(args.workers)
    ]

    iter_kwargs = {}
    if topic_id is not None:
        iter_kwargs['reply_to'] = topic_id
    if min_id > 0:
        # Инкрементальная синхронизация: сервер сам не отдаст то, что уже скачано
        iter_kwargs['min_id'] = min_id
        logger.info(_("info_incremental", min_id))

    max_id_seen = min_id
    try:
        async for message in client.iter_messages(entity, **iter_kwargs):
            if cancel.is_set():
                break
            if message.id > max_id_seen:
                max_id_seen = message.id
            if not message.media:
                continue

            file_name, is_heavy = resolve_file_name(message, heavy_threshold_bytes, max_name_len)
            if not file_name:
                continue

            if file_name in existing_names:
                with stats_lock:
                    stats["exists"] += 1
                continue

            if is_heavy:
                await heavy_queue.put((message, file_name, is_heavy))
            else:
                await light_queue.put((message, file_name, is_heavy))
    finally:
        try:
            for _worker in heavy_workers:
                await heavy_queue.put(None)
            for _worker in light_workers:
                await light_queue.put(None)

            await heavy_queue.join()
            await light_queue.join()

            await asyncio.gather(*heavy_workers, *light_workers, return_exceptions=True)
        except asyncio.CancelledError:
            # Демон гасят — добивать очередь уже некому
            for t in (*heavy_workers, *light_workers):
                t.cancel()
            raise

    # Чекпоинт двигаем только после полного прохода без потерь: иначе файлы,
    # упавшие в ошибку, больше никогда не будут перекачаны.
    if cancel.is_set() or stats.get("error") or stats.get("skipped"):
        return 0
    return max_id_seen


async def run(args, stats, stats_lock, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    apply_config(args)

    if not all([api_id, api_hash, phone]):
        logger.error(_("err_env_missing"))
        logger.info(_("info_fill_env", os.path.join(_get_app_dir(), '.env')))
        if sys.stdin and sys.stdin.isatty():
            try:
                input(_("prompt_exit"))
            except (KeyboardInterrupt, EOFError):
                pass
        return

    if not args.group_id:
        if sys.stdin and sys.stdin.isatty():
            try:
                print(_("ui_interactive_title"))
                g_in = input(_("prompt_group")).strip()
                if not g_in:
                    logger.error(_("err_no_group"))
                    return
                args.group_id = g_in
                
                if not args.output_dir:
                    clean_name = str(args.group_id).rstrip('/').split('/')[-1].lstrip('@')
                    default_base_dir = os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads')
                    default_out = os.path.join(default_base_dir, clean_name)
                    out_in = input(_("prompt_output_dir", default_out)).strip()
                    args.output_dir = out_in if out_in else default_out
            except (KeyboardInterrupt, EOFError):
                return
        else:
            logger.error(_("err_args_missing"))
            return

    if not args.output_dir:
        clean_name = str(args.group_id).rstrip('/').split('/')[-1].lstrip('@')
        default_base_dir = os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads')
        args.output_dir = os.path.join(default_base_dir, clean_name)

    client_kwargs = build_client_kwargs(args)
    if client_kwargs is None:
        return

    session_path = os.path.join(_get_app_dir(), 'tg_session')
    async with TelegramClient(session_path, int(api_id), api_hash, **client_kwargs) as client:
        harden_permissions(session_path + '.session')
        if not await client.is_user_authorized():
            await authorize(client, phone)

        await download_group_messages(client, args.group_id, args.topic, args.output_dir, args, stats, stats_lock, cancel)


async def run_daemon(args, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    apply_config(args)

    if not all([api_id, api_hash, phone]):
        logger.error(_("err_env_missing"))
        return

    data_dir = args.data or _get_app_dir()
    os.makedirs(data_dir, exist_ok=True)
    groups_path = os.path.join(data_dir, 'groups.json')
    store = GroupStore(groups_path)

    client_kwargs = build_client_kwargs(args)
    if client_kwargs is None:
        return

    session_path = os.path.join(data_dir, 'tg_session')
    loop = asyncio.get_running_loop()

    # Клиент создаём, но подключаемся позже — сначала нужно убедиться, что порт
    # веб-сервера свободен, иначе демон падал уже после запроса кода из SMS.
    client = TelegramClient(session_path, int(api_id), api_hash, **client_kwargs)

    active_tasks = {}
    active_cancels = {}
    # Сильные ссылки на фоновые задачи: create_task() без сохранения ссылки
    # разрешает сборщику мусора убить задачу на середине выполнения.
    bg_tasks = set()

    def spawn(coro):
        task = asyncio.create_task(coro)
        bg_tasks.add(task)
        task.add_done_callback(bg_tasks.discard)
        return task

    async def resolve_and_add(group_input, topic_input):
        try:
            g_input = group_input
            if str(g_input).lstrip('-').isdigit():
                g_input = int(g_input)
            entity = await client.get_entity(g_input)
            title = getattr(entity, 'title', str(group_input))
            res_id = getattr(entity, 'id', 0)

            # В store уходит ЧИСТЫЙ id темы, название — отдельным полем.
            topic_key, topic_title = "", ""
            if topic_input:
                t_id, t_title = await resolve_topic(client, entity, topic_input)
                if t_id is not None:
                    topic_key = str(t_id)
                    topic_title = t_title or str(topic_input)
                else:
                    # Тема не нашлась (например, список тем длиннее 100) —
                    # сохраняем как ввели, попробуем разрешить при загрузке.
                    topic_key = str(topic_input)
                    topic_title = str(topic_input)

            store.add(group_input, title, topic_key, res_id, topic_title)
            logger.info(_("webui_added_group", title, group_input))
        except Exception as e:
            logger.error(_("webui_err_add_group", group_input, e))
            store.add(group_input, group_input, topic_input)

    def job_output_dir(g_item, group_id, topic):
        """Папка загрузки задачи. Раньше зависела только от названия группы:
        две разные группы с одинаковым title писали в одну папку и затирали
        файлы друг друга (ID сообщений в разных чатах совпадают)."""
        title = (g_item or {}).get('title') or str(group_id)
        folder = sanitize_filename(str(title), 120)
        taken = {sanitize_filename(str(t), 120) for t in store.titles_before(group_id, topic)}
        if folder in taken:
            suffix = str((g_item or {}).get('resolved_id') or group_id)
            folder = f"{folder}_{sanitize_filename(suffix, 24)}"

        base = os.path.join(data_dir, os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads'), folder)
        if topic and _env_flag('TOPIC_SUBDIR', True):
            label = (g_item or {}).get('topic_title') or f"topic_{topic}"
            base = os.path.join(base, sanitize_filename(str(label), 64))
        return base

    async def trigger_download(group_id, topic):
        task_key = f"{group_id}|{topic}"
        if active_tasks.get(task_key):
            logger.info(_("webui_task_running", group_id, topic))
            return

        job_cancel = threading.Event()
        active_cancels[task_key] = job_cancel
        active_tasks[task_key] = True
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        store.update_stats(group_id, topic, status="downloading", stats=json.dumps({"text_key": "preparing"}), last_error="", last_run=ts)
        web_server.broadcast_task_update(group_id, topic, "downloading", json.dumps({"text_key": "preparing"}), "", ts)

        async def _download_job():
            try:
                g_item = store.get(group_id, topic)
                output_dir = job_output_dir(g_item, group_id, topic)
                # Инкрементальная синхронизация выключена по умолчанию: с ней
                # файл, удалённый с диска вручную или через verify --delete,
                # уже не будет перекачан.
                min_id = int((g_item or {}).get('last_max_id') or 0) if _env_flag('INCREMENTAL', False) else 0

                stats = Counter({"done": 0, "exists": 0, "error": 0, "skipped": 0, "bytes": 0})
                stats_lock = threading.Lock()
                job_done = asyncio.Event()

                async def _periodic_broadcaster():
                    last_b = 0
                    last_t = time.time()
                    while not job_done.is_set():
                        try:
                            await asyncio.sleep(2.0)
                        except asyncio.CancelledError:
                            break

                        now = time.time()
                        dt = now - last_t
                        with stats_lock:
                            d = stats['done']
                            ex = stats['exists']
                            sk = stats['skipped']
                            err = stats['error']
                            cur_b = stats['bytes']

                        speed = ((cur_b - last_b) / dt) / (1024 * 1024) if dt > 0 and (cur_b > last_b) else 0
                        # Отсечку двигаем всегда, даже когда дашборд никто не
                        # смотрит: иначе первый замер после открытия страницы
                        # усреднялся по всему времени простоя и врал в разы.
                        last_b = cur_b
                        last_t = now

                        if web_server.has_listeners():
                            if job_cancel.is_set():
                                cur_status = "stopping"
                                stats_data = {"text_key": "waiting_stop", "new": d, "exists": ex, "speed": speed}
                            else:
                                cur_status = "downloading"
                                stats_data = {"new": d, "exists": ex, "skipped": sk, "error": err, "speed": speed}

                            st_text = json.dumps(stats_data)
                            store.update_stats(group_id, topic, status=cur_status, stats=st_text)
                            web_server.broadcast_task_update(group_id, topic, cur_status, st_text)

                broadcaster_task = spawn(_periodic_broadcaster())

                try:
                    new_max_id = await download_group_messages(
                        client, group_id, topic, output_dir, args,
                        stats, stats_lock, job_cancel, min_id=min_id,
                    )
                finally:
                    job_done.set()
                    broadcaster_task.cancel()

                with stats_lock:
                    stats_data = {"new": stats['done'], "exists": stats['exists'], "skipped": stats['skipped'], "error": stats['error']}
                    err_str = _("err_errors_count", stats['error']) if stats['error'] > 0 else ""

                if job_cancel.is_set():
                    await asyncio.sleep(1.0)
                    status_str = "cancelled"
                    stats_data["text_key"] = "stopped"
                else:
                    status_str = "done" if stats['error'] == 0 else "done_errors"

                res_str = json.dumps(stats_data)
                checkpoint = new_max_id if new_max_id > min_id else None
                store.update_stats(group_id, topic, status=status_str, stats=res_str, last_error=err_str,
                                   last_max_id=checkpoint, persist=True)
                web_server.broadcast_task_update(group_id, topic, status_str, res_str, err_str)
            except Exception as e:
                logger.error(_("webui_err_download", group_id, e))
                err_msg = str(e)
                store.update_stats(group_id, topic, status="error", stats="", last_error=err_msg, persist=True)
                web_server.broadcast_task_update(group_id, topic, "error", "", err_msg)
            finally:
                active_cancels.pop(task_key, None)
                active_tasks.pop(task_key, None)

        spawn(_download_job())

    async def cancel_download(group_id, topic):
        task_key = f"{group_id}|{topic}"
        job_cancel = active_cancels.get(task_key)
        if job_cancel:
            logger.info(_("webui_task_cancel", group_id, topic))
            job_cancel.set()
            store.update_stats(group_id, topic, status="stopping", stats=json.dumps({"text_key": "waiting_stop"}), last_error="")
            web_server.broadcast_task_update(group_id, topic, "stopping", json.dumps({"text_key": "waiting_stop"}))

    async def restart_daemon():
        logger.info(_("info_restart_requested"))

        async def _do_restart():
            # Небольшая пауза, чтобы успел уйти HTTP-редирект пользователю
            await asyncio.sleep(0.3)
            try:
                web_server.stop()
            except Exception:
                pass
            try:
                await client.disconnect()
            except Exception:
                pass
            # Используем subprocess.Popen + os._exit, чтобы избежать гонки (race condition) 
            # с очисткой папки _MEIPASS в Windows при использовании os.execv в PyInstaller.
            import subprocess
            env = os.environ.copy()
            if getattr(sys, 'frozen', False):
                env.pop('_MEIPASS2', None)
                cmd = [sys.executable] + sys.argv[1:]
            else:
                cmd = [sys.executable] + sys.argv
            subprocess.Popen(cmd, env=env)
            os._exit(0)

        spawn(_do_restart())

    async def _resource_monitor():
        """Раз в несколько секунд шлёт в тот же SSE-канал метрики ТОЛЬКО этого
        процесса (не всей системы). Ничего не считает, если на дашборд никто
        не смотрит. Источник метрик: psutil, если он установлен, иначе —
        встроенный сэмплер на /proc (см. utils.ProcessResourceSampler),
        который не требует сторонних пакетов и работает на любом Linux,
        включая NAS без доступа к pip."""
        proc = psutil.Process(os.getpid()) if HAS_PSUTIL else None
        sampler = None if HAS_PSUTIL else ProcessResourceSampler()
        if not HAS_PSUTIL:
            logger.info(_("warn_psutil_not_found"))
        if proc:
            proc.cpu_percent(interval=None)  # прайминг — первый вызов всегда вернёт 0.0
        else:
            sampler.sample()  # прайминг — то же самое: первая точка отсчёта для delta по CPU-времени
        started_at = time.time()
        while True:
            await asyncio.sleep(5.0)
            if not web_server.has_listeners():
                continue
            try:
                if proc:
                    cpu = proc.cpu_percent(interval=None)
                    rss_mb = proc.memory_info().rss / (1024 * 1024)
                else:
                    cpu, rss_mb = sampler.sample()
            except Exception:
                cpu = rss_mb = None
            web_server.broadcast_resource({
                "cpu": cpu,
                "rss_mb": rss_mb,
                "threads": threading.active_count(),
                "active_jobs": len(active_tasks),
                "uptime": int(time.time() - started_at),
            })

    daemon_callbacks = {
        'resolve_and_add': resolve_and_add,
        'trigger_download': trigger_download,
        'cancel_download': cancel_download,
        'restart': restart_daemon,
    }

    host, port_str = args.addr.rsplit(':', 1) if ':' in args.addr else ('127.0.0.1', args.addr)
    host = host.strip('[]')  # допускаем запись вида [::1]:8080
    try:
        port = int(port_str)
    except ValueError:
        logger.error(_("err_port_busy", host, port_str, "invalid port"))
        return

    # Сокет занимаем ДО подключения к Telegram: раньше занятый порт ронял демон
    # уже после запроса кода подтверждения.
    try:
        web_server = WebUIServer(host, port, store, loop, daemon_callbacks)
    except OSError as e:
        logger.error(_("err_port_busy", host, port, e))
        return

    if host not in ('127.0.0.1', 'localhost', '::1') and not web_server.auth_enabled:
        logger.warning(_("warn_webui_public_no_auth", f"{host}:{port}"))

    try:
        await client.connect()
        harden_permissions(session_path + '.session')
        if not await client.is_user_authorized():
            await authorize(client, phone)

        web_server.start()
        spawn(_resource_monitor())

        # SIGTERM от systemd / docker stop / NAS раньше просто убивал процесс
        def _request_stop(signum):
            logger.info(_("info_signal_shutdown", signum))
            cancel.set()
            for job_cancel in list(active_cancels.values()):
                job_cancel.set()

        for sig_name in ('SIGINT', 'SIGTERM', 'SIGBREAK'):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, _request_stop, int(sig))
            except (NotImplementedError, RuntimeError, ValueError):
                # Windows и не-главный поток: там останов приходит из main()
                pass

        logger.info(_("info_daemon_started", host, port))
        while not cancel.is_set():
            await asyncio.sleep(1.0)

        # Даём активным загрузкам корректно закрыть текущие файлы
        for job_cancel in list(active_cancels.values()):
            job_cancel.set()
        deadline = time.monotonic() + 30
        while active_tasks and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
    finally:
        web_server.stop()
        pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        store.save()
        try:
            await client.disconnect()
        except Exception:
            pass


DEFAULT_ENV_TEMPLATE = """APP_API_ID=
APP_API_HASH=
PHONE_NUMBER=

# Опциональные параметры:
# DEFAULT_DOWNLOAD_DIR=downloads
# PROXY=tg://proxy?server=...
# PROXY=socks5://user:pass@127.0.0.1:10808
# TIMEOUT=3600
# TIMEOUT_PER_MB=6
# RETRIES=3
# WORKERS=6
# HEAVY_WORKERS=1
# HEAVY_THRESHOLD=100
# QUEUE_SIZE=50

# Отдельная подпапка для каждой темы форума (по умолчанию включено)
# TOPIC_SUBDIR=1
# Инкрементальная синхронизация: качать только сообщения новее последнего
# успешного запуска. Ускоряет повторные прогоны, но удалённые с диска файлы
# заново скачаны НЕ будут.
# INCREMENTAL=0

# Авторизация веб-интерфейса (обязательна, если --addr смотрит наружу)
# WEBUI_USER=admin
# WEBUI_PASS=
"""


def ensure_env_file(env_path: str):
    if not os.path.exists(env_path):
        try:
            target_dir = os.path.dirname(os.path.abspath(env_path))
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(DEFAULT_ENV_TEMPLATE)
            harden_permissions(env_path)  # в файле будут api_hash и номер телефона
            logger.info(_("info_env_created", env_path))
        except Exception as e:
            logger.warning(_("warn_env_create_fail", env_path, e))


def main():
    init_lang()
    args = parse_args()
    if args.env == '.env':
        args.env = os.path.join(_get_app_dir(), '.env')
    ensure_env_file(args.env)
    load_dotenv(args.env)

    cancel = threading.Event()
    stats = Counter({"done": 0, "exists": 0, "error": 0, "skipped": 0})
    stats_lock = threading.Lock()
    done = threading.Event()

    def _run_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            if args.daemon:
                loop.run_until_complete(run_daemon(args, cancel))
            else:
                loop.run_until_complete(run(args, stats, stats_lock, cancel))
        except Exception as e:
            logger.error(_("err_critical", e))
        finally:
            try:
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                if pending:
                    for t in pending:
                        t.cancel()
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()
            done.set()

    thread = threading.Thread(target=_run_thread, daemon=True)
    thread.start()

    # Ctrl+C ловится одинаково в обоих режимах: демон раньше просто убивался
    # через sys.exit(1), не закрывая ни соединение, ни текущие файлы.
    try:
        while not done.wait(timeout=0.2):
            pass
    except KeyboardInterrupt:
        sys.stderr.write(f"\r\033[K\033[33m{_('stop_graceful')}\033[0m\n")
        sys.stderr.flush()
        cancel.set()
        try:
            while not done.wait(timeout=0.2):
                pass
        except KeyboardInterrupt:
            sys.stderr.write(f"\r\033[K\033[1;31m{_('stop_forced')}\033[0m\n")
            sys.stderr.flush()
            sys.exit(1)

    if not args.daemon:
        print()
        with stats_lock:
            logger.info(_("summary_done", stats['done'], stats['exists'],
                          stats['skipped'], stats['error']))


if __name__ == '__main__':
    main()

