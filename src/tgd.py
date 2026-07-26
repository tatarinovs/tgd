import os
import re
import argparse
import logging
import asyncio
import sys
import threading
import glob
import time
from collections import Counter
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo
)
from tqdm import tqdm
from utils import setup_tqdm_logger


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




# TqdmStream импортируется из utils.py


logger = setup_tqdm_logger(__name__)

# Убираем назойливые INFO логи от библиотек
logging.getLogger('FastTelethonhelper').setLevel(logging.WARNING)
logging.getLogger('telethon').setLevel(logging.ERROR)

if not HAS_FAST:
    logger.warning("FastTelethonhelper не найден — тяжёлые файлы будут качаться стандартным методом")


def _get_app_dir() -> str:
    """Возвращает директорию .exe при запуске как frozen, иначе директорию проекта."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    base = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(base) == 'src':
        return os.path.dirname(base)
    return base


def parse_args():
    parser = argparse.ArgumentParser(description="TGD: Загрузчик из Telegram")
    parser.add_argument('group_id', type=str, nargs='?', default=None, help="ID группы или ссылка (@username, https://t.me/...)")
    parser.add_argument('output_dir', type=str, nargs='?', default=None, help="Папка сохранения")
    parser.add_argument('-d', '--daemon', action='store_true', help="Запустить в режиме сервера/демона")
    parser.add_argument('--addr', type=str, default="127.0.0.1:8080", help="Адрес веб-сервера (только для -d)")
    parser.add_argument('--data', type=str, default=None, help="Папка данных (сессия, groups.json)")
    parser.add_argument('--env', type=str, default='.env', help="Путь до .env")
    parser.add_argument('--timeout', type=int, default=None, help="Таймаут на файл (переопределяет .env TIMEOUT)")
    parser.add_argument('--retries', type=int, default=None, help="Попыток при сбое (переопределяет .env RETRIES)")
    parser.add_argument('--workers', type=int, default=None, help="Количество воркеров для лёгких файлов (переопределяет .env WORKERS)")
    parser.add_argument('--heavy-workers', type=int, default=None, help="Количество воркеров для тяжёлых файлов (переопределяет .env HEAVY_WORKERS)")
    parser.add_argument('--heavy-threshold', type=int, default=None, help="Порог 'тяжёлого' файла в МБ (переопределяет .env HEAVY_THRESHOLD)")
    parser.add_argument('--queue-size', type=int, default=None, help="Размер очереди (переопределяет .env QUEUE_SIZE)")
    parser.add_argument('--proxy', type=str, default=None, help="SOCKS5/HTTP/MTProto прокси (например: tg://proxy?server=...)")
    parser.add_argument('--topic', type=str, default=None, help="Название темы (раздела) или ID темы для скачивания только из неё")
    return parser.parse_args()



_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def sanitize_filename(name: str, max_len: int = 200) -> str:
    """Очищает имя файла от опасных символов (path traversal, Windows-запрещённые)."""
    name = _UNSAFE_CHARS.sub('_', name)
    name = name.strip('. ')
    return name[:max_len] or 'file'


def resolve_file_name(message, heavy_threshold_bytes: int) -> tuple:
    """
    Возвращает (file_name, is_heavy) или (None, False) если медиа не поддерживается.
    Вынесено отдельно для читаемости и тестируемости.
    """
    media = message.media

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
            return f"{message.id}_{sanitize_filename(orig_name)}", is_heavy

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


async def download_task(client, message, file_name, is_heavy, output_dir, timeout, retries, cancel, show_progress=True, stats=None, stats_lock=None):
    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return "exists"

    tmp_path = file_path + ".part"

    if cancel.is_set():
        return "skipped"

    for attempt in range(retries):
        try:
            pbar = None
            progress_cb = None
            last_downloaded = [0]

            def on_progress(current, total):
                if pbar is not None:
                    if total and not pbar.total:
                        pbar.total = total
                    pbar.update(current - pbar.n)
                if stats_lock is not None and stats is not None:
                    inc = current - last_downloaded[0]
                    if inc > 0:
                        with stats_lock:
                            stats["bytes"] += inc
                        last_downloaded[0] = current

            if show_progress:
                pbar = tqdm(unit='B', unit_scale=True, desc=file_name[:25], leave=False)

            progress_cb = on_progress

            try:
                if is_heavy and HAS_FAST:
                    # FastTelethonhelper: многопоточная докачка одного файла
                    with open(tmp_path, 'wb') as f:
                        await asyncio.wait_for(
                            fast_download(
                                client,
                                message.media.document,
                                f,
                                progress_callback=progress_cb
                            ),
                            timeout=timeout
                        )
                else:
                    # Стандартный Telethon: потоковая запись без расхода ОЗУ
                    with open(tmp_path, 'wb') as f:
                        await asyncio.wait_for(
                            client.download_media(message, file=f, progress_callback=progress_cb),
                            timeout=timeout
                        )
            finally:
                if pbar is not None:
                    pbar.close()

            try:
                actual_size = os.path.getsize(tmp_path)
            except OSError:
                actual_size = 0
            expected_size = getattr(message.file, 'size', None)
            if not actual_size or (expected_size and actual_size < expected_size * 0.99):
                raise ValueError(f"Неполная загрузка: {actual_size}/{expected_size} байт")

            os.replace(tmp_path, file_path)
            return "done"

        except Exception as e:
            logger.warning(f"[{file_name}] Попытка {attempt + 1}/{retries} не удалась: {e}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                return "error"


async def worker(queue, client, output_dir, timeout, retries, stats, stats_lock, cancel, show_progress=True):
    """Воркер из пула — тянет задачи (message, file_name, is_heavy) из очереди до sentinel (None)."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        message, file_name, is_heavy = item

        if cancel.is_set():
            queue.task_done()
            del item, message
            continue

        try:
            result = await download_task(client, message, file_name, is_heavy, output_dir, timeout, retries, cancel, show_progress=show_progress, stats=stats, stats_lock=stats_lock)
            if result != "skipped":
                with stats_lock:
                    stats[result] += 1
        except Exception as e:
            logger.error(f"Необработанная ошибка воркера: {e}")
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

    await client.send_code_request(phone)
    code = await loop.run_in_executor(None, lambda: input('Код подтверждения: ').strip())

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        # Включён двухфакторный пароль (2FA)
        password = await loop.run_in_executor(None, lambda: input('Пароль 2FA: ').strip())
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
                logger.info(f"Используется MTProto FakeTLS прокси: {server}:{port}")
            except ImportError:
                logger.warning(
                    "Секрет начинается с 'ee' (FakeTLS), но модуль TelethonFakeTLS не загружен. "
                    "Подключение может не удаться."
                )
                from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
                client_kwargs['connection'] = ConnectionTcpMTProxyRandomizedIntermediate
                logger.info(f"Используется MTProto прокси: {server}:{port}")
        else:
            from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
            client_kwargs['connection'] = ConnectionTcpMTProxyRandomizedIntermediate
            logger.info(f"Используется MTProto прокси: {server}:{port}")
    else:
        import python_socks
        ptype = python_socks.ProxyType.SOCKS5 if parsed.scheme.startswith('socks') else python_socks.ProxyType.HTTP
        proxy_dict = {'proxy_type': ptype, 'addr': parsed.hostname, 'port': parsed.port}
        if parsed.username:
            proxy_dict['username'] = parsed.username
            proxy_dict['password'] = parsed.password
        client_kwargs['proxy'] = proxy_dict
        logger.info(f"Используется {parsed.scheme} прокси: {parsed.hostname}:{parsed.port}")


from store import GroupStore
from webui import WebUIServer


async def download_group_messages(client, group_id, topic_input, output_dir, args, stats, stats_lock, cancel):
    heavy_threshold_bytes = args.heavy_threshold * 1024 * 1024
    os.makedirs(output_dir, exist_ok=True)

    for tmp in glob.iglob(os.path.join(output_dir, "*.part")):
        try:
            if time.time() - os.path.getmtime(tmp) > 60:
                os.remove(tmp)
        except OSError:
            pass

    heavy_queue = asyncio.Queue(maxsize=args.queue_size)
    light_queue = asyncio.Queue(maxsize=args.queue_size)

    group_input = group_id
    if str(group_input).lstrip('-').isdigit():
        group_input = int(group_input)

    entity = await client.get_entity(group_input)
    logger.info(f"Старт: {getattr(entity, 'title', str(group_input))}")

    topic_id = None
    if topic_input:
        from telethon.tl.functions.channels import GetForumTopicsRequest
        try:
            topic_id = int(topic_input)
            logger.info(f"Фильтрация по ID темы: {topic_id}")
        except ValueError:
            logger.info(f"Поиск темы с названием: '{topic_input}'...")
            try:
                result = await client(GetForumTopicsRequest(
                    channel=entity,
                    offset_date=None,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                ))
                matched_topic = None
                if result and hasattr(result, 'topics'):
                    for t in result.topics:
                        if str(topic_input).lower() in t.title.lower():
                            matched_topic = t
                            break
                if matched_topic:
                    topic_id = matched_topic.id
                    logger.info(f"Найдена тема '{matched_topic.title}' с ID: {topic_id}")
                else:
                    logger.error(f"Тема '{topic_input}' не найдена!")
                    return
            except Exception as e:
                logger.error(f"Не удалось получить список тем: {e}")
                return

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
    )

    heavy_workers = [
        asyncio.create_task(worker(heavy_queue, **worker_kwargs))
        for _ in range(args.heavy_workers)
    ]
    light_workers = [
        asyncio.create_task(worker(light_queue, **worker_kwargs))
        for _ in range(args.workers)
    ]

    iter_kwargs = {}
    if topic_id is not None:
        iter_kwargs['reply_to'] = topic_id

    async for message in client.iter_messages(entity, **iter_kwargs):
        if cancel.is_set():
            break
        if message.media:
            file_name, is_heavy = resolve_file_name(message, heavy_threshold_bytes)
            if not file_name:
                continue

            if is_heavy:
                await heavy_queue.put((message, file_name, is_heavy))
            else:
                await light_queue.put((message, file_name, is_heavy))

    for _ in heavy_workers:
        await heavy_queue.put(None)
    for _ in light_workers:
        await light_queue.put(None)

    await heavy_queue.join()
    await light_queue.join()

    await asyncio.gather(*heavy_workers)
    await asyncio.gather(*light_workers)

    import gc
    gc.collect()


async def run(args, stats, stats_lock, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    args.timeout = args.timeout if args.timeout is not None else int(os.getenv('TIMEOUT', 3600))
    args.retries = args.retries if args.retries is not None else int(os.getenv('RETRIES', 3))
    args.workers = args.workers if args.workers is not None else int(os.getenv('WORKERS', 6))
    args.heavy_workers = args.heavy_workers if args.heavy_workers is not None else int(os.getenv('HEAVY_WORKERS', 1))
    env_heavy_threshold = os.getenv('HEAVY_THRESHOLD', '100')
    args.heavy_threshold = args.heavy_threshold if args.heavy_threshold is not None else int(env_heavy_threshold)
    args.queue_size = args.queue_size if args.queue_size is not None else int(os.getenv('QUEUE_SIZE', 50))

    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env")
        logger.info(f"Заполните файл {os.path.join(_get_app_dir(), '.env')} и запустите программу снова.")
        if sys.stdin and sys.stdin.isatty():
            try:
                input("\nНажмите Enter для выхода...")
            except (KeyboardInterrupt, EOFError):
                pass
        return

    if not args.group_id:
        if sys.stdin and sys.stdin.isatty():
            try:
                print("\n=== Интерактивный режим TGD ===")
                g_in = input("Введите ID группы/канала, @username или ссылку: ").strip()
                if not g_in:
                    logger.error("Группа не указана. Завершение работы.")
                    return
                args.group_id = g_in
                
                if not args.output_dir:
                    clean_name = str(args.group_id).rstrip('/').split('/')[-1].lstrip('@')
                    default_base_dir = os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads')
                    default_out = os.path.join(default_base_dir, clean_name)
                    out_in = input(f"Папка сохранения [{default_out}]: ").strip()
                    args.output_dir = out_in if out_in else default_out
            except (KeyboardInterrupt, EOFError):
                return
        else:
            logger.error("Ошибка: не указаны group_id или output_dir. Используйте -d для веб-интерфейса.")
            return

    if not args.output_dir:
        clean_name = str(args.group_id).rstrip('/').split('/')[-1].lstrip('@')
        default_base_dir = os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads')
        args.output_dir = os.path.join(default_base_dir, clean_name)

    client_kwargs = {'receive_updates': False}
    p_url = args.proxy or os.getenv('PROXY')
    if p_url:
        try:
            parse_proxy(p_url, client_kwargs)
        except Exception as e:
            logger.warning(f"Ошибка парсинга прокси: {e}")

    session_path = os.path.join(_get_app_dir(), 'tg_session')
    async with TelegramClient(session_path, int(api_id), api_hash, **client_kwargs) as client:
        if not await client.is_user_authorized():
            await authorize(client, phone)

        await download_group_messages(client, args.group_id, args.topic, args.output_dir, args, stats, stats_lock, cancel)


async def run_daemon(args, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    args.timeout = args.timeout if args.timeout is not None else int(os.getenv('TIMEOUT', 3600))
    args.retries = args.retries if args.retries is not None else int(os.getenv('RETRIES', 3))
    args.workers = args.workers if args.workers is not None else int(os.getenv('WORKERS', 6))
    args.heavy_workers = args.heavy_workers if args.heavy_workers is not None else int(os.getenv('HEAVY_WORKERS', 1))
    env_heavy_threshold = os.getenv('HEAVY_THRESHOLD', '100')
    args.heavy_threshold = args.heavy_threshold if args.heavy_threshold is not None else int(env_heavy_threshold)
    args.queue_size = args.queue_size if args.queue_size is not None else int(os.getenv('QUEUE_SIZE', 50))

    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env")
        return

    data_dir = args.data or _get_app_dir()
    os.makedirs(data_dir, exist_ok=True)
    groups_path = os.path.join(data_dir, 'groups.json')
    store = GroupStore(groups_path)

    client_kwargs = {'receive_updates': False}
    p_url = args.proxy or os.getenv('PROXY')
    if p_url:
        try:
            parse_proxy(p_url, client_kwargs)
        except Exception as e:
            logger.warning(f"Ошибка парсинга прокси: {e}")

    session_path = os.path.join(data_dir, 'tg_session')
    loop = asyncio.get_running_loop()

    client = TelegramClient(session_path, int(api_id), api_hash, **client_kwargs)
    await client.connect()
    if not await client.is_user_authorized():
        await authorize(client, phone)

    active_tasks = {}

    async def resolve_and_add(group_input, topic_input):
        try:
            g_input = group_input
            if str(g_input).lstrip('-').isdigit():
                g_input = int(g_input)
            entity = await client.get_entity(g_input)
            title = getattr(entity, 'title', str(group_input))
            res_id = getattr(entity, 'id', 0)

            topic_title = topic_input
            if topic_input:
                from telethon.tl.functions.channels import GetForumTopicsRequest
                try:
                    t_id = int(topic_input)
                except ValueError:
                    try:
                        res = await client(GetForumTopicsRequest(channel=entity, offset_date=None, offset_id=0, offset_topic=0, limit=100))
                        if res and hasattr(res, 'topics'):
                            for t in res.topics:
                                if topic_input.lower() in t.title.lower():
                                    topic_title = f"{t.id} ({t.title})"
                                    break
                    except Exception:
                        pass
            store.add(group_input, title, topic_title, res_id)
            logger.info(f"[WebUI] Добавлена группа: {title} ({group_input})")
        except Exception as e:
            logger.error(f"[WebUI] Ошибка добавления группы {group_input}: {e}")
            store.add(group_input, group_input, topic_input)

    active_cancels = {}

    async def trigger_download(group_id, topic):
        task_key = f"{group_id}|{topic}"
        if active_tasks.get(task_key):
            logger.info(f"[WebUI] Задача {task_key} уже выполняется.")
            return

        job_cancel = threading.Event()
        active_cancels[task_key] = job_cancel
        active_tasks[task_key] = True
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        store.update_stats(group_id, topic, status="Скачивание...", stats="Подготовка...", last_error="", last_run=ts)
        web_server.broadcast_task_update(group_id, topic, "Скачивание...", "Подготовка...", "", ts)

        async def _download_job():
            try:
                g_item = next((g for g in store.list() if str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic)), None)
                folder_name = sanitize_filename(g_item.get('title', str(group_id)) if g_item else str(group_id))
                default_base_dir = os.getenv('DEFAULT_DOWNLOAD_DIR', 'downloads')
                output_dir = os.path.join(data_dir, default_base_dir, folder_name)

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
                        if web_server.broadcaster._listeners:
                            now = time.time()
                            dt = now - last_t
                            with stats_lock:
                                d = stats['done']
                                ex = stats['exists']
                                sk = stats['skipped']
                                err = stats['error']
                                cur_b = stats['bytes']

                            speed = ((cur_b - last_b) / dt) / (1024 * 1024) if dt > 0 and (cur_b > last_b) else 0
                            last_b = cur_b
                            last_t = now

                            if job_cancel.is_set():
                                cur_status = "Остановка..."
                                speed_txt = f" [{speed:.1f} MB/s]" if speed > 0.05 else ""
                                st_text = f"Ждём завершения текущих загрузок... (Новых: {d}, Было: {ex}){speed_txt}"
                            else:
                                cur_status = "Скачивание..."
                                speed_txt = f" [{speed:.1f} MB/s]" if speed > 0.05 else ""
                                st_text = f"Новых: {d}, Было: {ex}, Пропущено: {sk}, Ошибок: {err}{speed_txt}"

                            store.update_stats(group_id, topic, status=cur_status, stats=st_text)
                            web_server.broadcast_task_update(group_id, topic, cur_status, st_text)

                broadcaster_task = asyncio.create_task(_periodic_broadcaster())

                try:
                    await download_group_messages(client, group_id, topic, output_dir, args, stats, stats_lock, job_cancel)
                finally:
                    job_done.set()
                    broadcaster_task.cancel()

                with stats_lock:
                    res_str = f"Новых: {stats['done']}, Существует: {stats['exists']}, Пропущено: {stats['skipped']}, Ошибок: {stats['error']}"
                    err_str = f"Ошибок: {stats['error']}" if stats['error'] > 0 else ""

                if job_cancel.is_set():
                    await asyncio.sleep(1.0)
                    status_str = "Отменено"
                    res_str += " (Остановлено)"
                else:
                    status_str = "Завершено" if stats['error'] == 0 else "Завершено с ошибками"

                store.update_stats(group_id, topic, status=status_str, stats=res_str, last_error=err_str, persist=True)
                web_server.broadcast_task_update(group_id, topic, status_str, res_str, err_str)
            except Exception as e:
                logger.error(f"[WebUI] Ошибка скачивания {group_id}: {e}")
                err_msg = str(e)
                store.update_stats(group_id, topic, status="Ошибка", stats="", last_error=err_msg, persist=True)
                web_server.broadcast_task_update(group_id, topic, "Ошибка", "", err_msg)
            finally:
                active_cancels.pop(task_key, None)
                active_tasks.pop(task_key, None)
                import gc
                gc.collect()

        asyncio.create_task(_download_job())

    async def cancel_download(group_id, topic):
        task_key = f"{group_id}|{topic}"
        job_cancel = active_cancels.get(task_key)
        if job_cancel:
            logger.info(f"[WebUI] Отмена задачи {task_key}...")
            job_cancel.set()
            store.update_stats(group_id, topic, status="Остановка...", stats="Ждём завершения текущих загрузок...", last_error="")
            web_server.broadcast_task_update(group_id, topic, "Остановка...", "Ждём завершения текущих загрузок...")

    daemon_callbacks = {
        'resolve_and_add': resolve_and_add,
        'trigger_download': trigger_download,
        'cancel_download': cancel_download
    }

    host, port_str = args.addr.split(':') if ':' in args.addr else ('127.0.0.1', args.addr)
    web_server = WebUIServer(host, int(port_str), store, loop, daemon_callbacks)
    web_server.start()

    logger.info(f"TGD Daemon запущен на http://{host}:{port_str}")
    try:
        while not cancel.is_set():
            await asyncio.sleep(0.5)
    finally:
        web_server.stop()
        pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
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
# RETRIES=3
# WORKERS=6
# HEAVY_WORKERS=1
# HEAVY_THRESHOLD=100
# QUEUE_SIZE=50
"""


def ensure_env_file(env_path: str):
    if not os.path.exists(env_path):
        try:
            target_dir = os.path.dirname(os.path.abspath(env_path))
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(DEFAULT_ENV_TEMPLATE)
            logger.info(f"Создан шаблон конфигурации: {env_path}")
        except Exception as e:
            logger.warning(f"Не удалось создать файл {env_path}: {e}")


def main():
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
            logger.error(f"Критическая ошибка: {e}")
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

    try:
        while not done.wait(timeout=0.2):
            pass
    except KeyboardInterrupt:
        sys.stderr.write("\r\033[K\033[33m[СТОП] Завершаем текущие процессы... (повторный Ctrl+C — прервать немедленно)\033[0m\n")
        sys.stderr.flush()
        cancel.set()
        try:
            while not done.wait(timeout=0.2):
                pass
        except KeyboardInterrupt:
            sys.stderr.write("\r\033[K\033[1;31m[ПРИНУДИТЕЛЬНО] Прерываем немедленно...\033[0m\n")
            sys.stderr.flush()
            sys.exit(1)

    if not args.daemon:
        print()
        with stats_lock:
            logger.info(
                f"Завершено! Новых: {stats['done']}, "
                f"Существует: {stats['exists']}, "
                f"Пропущено: {stats['skipped']}, "
                f"Ошибок: {stats['error']}"
            )


if __name__ == '__main__':
    main()

