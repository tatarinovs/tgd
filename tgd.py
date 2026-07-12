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
    """Возвращает директорию .exe при запуске как frozen, иначе директорию скрипта."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def parse_args():
    parser = argparse.ArgumentParser(description="TGD: Загрузчик из Telegram")
    parser.add_argument('group_id', type=str, help="ID группы или ссылка (@username, https://t.me/...)")
    parser.add_argument('output_dir', type=str, help="Папка сохранения")
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


async def download_task(client, message, file_name, is_heavy, output_dir, timeout, retries, cancel):
    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return "exists"

    tmp_path = file_path + ".part"

    if cancel.is_set():
        return "skipped"

    for attempt in range(retries):
        try:
            with tqdm(unit='B', unit_scale=True, desc=file_name[:25], leave=False) as pbar:
                def make_progress(bar):
                    def on_progress(current, total):
                        if total and not bar.total:
                            bar.total = total
                        bar.update(current - bar.n)
                    return on_progress

                progress_cb = make_progress(pbar)

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
                    # Стандартный Telethon: передаём открытый дескриптор для
                    # потоковой записи без расхода ОЗУ.
                    with open(tmp_path, 'wb') as f:
                        await asyncio.wait_for(
                            client.download_media(message, file=f, progress_callback=progress_cb),
                            timeout=timeout
                        )

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


async def worker(queue, client, output_dir, timeout, retries, stats, stats_lock, cancel):
    """Воркер из пула — тянет задачи (message, file_name, is_heavy) из очереди до sentinel (None)."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        message, file_name, is_heavy = item

        if cancel.is_set():
            queue.task_done()
            continue

        try:
            result = await download_task(client, message, file_name, is_heavy, output_dir, timeout, retries, cancel)
            # Не считаем skipped — воркер пропустил задачу из-за отмены
            if result != "skipped":
                with stats_lock:
                    stats[result] += 1
        except Exception as e:
            logger.error(f"Необработанная ошибка воркера: {e}")
            with stats_lock:
                stats["error"] += 1
        finally:
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


async def run(args, stats, stats_lock, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    # Слияние аргументов консоли с переменными окружения (.env)
    args.timeout = args.timeout if args.timeout is not None else int(os.getenv('TIMEOUT', 3600))
    args.retries = args.retries if args.retries is not None else int(os.getenv('RETRIES', 3))
    args.workers = args.workers if args.workers is not None else int(os.getenv('WORKERS', 6))
    args.heavy_workers = args.heavy_workers if args.heavy_workers is not None else int(os.getenv('HEAVY_WORKERS', 1))
    
    env_heavy_threshold = os.getenv('HEAVY_THRESHOLD', '100')
    args.heavy_threshold = args.heavy_threshold if args.heavy_threshold is not None else int(env_heavy_threshold)
    heavy_threshold_bytes = args.heavy_threshold * 1024 * 1024
    
    args.queue_size = args.queue_size if args.queue_size is not None else int(os.getenv('QUEUE_SIZE', 50))

    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env")
        return

    os.makedirs(args.output_dir, exist_ok=True)

    # Убираем незавершённые загрузки прошлого запуска.
    # Только файлы старше 60 секунд — чтобы не трогать активные загрузки
    # при случайном параллельном запуске.
    for tmp in glob.iglob(os.path.join(args.output_dir, "*.part")):
        try:
            if time.time() - os.path.getmtime(tmp) > 60:
                os.remove(tmp)
        except OSError:
            pass

    # Две независимые очереди, чтобы избежать "голодания воркеров" (worker starvation)
    heavy_queue = asyncio.Queue(maxsize=args.queue_size)
    light_queue = asyncio.Queue(maxsize=args.queue_size)

    client_kwargs = {}
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

        try:
            group_input = args.group_id
            if group_input.lstrip('-').isdigit():
                group_input = int(group_input)

            entity = await client.get_entity(group_input)
            logger.info(f"Старт: {getattr(entity, 'title', str(group_input))}")
        except Exception as e:
            logger.error(f"Ошибка доступа к группе: {e}")
            return

        topic_id = None
        if args.topic:
            from telethon.tl.functions.channels import GetForumTopicsRequest
            try:
                topic_id = int(args.topic)
                logger.info(f"Фильтрация по ID темы: {topic_id}")
            except ValueError:
                logger.info(f"Поиск темы с названием: '{args.topic}'...")
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
                            if args.topic.lower() in t.title.lower():
                                matched_topic = t
                                break
                    
                    if matched_topic:
                        topic_id = matched_topic.id
                        logger.info(f"Найдена тема '{matched_topic.title}' с ID: {topic_id}")
                    else:
                        logger.error(f"Тема '{args.topic}' не найдена!")
                        if result and hasattr(result, 'topics') and result.topics:
                            logger.info("Доступные темы в этой группе:")
                            for t in result.topics:
                                logger.info(f" - {t.title} (ID: {t.id})")
                        return
                except Exception as e:
                    logger.error(f"Не удалось получить список тем (возможно, группа не является форумом): {e}")
                    return

        worker_kwargs = dict(
            client=client,
            output_dir=args.output_dir,
            timeout=args.timeout,
            retries=args.retries,
            stats=stats,
            stats_lock=stats_lock,
            cancel=cancel,
        )

        # Воркеры для тяжёлых файлов (настраиваемое количество)
        heavy_workers = [
            asyncio.create_task(worker(heavy_queue, **worker_kwargs))
            for _ in range(args.heavy_workers)
        ]

        # Воркеры для лёгких файлов
        light_workers = [
            asyncio.create_task(worker(light_queue, **worker_kwargs))
            for _ in range(args.workers)
        ]

        # Продюсер: распределяет сообщения по очередям
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

        # Отправляем sentinel каждому воркеру
        for _ in heavy_workers:
            await heavy_queue.put(None)
        for _ in light_workers:
            await light_queue.put(None)

        await heavy_queue.join()
        await light_queue.join()

        await asyncio.gather(*heavy_workers)
        await asyncio.gather(*light_workers)


def main():
    # if os.name == 'nt':
    #     import warnings
    #     with warnings.catch_warnings():
    #         warnings.simplefilter("ignore", DeprecationWarning)
    #         asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    args = parse_args()
    # Если путь не задан явно — ищем .env рядом с exe/скриптом
    if args.env == '.env':
        args.env = os.path.join(_get_app_dir(), '.env')
    load_dotenv(args.env)

    cancel = threading.Event()
    stats = Counter({"done": 0, "exists": 0, "error": 0, "skipped": 0})
    stats_lock = threading.Lock()
    done = threading.Event()

    def _run_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run(args, stats, stats_lock, cancel))
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
        finally:
            loop.close()
            done.set()

    thread = threading.Thread(target=_run_thread, daemon=True)
    thread.start()

    try:
        # join()/wait() с таймаутом — надёжный способ поймать Ctrl+C на Windows
        while not done.wait(timeout=0.2):
            pass
    except KeyboardInterrupt:
        sys.stderr.write("\r\033[K\033[33m[СТОП] Завершаем текущие загрузки... (повторный Ctrl+C — прервать немедленно)\033[0m\n")
        sys.stderr.flush()
        cancel.set()
        try:
            while not done.wait(timeout=0.2):
                pass
        except KeyboardInterrupt:
            sys.stderr.write("\r\033[K\033[1;31m[ПРИНУДИТЕЛЬНО] Прерываем немедленно...\033[0m\n")
            sys.stderr.flush()
            sys.exit(1)

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
