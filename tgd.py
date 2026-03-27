import os
import argparse
import logging
import asyncio
import sys
import threading
import glob
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo
)
from tqdm import tqdm


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
            assert isinstance(key, bytes)
            assert isinstance(iv, bytes) and len(iv) == 16
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


# Кастомный поток для перенаправления вывода в tqdm.write
class TqdmStream:
    def write(self, x):
        if len(x.rstrip()) > 0:
            tqdm.write(x, end='')
    def flush(self):
        pass


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    stream=TqdmStream()
)
logger = logging.getLogger(__name__)

# Убираем назойливые INFO логи от библиотек
logging.getLogger('FastTelethonhelper').setLevel(logging.WARNING)
logging.getLogger('telethon').setLevel(logging.WARNING)

if not HAS_FAST:
    logger.warning("FastTelethonhelper не найден — тяжёлые файлы будут качаться стандартным методом")


class SmartDownloader:
    """
    Исправленная версия: asyncio.Condition + счётчик вместо busy-wait на _value.

    Инварианты:
    - Тяжёлая закачка запускается только если нет других тяжёлых И нет лёгких.
    - Лёгкие закачки запускаются только если нет тяжёлой и есть свободный слот.
    - notify_all() после каждого release гарантирует пробуждение всех ждущих.
    """

    def __init__(self, light_limit: int = 5):
        self.light_limit = light_limit
        self._cond = asyncio.Condition()
        self._active_light = 0
        self._heavy_active = False

    async def acquire(self, is_heavy: bool):
        async with self._cond:
            if is_heavy:
                await self._cond.wait_for(
                    lambda: not self._heavy_active and self._active_light == 0
                )
                self._heavy_active = True
            else:
                await self._cond.wait_for(
                    lambda: not self._heavy_active and self._active_light < self.light_limit
                )
                self._active_light += 1

    async def release(self, is_heavy: bool):
        async with self._cond:
            if is_heavy:
                self._heavy_active = False
            else:
                self._active_light -= 1
            self._cond.notify_all()


def parse_args():
    parser = argparse.ArgumentParser(description="TGD: Загрузчик из Telegram")
    parser.add_argument('group_id', type=str, help="ID группы или ссылка (@username, https://t.me/...)")
    parser.add_argument('output_dir', type=str, help="Папка сохранения")
    parser.add_argument('--env', type=str, default='.env', help="Путь до .env")
    parser.add_argument('--timeout', type=int, default=None, help="Таймаут на файл (переопределяет .env TIMEOUT)")
    parser.add_argument('--retries', type=int, default=None, help="Попыток при сбое (переопределяет .env RETRIES)")
    parser.add_argument('--workers', type=int, default=None, help="Количество воркеров (переопределяет .env WORKERS)")
    parser.add_argument('--queue-size', type=int, default=None, help="Размер очереди (переопределяет .env QUEUE_SIZE)")
    parser.add_argument('--proxy', type=str, default=None, help="SOCKS5/HTTP/MTProto прокси (например: tg://proxy?server=...)")
    return parser.parse_args()


def resolve_file_name(message) -> tuple:
    """
    Возвращает (file_name, is_heavy) или (None, False) если медиа не поддерживается.
    Вынесено отдельно для читаемости и тестируемости.
    """
    media = message.media

    if isinstance(media, MessageMediaPhoto):
        return f"{message.id}_photo.jpg", False

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        attrs = doc.attributes or []
        is_round = any(
            isinstance(a, DocumentAttributeVideo) and a.round_message
            for a in attrs
        )
        if is_round:
            return f"{message.id}_round.mp4", True

        orig_name = next(
            (a.file_name for a in attrs if hasattr(a, 'file_name')),
            None
        )
        if orig_name:
            return f"{message.id}_{orig_name}", True

        mime = doc.mime_type or ""
        ext = mime.split('/')[-1].split(';')[0]
        ext_map = {'quicktime': 'mov', 'x-matroska': 'mkv', 'mpeg': 'mp3', 'ogg': 'ogg'}
        ext = ext_map.get(ext, ext) or 'bin'
        prefix = (
            "video" if mime.startswith('video/') else
            "audio" if mime.startswith('audio/') else "file"
        )
        return f"{message.id}_{prefix}.{ext}", True

    return None, False


async def download_task(client, message, output_dir, timeout, retries, downloader, cancel):
    file_name, is_heavy = resolve_file_name(message)
    if not file_name:
        return "skipped"

    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return "exists"

    tmp_path = file_path + ".part"

    await downloader.acquire(is_heavy)
    try:
        if cancel.is_set():
            return "skipped"
        for attempt in range(retries):
            try:
                with tqdm(unit='B', unit_scale=True, desc=file_name[:25], leave=False) as pbar:
                    def on_progress(current, total):
                        if total and not pbar.total:
                            pbar.total = total
                        pbar.update(current - pbar.n)

                    if is_heavy and HAS_FAST:
                        # FastTelethonhelper: многопоточная докачка одного файла
                        with open(tmp_path, 'wb') as f:
                            await asyncio.wait_for(
                                fast_download(
                                    client,
                                    message.media.document,
                                    f,
                                    progress_callback=on_progress
                                ),
                                timeout=timeout
                            )
                    else:
                        # Стандартный Telethon: качаем напрямую в файл.
                        # Передача пути напрямую в download_media опасна,
                        # поэтому передаем открытый дескриптор для потоковой записи без расхода ОЗУ.
                        with open(tmp_path, 'wb') as f:
                            await asyncio.wait_for(
                                client.download_media(message, file=f, progress_callback=on_progress),
                                timeout=timeout
                            )

                if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0:
                    raise ValueError("Downloaded file is empty")

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
    finally:
        await downloader.release(is_heavy)


async def worker(queue, client, output_dir, timeout, retries, downloader, stats, cancel):
    """Воркер из пула — тянет задачи из очереди до sentinel (None)."""
    while True:
        message = await queue.get()
        if message is None:
            queue.task_done()
            break
        if cancel.is_set():
            queue.task_done()
            continue
        try:
            result = await download_task(client, message, output_dir, timeout, retries, downloader, cancel)
            if not (cancel.is_set() and result == "skipped"):
                stats[result] = stats.get(result, 0) + 1
        except Exception as e:
            logger.error(f"Необработанная ошибка воркера: {e}")
            stats["error"] = stats.get("error", 0) + 1
        finally:
            queue.task_done()


async def run(args, stats, cancel):
    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    # Слияние аргументов консоли с переменными окружения (.env)
    args.timeout = args.timeout if args.timeout is not None else int(os.getenv('TIMEOUT', 3600))
    args.retries = args.retries if args.retries is not None else int(os.getenv('RETRIES', 3))
    args.workers = args.workers if args.workers is not None else int(os.getenv('WORKERS', 6))
    args.queue_size = args.queue_size if args.queue_size is not None else int(os.getenv('QUEUE_SIZE', 50))

    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env")
        return

    os.makedirs(args.output_dir, exist_ok=True)

    # Убираем незавершённые загрузки прошлого запуска
    for tmp in glob.iglob(os.path.join(args.output_dir, "*.part")):
        os.remove(tmp)

    downloader = SmartDownloader(light_limit=5)
    # back-pressure: продюсер встанет, если воркеры не успевают
    queue = asyncio.Queue(maxsize=args.queue_size)

    client_kwargs = {}
    p_url = args.proxy or os.getenv('PROXY')
    if p_url:
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(p_url)
            
            is_tg_proxy = (parsed.scheme == 'tg' and parsed.netloc == 'proxy')
            is_mtproxy_scheme = parsed.scheme in ('mtproxy', 'mtproto')
            
            if is_tg_proxy or is_mtproxy_scheme:
                qs = parse_qs(parsed.query)
                if is_tg_proxy:
                    server = qs.get('server', [''])[0]
                    port = int(qs.get('port', ['0'])[0])
                else:
                    server = parsed.hostname
                    port = parsed.port
                
                secret = qs.get('secret', [''])[0]
                
                client_kwargs['proxy'] = (server, port, secret)
                if secret.lower().startswith('ee'):
                    try:
                        from TelethonFakeTLS import ConnectionTcpMTProxyFakeTLS
                        client_kwargs['connection'] = ConnectionTcpMTProxyFakeTLS
                        logger.info(f"Используется MTProto FakeTLS прокси: {server}:{port}")
                    except ImportError:
                        logger.warning("Секрет начинается с 'ee' (FakeTLS), но модуль TelethonFakeTLS не загружен. Подключение может не удаться.")
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
                logger.info(f"Используется прокси: {p_url}")
        except Exception as e:
            logger.warning(f"Ошибка парсинга прокси {p_url}: {e}")

    async with TelegramClient('tg_session', int(api_id), api_hash, **client_kwargs) as client:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            await client.sign_in(phone, input('Код: '))

        try:
            group_input = args.group_id
            if group_input.lstrip('-').isdigit():
                group_input = int(group_input)
                
            entity = await client.get_entity(group_input)
            logger.info(f"Старт: {getattr(entity, 'title', str(group_input))}")
        except Exception as e:
            logger.error(f"Ошибка доступа к группе: {e}")
            return

        # Запускаем пул воркеров
        workers = [
            asyncio.create_task(
                worker(queue, client, args.output_dir, args.timeout, args.retries, downloader, stats, cancel)
            )
            for _ in range(args.workers)
        ]

        # Продюсер: кладём сообщения в очередь (блокируется при maxsize)
        async for message in client.iter_messages(entity):
            if cancel.is_set():
                break
            if message.media:
                await queue.put(message)

        # Отправляем sentinel каждому воркеру
        for _ in workers:
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*workers)


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    args = parse_args()
    load_dotenv(args.env)
    
    cancel = threading.Event()
    stats = {"done": 0, "exists": 0, "error": 0, "skipped": 0}
    done = threading.Event()

    def _run_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run(args, stats, cancel))
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
    logger.info(
        f"Завершено! Новых: {stats.get('done', 0)}, "
        f"Существует: {stats.get('exists', 0)}, "
        f"Пропущено: {stats.get('skipped', 0)}, "
        f"Ошибок: {stats.get('error', 0)}"
    )

if __name__ == '__main__':
    main()
