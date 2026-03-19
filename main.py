import os
import argparse
import logging
import asyncio
import glob
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo
)
from tqdm.asyncio import tqdm

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
    parser.add_argument('group_id', type=int, help="ID группы")
    parser.add_argument('output_dir', type=str, help="Папка сохранения")
    parser.add_argument('--env', type=str, default='.env', help="Путь до .env")
    parser.add_argument('--timeout', type=int, default=3600, help="Таймаут на файл")
    parser.add_argument('--retries', type=int, default=3, help="Попыток при сбое")
    parser.add_argument('--workers', type=int, default=6, help="Количество воркеров")
    parser.add_argument('--queue-size', type=int, default=50, help="Размер очереди (back-pressure)")
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
        is_round = any(
            isinstance(a, DocumentAttributeVideo) and a.round_message
            for a in doc.attributes
        )
        if is_round:
            return f"{message.id}_round.mp4", True

        orig_name = next(
            (a.file_name for a in doc.attributes if hasattr(a, 'file_name')),
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


async def download_task(client, message, output_dir, timeout, retries, downloader):
    file_name, is_heavy = resolve_file_name(message)
    if not file_name:
        return "skipped"

    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return "exists"

    tmp_path = file_path + ".part"

    await downloader.acquire(is_heavy)
    try:
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


async def worker(queue, client, output_dir, timeout, retries, downloader, stats):
    """Воркер из пула — тянет задачи из очереди до sentinel (None)."""
    while True:
        message = await queue.get()
        if message is None:
            queue.task_done()
            break
        try:
            result = await download_task(client, message, output_dir, timeout, retries, downloader)
            stats[result] = stats.get(result, 0) + 1

            total = sum(stats.values())
            if total % 50 == 0:
                logger.info(
                    f"Прогресс: Новых {stats.get('done', 0)}, "
                    f"Существует {stats.get('exists', 0)}, "
                    f"Пропущено {stats.get('skipped', 0)}, "
                    f"Ошибок {stats.get('error', 0)}"
                )
        except Exception as e:
            logger.error(f"Необработанная ошибка воркера: {e}")
            stats["error"] = stats.get("error", 0) + 1
        finally:
            queue.task_done()


async def main():
    args = parse_args()
    load_dotenv(args.env)

    api_id = os.getenv('APP_API_ID')
    api_hash = os.getenv('APP_API_HASH')
    phone = os.getenv('PHONE_NUMBER')

    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка: не заданы APP_API_ID, APP_API_HASH или PHONE_NUMBER в .env")
        return

    os.makedirs(args.output_dir, exist_ok=True)

    # Убираем незавершённые загрузки прошлого запуска
    for tmp in glob.glob(os.path.join(args.output_dir, "*.part")):
        os.remove(tmp)

    downloader = SmartDownloader(light_limit=5)
    # back-pressure: продюсер встанет, если воркеры не успевают
    queue = asyncio.Queue(maxsize=args.queue_size)
    stats = {"done": 0, "exists": 0, "error": 0, "skipped": 0}

    async with TelegramClient('tg_session', int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            await client.sign_in(phone, input('Код: '))

        try:
            entity = await client.get_entity(args.group_id)
            logger.info(f"Старт: {getattr(entity, 'title', args.group_id)}")
        except Exception as e:
            logger.error(f"Ошибка доступа к группе: {e}")
            return

        # Запускаем пул воркеров
        workers = [
            asyncio.create_task(
                worker(queue, client, args.output_dir, args.timeout, args.retries, downloader, stats)
            )
            for _ in range(args.workers)
        ]

        # Продюсер: кладём сообщения в очередь (блокируется при maxsize)
        async for message in client.iter_messages(entity):
            if message.media:
                await queue.put(message)

        # Отправляем sentinel каждому воркеру
        for _ in workers:
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*workers)

    print()
    logger.info(
        f"Завершено! Новых: {stats.get('done', 0)}, "
        f"Существует: {stats.get('exists', 0)}, "
        f"Пропущено: {stats.get('skipped', 0)}, "
        f"Ошибок: {stats.get('error', 0)}"
    )


if __name__ == '__main__':
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
