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
from FastTelethonhelper import download_file

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SmartDownloader:
    """Уровни доступа: Тяжелые файлы (эксклюзивно), Легкие (параллельно до 5)"""
    def __init__(self, light_limit=5):
        self.light_limit = light_limit
        self.light_semaphore = asyncio.Semaphore(light_limit)
        self.heavy_lock = asyncio.Lock()
        self.heavy_active_event = asyncio.Event()
        self.heavy_active_event.set() # Set = Тяжелых закачек НЕТ

    async def acquire(self, is_heavy: bool):
        if is_heavy:
            # 1. Ждем своей очереди среди тяжелых
            await self.heavy_lock.acquire()
            # 2. Блокируем запуск новых легких
            self.heavy_active_event.clear()
            # 3. Ждем, пока докачаются текущие легкие, чтобы быть в полном одиночестве
            while self.light_semaphore._value < self.light_limit:
                await asyncio.sleep(0.1)
        else:
            # 1. Ждем, пока не будет тяжелых закачек
            await self.heavy_active_event.wait()
            # 2. Занимаем слот в семафоре
            await self.light_semaphore.acquire()

    def release(self, is_heavy: bool):
        if is_heavy:
            self.heavy_active_event.set()
            self.heavy_lock.release()
        else:
            self.light_semaphore.release()

# Глобальный менеджер закачек
downloader_manager = SmartDownloader(light_limit=5)

def parse_args():
    parser = argparse.ArgumentParser(description="TGD: Загрузчик из Telegram")
    parser.add_argument('group_id', type=int, help="ID группы")
    parser.add_argument('output_dir', type=str, help="Папка сохранения")
    parser.add_argument('--env', type=str, default='.env', help="Путь до .env")
    parser.add_argument('--timeout', type=int, default=3600, help="Таймаут на файл")
    parser.add_argument('--retries', type=int, default=3, help="Попыток при сбое")
    return parser.parse_args()

def get_file_ext(mime_type):
    if not mime_type: return "bin"
    ext = mime_type.split('/')[-1].split(';')[0]
    mapping = {'quicktime': 'mov', 'x-matroska': 'mkv', 'mpeg': 'mp3', 'ogg': 'ogg'}
    return mapping.get(ext, ext)

async def download_task(client, message, output_dir, timeout, retries):
    is_heavy = False
    file_name = None
    
    # Определение типа и имени
    if isinstance(message.media, MessageMediaPhoto):
        file_name = f"{message.id}_photo.jpg"
        is_heavy = False
    elif isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        mime_type = doc.mime_type
        is_heavy = True # Документы (видео, аудио, файлы) считаем тяжелыми
        
        is_round = any(isinstance(a, DocumentAttributeVideo) and a.round_message for a in doc.attributes)
        if is_round:
            file_name = f"{message.id}_round.mp4"
        else:
            orig_name = next((a.file_name for a in doc.attributes if hasattr(a, 'file_name')), None)
            if orig_name:
                file_name = f"{message.id}_{orig_name}"
            else:
                prefix = "video" if mime_type.startswith('video/') else "audio" if mime_type.startswith('audio/') else "file"
                file_name = f"{message.id}_{prefix}.{get_file_ext(mime_type)}"

    if not file_name:
        return "skipped"

    file_path = os.path.join(output_dir, file_name)
    tmp_path = file_path + ".part"

    if os.path.exists(file_path):
        return "exists"

    # Умное управление доступом
    await downloader_manager.acquire(is_heavy)
    
    try:
        for attempt in range(retries):
            pbar = None
            try:
                def progress_callback(current, total):
                    nonlocal pbar
                    if not pbar and total:
                        pbar = tqdm(total=total, unit='B', unit_scale=True, desc=file_name[:20], leave=False)
                    if pbar: pbar.update(current - pbar.n)

                if is_heavy:
                    with open(tmp_path, "wb") as f:
                        await asyncio.wait_for(
                            download_file(client, message.media.document, f, progress_callback=progress_callback),
                            timeout=timeout
                        )
                else:
                    await asyncio.wait_for(
                        client.download_media(message, tmp_path, progress_callback=progress_callback),
                        timeout=timeout
                    )
                
                if pbar: pbar.close()
                if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
                    os.replace(tmp_path, file_path)
                    return "done"
                else:
                    raise Exception("Downloaded file is empty")

            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"[{file_name}] Попытка {attempt+1}/{retries} не удалась: {e}")
                if os.path.exists(tmp_path): 
                    try: os.remove(tmp_path)
                    except: pass
                if attempt == retries - 1: return "error"
                await asyncio.sleep(2 ** attempt)
            finally:
                if pbar: 
                    pbar.close()
                    pbar = None
    finally:
        downloader_manager.release(is_heavy)

async def main():
    args = parse_args()
    load_dotenv(args.env)
    
    api_id, api_hash, phone = os.getenv('APP_API_ID'), os.getenv('APP_API_HASH'), os.getenv('PHONE_NUMBER')
    if not all([api_id, api_hash, phone]):
        logger.error("Ошибка переменных окружения")
        return

    os.makedirs(args.output_dir, exist_ok=True)
    for t in glob.glob(os.path.join(args.output_dir, "*.part")): os.remove(t)

    async with TelegramClient('tg_session', int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            await client.sign_in(phone, input('Код: '))

        try:
            entity = await client.get_entity(args.group_id)
            logger.info(f"Старт: {getattr(entity, 'title', args.group_id)}")
        except Exception as e:
            logger.error(f"Ошибка доступа: {e}"); return

        tasks = []
        stats = {"done": 0, "exists": 0, "error": 0, "skipped": 0}
        
        async for message in client.iter_messages(entity):
            if not message.media: continue
            
            t = asyncio.create_task(download_task(client, message, args.output_dir, args.timeout, args.retries))
            tasks.append(t)
            
            if len(tasks) >= 20: # Ограничиваем количество задач в очереди
                finished, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for f in finished:
                    try:
                        res = f.result()
                        if res in stats: stats[res] += 1
                    except Exception as e:
                        logger.error(f"Ошибка при получении результата задачи: {e}")
                        stats["error"] += 1
                
                tasks = list(pending)
                total_processed = stats["done"] + stats["exists"] + stats["skipped"] + stats["error"]
                if total_processed % 50 == 0:
                    logger.info(f"Прогресс: Новых {stats['done']}, Существует {stats['exists']}, Пропущено {stats['skipped']}, Ошибок {stats['error']}")

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Задача завершилась с исключением: {res}")
                    stats["error"] += 1
                elif res in stats:
                    stats[res] += 1

        print()
        logger.info(f"Завершено! Новых: {stats['done']}, Существует: {stats['exists']}, Пропущено: {stats['skipped']}, Ошибок: {stats['error']}")

if __name__ == '__main__':
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Остановлено")
