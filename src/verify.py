import os
import sys
import argparse
import logging
import av
av.logging.set_level(av.logging.PANIC)
from PIL import Image, ImageFile
# Лимит поднят (телеграм отдаёт и панорамы), но НЕ снят: полностью отключённая
# защита позволяет одним подложенным файлом съесть всю память.
Image.MAX_IMAGE_PIXELS = 512_000_000
# Обрезанные картинки должны падать при load(), а не дорисовываться серым
ImageFile.LOAD_TRUNCATED_IMAGES = False
from tqdm import tqdm

from utils import setup_tqdm_logger


def _pick_folder_gui() -> str:
    """Открывает нативный диалог выбора папки через tkinter.
    Возвращает выбранный путь или пустую строку, если отменено/недоступно.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(
            title="Выберите папку с файлами для проверки",
        )
        root.destroy()
        return folder or ""
    except Exception:
        return ""


def get_script_dir() -> str:
    """Возвращает папку с .exe при frozen-запуске, или папку скрипта при обычном."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


# Настройка логирования
logger = setup_tqdm_logger(__name__)

# Результаты проверки одного файла
OK = "ok"                  # файл читается и декодируется
BROKEN = "broken"          # файл повреждён или недокачан — кандидат на удаление
UNREADABLE = "unreadable"  # до содержимого не добрались (права, занят, ввод-вывод)


def _access_error(file_path):
    """Отличает проблему доступа от повреждения: файл без прав или занятый
    другим процессом раньше молча попадал в 'битые' и удалялся вместе с ними."""
    try:
        if os.path.getsize(file_path) == 0:
            return None  # пустой файл — это именно недокачанный файл
        with open(file_path, 'rb') as f:
            f.read(1)
    except OSError as e:
        return str(e)
    return None


def check_image(file_path):
    err = _access_error(file_path)
    if err:
        logger.debug(f"Нет доступа к {file_path}: {err}")
        return UNREADABLE
    try:
        # verify() смотрит только структуру и пропускает обрезанные файлы,
        # поэтому следом честно декодируем пиксели через load().
        with Image.open(file_path) as img:
            img.verify()
        with Image.open(file_path) as img:
            img.load()
        return OK
    except Exception as e:
        logger.debug(f"Ошибка Pillow для {file_path}: {e}")
        return BROKEN


def _decode_some(container, stream, limit=3):
    decoded = 0
    for _frame in container.decode(stream):
        decoded += 1
        if decoded >= limit:
            break
    return decoded


# Насколько конец потока может отставать от заявленной длительности, прежде чем
# файл считается обрезанным. Хвост короче этого порога бывает и у целых файлов
# (последний фрагмент аудио, неточная длительность в контейнере).
TAIL_GAP_TOLERANCE = 2.0


def check_video(file_path, quick=False):
    err = _access_error(file_path)
    if err:
        logger.debug(f"Нет доступа к {file_path}: {err}")
        return UNREADABLE
    try:
        with av.open(file_path) as container:
            stream = None
            if container.streams.video:
                stream = container.streams.video[0]
            elif container.streams.audio:
                stream = container.streams.audio[0]
            if stream is None:
                return BROKEN

            # Открывшийся контейнер ничего не доказывает: у недокачанного mp4
            # с moov в начале (faststart, как отдаёт Telegram) заголовок цел,
            # а данных за ним нет. Поэтому декодируем начало...
            if _decode_some(container, stream) == 0:
                return BROKEN

            duration = float(container.duration) / av.time_base if container.duration else 0.0
            if quick or duration <= TAIL_GAP_TOLERANCE * 3:
                return OK

            # ...и сверяем конец: перематываем к последнему ключевому кадру и
            # смотрим, доходят ли пакеты до заявленной длительности.
            try:
                container.seek(int(max(0.0, duration - 1.0) * av.time_base))
            except Exception:
                return OK  # перемотка не поддерживается — довольствуемся началом

            last_pts = None
            for packet in container.demux(stream):
                if packet.pts is not None:
                    last_pts = packet.pts
            if last_pts is None:
                return BROKEN
            end_ts = float(last_pts * stream.time_base)
            if duration - end_ts > TAIL_GAP_TOLERANCE:
                logger.debug(f"{file_path}: поток обрывается на {end_ts:.1f} с из {duration:.1f} с")
                return BROKEN
        return OK
    except Exception as e:
        logger.debug(f"Ошибка PyAV для {file_path}: {e}")
        return BROKEN


def main():
    print(r"""
==================================================
   T G D   V E R I F Y                              
   High-Performance Media Integrity Checker         
==================================================
""")
    parser = argparse.ArgumentParser(description="TGD: Верификация целостности медиафайлов")
    parser.add_argument('target_dir', type=str, nargs='?', help="Папка с файлами для проверки")
    parser.add_argument('--delete', action='store_true', help="Удалять битые файлы")
    parser.add_argument('--quick', action='store_true',
                        help="Быстро: только начало файла, без сверки хвоста (не находит обрезанные видео)")
    args = parser.parse_args()

    # Интерактивный ввод параметров, если они не указаны
    interactive = not args.target_dir  # True = запущен без аргументов (двойной клик)
    auto_delete = args.delete
    target_dir = args.target_dir

    if interactive:
        # Пробуем GUI-диалог (Windows/macOS/Linux с Tk)
        target_dir = _pick_folder_gui()
        if not target_dir:
            # Fallback: консольный ввод
            print("--- Интерактивный режим ---")
            target_dir = input("Введите путь к папке для проверки: ").strip().strip('"\'')

    import concurrent.futures

    while True:
        if not target_dir:
            if not interactive:
                break
            
            target_dir = _pick_folder_gui()
            if not target_dir:
                print("--- Интерактивный режим ---")
                target_dir = input("Введите путь к папке для проверки (Enter для выхода): ").strip().strip('"\'')
                if not target_dir:
                    break

        if not os.path.isdir(target_dir):
            logger.error(f"Директория {target_dir} не найдена или это не папка")
            if interactive:
                target_dir = ""
                continue
            else:
                break

        # Рекурсивный сбор всех файлов
        files_to_check = []
        for root, _, files in os.walk(target_dir):
            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, target_dir)
                files_to_check.append((rel_path, abs_path))

        broken_files = []
        unreadable_files = []
        skipped_count = 0
        healthy_count = 0

        image_exts = ('.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.tiff')
        video_exts = ('.mp4', '.mov', '.mkv', '.avi', '.webm', '.m4v', '.mp3', '.m4a', '.ogg', '.opus', '.flac', '.wav')

        logger.info(f"Начинаю проверку {len(files_to_check)} файлов (включая поддиректории) в {target_dir}...")

        def check_file(item):
            rel_path, abs_path = item
            file_name = os.path.basename(abs_path)
            ext = os.path.splitext(file_name)[1].lower()

            if ext in image_exts:
                return (rel_path, abs_path, check_image(abs_path), "image")
            elif ext in video_exts:
                return (rel_path, abs_path, check_video(abs_path, quick=args.quick), "media")
            return None

        # Оптимальное количество потоков зависит от диска (SSD/HDD) и процессора
        max_workers = min(32, (os.cpu_count() or 1) * 2)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Запускаем задачи
            futures = [executor.submit(check_file, item) for item in files_to_check]
            
            # tqdm будет обновляться по мере завершения задач
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(files_to_check), desc="Проверка"):
                res = future.result()
                if res is None:
                    skipped_count += 1  # не медиафайл — раньше просто исчезал из отчёта
                    continue

                rel_path, abs_path, status, f_type = res

                if status == BROKEN:
                    broken_files.append((rel_path, abs_path))
                    if auto_delete:
                        try:
                            os.remove(abs_path)
                        except Exception as e:
                            logger.error(f"Не удалось удалить {rel_path}: {e}")
                elif status == UNREADABLE:
                    # Ни целым, ни битым не считаем и НИКОГДА не удаляем
                    unreadable_files.append((rel_path, abs_path))
                else:
                    healthy_count += 1

        print()  # Перенос строки после прогресс-бара
        print("=" * 30)
        print("РЕЗУЛЬТАТ ПРОВЕРКИ:")
        print(f"OK   Целых файлов:      {healthy_count}")
        print(f"FAIL Битых файлов:      {len(broken_files)}")
        print(f"SKIP Не медиафайлов:    {skipped_count}")
        if unreadable_files:
            print(f"WARN Недоступных:       {len(unreadable_files)} (нет прав / файл занят — не удаляются)")

        if broken_files:
            print("\nСписок битых файлов:")
            for rel_path, abs_path in broken_files:
                status = "(УДАЛЕН)" if auto_delete or not os.path.exists(abs_path) else ""
                print(f"  - {rel_path} {status}")
        if unreadable_files:
            print("\nНе удалось прочитать:")
            for rel_path, _abs in unreadable_files:
                print(f"  - {rel_path}")
        print("=" * 30)

        # Пауза при интерактивном запуске (двойной клик), чтобы статистика не исчезла
        if interactive:
            while True:
                action = input("\nНажмите D для удаления битых файлов, O для выбора новой папки, Enter для выхода..: ").strip().lower()
                if action in ('d', 'в'):
                    if not broken_files:
                        print("Нет битых файлов для удаления.")
                    else:
                        deleted_count = 0
                        for rel_path, abs_path in broken_files:
                            if os.path.exists(abs_path):
                                try:
                                    os.remove(abs_path)
                                    print(f"Удален: {rel_path}")
                                    deleted_count += 1
                                except Exception as e:
                                    logger.error(f"Не удалось удалить {rel_path}: {e}")
                        if deleted_count > 0:
                            print(f"Успешно удалено {deleted_count} файлов.")
                        else:
                            print("Файлы уже удалены или возникла ошибка.")
                elif action in ('o', 'щ'):
                    target_dir = ""
                    break
                elif action == '':
                    return
                else:
                    print("Неизвестная команда. Повторите ввод.")
        else:
            break


if __name__ == "__main__":
    main()
