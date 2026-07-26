import os
import sys
import argparse
import logging
import av
av.logging.set_level(av.logging.PANIC)
from PIL import Image
Image.MAX_IMAGE_PIXELS = None  # Отключаем защиту от очень больших изображений (DecompressionBomb)
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

def is_image_broken(file_path):
    try:
        with Image.open(file_path) as img:
            img.verify()  # Проверка структуры файла
        return False
    except Exception:
        return True


def is_video_broken(file_path):
    try:
        with av.open(file_path) as container:
            if not container.streams.video and not container.streams.audio:
                return True
        return False
    except Exception as e:
        logger.debug(f"Ошибка PyAV для {file_path}: {e}")
        return True


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
        skipped_count = 0
        healthy_count = 0

        image_exts = ('.jpg', '.jpeg', '.png', '.webp')
        video_exts = ('.mp4', '.mov', '.mkv', '.avi', '.webm')

        logger.info(f"Начинаю проверку {len(files_to_check)} файлов (включая поддиректории) в {target_dir}...")

        def check_file(item):
            rel_path, abs_path = item
            file_name = os.path.basename(abs_path)
            ext = os.path.splitext(file_name)[1].lower()

            if ext in image_exts:
                is_broken = is_image_broken(abs_path)
                return (rel_path, abs_path, is_broken, "image")
            elif ext in video_exts:
                result = is_video_broken(abs_path)
                return (rel_path, abs_path, result, "video")
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
                    continue
                    
                rel_path, abs_path, is_broken, f_type = res
                
                if is_broken:
                    broken_files.append((rel_path, abs_path))
                    if auto_delete:
                        try:
                            os.remove(abs_path)
                        except Exception as e:
                            logger.error(f"Не удалось удалить {rel_path}: {e}")
                else:
                    healthy_count += 1

        print()  # Перенос строки после прогресс-бара
        print("=" * 30)
        print("РЕЗУЛЬТАТ ПРОВЕРКИ:")
        print(f"OK   Целых файлов:   {healthy_count}")
        print(f"FAIL Битых файлов:   {len(broken_files)}")

        if broken_files:
            print("\nСписок битых файлов:")
            for rel_path, abs_path in broken_files:
                status = "(УДАЛЕН)" if auto_delete or not os.path.exists(abs_path) else ""
                print(f"  - {rel_path} {status}")
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
