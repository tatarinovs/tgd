import os
import argparse
import logging
import subprocess
from PIL import Image
import cv2
from tqdm import tqdm
import shutil

# Кастомный поток для tqdm
class TqdmStream:
    def write(self, x):
        if len(x.rstrip()) > 0:
            tqdm.write(x, end='')
    def flush(self):
        pass

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    stream=TqdmStream()
)
logger = logging.getLogger(__name__)

# Глобальные переменные для ffprobe
FFPROBE_PATH = None
HAS_FFPROBE = False

def is_image_broken(file_path):
    try:
        with Image.open(file_path) as img:
            img.verify()  # Проверка структуры файла
        return False
    except Exception:
        return True

def is_video_broken(file_path):
    # Пытаемся использовать ffprobe если он есть
    if HAS_FFPROBE:
        try:
            # -v error: только ошибки
            # -show_entries format=duration: пытаемся прочитать длительность (требует парсинга контейнера)
            # -i: входной файл
            result = subprocess.run(
                [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration", "-i", file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            # Если ffprobe вернул ошибку в stderr или не смог прочитать файл
            if result.stderr or result.returncode != 0:
                return True
            return False
        except Exception as e:
            logger.debug(f"Ошибка ffprobe для {file_path}: {e}")
            # Если ffprobe упал, падаем на OpenCV
            pass

    # Резервный метод: OpenCV (только первый кадр)
    try:
        cap = cv2.VideoCapture(file_path)
        if not cap.isOpened():
            return True
        ret, frame = cap.read()
        cap.release()
        return not ret
    except Exception:
        return True

def main():
    parser = argparse.ArgumentParser(description="TGD: Верификация целостности медиафайлов")
    parser.add_argument('target_dir', type=str, nargs='?', help="Папка с файлами для проверки")
    parser.add_argument('--delete', action='store_true', help="Удалять битые файлы")
    parser.add_argument('--ffprobe', type=str, help="Путь к исполняемому файлу ffprobe")
    args = parser.parse_args()

    global FFPROBE_PATH, HAS_FFPROBE
    
    # Поиск ffprobe: 
    # 1. Явный путь из аргументов
    # 2. Файл в папке скрипта
    # 3. Системный PATH
    script_dir_ffprobe = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe.exe")
    
    if args.ffprobe and os.path.exists(args.ffprobe):
        FFPROBE_PATH = args.ffprobe
    elif os.path.exists(script_dir_ffprobe):
        FFPROBE_PATH = script_dir_ffprobe
    else:
        FFPROBE_PATH = shutil.which("ffprobe")

    HAS_FFPROBE = FFPROBE_PATH is not None

    # Интерактивный ввод параметров, если они не указаны
    if not args.target_dir:
        print("--- Интерактивный режим ---")
        args.target_dir = input("Введите путь к папке для проверки: ").strip().strip('"\'')
        if not args.delete:
            delete_choice = input("Удалять битые файлы? (y/N): ").lower()
            if delete_choice in ('y', 'yes', 'д', 'да'):
                args.delete = True

    if not os.path.isdir(args.target_dir):
        logger.error(f"Директория {args.target_dir} не найдена или это не папка")
        return

    # Рекурсивный сбор всех файлов
    files_to_check = []
    for root, _, files in os.walk(args.target_dir):
        for f in files:
            # Сохраняем путь относительно целевой папки
            abs_path = os.path.join(root, f)
            rel_path = os.path.relpath(abs_path, args.target_dir)
            files_to_check.append((rel_path, abs_path))
    
    broken_files = []
    healthy_count = 0

    image_exts = ('.jpg', '.jpeg', '.png', '.webp')
    video_exts = ('.mp4', '.mov', '.mkv', '.avi', '.webm')

    if HAS_FFPROBE:
        logger.info("Обнаружен ffprobe.exe, использую его для глубокой проверки видео.")
    else:
        logger.warning("ffprobe.exe не найден в папке проекта. Видео будут проверены только поверхностно (OpenCV).")

    logger.info(f"Начинаю проверку {len(files_to_check)} файлов (включая поддиректории) в {args.target_dir}...")

    for rel_path, abs_path in tqdm(files_to_check, desc="Проверка"):
        file_name = os.path.basename(abs_path)
        ext = os.path.splitext(file_name)[1].lower()
        
        is_broken = False
        if ext in image_exts:
            is_broken = is_image_broken(abs_path)
        elif ext in video_exts:
            is_broken = is_video_broken(abs_path)
        else:
            continue

        if is_broken:
            broken_files.append(rel_path)
            if args.delete:
                try:
                    os.remove(abs_path)
                except Exception as e:
                    logger.error(f"Не удалось удалить {rel_path}: {e}")
        else:
            healthy_count += 1

    print() # Перенос строки после прогресс-бара
    print("="*30)
    print(f"РЕЗУЛЬТАТ ПРОВЕРКИ:")
    print(f"OK Целых файлов: {healthy_count}")
    print(f"FAIL Битых файлов: {len(broken_files)}")
    
    if broken_files:
        print("\nСписок битых файлов:")
        for f in broken_files:
            status = "(УДАЛЕН)" if args.delete else ""
            print(f"- {f} {status}")
    print("="*30)

if __name__ == "__main__":
    main()
