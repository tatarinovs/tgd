import logging
import os
import re
import sys
import time
from typing import Optional
from tqdm import tqdm


_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
# Имена устройств MS-DOS: Win32 резолвит их в объект-устройство в ЛЮБОЙ папке,
# а не в файл. Критично для имени папки (название группы): канал "CON" или "AUX"
# роняет makedirs, а "NUL" молча глотает всю закачку. На старых Windows и на
# сетевых шарах то же самое происходит и с "con.mp4" — парсер обрезал имя по
# первой точке (Windows 11 это ужесточил до имени без расширения).
_WIN_RESERVED = {
    'con', 'prn', 'aux', 'nul',
    *(f'com{i}' for i in range(1, 10)),
    *(f'lpt{i}' for i in range(1, 10)),
}


def _truncate_stem(name: str, keep: int) -> str:
    """Режет основу имени, сохраняя расширение: длинное_имя.mp4 → длинн.mp4."""
    stem, dot, ext = name.rpartition('.')
    if dot and 0 < len(ext) <= 8:
        return stem[:max(1, keep - len(ext) - 1)] + '.' + ext
    return name[:keep]


def sanitize_filename(name: str, max_len: int = 200, max_bytes: int = 250,
                      guard_reserved: bool = True) -> str:
    """Очищает имя файла от опасных символов (path traversal, Windows-запрещённые)
    и укладывает его в лимиты ОС.

    max_len  — символы (лимит Windows на компонент пути);
    max_bytes — байты в UTF-8 (ext4/APFS считают именно их: 200 кириллических
                символов — это 400 байт, и запись на NAS падала с ENAMETOOLONG);
    guard_reserved — экранировать имена устройств MS-DOS. Отключайте, если к
                результату гарантированно приклеивается префикс (например
                "{message.id}_"): он и сам снимает проблему, а лишнее
                подчёркивание только портит имя файла."""
    name = _UNSAFE_CHARS.sub('_', str(name))
    name = name.strip('. ')
    if guard_reserved and name.split('.')[0].lower() in _WIN_RESERVED:
        name = '_' + name
    if len(name) > max_len:
        name = _truncate_stem(name, max_len)
    while len(name.encode('utf-8', 'ignore')) > max_bytes and len(name) > 1:
        # Отрезаем по символу: у многобайтных имён шаг в байтах непредсказуем
        name = _truncate_stem(name, len(name) - 1)
    return name or 'file'


def name_budget(output_dir: str) -> int:
    """Сколько символов можно отдать под имя файла, чтобы путь влез в лимит ОС.
    На Windows полный путь ограничен 260 символами (если не включён long paths),
    и папка вида downloads/<длинное название группы>/ съедает большую его часть."""
    limit = 259 if os.name == 'nt' else 4095
    try:
        used = len(os.path.abspath(output_dir))
    except Exception:
        used = len(output_dir)
    # +1 на разделитель, +24 на суффикс временного файла ".<tag><seq>.part"
    budget = limit - used - 1 - 24
    return max(24, min(200, budget))


class TqdmStream:
    """Перенаправляет вывод logging в tqdm.write, не ломая полосу прогресса."""
    def write(self, x):
        if len(x.rstrip()) > 0:
            tqdm.write(x, end='')

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        pass

    def isatty(self):
        # Библиотеки спрашивают об этом у любого потока вывода
        return getattr(sys.stderr, 'isatty', lambda: False)()


def setup_tqdm_logger(name, level=logging.INFO):
    """
    Настраивает глобальный логгер с использованием TqdmStream 
    и перехватывает системные предупреждения (warnings),
    чтобы они не дублировали прогресс-бары tqdm.
    """
    logging.basicConfig(
        level=level,
        format='%(levelname)s - %(message)s',
        stream=TqdmStream()
    )
    logging.captureWarnings(True)
    return logging.getLogger(name)


try:
    import resource as _resource_mod  # доступен только на Unix (Linux/macOS/BSD)
except ImportError:
    _resource_mod = None


class ProcessResourceSampler:
    """
    Лёгкий сэмплер CPU/RSS ТЕКУЩЕГО процесса (не всей системы) без сторонних
    зависимостей вроде psutil. Приоритет источников:

      1. /proc/self/stat + /proc/self/status — Linux (в т.ч. большинство NAS,
         включая Synology DSM). Даёт и CPU-время, и текущий (не пиковый) RSS.
      2. stdlib-модуль `resource` (RUSAGE_SELF) — прочие Unix (macOS/BSD),
         где /proc нет. RSS в этом случае — это ПИКОВОЕ значение за всё время
         жизни процесса (ru_maxrss), а не текущее — это менее точно, но лучше,
         чем ничего.
      3. Ничего не доступно (например, чистый Windows без psutil) —
         sample() вернёт (None, None), вызывающий код должен показать
         "недоступно".

    Число потоков сюда не входит — оно и так доступно везде через
    threading.active_count(), без нужды в /proc или resource.
    """

    def __init__(self):
        self._prev_cpu_time = None
        self._prev_wall = None
        self._clk_tck = None
        if hasattr(os, 'sysconf') and hasattr(os, 'sysconf_names') and 'SC_CLK_TCK' in os.sysconf_names:
            try:
                self._clk_tck = os.sysconf('SC_CLK_TCK')
            except (ValueError, OSError):
                self._clk_tck = None

    def _read_proc_cpu_time(self) -> Optional[float]:
        """CPU-секунды (user+system) процесса из /proc/self/stat."""
        with open('/proc/self/stat', 'r') as f:
            data = f.read()
        # comm (имя процесса) в скобках может содержать пробелы и даже ')' —
        # поэтому режем по последней ')' и дальше поля идут по фиксированным номерам
        after = data.rsplit(')', 1)[-1].split()
        utime_ticks = int(after[11])  # поле 14 (1-indexed) — сдвиг на 2 после (pid, comm)
        stime_ticks = int(after[12])  # поле 15
        return (utime_ticks + stime_ticks) / self._clk_tck

    def _read_proc_rss_mb(self) -> Optional[float]:
        """Текущий (не пиковый) RSS в МБ из /proc/self/status."""
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) / 1024.0  # кБ -> МБ
        return None

    def sample(self):
        """Возвращает (cpu_percent | None, rss_mb | None)."""
        cpu_time = None
        rss_mb = None

        if self._clk_tck and os.path.exists('/proc/self/stat'):
            try:
                cpu_time = self._read_proc_cpu_time()
                rss_mb = self._read_proc_rss_mb()
            except Exception:
                cpu_time = None
        elif _resource_mod is not None:
            try:
                usage = _resource_mod.getrusage(_resource_mod.RUSAGE_SELF)
                cpu_time = usage.ru_utime + usage.ru_stime
                # ru_maxrss: КБ на Linux, байты на macOS — но сюда попадаем,
                # только если ветка /proc выше не сработала (не Linux)
                divisor = 1024.0 * 1024.0 if sys.platform == 'darwin' else 1024.0
                rss_mb = usage.ru_maxrss / divisor
            except Exception:
                cpu_time = None

        cpu_percent = None
        if cpu_time is not None:
            wall = time.time()
            if self._prev_cpu_time is not None:
                dt = wall - self._prev_wall
                if dt > 0:
                    cpu_percent = max(0.0, (cpu_time - self._prev_cpu_time) / dt * 100.0)
            self._prev_cpu_time = cpu_time
            self._prev_wall = wall

        return cpu_percent, rss_mb
