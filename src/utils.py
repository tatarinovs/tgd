import logging
import os
import sys
import time
from typing import Optional
from tqdm import tqdm


class TqdmStream:
    """Перенаправляет вывод logging в tqdm.write, не ломая полосу прогресса."""
    def write(self, x):
        if len(x.rstrip()) > 0:
            tqdm.write(x, end='')

    def flush(self):
        pass


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
