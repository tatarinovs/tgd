import logging
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
