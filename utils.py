from tqdm import tqdm


class TqdmStream:
    """Перенаправляет вывод logging в tqdm.write, не ломая полосу прогресса."""
    def write(self, x):
        if len(x.rstrip()) > 0:
            tqdm.write(x, end='')

    def flush(self):
        pass
