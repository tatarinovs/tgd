import os
import re
import json
import shutil
import threading
import time
from typing import List, Dict, Any, Optional

from i18n import _

# Темы раньше сохранялись как "123 (Название темы)" — такая строка потом уезжала
# в download_group_messages как topic_input и там уже не парсилась. Теперь в
# поле topic лежит только ID, а человекочитаемое имя — в topic_title.
_LEGACY_TOPIC_RE = re.compile(r'^\s*(\d+)\s*\((.*)\)\s*$')

# Поля, которые переживают перезапуск демона. Раньше список был захардкожен
# внутри _save_unlocked и молча терял 'stats' — статистика исчезала из веб-морды
# после рестарта.
_PERSISTED_FIELDS = (
    'id', 'title', 'topic', 'topic_title', 'resolved_id',
    'status', 'stats', 'last_error', 'last_run', 'last_max_id',
)

_RUNNING_STATUSES = ("downloading", "stopping")


def _is_running(status: str) -> bool:
    return status in _RUNNING_STATUSES or "Скачивание" in status or "Остановка" in status


def _fsync_dir(dirpath: str):
    """Сбрасывает на диск запись КАТАЛОГА после os.replace().

    os.replace() атомарен для читателей — конкурентный процесс видит либо старое
    содержимое, либо новое, никогда половину. Но долговечность (durability) — это
    другое свойство: переименование меняет метаданные каталога, и они остаются в
    кэше. Если питание пропадёт до сброса, ФС может откатить переименование, и
    вернётся предыдущая версия файла — притом что данные новой уже на диске.

    На Windows это no-op: каталог нельзя открыть как файл, а метаданные NTFS
    журналирует сама (аналог — FlushFileBuffers на томе, требует прав админа)."""
    if os.name == 'nt':
        return
    try:
        fd = os.open(dirpath, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # Часть ФС (некоторые сетевые/FUSE) не умеет fsync каталога — не критично
        pass
    finally:
        os.close(fd)


class GroupStore:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._lock = threading.Lock()
        self._groups: List[Dict[str, Any]] = []
        self.load()

    # ── чтение/запись ────────────────────────────────────────────────
    def load(self):
        with self._lock:
            if not os.path.exists(self.filepath):
                self._groups = []
                return
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                # Битый JSON больше не приводит к тихой потере всего списка групп —
                # исходный файл откладывается в сторону, его можно починить руками.
                backup = f"{self.filepath}.corrupt-{int(time.time())}"
                try:
                    shutil.copy2(self.filepath, backup)
                except OSError:
                    backup = "-"
                print(f"[GroupStore] {_('warn_store_corrupt', self.filepath, backup)} ({e})")
                self._groups = []
                return

            if not isinstance(data, list):
                print(f"[GroupStore] {self.filepath}: expected a list, got {type(data).__name__}")
                self._groups = []
                return

            self._groups = [g for g in data if isinstance(g, dict)]
            needs_save = False
            for g in self._groups:
                if _is_running(str(g.get('status', ''))):
                    g['status'] = "cancelled"
                    needs_save = True
                if self._migrate_topic(g):
                    needs_save = True
            if needs_save:
                self._save_unlocked()

    @staticmethod
    def _migrate_topic(g: Dict[str, Any]) -> bool:
        """Старый формат topic='123 (Название)' → topic='123', topic_title='Название'."""
        m = _LEGACY_TOPIC_RE.match(str(g.get('topic', '')))
        if not m:
            return False
        g['topic'] = m.group(1)
        if not g.get('topic_title'):
            g['topic_title'] = m.group(2)
        return True

    def _save_unlocked(self):
        tmp_file = f"{self.filepath}.tmp"
        target_dir = os.path.dirname(os.path.abspath(self.filepath))
        try:
            os.makedirs(target_dir, exist_ok=True)
            clean_groups = []
            for g in self._groups:
                item = {k: g.get(k, '') for k in _PERSISTED_FIELDS}
                item['id'] = str(item['id'])
                item['topic'] = str(item['topic'])
                item['resolved_id'] = g.get('resolved_id', 0) or 0
                item['last_max_id'] = int(g.get('last_max_id', 0) or 0)
                # Незавершённая загрузка не должна воскреснуть как "downloading"
                if _is_running(str(item['status'])):
                    item['status'] = "cancelled"
                clean_groups.append(item)
            # Три шага, каждый нужен: содержимое на диск → атомарная подмена →
            # запись каталога на диск. Без последнего переименование может
            # откатиться при сбое питания (см. _fsync_dir).
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(clean_groups, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, self.filepath)
            _fsync_dir(target_dir)
        except Exception as e:
            print(f"[GroupStore] Ошибка сохранения {self.filepath}: {e}")
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

    def save(self):
        with self._lock:
            self._save_unlocked()

    # ── доступ ───────────────────────────────────────────────────────
    @staticmethod
    def _same(g: Dict[str, Any], group_id, topic) -> bool:
        return str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic)

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(g) for g in self._groups]

    def get(self, group_id: str, topic: str = "") -> Optional[Dict[str, Any]]:
        with self._lock:
            for g in self._groups:
                if self._same(g, group_id, topic):
                    return dict(g)
            return None

    def titles_before(self, group_id: str, topic: str = "") -> List[str]:
        """Заголовки ДРУГИХ групп, добавленных раньше указанной записи. Нужны,
        чтобы новая группа с уже занятым названием не писала файлы в чужую папку.
        Записи той же группы (другая тема) пропускаются — им общая папка нужна."""
        out = []
        with self._lock:
            for g in self._groups:
                if self._same(g, group_id, topic):
                    break
                if str(g.get('id')) == str(group_id):
                    continue
                out.append(str(g.get('title', '')))
        return out

    def add(self, group_id: str, title: str, topic: str = "", resolved_id: int = 0,
            topic_title: str = "") -> bool:
        with self._lock:
            for g in self._groups:
                if self._same(g, group_id, topic):
                    if title:
                        g['title'] = title
                    if resolved_id:
                        g['resolved_id'] = resolved_id
                    if topic_title:
                        g['topic_title'] = topic_title
                    self._save_unlocked()
                    return False

            self._groups.append({
                'id': str(group_id),
                'title': title or str(group_id),
                'topic': str(topic) if topic else "",
                'topic_title': topic_title or "",
                'resolved_id': resolved_id,
                'status': '',
                'stats': '',
                'last_error': '',
                'last_run': '',
                'last_max_id': 0,
            })
            self._save_unlocked()
            return True

    def remove(self, group_id: str, topic: str = ""):
        with self._lock:
            self._groups = [g for g in self._groups if not self._same(g, group_id, topic)]
            self._save_unlocked()

    def update_stats(self, group_id: str, topic: str = "", status: Optional[str] = None,
                     stats: Optional[str] = None, last_error: Optional[str] = None,
                     last_run: Optional[str] = None, last_max_id: Optional[int] = None,
                     persist: bool = False):
        with self._lock:
            updated = False
            fields = {
                'status': status, 'stats': stats, 'last_error': last_error,
                'last_run': last_run, 'last_max_id': last_max_id,
            }
            for g in self._groups:
                if not self._same(g, group_id, topic):
                    continue
                for key, value in fields.items():
                    if value is not None and g.get(key) != value:
                        g[key] = value
                        updated = True
                break
            if updated and persist:
                self._save_unlocked()
