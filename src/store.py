import os
import json
import threading
from typing import List, Dict, Any, Optional


class GroupStore:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._lock = threading.Lock()
        self._groups: List[Dict[str, Any]] = []
        self.load()

    def load(self):
        with self._lock:
            if not os.path.exists(self.filepath):
                self._groups = []
                return
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self._groups = json.load(f)
                needs_save = False
                for g in self._groups:
                    st = g.get('status', '')
                    if "Скачивание" in st or "Остановка" in st or st in ("downloading", "stopping"):
                        g['status'] = "cancelled"
                        needs_save = True
                if needs_save:
                    self._save_unlocked()
            except Exception as e:
                print(f"[GroupStore] Ошибка загрузки {self.filepath}: {e}")
                self._groups = []

    def _save_unlocked(self):
        tmp_file = f"{self.filepath}.tmp"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.filepath)), exist_ok=True)
            clean_groups = []
            for g in self._groups:
                st = g.get('status', '')
                if "Скачивание" in st or "Остановка" in st or st in ("downloading", "stopping"):
                    st = "cancelled"
                clean_groups.append({
                    'id': str(g.get('id', '')),
                    'title': g.get('title', ''),
                    'topic': str(g.get('topic', '')),
                    'resolved_id': g.get('resolved_id', 0),
                    'status': st,
                    'last_error': g.get('last_error', ''),
                    'last_run': g.get('last_run', '')
                })
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(clean_groups, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, self.filepath)
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

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            # Return deep copy of items
            return [dict(g) for g in self._groups]

    def get(self, group_id: str, topic: str = "") -> Optional[Dict[str, Any]]:
        with self._lock:
            for g in self._groups:
                if str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic):
                    return dict(g)
            return None

    def add(self, group_id: str, title: str, topic: str = "", resolved_id: int = 0) -> bool:
        with self._lock:
            # Check if group+topic combination already exists
            for g in self._groups:
                if str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic):
                    # Update title/resolved_id if available
                    if title:
                        g['title'] = title
                    if resolved_id:
                        g['resolved_id'] = resolved_id
                    self._save_unlocked()
                    return False

            self._groups.append({
                'id': str(group_id),
                'title': title or str(group_id),
                'topic': str(topic) if topic else "",
                'resolved_id': resolved_id,
                'status': '',
                'stats': '',
                'last_error': '',
                'last_run': ''
            })
            self._save_unlocked()
            return True

    def remove(self, group_id: str, topic: str = ""):
        with self._lock:
            self._groups = [
                g for g in self._groups
                if not (str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic))
            ]
            self._save_unlocked()

    def update_stats(self, group_id: str, topic: str = "", status: Optional[str] = None,
                     stats: Optional[str] = None, last_error: Optional[str] = None,
                     last_run: Optional[str] = None, persist: bool = False):
        with self._lock:
            updated = False
            for g in self._groups:
                if str(g.get('id')) == str(group_id) and str(g.get('topic', '')) == str(topic):
                    if status is not None and g.get('status') != status:
                        g['status'] = status
                        updated = True
                    if stats is not None and g.get('stats') != stats:
                        g['stats'] = stats
                        updated = True
                    if last_error is not None and g.get('last_error') != last_error:
                        g['last_error'] = last_error
                        updated = True
                    if last_run is not None and g.get('last_run') != last_run:
                        g['last_run'] = last_run
                        updated = True
                    break
            if updated and persist:
                self._save_unlocked()
