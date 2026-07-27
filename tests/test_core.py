"""Юнит-тесты чистых функций TGD.

Запуск (без сторонних зависимостей, только stdlib):
    python -m unittest discover -s tests -v

Покрывают ровно те места, где аудит нашёл баги: имена файлов, разбор
конфигурации, схема groups.json и таймауты.
"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

from utils import sanitize_filename, name_budget  # noqa: E402
from store import GroupStore  # noqa: E402


class TestSanitizeFilename(unittest.TestCase):
    def test_strips_path_traversal(self):
        self.assertNotIn('/', sanitize_filename('../../etc/passwd'))
        self.assertNotIn('\\', sanitize_filename(r'..\..\windows\system32'))

    def test_windows_forbidden_chars(self):
        self.assertEqual(sanitize_filename('a<b>c:d"e|f?g*h.mp4'), 'a_b_c_d_e_f_g_h.mp4')

    def test_windows_reserved_device_names(self):
        # Важно для имени ПАПКИ: канал "CON"/"AUX" роняет makedirs, "NUL" молча
        # глотает запись. Имя без расширения опасно и на актуальной Windows 11.
        self.assertEqual(sanitize_filename('con.mp4'), '_con.mp4')
        self.assertEqual(sanitize_filename('LPT1.txt'), '_LPT1.txt')
        self.assertEqual(sanitize_filename('nul'), '_nul')

    def test_reserved_guard_can_be_disabled(self):
        # У имён файлов есть префикс "{message.id}_", он и уводит имя из
        # пространства устройств — второе подчёркивание было бы мусором
        self.assertEqual(sanitize_filename('con.mp4', guard_reserved=False), 'con.mp4')

    def test_keeps_extension_when_truncating(self):
        name = sanitize_filename('a' * 300 + '.mp4', max_len=50)
        self.assertTrue(name.endswith('.mp4'))
        self.assertEqual(len(name), 50)

    def test_respects_byte_limit_for_cyrillic(self):
        # ext4/APFS считают байты: 200 кириллических символов — это 400 байт
        name = sanitize_filename('я' * 300 + '.mp4')
        self.assertLessEqual(len(name.encode('utf-8')), 250)
        self.assertTrue(name.endswith('.mp4'))

    def test_never_returns_empty(self):
        self.assertEqual(sanitize_filename('...'), 'file')
        self.assertEqual(sanitize_filename(''), 'file')


class TestNameBudget(unittest.TestCase):
    def test_short_path_gets_full_budget(self):
        self.assertEqual(name_budget(os.path.join(tempfile.gettempdir(), 'x')), 200)

    def test_long_path_shrinks_budget(self):
        deep = os.path.join(tempfile.gettempdir(), *(['directory' * 3] * 8))
        self.assertLess(name_budget(deep), 200)
        self.assertGreaterEqual(name_budget(deep), 24)


class TestGroupStore(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, 'groups.json')

    def _write(self, data):
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)

    def test_legacy_topic_is_migrated(self):
        self._write([{'id': '-100', 'title': 'Chat', 'topic': '55 (Фото)', 'status': ''}])
        item = GroupStore(self.path).get('-100', '55')
        self.assertIsNotNone(item)
        self.assertEqual(item['topic'], '55')
        self.assertEqual(item['topic_title'], 'Фото')

    def test_stats_survive_save(self):
        store = GroupStore(self.path)
        store.add('-100', 'Chat')
        store.update_stats('-100', '', stats='{"new": 3}', status='done', persist=True)
        self.assertEqual(GroupStore(self.path).get('-100', '')['stats'], '{"new": 3}')

    def test_checkpoint_persisted(self):
        store = GroupStore(self.path)
        store.add('-100', 'Chat')
        store.update_stats('-100', '', last_max_id=777, persist=True)
        self.assertEqual(GroupStore(self.path).get('-100', '')['last_max_id'], 777)

    def test_running_status_reset_on_load(self):
        self._write([{'id': '-100', 'title': 'Chat', 'topic': '', 'status': 'downloading'}])
        self.assertEqual(GroupStore(self.path).get('-100', '')['status'], 'cancelled')

    def test_corrupt_json_is_backed_up_not_lost(self):
        with open(self.path, 'w', encoding='utf-8') as f:
            f.write('{ this is not json')
        store = GroupStore(self.path)
        self.assertEqual(store.list(), [])
        self.assertTrue([f for f in os.listdir(self.dir) if '.corrupt-' in f])

    def test_titles_before_ignores_same_group(self):
        store = GroupStore(self.path)
        store.add('-100', 'Chat', '')
        store.add('-100', 'Chat', '55')
        store.add('-200', 'Chat', '')
        # Другая тема той же группы не считается конфликтом папок
        self.assertEqual(store.titles_before('-100', '55'), [])
        # А одноимённая чужая группа — считается
        self.assertIn('Chat', store.titles_before('-200', ''))


class TestConfig(unittest.TestCase):
    def setUp(self):
        import types
        self.args = types.SimpleNamespace(
            timeout=None, retries=None, workers=None, heavy_workers=None,
            heavy_threshold=None, queue_size=None,
        )
        for key in ('TIMEOUT', 'RETRIES', 'WORKERS', 'HEAVY_WORKERS',
                    'HEAVY_THRESHOLD', 'QUEUE_SIZE', 'TIMEOUT_PER_MB'):
            os.environ.pop(key, None)

    def test_zero_retries_normalized(self):
        import tgd
        self.args.retries = 0
        self.assertEqual(tgd.apply_config(self.args).retries, 1)

    def test_garbage_in_env_falls_back_to_default(self):
        import tgd
        os.environ['WORKERS'] = 'шесть'
        self.assertEqual(tgd.apply_config(self.args).workers, 6)

    def test_timeout_scales_with_size(self):
        import tgd
        self.assertEqual(tgd._effective_timeout(3600, 6, 1024 ** 2), 3600)
        self.assertGreater(tgd._effective_timeout(3600, 6, 2 * 1024 ** 3), 3600)
        self.assertEqual(tgd._effective_timeout(3600, 0, 2 * 1024 ** 3), 3600)


if __name__ == '__main__':
    unittest.main()
