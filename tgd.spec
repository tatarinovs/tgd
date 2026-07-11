# -*- mode: python ; coding: utf-8 -*-
import os
import sys

# Динамически определяем путь к FastTelethonhelper, чтобы spec работал
# на любой машине независимо от расположения Python
try:
    import FastTelethonhelper as _fth
    _fth_dir = os.path.dirname(_fth.__file__)
except ImportError:
    _fth_dir = None

excludes = [
    'tkinter',
    'unittest',
    'pydoc',
    'PyQt5',
    'PyQt6',
    'PySide2',
    'PySide6',
    'matplotlib',
    'numpy',
    'pandas',
    'IPython',
    'jupyter',
    'scipy',
    'PIL',
    'distutils',
    'setuptools',
    'pkg_resources',
    'curses',
]

a = Analysis(
    ['tgd.py'],
    pathex=[_fth_dir] if _fth_dir else [],
    binaries=[],
    datas=[],
    hiddenimports=[
        # FastTelethon использует динамический sys.path.insert в __init__.py,
        # поэтому PyInstaller не видит его при статическом анализе
        'FastTelethon',
        # Условные импорты (внутри try/except или функций)
        'python_socks',
        'TelethonFakeTLS',
        'utils',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='tgd',
    icon='icon.ico',
    version='file_version_info.txt',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,   # Удаление отладочных символов (уменьшает размер)
    upx=True,     # Сжатие с UPX
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
