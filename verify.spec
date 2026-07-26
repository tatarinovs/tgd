# -*- mode: python ; coding: utf-8 -*-

excludes = [
    'unittest',
    'pydoc',
    'PyQt5',
    'PyQt6',
    'PySide2',
    'PySide6',
    'matplotlib',
    'pandas',
    'IPython',
    'jupyter',
    'scipy',
    'distutils',
    'setuptools',
    'pkg_resources',
    'curses',
    # Тяжёлые зависимости, которые больше не нужны
    'cv2',
    'numpy',
]

a = Analysis(
    ['src/verify.py'],
    pathex=['src'],
    binaries=[],
    datas=[],
    hiddenimports=[
        # PIL/Pillow — часть модулей загружается динамически
        'PIL',
        'PIL.Image',
        'PIL.JpegImagePlugin',
        'PIL.PngImagePlugin',
        'PIL.WebPImagePlugin',
        'PIL._imaging',
        # Локальный модуль
        'utils',
        # tqdm
        'tqdm',
        'tqdm.auto',
        'tqdm.utils',
        'av',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False,
    collect_all=['PIL', 'av'],
)

# Исключаем библиотеки api-ms-win-*, так как в Windows 10/11 они уже есть в системе.
# Это позволит немного уменьшить размер итогового exe.
a.binaries = [x for x in a.binaries if not x[0].startswith("api-ms-win-")]

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='verify',
    icon='verify_icon.ico',
    version='verify_file_version_info.txt',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,    # Удаление отладочных символов (уменьшает размер)
    upx=True,      # Сжатие с UPX
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
