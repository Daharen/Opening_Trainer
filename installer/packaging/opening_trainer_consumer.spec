# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

repo_root = Path(SPECPATH).resolve().parents[1]

block_cipher = None

a = Analysis(
    [str(repo_root / 'main.py')],
    pathex=[str(repo_root), str(repo_root / 'src')],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='OpeningTrainer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)
