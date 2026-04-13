from __future__ import annotations

import importlib
from pathlib import Path


class QtQmlWindowRuntime:
    """Minimal reusable Qt/QML launcher seam for staged UI migration."""

    def __init__(self, qml_path: Path):
        self._qml_path = Path(qml_path)
        self._qt_app = None
        self._qt_window = None

    def open_or_raise(self) -> None:
        if self._qt_window is not None and self._qt_window.isVisible():
            self._qt_window.raise_()
            self._qt_window.requestActivate()
            return
        self._ensure_runtime()
        self._load_window()

    def process_events(self) -> None:
        if self._qt_app is None:
            return
        self._qt_app.processEvents()

    def close(self) -> None:
        if self._qt_window is None:
            return
        self._qt_window.close()
        self._qt_window = None

    def _ensure_runtime(self) -> None:
        if importlib.util.find_spec('PySide6') is None:
            raise RuntimeError('PySide6 is required for Qt/QML training settings migration.')

        qt_gui = importlib.import_module('PySide6.QtGui')
        app_instance = qt_gui.QGuiApplication.instance()
        if app_instance is None:
            app_instance = qt_gui.QGuiApplication([])
        self._qt_app = app_instance

    def _load_window(self) -> None:
        qt_core = importlib.import_module('PySide6.QtCore')
        qt_qml = importlib.import_module('PySide6.QtQml')

        engine = qt_qml.QQmlApplicationEngine()
        qml_url = qt_core.QUrl.fromLocalFile(str(self._qml_path.resolve()))
        engine.load(qml_url)
        root_objects = engine.rootObjects()
        if not root_objects:
            raise RuntimeError(f'Failed to load QML window from {self._qml_path}.')
        self._qt_window = root_objects[0]
        self._qt_window._migration_engine = engine
        self._qt_window.destroyed.connect(self._on_window_destroyed)
        self._qt_window.show()

    def _on_window_destroyed(self, _obj=None) -> None:
        self._qt_window = None
