from __future__ import annotations

import importlib
from pathlib import Path
from typing import Callable


class QtQmlWindowRuntime:
    """Reusable Qt/QML launcher seam for staged UI migration."""

    def __init__(self, qml_path: Path, on_window_closed: Callable[[], None] | None = None):
        self._qml_path = Path(qml_path).resolve()
        self._on_window_closed_callback = on_window_closed
        self._qt_app = None
        self._qml_engine = None
        self._qt_window = None

    def open_or_raise(self) -> None:
        self._ensure_runtime()
        if self.has_active_window():
            self._show_raise_activate(self._qt_window)
            return
        self._load_window()
        self._show_raise_activate(self._qt_window)

    def has_active_window(self) -> bool:
        if self._qt_window is None:
            return False
        is_visible = getattr(self._qt_window, 'isVisible', None)
        if callable(is_visible):
            return bool(is_visible())
        return True

    def process_events(self) -> bool:
        if self._qt_app is None:
            return False
        if not self.has_active_window():
            return False
        self._qt_app.processEvents()
        return True

    def close(self) -> None:
        if self._qt_window is None:
            return
        self._qt_window.close()
        self._clear_window_reference()

    def _ensure_runtime(self) -> None:
        if importlib.util.find_spec('PySide6') is None:
            raise RuntimeError('PySide6 is required for Qt/QML training settings migration.')

        qt_gui = importlib.import_module('PySide6.QtGui')
        app_instance = qt_gui.QGuiApplication.instance()
        if app_instance is None:
            app_instance = qt_gui.QGuiApplication([])
        self._qt_app = app_instance

    def _load_window(self) -> None:
        if not self._qml_path.exists():
            raise RuntimeError(f'QML window file does not exist: {self._qml_path}')

        qt_core = importlib.import_module('PySide6.QtCore')
        qt_qml = importlib.import_module('PySide6.QtQml')

        self._qml_engine = qt_qml.QQmlApplicationEngine()
        qml_url = qt_core.QUrl.fromLocalFile(str(self._qml_path))
        self._qml_engine.load(qml_url)
        root_objects = self._qml_engine.rootObjects()
        if not root_objects:
            diagnostics = self._qml_diagnostics()
            self._qml_engine = None
            raise RuntimeError(
                f'Failed to load QML window from {self._qml_path}. Root object list is empty. {diagnostics}'
            )

        candidate = root_objects[0]
        if not self._is_top_level_window(candidate):
            self._qml_engine = None
            raise RuntimeError(
                f'Loaded QML root object is not a usable top-level window: {candidate.__class__.__name__}'
            )

        self._qt_window = candidate
        self._qt_window.destroyed.connect(self._on_window_destroyed)

    def _qml_diagnostics(self) -> str:
        if self._qml_engine is None:
            return 'No QML engine diagnostics available.'
        warnings = getattr(self._qml_engine, 'warnings', None)
        if not callable(warnings):
            return 'QML engine did not provide warnings().'
        reported = warnings()
        if not reported:
            return 'QML engine reported no explicit warnings.'
        formatted: list[str] = []
        for warning in reported:
            to_string = getattr(warning, 'toString', None)
            if callable(to_string):
                formatted.append(str(to_string()))
            else:
                formatted.append(str(warning))
        return ' | '.join(formatted)

    @staticmethod
    def _is_top_level_window(candidate) -> bool:
        required_methods = ('show', 'raise_', 'requestActivate')
        return all(callable(getattr(candidate, method, None)) for method in required_methods)

    @staticmethod
    def _show_raise_activate(window) -> None:
        show_normal = getattr(window, 'showNormal', None)
        if callable(show_normal):
            show_normal()
        window.show()
        window.raise_()
        window.requestActivate()

    def _on_window_destroyed(self, _obj=None) -> None:
        self._clear_window_reference()

    def _clear_window_reference(self) -> None:
        self._qt_window = None
        self._qml_engine = None
        if callable(self._on_window_closed_callback):
            self._on_window_closed_callback()
