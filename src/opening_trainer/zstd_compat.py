from __future__ import annotations

import io
import subprocess
from pathlib import Path
from typing import BinaryIO

try:
    from compression import zstd as _stdlib_zstd
except ImportError:  # pragma: no cover - exercised on Python < 3.14
    _stdlib_zstd = None

try:
    import zstandard as _third_party_zstd
except ImportError:  # pragma: no cover - exercised when dependency is absent
    _third_party_zstd = None

_PYTHON_314 = Path("/root/.pyenv/versions/3.14.0/bin/python")


class ZstdUnavailableError(RuntimeError):
    pass


class _SubprocessTextReader(io.TextIOBase):
    def __init__(self, process: subprocess.Popen[bytes]):
        self._process = process
        self._stream = io.TextIOWrapper(process.stdout, encoding="utf-8")

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> str:
        return self._stream.read(size)

    def readline(self, size: int = -1) -> str:
        return self._stream.readline(size)

    def __iter__(self):
        return iter(self._stream)

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._stream.close()
            return_code = self._process.wait()
            if return_code != 0:
                stderr = b""
                if self._process.stderr is not None:
                    stderr = self._process.stderr.read()
                raise ZstdUnavailableError(stderr.decode("utf-8", errors="replace") or "zstd subprocess failed")
        finally:
            super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def _python_314_available() -> bool:
    return _PYTHON_314.exists()




def open_binary_reader(binary_handle: BinaryIO):
    if _stdlib_zstd is not None:
        return _stdlib_zstd.open(binary_handle, mode="rb")
    if _third_party_zstd is not None:
        return _third_party_zstd.ZstdDecompressor().stream_reader(binary_handle)
    if _python_314_available():
        process = subprocess.Popen(
            [
                str(_PYTHON_314),
                "-c",
                "import sys; from compression import zstd; sys.stdout.buffer.write(zstd.decompress(sys.stdin.buffer.read()))",
            ],
            stdin=binary_handle,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        return process.stdout
    raise ZstdUnavailableError(
        "Zstandard support requires Python 3.14+, the zstandard package, or the bundled Python 3.14 helper."
    )

def open_text_reader(binary_handle: BinaryIO):
    if _stdlib_zstd is not None:
        return _stdlib_zstd.open(binary_handle, mode="rt", encoding="utf-8")
    if _third_party_zstd is not None:
        reader = _third_party_zstd.ZstdDecompressor().stream_reader(binary_handle)
        return io.TextIOWrapper(reader, encoding="utf-8")
    if _python_314_available():
        process = subprocess.Popen(
            [
                str(_PYTHON_314),
                "-c",
                "import sys; from compression import zstd; sys.stdout.buffer.write(zstd.decompress(sys.stdin.buffer.read()))",
            ],
            stdin=binary_handle,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        return _SubprocessTextReader(process)
    raise ZstdUnavailableError(
        "Zstandard support requires Python 3.14+, the zstandard package, or the bundled Python 3.14 helper."
    )


def compress(data: bytes) -> bytes:
    if _stdlib_zstd is not None:
        return _stdlib_zstd.compress(data)
    if _third_party_zstd is not None:
        return _third_party_zstd.ZstdCompressor().compress(data)
    if _python_314_available():
        result = subprocess.run(
            [
                str(_PYTHON_314),
                "-c",
                "import sys; from compression import zstd; sys.stdout.buffer.write(zstd.compress(sys.stdin.buffer.read()))",
            ],
            input=data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        return result.stdout
    raise ZstdUnavailableError(
        "Zstandard support requires Python 3.14+, the zstandard package, or the bundled Python 3.14 helper."
    )
