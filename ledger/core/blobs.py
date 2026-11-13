"""Content-addressed blob store for large payloads.

Each blob is a file named by the SHA-256 of its bytes, under a two-character
fan-out directory: ``<root>/ab/cdef...``. A blob is written to a temporary
file, fsynced and renamed into place, so a reader never sees a partial blob.
Since the name is the digest, writing the same bytes twice is a no-op, and a
recording and its replays share every large payload on disk.

Reads check the digest, so a blob that was changed or truncated on disk is
reported instead of being replayed.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

__all__ = ["BlobStore", "BlobError", "BlobMissing", "BlobCorrupt"]

_DIGEST = re.compile(r"[0-9a-f]{64}")


class BlobError(RuntimeError):
    """A blob could not be read back intact."""


class BlobMissing(BlobError):
    pass


class BlobCorrupt(BlobError):
    pass


def _check_digest(digest: str) -> None:
    if not _DIGEST.fullmatch(digest):
        raise ValueError(f"not a lowercase SHA-256 hex digest: {digest!r}")


def _fsync_dir(path: Path) -> None:
    # Make the rename itself durable. Not supported on every platform.
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class BlobStore:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)

    def path_for(self, digest: str) -> Path:
        _check_digest(digest)
        return self.root / digest[:2] / digest[2:]

    def __contains__(self, digest: str) -> bool:
        return self.path_for(digest).exists()

    def put(self, data: bytes, digest: str | None = None) -> str:
        """Store ``data`` durably and return its digest.

        If ``digest`` is given it must match ``data``; this catches a payload
        whose bytes and digest have drifted apart before it reaches disk.
        """
        actual = hashlib.sha256(data).hexdigest()
        if digest is not None and digest != actual:
            raise ValueError(f"digest {digest[:12]} does not match the data ({actual[:12]})")
        path = self.path_for(actual)
        if path.exists():
            return actual
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        _fsync_dir(path.parent)
        return actual

    def get(self, digest: str) -> bytes:
        path = self.path_for(digest)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise BlobMissing(f"blob {digest[:12]} is missing from {self.root}") from None
        if hashlib.sha256(data).hexdigest() != digest:
            raise BlobCorrupt(f"blob {digest[:12]} in {self.root} does not match its digest")
        return data

    def size(self, digest: str) -> int:
        return self.path_for(digest).stat().st_size
