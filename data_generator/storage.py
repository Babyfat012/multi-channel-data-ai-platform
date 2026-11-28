"""Write data to a local directory or object storage (MinIO, S3) via fsspec.

Wherever something else relies on "fully written", `_fsync` is called; `write_json_atomic` writes a temp file then
renames it so no half-written file is left behind. On an object store, data is durable once `close` finishes uploading.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


def _fsync(handle: Any) -> bool:
    """Force the OS to flush `handle` to disk; return False for object-store files (no `fileno()`)."""
    try:
        handle.flush()
        os.fsync(handle.fileno())
    except (AttributeError, OSError, ValueError, io.UnsupportedOperation):
        return False
    return True


class JsonlAppender:
    """Incremental writer for an open JSON Lines file. Counts rows and bytes written."""

    def __init__(self, handle: Any) -> None:
        """Wrap a file opened in binary mode. The pool owns the file's lifecycle."""
        self._handle = handle
        self.rows = 0
        self.bytes = 0

    def write(self, record: dict[str, Any]) -> int:
        """Write one record as one JSON row. Return that row's byte count."""
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        data = line.encode("utf-8")
        self._handle.write(data)
        self.rows += 1
        self.bytes += len(data)
        return len(data)

    def close(self) -> tuple[int, int]:
        """`fsync` then close the file, returning `(rows, bytes)`.

        The caller writes the "day done" marker right after, so the file must be durable first.
        """
        _fsync(self._handle)
        self._handle.close()
        return self.rows, self.bytes


class JsonlWriterPool:
    """Keep many JSON Lines files open at once, one per key.

    Used when files receive interleaved rows: an Order from today may be exported two weeks later, so
    keeping files open holds RAM constant instead of buffering per day.
    """

    def __init__(self, store: OutputStore) -> None:
        """An empty pool writing to `store`."""
        self._store = store
        self._open: dict[Any, JsonlAppender] = {}
        self._closed: set[Any] = set()

    def keys(self) -> list[Any]:
        """The open keys. Returns a snapshot so the caller can iterate and close at the same time."""
        return list(self._open)

    def write(self, key: Any, relative: str, record: dict[str, Any]) -> int:
        """Write `record` to the file of `key`, opening `relative` on the first row.

        Writing to a closed key raises: reopening would wipe the finished file.
        """
        appender = self._open.get(key)
        if appender is None:
            if key in self._closed:
                raise ValueError(
                    f"key {key!r} is already closed, writing more would wipe {relative}"
                )
            appender = self._open[key] = JsonlAppender(self._store.open_write(relative))
        return appender.write(record)

    def close(self, key: Any) -> tuple[int, int]:
        """Close the file of `key`, returning `(rows, bytes)`. An unopened key returns `(0, 0)`."""
        appender = self._open.pop(key, None)
        self._closed.add(key)
        return appender.close() if appender is not None else (0, 0)

    def close_all(self) -> None:
        """Close every file still open. Called at the end, including on error."""
        while self._open:
            self.close(next(iter(self._open)))


class ParquetAppender:
    """Incremental writer for a Parquet file: each `write` is one row group."""

    def __init__(self, writer: pq.ParquetWriter, schema: pa.Schema) -> None:
        """Wrap an open `ParquetWriter`. `OutputStore` owns the file's lifecycle."""
        self._writer = writer
        self._schema = schema
        self.rows = 0
        self.row_groups = 0

    def write(self, columns: dict[str, list[Any]]) -> int:
        """Write a columnar block as a new row group. Return the number of rows written.

        Empty blocks are skipped: a row group with no rows only bloats the metadata.
        """
        rows = len(next(iter(columns.values()), ()))
        if not rows:
            return 0
        self._writer.write_table(pa.table(columns, schema=self._schema))
        self.rows += rows
        self.row_groups += 1
        return rows


class OutputStore:
    """Where all output of a run is written, rooted at `root`."""

    def __init__(self, root: str) -> None:
        """Open the filesystem for URL `root` (local, s3://, ...)."""
        self.fs, self.root = fsspec.core.url_to_fs(root)

    def path(self, relative: str) -> str:
        """Full path of `relative` under the root directory."""
        return f"{self.root.rstrip('/')}/{relative}"

    def write_jsonl(self, relative: str, records: Iterable[dict[str, Any]]) -> int:
        """Write each record as one JSON row. Return the number of bytes written."""
        buffer = io.StringIO()
        for record in records:
            buffer.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            buffer.write("\n")
        return self._write_bytes(relative, buffer.getvalue().encode("utf-8"))

    def write_json(self, relative: str, obj: Any) -> int:
        """Write an object as a readable (indented) JSON file. Return the byte count."""
        return self._write_bytes(
            relative, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        )

    def open_write(self, relative: str) -> Any:
        """Open `relative` for binary writing, creating parent directories. The caller must close it."""
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        return self.fs.open(full, "wb")

    @contextmanager
    def jsonl_writers(self) -> Iterator[JsonlWriterPool]:
        """Open a `JsonlWriterPool`; any remaining files are closed on leaving the block."""
        pool = JsonlWriterPool(self)
        try:
            yield pool
        finally:
            pool.close_all()

    @contextmanager
    def parquet_writer(self, relative: str, schema: pa.Schema) -> Iterator[ParquetAppender]:
        """Open `relative` for incremental writing, returning a `ParquetAppender` for use in a `with` block.

        The Parquet footer is only written on `close`, so the `with` block closes the file even on error.
        """
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(full, "wb") as sink:
            with pq.ParquetWriter(sink, schema) as writer:
                yield ParquetAppender(writer, schema)
            # The footer is now in `sink`; fsync like `JsonlAppender.close`.
            _fsync(sink)

    def write_json_atomic(self, relative: str, obj: Any) -> int:
        """Write JSON via temp file then rename, so readers never see a half-written file.

        Order: write temp -> `fsync` temp file -> rename -> `fsync` parent directory.
        """
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        full = self.path(relative)
        parent = full.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)
        tmp = f"{full}.tmp"
        with self.fs.open(tmp, "wb") as f:
            f.write(data)
            _fsync(f)
        if self._is_local:
            os.replace(tmp, full)
        else:
            self.fs.mv(tmp, full)
        self._fsync_dir(parent)
        return len(data)

    def read_json(self, relative: str) -> Any:
        """Read a written JSON file."""
        with self.fs.open(self.path(relative), "rt", encoding="utf-8") as f:
            return json.load(f)

    def exists(self, relative: str) -> bool:
        """Check whether an output file exists, without creating or changing anything."""
        return self.fs.exists(self.path(relative))

    def remove(self, relative: str) -> None:
        """Delete a file. A missing file counts as done, no error."""
        with suppress(FileNotFoundError):
            self.fs.rm_file(self.path(relative))

    @property
    def _is_local(self) -> bool:
        """True when writing to a local directory, where `os.replace` works."""
        protocol = self.fs.protocol
        return "file" in (protocol if isinstance(protocol, tuple) else (protocol,))

    def _fsync_dir(self, path: str) -> None:
        """Flush the directory to disk so the rename is durable too. Skipped if not local."""
        if not self._is_local:
            return
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def glob(self, pattern: str) -> list[str]:
        """Sorted paths (relative to the root) matching `pattern`."""
        prefix = self.path("")
        return sorted(p.removeprefix(prefix) for p in self.fs.glob(self.path(pattern)))

    def read_jsonl(self, relative: str) -> Iterator[dict[str, Any]]:
        """Read the records of a JSON Lines file one by one."""
        with self.fs.open(self.path(relative), "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    def _write_bytes(self, relative: str, data: bytes) -> int:
        """Write `data` to `relative`, creating parent directories if needed."""
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(full, "wb") as f:
            f.write(data)
        return len(data)
