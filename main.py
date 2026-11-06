"""
Astra-SIT Embedded Key-Value Database Engine.
Core engine orchestration, public API, writer synchronization, durability,
poisoned-state protection, and process lock integration.
"""

import os
import sys
import threading
from typing import Any, Dict, List, Optional, Union

from batch import BatchManager, BatchOperation
from index import IndexEntry, MemoryIndex
from storage import (
    AstraError,
    DatabaseClosedError,
    DatabaseCorruptedError,
    DatabaseLockedError,
    KeyNotFoundError,
    OpType,
    RecordCodec,
    StorageEngine,
    TransactionError,
)

# --- Official Constraints ---
MAX_KEY_SIZE = 1024          # 1 to 1,024 bytes (1 KB)
MAX_VALUE_SIZE = 1048576      # 0 to 1,048,576 bytes (1 MB)
MAX_BATCH_SIZE = 10000        # 1 to 10,000 operations


class ProcessLock:
    """
    Cross-platform inter-process file lock.
    Guarantees single-process access to the database instance.
    Uses msvcrt.locking on Windows and fcntl.flock on POSIX.
    """

    def __init__(self, lock_path: str):
        self.lock_path = os.path.abspath(lock_path)
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        """Acquire an exclusive non-blocking lock. Raises DatabaseLockedError if busy."""
        parent = os.path.dirname(self.lock_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        try:
            self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR)
        except OSError as exc:
            raise DatabaseLockedError(f"Unable to open lockfile '{self.lock_path}': {exc}") from exc

        try:
            if sys.platform == "win32":
                import msvcrt
                # Ensure at least 1 byte exists to lock
                if os.fstat(self._fd).st_size == 0:
                    os.write(self._fd, b"L")
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
            raise DatabaseLockedError(
                f"Database at '{self.lock_path}' is currently locked by another process."
            ) from exc

    def release(self) -> None:
        """Release the exclusive lock and close the file descriptor."""
        if self._fd is not None:
            try:
                if sys.platform == "win32":
                    import msvcrt
                    try:
                        os.lseek(self._fd, 0, os.SEEK_SET)
                        msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    import fcntl
                    try:
                        fcntl.flock(self._fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None


class AstraDB:
    """
    Astra-SIT Embedded Key-Value Database Engine.

    Features:
    - Append-only log storage with in-memory hash index.
    - End-to-end CRC32 record integrity.
    - Durable writes via flush() and os.fsync().
    - Atomic multi-operation batches (commit-marker protocol).
    - Multi-threaded writer synchronization via shared lock.
    - Windows-safe concurrent reading.
    - Poisoned-state write protection after fsync failures.
    - Process-level exclusive locking.
    """

    def __init__(self, db_path: str):
        """
        Initialize the database handle.
        db_path can be a directory path or direct log file path.
        """
        raw_path = os.path.abspath(db_path)
        if raw_path.endswith(".db"):
            self._log_path = raw_path
            self._db_dir = os.path.dirname(raw_path) or "."
            self._lock_path = raw_path + ".lock"
        else:
            self._db_dir = raw_path
            self._log_path = os.path.join(self._db_dir, "data.db")
            self._lock_path = os.path.join(self._db_dir, "data.db.lock")

        self._db_path = db_path
        self._storage: Optional[StorageEngine] = None
        self._index: Optional[MemoryIndex] = None
        self._batch_mgr: Optional[BatchManager] = None
        self._proc_lock: Optional[ProcessLock] = None

        self._write_lock = threading.RLock()
        self._is_open = False
        self._is_poisoned = False

    def open(self) -> None:
        """
        Open or initialize the database.
        Acquires process lock, opens append-only storage, and recovers index.
        """
        with self._write_lock:
            if self._is_open:
                return

            os.makedirs(self._db_dir, exist_ok=True)

            # 1. Acquire OS Process Lock
            self._proc_lock = ProcessLock(self._lock_path)
            self._proc_lock.acquire()

            try:
                # 2. Open Storage Engine
                self._storage = StorageEngine(self._log_path)
                self._storage.open()

                # 3. Crash Recovery Integration
                # RecoveryManager is assigned to teammate Dakshin (recovery.py)
                self._index = None
                try:
                    import recovery
                    if hasattr(recovery, "RecoveryManager") and hasattr(recovery.RecoveryManager, "recover"):
                        self._index, _ = recovery.RecoveryManager.recover(self._storage)
                except (ImportError, AttributeError):
                    pass

                # If recovery is not yet implemented by Dakshin, initialize fresh index
                if self._index is None:
                    self._index = MemoryIndex()

                # 4. Initialize Batch Manager
                self._batch_mgr = BatchManager()

                self._is_open = True
                self._is_poisoned = False

            except Exception:
                if self._storage:
                    try:
                        self._storage.close()
                    except Exception:
                        pass
                    self._storage = None
                if self._proc_lock:
                    try:
                        self._proc_lock.release()
                    except Exception:
                        pass
                    self._proc_lock = None
                raise

    def close(self) -> None:
        """
        Close the database cleanly.
        Flushes and syncs pending data, closes storage handles, and releases process lock.
        """
        with self._write_lock:
            if not self._is_open:
                return

            try:
                if self._storage:
                    self._storage.close()
            finally:
                self._storage = None
                if self._proc_lock:
                    self._proc_lock.release()
                    self._proc_lock = None
                self._is_open = False

    def is_open(self) -> bool:
        """Return True if the database is open and ready for operations."""
        return self._is_open

    def is_poisoned(self) -> bool:
        """Return True if the database has entered a poisoned state due to fsync failure."""
        return self._is_poisoned

    def _ensure_active(self) -> None:
        """Validate that the database is open and not in a poisoned state."""
        if not self._is_open or self._storage is None or self._index is None:
            raise DatabaseClosedError("Database is closed. Call open() first.")
        if self._is_poisoned:
            raise DatabaseCorruptedError(
                "Database is in a poisoned state due to a prior fsync failure. "
                "Write operations are blocked until the database is closed and reopened."
            )

    @staticmethod
    def _normalize_key(key: Union[str, bytes]) -> bytes:
        """Validate and convert key to bytes."""
        if isinstance(key, str):
            k = key.encode("utf-8")
        elif isinstance(key, bytes):
            k = key
        else:
            raise TypeError(f"Key must be str or bytes, got {type(key).__name__}")

        if len(k) == 0:
            raise ValueError("Key cannot be empty (min 1 byte).")
        if len(k) > MAX_KEY_SIZE:
            raise ValueError(f"Key length {len(k)} exceeds maximum allowed {MAX_KEY_SIZE} bytes.")
        return k

    @staticmethod
    def _normalize_value(value: Union[str, bytes]) -> bytes:
        """Validate and convert value to bytes."""
        if isinstance(value, str):
            v = value.encode("utf-8")
        elif isinstance(value, bytes):
            v = value
        else:
            raise TypeError(f"Value must be str or bytes, got {type(value).__name__}")

        if len(v) > MAX_VALUE_SIZE:
            raise ValueError(f"Value length {len(v)} exceeds maximum allowed {MAX_VALUE_SIZE} bytes.")
        return v

    def put(self, key: Union[str, bytes], value: Union[str, bytes]) -> bool:
        """
        Insert or overwrite a key-value pair.
        Persists record to append-only log, fsyncs to disk, and updates memory index.
        """
        self._ensure_active()
        k = self._normalize_key(key)
        v = self._normalize_value(value)

        with self._write_lock:
            # Capture initial EOF before write
            initial_eof = self._storage.get_eof()

            try:
                # 1. Append record to log
                offset, total_len = self._storage.append_record(
                    op_type=OpType.PUT,
                    key=k,
                    value=v,
                    tx_id=0,
                )
            except Exception as exc:
                try:
                    self._storage.truncate(initial_eof)
                except Exception:
                    pass
                raise exc

            # 2. Durable sync (flush + fsync) BEFORE index update
            try:
                self._storage.sync()
            except OSError as exc:
                self._is_poisoned = True
                raise DatabaseCorruptedError(
                    f"Fsync failure during PUT; database entered poisoned state: {exc}"
                ) from exc

            # 3. Update in-memory index
            val_offset = offset + RecordCodec.HEADER_SIZE + len(k)
            entry = IndexEntry(
                offset=offset,
                total_len=total_len,
                val_offset=val_offset,
                val_len=len(v),
                timestamp=0,
            )
            self._index.set(k, entry)
            return True

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """
        Retrieve value for key.
        Returns bytes if found, None if key does not exist or has been deleted.
        """
        self._ensure_active()
        k = self._normalize_key(key)

        entry = self._index.get(k)
        if entry is None:
            return None

        # Read value directly from storage using thread-safe handle
        return self._storage.read_value(entry.val_offset, entry.val_len)

    def delete(self, key: Union[str, bytes]) -> bool:
        """
        Delete a key by appending a tombstone record.
        Returns True if key was present and deleted, False if key was absent.
        """
        self._ensure_active()
        k = self._normalize_key(key)

        with self._write_lock:
            if not self._index.contains(k):
                return False

            initial_eof = self._storage.get_eof()
            try:
                self._storage.append_record(
                    op_type=OpType.DELETE,
                    key=k,
                    value=b"",
                    tx_id=0,
                )
            except Exception as exc:
                try:
                    self._storage.truncate(initial_eof)
                except Exception:
                    pass
                raise exc

            # Fsync tombstone before removing from index
            try:
                self._storage.sync()
            except OSError as exc:
                self._is_poisoned = True
                raise DatabaseCorruptedError(
                    f"Fsync failure during DELETE; database entered poisoned state: {exc}"
                ) from exc

            self._index.delete(k)
            return True

    def batch(self, operations: List[BatchOperation]) -> bool:
        """
        Execute an atomic batch of operations (all-or-nothing).
        Ensures full rollback to initial EOF on append failure.
        """
        self._ensure_active()

        if not operations or len(operations) == 0:
            raise ValueError("Batch must contain at least 1 operation.")
        if len(operations) > MAX_BATCH_SIZE:
            raise ValueError(
                f"Batch size {len(operations)} exceeds maximum allowed {MAX_BATCH_SIZE} operations."
            )

        # Pre-validate and normalize all operations upfront
        normalized_ops: List[BatchOperation] = []
        for op in operations:
            if not isinstance(op, BatchOperation):
                raise TypeError(f"Expected BatchOperation instance, got {type(op).__name__}")

            k = self._normalize_key(op.key)
            if op.op_type == OpType.PUT:
                if op.value is None:
                    raise ValueError(f"PUT operation for key {k!r} requires a value.")
                v = self._normalize_value(op.value)
                normalized_ops.append(BatchOperation(OpType.PUT, k, v))
            elif op.op_type == OpType.DELETE:
                normalized_ops.append(BatchOperation(OpType.DELETE, k, b""))
            else:
                raise ValueError(f"Unsupported operation type in batch: {op.op_type}")

        with self._write_lock:
            initial_eof = self._storage.get_eof()

            try:
                return self._batch_mgr.execute_batch(self._storage, self._index, normalized_ops)
            except OSError as exc:
                # Truncate back to initial_eof to purge incomplete batch
                try:
                    self._storage.truncate(initial_eof)
                except Exception:
                    pass
                self._is_poisoned = True
                raise DatabaseCorruptedError(
                    f"Fsync / I/O failure during batch transaction; database poisoned: {exc}"
                ) from exc
            except Exception as exc:
                # Rollback on append error
                try:
                    self._storage.truncate(initial_eof)
                except Exception:
                    pass
                raise exc

    def compact(self) -> Dict[str, Any]:
        """
        Reclaim storage space by executing log compaction.
        Delegates to Compactor (compaction.py) if implemented.
        """
        self._ensure_active()

        with self._write_lock:
            try:
                import compaction
                if hasattr(compaction, "Compactor") and hasattr(compaction.Compactor, "compact"):
                    return compaction.Compactor.compact(self._db_dir, self._index, self._storage)
                raise NotImplementedError(
                    "Compaction module is not yet implemented (compaction.py is empty)."
                )
            except (ImportError, AttributeError) as exc:
                raise NotImplementedError(
                    "Compaction module is not yet implemented (compaction.py is empty)."
                ) from exc

    def __enter__(self) -> "AstraDB":
        if not self._is_open:
            self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# Re-export exceptions and types for external callers
__all__ = [
    "AstraDB",
    "AstraError",
    "KeyNotFoundError",
    "DatabaseCorruptedError",
    "DatabaseLockedError",
    "TransactionError",
    "DatabaseClosedError",
    "BatchOperation",
    "OpType",
    "MAX_KEY_SIZE",
    "MAX_VALUE_SIZE",
    "MAX_BATCH_SIZE",
]
