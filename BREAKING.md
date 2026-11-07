# Astra-SIT: Known Limitations, Unsupported Behaviors & Breaking Changes

This document details intentional design choices, non-supported usage patterns, and critical operational constraints.

---

## 1. Concurrency: Multi-Process Writers Are Not Supported

- **Behavior**: Opening an Astra-SIT database acquires an exclusive non-blocking OS lock (`.lock` file via `msvcrt.locking` on Windows and `fcntl.flock` on POSIX).
- **Consequence**: If a second process attempts to call `AstraDB.open()` on an already-opened database directory, it will immediately raise:
  ```
  main.DatabaseLockedError: Database at '<path>' is currently locked by another process.
  ```
- **Rationale**: Astra-SIT uses an append-only binary log without inter-process coordination protocols. Simultaneous multi-process writing would interleave binary struct bytes and corrupt the log.
- **Supported Concurrency**:
  - In-process multi-threading: Handled safely with `threading.RLock`.
  - Multi-threaded reads: Supported concurrently via thread-local file handles.

---

## 2. In-Memory Index RAM Capacity Ceiling

- **Behavior**: `MemoryIndex` maintains an in-memory Python dictionary of all active keys mapped to `IndexEntry` (offset, record length, value offset, value length, timestamp).
- **Consequence**: Datasets whose key spaces exceed available system memory cannot be supported. A dataset of 1,000,000 keys requires approximately 100–140 MB of RAM in CPython.
- **Unverified Scale**: While mathematically supported by memory capacity on modern workstations, 1,000,000-key capacity and recovery under 2 seconds remain **unverified targets** as no automated 1M-key benchmark has been run.

---

## 3. Strict Size Bounds

Astra-SIT strictly validates inputs at the API entrypoint:
- **Keys**: Must be between 1 and 1,024 bytes (`MAX_KEY_SIZE = 1024`). Empty keys or keys $> 1024$ bytes raise `ValueError`.
- **Values**: Maximum size is 1,048,576 bytes (`MAX_VALUE_SIZE = 1048576`, 1 MB). Values exceeding 1 MB raise `ValueError`.
- **Batches**: Maximum batch size is 10,000 operations (`MAX_BATCH_SIZE = 10000`). Batches with 0 operations return `True` (no-op). Batches $> 10000$ operations raise `ValueError`.

---

## 4. Poisoned State on Disk / fsync Failures

- **Behavior**: If an `os.fsync()` or storage append encounters an `OSError` (e.g. disk full `ENOSPC`, I/O failure, permission revoked), the database marks itself poisoned (`_is_poisoned = True`) and raises `DatabaseCorruptedError`.
- **Consequence**: All subsequent write attempts (`put`, `delete`, `batch`) immediately fail with `DatabaseCorruptedError("Database is in poisoned state due to earlier fsync failure; writes disabled.")`.
- **Recovery**: The application process must close and reopen the database, allowing `RecoveryManager.recover()` to truncate any uncommitted or torn tail writes.

---

## 5. CLI Command Stubs

- In `cli.py`:
  - `python cli.py dump`: Returns exit code `1` with message `"Error: dump() public interface is not yet confirmed. Pending interface from team leader."`
  - `python cli.py check`: Returns exit code `1` with message `"Error: check() public interface is not yet confirmed. Pending interface from team leader."`
  - `compact`: Supported in Python API (`AstraDB.compact()`), but not currently exposed as a CLI subcommand.
