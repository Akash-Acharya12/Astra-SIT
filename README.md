# Astra-SIT

Crash-safe, embedded, append-only key-value database engine implemented entirely in pure Python standard library.

---

## Features

- **Append-Only Storage**: Bitcask-inspired binary log format (`data.db`) with fast $O(1)$ in-memory hash indexing (`index.py`).
- **Absolute Durability**: Every successful mutation executes `flush()` and `os.fsync()` before returning `True` or emitting `ACK`.
- **End-to-End CRC32**: Every record is checksummed with CRC32. Bit-level corruption and mid-log damage are immediately detected and rejected.
- **Atomic Batches**: Multi-operation batch transactions using the write-ahead commit-marker protocol (`TX_BEGIN` -> records -> `TX_COMMIT` -> single `fsync`).
- **Automatic Crash Recovery**: On startup, incomplete headers or torn payloads at EOF are safely truncated, and uncommitted batch transactions are automatically rolled back.
- **Zero Dependencies**: Pure Python 3 standard library only (`struct`, `zlib`, `threading`, `os`, `sys`, `json`, `dataclasses`, `enum`). 100% offline.
- **Process & Thread Safety**: Inter-process exclusivity via OS file lock (`.lock`), multi-threaded writer safety via `RLock`, and thread-local concurrent readers.
- **CLI & JSONL Protocol**: Interactive subcommands and line-delimited streaming protocol over standard I/O pipes.

---

## Installation & Requirements

Astra-SIT requires **Python 3.8+** and has **zero third-party dependencies**. No `pip install` required.

```bash
git clone <repo_url>
cd Astra-SIT
```

Verify compilation:
```bash
python -m py_compile cli.py main.py storage.py index.py batch.py compaction.py recovery.py
```

---

## Python API Usage

```python
from main import AstraDB
from batch import BatchOperation

# 1. Open database (creates directory and log file if not present)
db = AstraDB("./my_database.db")
db.open()

# Or using context manager:
# with AstraDB("./my_database.db") as db:

# 2. Put key-value pairs (supports str or bytes)
db.put("user:1001", '{"name": "Alice", "role": "admin"}')
db.put(b"binary_key", b"\x01\x02\x03\x04")

# 3. Get values (returns bytes or None if missing)
val = db.get("user:1001")
print(val.decode("utf-8"))  # '{"name": "Alice", "role": "admin"}'

missing = db.get("nonexistent_key")
print(missing)  # None

# 4. Atomic Multi-Operation Batch
ops = [
    BatchOperation.put("account:A", "900"),
    BatchOperation.put("account:B", "1100"),
    BatchOperation.delete("pending_transfer:123")
]
success = db.batch(ops)  # Atomically committed with single fsync

# 5. Delete key (returns True if deleted, False if key did not exist)
deleted = db.delete("user:1001")
print(deleted)  # True

# 6. Close database cleanly (releases OS lock and read/write handles)
db.close()
```

---

## Command-Line Interface (CLI)

```bash
# Open and close database cleanly
python cli.py open ./my_database.db

# Insert key-value pair (prints OK on durable write)
python cli.py put ./my_database.db key1 value1

# Retrieve key value (prints value with exit code 0; prints None with exit code 1 if missing)
python cli.py get ./my_database.db key1

# Delete key (prints OK with exit code 0; prints error with exit code 1 if key missing)
python cli.py del ./my_database.db key1

# Execute batch operations from a JSON file
python cli.py batch ./my_database.db operations.json
```

---

## Line-Delimited JSON (JSONL) Serve Protocol

Start the continuous streaming server over stdin/stdout:
```bash
python cli.py serve ./my_database.db
```

### Request & Response Specifications
Each request and response is formatted as a single line ending in a newline (`\n`):

| Operation | Input JSON Line | Success Output Line | Failure / Miss Output Line |
|---|---|---|---|
| **PUT** | `{"id": 1, "op": "put", "key": "k1", "value": "v1"}` | `ACK 1` | `ERROR 1` |
| **GET** | `{"id": 2, "op": "get", "key": "k1"}` | `{"id": 2, "status": "OK", "value": "v1"}` | `NOTFOUND 2` (or `ERROR 2`) |
| **DELETE** | `{"id": 3, "op": "delete", "key": "k1"}` | `ACK 3` | `ERROR 3` |
| **BATCH** | `{"id": 4, "op": "batch", "operations": [{"op": "put", "key": "a", "value": "1"}]}` | `ACK 4` | `ERROR 4` |

---

## Running Tests

Run the full automated test suite:
```bash
python -m unittest discover -s tests -v
```

Run crash recovery and durability tests:
```bash
python scratch/run_all_tests.py -v
```

---

## Architectural Details

For deep technical specifications, binary layouts, recovery algorithms, and known constraints, see [ARCHITECTURE.md](ARCHITECTURE.md).
