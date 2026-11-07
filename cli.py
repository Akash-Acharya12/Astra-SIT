"""
Astra-SIT Command Line Interface (cli.py)

Crash-safe key-value database CLI implementation.
Supports commands:
  open, put, get, del, batch, dump, check, serve

Confirmed API specification:
- from main import AstraDB
- from batch import BatchOperation
- db = AstraDB(path)
- db.open()
- db.close()
- db.put(key: bytes, value: bytes) -> bool
- db.get(key: bytes) -> bytes | None (missing returns None)
- db.delete(key: bytes) -> bool (returns False when key does not exist)
- db.batch(operations: List[BatchOperation]) -> bool
- BatchOperation.put(...) / BatchOperation.delete(...)
- Keys and values use bytes (CLI inputs encoded as UTF-8)
- Retrieved bytes decoded using UTF-8 with errors="replace"
"""
import sys
import os
import re
import json
import inspect
from typing import List, Any

try:
    from main import AstraDB
except ImportError:
    AstraDB = None

try:
    from batch import BatchOperation
except ImportError:
    BatchOperation = None


def safe_encode(val: Any) -> bytes:
    """Encode a string key or value into UTF-8 bytes."""
    if isinstance(val, bytes):
        return val
    return str(val).encode("utf-8")


def safe_decode(raw: Any) -> str:
    """Safely decode bytes to UTF-8 string with errors='replace' without crashing on invalid UTF-8."""
    if not isinstance(raw, (bytes, bytearray)):
        return str(raw)
    return raw.decode("utf-8", errors="replace")


def get_db(path: str):
    """Instantiate AstraDB with the given path."""
    if AstraDB is None:
        raise RuntimeError("AstraDB is not available. Ensure main.py defines AstraDB.")
    return AstraDB(path)


def create_batch_put(key: str, value: str) -> Any:
    """
    Inspect the actual BatchOperation.put implementation to determine
    whether its factory expects str or bytes. Do not guess.
    """
    if BatchOperation is None:
        raise RuntimeError("BatchOperation is not available. Ensure batch.py defines BatchOperation.")

    put_fn = getattr(BatchOperation, "put")
    sig = inspect.signature(put_fn)
    params = list(sig.parameters.values())

    expects_str = False
    expects_bytes = False

    for param in params:
        if param.annotation in (str, "str"):
            expects_str = True
        elif param.annotation in (bytes, "bytes"):
            expects_bytes = True

    if expects_str and not expects_bytes:
        return BatchOperation.put(key, value)
    elif expects_bytes:
        return BatchOperation.put(safe_encode(key), safe_encode(value))
    else:
        # If no explicit type annotation, inspect docstring
        doc = (put_fn.__doc__ or "").lower()
        if "str" in doc and "bytes" not in doc:
            return BatchOperation.put(key, value)
        if "bytes" in doc:
            return BatchOperation.put(safe_encode(key), safe_encode(value))

        # Default to canonical bytes representation, fallback to str if TypeError
        try:
            return BatchOperation.put(safe_encode(key), safe_encode(value))
        except TypeError:
            return BatchOperation.put(key, value)


def create_batch_delete(key: str) -> Any:
    """
    Inspect the actual BatchOperation.delete implementation to determine
    whether its factory expects str or bytes. Do not guess.
    """
    if BatchOperation is None:
        raise RuntimeError("BatchOperation is not available. Ensure batch.py defines BatchOperation.")

    del_fn = getattr(BatchOperation, "delete")
    sig = inspect.signature(del_fn)
    params = list(sig.parameters.values())

    expects_str = False
    expects_bytes = False

    for param in params:
        if param.annotation in (str, "str"):
            expects_str = True
        elif param.annotation in (bytes, "bytes"):
            expects_bytes = True

    if expects_str and not expects_bytes:
        return BatchOperation.delete(key)
    elif expects_bytes:
        return BatchOperation.delete(safe_encode(key))
    else:
        doc = (del_fn.__doc__ or "").lower()
        if "str" in doc and "bytes" not in doc:
            return BatchOperation.delete(key)
        if "bytes" in doc:
            return BatchOperation.delete(safe_encode(key))

        try:
            return BatchOperation.delete(safe_encode(key))
        except TypeError:
            return BatchOperation.delete(key)


def cmd_open(db_path: str) -> int:
    """Open the database at db_path and close it cleanly."""
    try:
        db = get_db(db_path)
        db.open()
        db.close()
        print(f"Database opened and closed successfully at {db_path}")
        return 0
    except Exception as e:
        sys.stderr.write(f"Error opening database: {e}\n")
        return 1


def cmd_get(db_path: str, key: str) -> int:
    """Retrieve key and print decoded UTF-8 string value (or None if missing)."""
    try:
        db = get_db(db_path)
        db.open()
        try:
            val_bytes = db.get(safe_encode(key))
            if val_bytes is None:
                print("None")
                return 1
            print(safe_decode(val_bytes))
            return 0
        finally:
            db.close()
    except Exception as e:
        sys.stderr.write(f"Error executing get: {e}\n")
        return 1


def cmd_put(db_path: str, key: str, value: str) -> int:
    """Store key-value pair in database. Never prints ACK/OK before durable success."""
    try:
        db = get_db(db_path)
        db.open()
        try:
            res = db.put(safe_encode(key), safe_encode(value))
            if res is True:
                print("OK")
                return 0
            else:
                sys.stderr.write("Error: put operation failed durably.\n")
                return 1
        finally:
            db.close()
    except Exception as e:
        sys.stderr.write(f"Error executing put: {e}\n")
        return 1


def cmd_del(db_path: str, key: str) -> int:
    """Delete key from database. Returns 0 on success, 1 if key did not exist."""
    try:
        db = get_db(db_path)
        db.open()
        try:
            res = db.delete(safe_encode(key))
            if res is True:
                print("OK")
                return 0
            else:
                sys.stderr.write(f"Key not found: {key}\n")
                return 1
        finally:
            db.close()
    except Exception as e:
        sys.stderr.write(f"Error executing del: {e}\n")
        return 1


def cmd_batch(db_path: str, *args) -> int:
    """Execute batch operations from a file, direct arguments, or stdin."""
    if not args:
        sys.stderr.write("Error: batch requires a file path or operations\n")
        return 1

    ops: List[Any] = []

    # If first arg is an existing file or '-':
    if os.path.exists(args[0]) or args[0] == "-":
        if args[0] == "-":
            content = sys.stdin.read()
        else:
            with open(args[0], "r", encoding="utf-8") as f:
                content = f.read()

        parsed = False
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        op_type = str(item.get("op", "")).lower()
                        if op_type == "put":
                            ops.append(create_batch_put(str(item["key"]), str(item["value"])))
                        elif op_type in ("del", "delete"):
                            ops.append(create_batch_delete(str(item["key"])))
                    elif isinstance(item, (list, tuple)):
                        op_type = str(item[0]).lower()
                        if op_type == "put":
                            ops.append(create_batch_put(str(item[1]), str(item[2])))
                        elif op_type in ("del", "delete"):
                            ops.append(create_batch_delete(str(item[1])))
                parsed = True
        except Exception:
            parsed = False

        if not parsed:
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(maxsplit=2)
                op_type = parts[0].lower()
                if op_type == "put" and len(parts) >= 3:
                    ops.append(create_batch_put(parts[1], parts[2]))
                elif op_type in ("del", "delete") and len(parts) >= 2:
                    ops.append(create_batch_delete(parts[1]))
    else:
        # Command line arguments parsing: e.g. put k1 v1 del k2
        i = 0
        raw = list(args)
        while i < len(raw):
            op_type = raw[i].lower()
            if op_type == "put":
                if i + 2 >= len(raw):
                    sys.stderr.write("Error: batch put requires key and value\n")
                    return 1
                ops.append(create_batch_put(raw[i + 1], raw[i + 2]))
                i += 3
            elif op_type in ("del", "delete"):
                if i + 1 >= len(raw):
                    sys.stderr.write("Error: batch del requires key\n")
                    return 1
                ops.append(create_batch_delete(raw[i + 1]))
                i += 2
            else:
                sys.stderr.write(f"Error: Unknown batch operation: {raw[i]}\n")
                return 1

    if not ops:
        sys.stderr.write("Error: No valid batch operations found\n")
        return 1

    try:
        db = get_db(db_path)
        db.open()
        try:
            res = db.batch(ops)
            if res is True:
                print("OK")
                return 0
            else:
                sys.stderr.write("Error: batch operation failed durably.\n")
                return 1
        finally:
            db.close()
    except Exception as e:
        sys.stderr.write(f"Error executing batch: {e}\n")
        return 1


def cmd_dump(db_path: str, *args) -> int:
    """Dump command - pending public interface from team leader."""
    sys.stderr.write("Error: dump() public interface is not yet confirmed. Pending interface from team leader.\n")
    return 1


def cmd_check(db_path: str, *args) -> int:
    """Check command - pending public interface from team leader."""
    sys.stderr.write("Error: check() public interface is not yet confirmed. Pending interface from team leader.\n")
    return 1


def cmd_serve(db_path: str, *args) -> int:
    """
    Run approved Astra-SIT JSONL serve protocol on stdin/stdout.

    Request format (one JSON per line):
      PUT:    {"id":1,"op":"put","key":"name","value":"Akash"}
      GET:    {"id":2,"op":"get","key":"name"}
      DELETE: {"id":3,"op":"delete","key":"name"}
      BATCH:  {"id":4,"op":"batch","operations":[{"op":"put","key":"a","value":"1"}]}

    Response contract on stdout:
      PUT success:    ACK <id>
      DELETE success: ACK <id>
      BATCH success:  ACK <id>
      GET existing:   {"id":<id>,"status":"OK","value":"<val>"}
      GET missing:    NOTFOUND <id>
      Failed:         ERROR <id>
    """
    try:
        db = get_db(db_path)
        db.open()
    except Exception as e:
        sys.stderr.write(f"Error opening database for serve: {e}\n")
        return 1

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
            except Exception as e:
                sys.stderr.write(f"Malformed JSON request: {e} - line: {line}\n")
                match = re.search(r'"id"\s*:\s*([^,\s}]+)', line)
                err_id = match.group(1) if match else "null"
                print(f"ERROR {err_id}", flush=True)
                continue

            if not isinstance(data, dict):
                sys.stderr.write(f"Invalid request format (expected JSON object): {data}\n")
                print("ERROR null", flush=True)
                continue

            req_id = data.get("id")
            op = str(data.get("op", "")).lower()

            if op == "put":
                key = data.get("key")
                val = data.get("value")
                if key is None or val is None:
                    sys.stderr.write(f"Missing key or value for put: {data}\n")
                    print(f"ERROR {req_id}", flush=True)
                    continue

                try:
                    k_bytes = safe_encode(key)
                    v_bytes = safe_encode(val)
                    res = db.put(k_bytes, v_bytes)
                    if res is True:
                        print(f"ACK {req_id}", flush=True)
                    else:
                        print(f"ERROR {req_id}", flush=True)
                except Exception as e:
                    sys.stderr.write(f"Put error: {e}\n")
                    print(f"ERROR {req_id}", flush=True)

            elif op == "get":
                key = data.get("key")
                if key is None:
                    sys.stderr.write(f"Missing key for get: {data}\n")
                    print(f"ERROR {req_id}", flush=True)
                    continue

                try:
                    k_bytes = safe_encode(key)
                    val_bytes = db.get(k_bytes)
                    if val_bytes is None:
                        print(f"NOTFOUND {req_id}", flush=True)
                    else:
                        val_str = safe_decode(val_bytes)
                        resp = json.dumps({"id": req_id, "status": "OK", "value": val_str})
                        print(resp, flush=True)
                except Exception as e:
                    sys.stderr.write(f"Get error: {e}\n")
                    print(f"ERROR {req_id}", flush=True)

            elif op in ("delete", "del"):
                key = data.get("key")
                if key is None:
                    sys.stderr.write(f"Missing key for delete: {data}\n")
                    print(f"ERROR {req_id}", flush=True)
                    continue

                try:
                    k_bytes = safe_encode(key)
                    res = db.delete(k_bytes)
                    if res is True:
                        print(f"ACK {req_id}", flush=True)
                    else:
                        print(f"ERROR {req_id}", flush=True)
                except Exception as e:
                    sys.stderr.write(f"Delete error: {e}\n")
                    print(f"ERROR {req_id}", flush=True)

            elif op == "batch":
                raw_ops = data.get("operations")
                if not isinstance(raw_ops, list):
                    sys.stderr.write(f"Invalid operations array for batch: {data}\n")
                    print(f"ERROR {req_id}", flush=True)
                    continue

                batch_ops = []
                valid = True
                for item in raw_ops:
                    if not isinstance(item, dict):
                        valid = False
                        break
                    sub_op = str(item.get("op", "")).lower()
                    sub_key = item.get("key")
                    if sub_key is None:
                        valid = False
                        break

                    try:
                        if sub_op == "put":
                            sub_val = item.get("value")
                            if sub_val is None:
                                valid = False
                                break
                            batch_ops.append(create_batch_put(str(sub_key), str(sub_val)))
                        elif sub_op in ("delete", "del"):
                            batch_ops.append(create_batch_delete(str(sub_key)))
                        else:
                            valid = False
                            break
                    except Exception as e:
                        sys.stderr.write(f"Error creating batch operation: {e}\n")
                        valid = False
                        break

                if not valid or not batch_ops:
                    sys.stderr.write(f"Batch validation failed for request: {data}\n")
                    print(f"ERROR {req_id}", flush=True)
                    continue

                try:
                    res = db.batch(batch_ops)
                    if res is True:
                        print(f"ACK {req_id}", flush=True)
                    else:
                        print(f"ERROR {req_id}", flush=True)
                except Exception as e:
                    sys.stderr.write(f"Batch execution error: {e}\n")
                    print(f"ERROR {req_id}", flush=True)

            else:
                sys.stderr.write(f"Unknown operation '{op}' for request: {data}\n")
                print(f"ERROR {req_id}", flush=True)

        return 0
    finally:
        try:
            db.close()
        except Exception as e:
            sys.stderr.write(f"Error closing database: {e}\n")


def print_usage():
    print("""Usage: python cli.py <command> <db_path> [args...]
   or: python cli.py <db_path> <command> [args...]

Commands:
  open  <db_path>                     Open and close database
  get   <db_path> <key>               Retrieve value for key
  put   <db_path> <key> <value>       Insert/update key-value pair
  del   <db_path> <key>               Delete key
  batch <db_path> <file_or_ops...>    Run atomic batch of put/del operations
  dump  <db_path>                     (Pending public API)
  check <db_path>                     (Pending public API)
  serve <db_path>                     Run JSONL serve protocol on stdin/stdout
""")


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    if not argv or argv[0] in ("-h", "--help", "help"):
        print_usage()
        return 0

    KNOWN_COMMANDS = {"open", "put", "get", "del", "batch", "dump", "check", "serve"}

    if argv[0] in KNOWN_COMMANDS:
        command = argv[0]
        if len(argv) < 2:
            sys.stderr.write(f"Error: Missing db_path for command '{command}'\n")
            return 1
        db_path = argv[1]
        args = argv[2:]
    elif len(argv) >= 2 and argv[1] in KNOWN_COMMANDS:
        db_path = argv[0]
        command = argv[1]
        args = argv[2:]
    else:
        sys.stderr.write(f"Error: Unknown command or invalid arguments: {argv}\n")
        print_usage()
        return 1

    if command == "open":
        return cmd_open(db_path)
    elif command == "get":
        if not args:
            sys.stderr.write("Error: get requires <key>\n")
            return 1
        return cmd_get(db_path, args[0])
    elif command == "put":
        if len(args) < 2:
            sys.stderr.write("Error: put requires <key> <value>\n")
            return 1
        return cmd_put(db_path, args[0], args[1])
    elif command == "del":
        if not args:
            sys.stderr.write("Error: del requires <key>\n")
            return 1
        return cmd_del(db_path, args[0])
    elif command == "batch":
        return cmd_batch(db_path, *args)
    elif command == "dump":
        return cmd_dump(db_path, *args)
    elif command == "check":
        return cmd_check(db_path, *args)
    elif command == "serve":
        return cmd_serve(db_path, *args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
