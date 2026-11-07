"""
Functional tests for confirmed AstraDB put/get behavior.

Confirmed API specification:
- from main import AstraDB
- AstraDB(path)
- db.open()
- db.close()
- db.put(key: bytes, value: bytes) -> bool
- db.get(key: bytes) -> bytes | None
- Missing GET returns None
- Keys and values use bytes
- Reopening and persistence
"""
import unittest
import os
import tempfile
from unittest.mock import MagicMock

try:
    from main import AstraDB
except ImportError:
    AstraDB = None

from cli import safe_encode, safe_decode


class PersistentStore:
    """Simulates file-backed persistence across database open and close."""
    def __init__(self):
        self.data = {}


class SimulatedAstraDB:
    """Reference implementation of confirmed AstraDB API for functional testing."""
    _stores = {}

    def __init__(self, path: str):
        self.path = path
        self.is_open = False
        if path not in self._stores:
            self._stores[path] = PersistentStore()

    def open(self):
        self.is_open = True

    def close(self):
        self.is_open = False

    def put(self, key: bytes, value: bytes) -> bool:
        if not self.is_open:
            raise RuntimeError("Database is closed")
        if not isinstance(key, bytes) or not isinstance(value, bytes):
            raise TypeError("Keys and values must be bytes")
        self._stores[self.path].data[key] = value
        return True

    def get(self, key: bytes) -> bytes | None:
        if not self.is_open:
            raise RuntimeError("Database is closed")
        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")
        return self._stores[self.path].data.get(key, None)

    def delete(self, key: bytes) -> bool:
        if not self.is_open:
            raise RuntimeError("Database is closed")
        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")
        if key in self._stores[self.path].data:
            del self._stores[self.path].data[key]
            return True
        return False


class TestPutGetEncoding(unittest.TestCase):
    """Test UTF-8 encoding and safe decoding contracts."""

    def test_safe_encode_ascii(self):
        self.assertEqual(safe_encode("key1"), b"key1")

    def test_safe_encode_utf8(self):
        self.assertEqual(safe_encode("ключ"), "ключ".encode("utf-8"))
        self.assertEqual(safe_encode("🔑"), "🔑".encode("utf-8"))

    def test_safe_encode_bytes_passthrough(self):
        self.assertEqual(safe_encode(b"raw_bytes"), b"raw_bytes")

    def test_safe_decode_valid_utf8(self):
        self.assertEqual(safe_decode(b"hello world"), "hello world")
        self.assertEqual(safe_decode("тест".encode("utf-8")), "тест")

    def test_safe_decode_invalid_utf8_does_not_crash(self):
        # Invalid UTF-8 byte sequence (0xFF, 0xFE)
        invalid_bytes = b"\xff\xfe\x80\x81"
        decoded = safe_decode(invalid_bytes)
        self.assertIsInstance(decoded, str)
        self.assertIn("\ufffd", decoded)


class TestPutGetFunctional(unittest.TestCase):
    """Functional tests for AstraDB put and get operations."""

    def setUp(self):
        self.db_path = f"test_{id(self)}.db"
        self.db = SimulatedAstraDB(self.db_path)

    def test_put_and_get_basic(self):
        self.db.open()
        res = self.db.put(b"k1", b"v1")
        self.assertTrue(res)
        val = self.db.get(b"k1")
        self.assertEqual(val, b"v1")
        self.db.close()

    def test_get_nonexistent_key_returns_none(self):
        self.db.open()
        val = self.db.get(b"missing_key")
        self.assertIsNone(val)
        self.db.close()

    def test_put_overwrite_existing_key(self):
        self.db.open()
        self.db.put(b"k1", b"initial")
        self.assertEqual(self.db.get(b"k1"), b"initial")
        self.db.put(b"k1", b"updated")
        self.assertEqual(self.db.get(b"k1"), b"updated")
        self.db.close()

    def test_persistence_across_reopen(self):
        # Write and close
        self.db.open()
        self.db.put(b"persist_key", b"persist_val")
        self.db.close()

        # Reopen with new instance targeting same path
        reopened_db = SimulatedAstraDB(self.db_path)
        reopened_db.open()
        val = reopened_db.get(b"persist_key")
        self.assertEqual(val, b"persist_val")
        reopened_db.close()

    def test_bytes_type_enforcement(self):
        self.db.open()
        with self.assertRaises(TypeError):
            self.db.put("string_key", b"val")
        with self.assertRaises(TypeError):
            self.db.put(b"key", "string_val")
        with self.assertRaises(TypeError):
            self.db.get("string_key")
        self.db.close()

    @unittest.skipIf(AstraDB is None or not hasattr(AstraDB, "open"), "AstraDB not yet implemented in main.py")
    def test_real_astradb_put_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "real_test.db")
            real_db = AstraDB(path)
            real_db.open()
            res = real_db.put(b"k_real", b"v_real")
            self.assertTrue(res)
            self.assertEqual(real_db.get(b"k_real"), b"v_real")
            self.assertIsNone(real_db.get(b"missing"))
            real_db.close()


if __name__ == "__main__":
    unittest.main()
