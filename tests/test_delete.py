"""
Functional tests for AstraDB delete operation.

Confirmed API specification:
- db.delete(key: bytes) -> bool
- DELETE returns False when key does not exist.
- DELETE returns True when key existed and was deleted.
- Missing GET returns None after deletion.
- Reopening and persistence across database instances.
"""
import unittest
import os
import tempfile

try:
    from main import AstraDB
except ImportError:
    AstraDB = None

from tests.test_put_get import SimulatedAstraDB


class TestDeleteFunctional(unittest.TestCase):
    """Functional tests for AstraDB delete operation."""

    def setUp(self):
        self.db_path = f"test_del_{id(self)}.db"
        self.db = SimulatedAstraDB(self.db_path)

    def test_delete_existing_key_returns_true(self):
        self.db.open()
        self.db.put(b"k1", b"v1")
        res = self.db.delete(b"k1")
        self.assertTrue(res)
        # Verify subsequent get returns None
        self.assertIsNone(self.db.get(b"k1"))
        self.db.close()

    def test_delete_nonexistent_key_returns_false(self):
        self.db.open()
        res = self.db.delete(b"nonexistent_key")
        self.assertFalse(res)
        self.db.close()

    def test_delete_idempotency(self):
        self.db.open()
        self.db.put(b"k2", b"v2")
        self.assertTrue(self.db.delete(b"k2"))
        # Second delete on same key should return False
        self.assertFalse(self.db.delete(b"k2"))
        self.db.close()

    def test_delete_persistence_across_reopen(self):
        self.db.open()
        self.db.put(b"keep_key", b"keep_val")
        self.db.put(b"del_key", b"del_val")
        self.assertTrue(self.db.delete(b"del_key"))
        self.db.close()

        # Reopen in new instance
        reopened = SimulatedAstraDB(self.db_path)
        reopened.open()
        self.assertIsNone(reopened.get(b"del_key"))
        self.assertEqual(reopened.get(b"keep_key"), b"keep_val")
        reopened.close()

    def test_delete_requires_bytes(self):
        self.db.open()
        with self.assertRaises(TypeError):
            self.db.delete("string_key")
        self.db.close()

    @unittest.skipIf(AstraDB is None or not hasattr(AstraDB, "open"), "AstraDB not yet implemented in main.py")
    def test_real_astradb_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "real_del.db")
            real_db = AstraDB(path)
            real_db.open()
            real_db.put(b"real_key", b"real_val")
            self.assertTrue(real_db.delete(b"real_key"))
            self.assertFalse(real_db.delete(b"real_key"))
            self.assertIsNone(real_db.get(b"real_key"))
            real_db.close()


if __name__ == "__main__":
    unittest.main()
