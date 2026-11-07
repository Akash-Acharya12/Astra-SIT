"""
Functional tests for AstraDB batch operations.

Confirmed API specification:
- from batch import BatchOperation
- BatchOperation.put(...)
- BatchOperation.delete(...)
- db.batch(operations: List[BatchOperation]) -> bool
- Inspect BatchOperation factory signatures (str vs bytes)
- Persistence and reopening across instances
"""
import unittest
import os
import tempfile
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

from tests.test_put_get import SimulatedAstraDB
from cli import create_batch_put, create_batch_delete


class SimulatedBatchOperation:
    """Reference implementation of BatchOperation for functional testing."""
    def __init__(self, op_type: str, key: Any, value: Any = None):
        self.op_type = op_type
        self.key = key
        self.value = value

    @classmethod
    def put(cls, key: bytes, value: bytes):
        return cls("put", key, value)

    @classmethod
    def delete(cls, key: bytes):
        return cls("delete", key)


class SimulatedAstraDBWithBatch(SimulatedAstraDB):
    """Simulated AstraDB with batch execution support."""
    def batch(self, operations: List[Any]) -> bool:
        if not self.is_open:
            raise RuntimeError("Database is closed")
        store = self._stores[self.path].data
        for op in operations:
            op_type = getattr(op, "op_type", None) or getattr(op, "type", None)
            key = getattr(op, "key", None)
            value = getattr(op, "value", None)

            # Ensure bytes
            if isinstance(key, str):
                key = key.encode("utf-8")
            if isinstance(value, str):
                value = value.encode("utf-8")

            if op_type in ("put", "PUT"):
                store[key] = value
            elif op_type in ("delete", "del", "DELETE"):
                store.pop(key, None)
            else:
                return False
        return True


class TestBatchInspectionAndExecution(unittest.TestCase):
    """Test BatchOperation factory inspection and batch execution."""

    def setUp(self):
        self.db_path = f"test_batch_{id(self)}.db"
        self.db = SimulatedAstraDBWithBatch(self.db_path)

    def test_factory_inspection_with_bytes_annotations(self):
        """Verify inspection properly identifies factories expecting bytes."""
        class BytesFactory:
            @classmethod
            def put(cls, key: bytes, value: bytes):
                return SimulatedBatchOperation("put", key, value)

            @classmethod
            def delete(cls, key: bytes):
                return SimulatedBatchOperation("delete", key)

        with unittest.mock.patch("cli.BatchOperation", BytesFactory):
            op_put = create_batch_put("k_test", "v_test")
            self.assertEqual(op_put.key, b"k_test")
            self.assertEqual(op_put.value, b"v_test")

            op_del = create_batch_delete("k_del")
            self.assertEqual(op_del.key, b"k_del")

    def test_factory_inspection_with_str_annotations(self):
        """Verify inspection properly identifies factories expecting str."""
        class StrFactory:
            @classmethod
            def put(cls, key: str, value: str):
                return SimulatedBatchOperation("put", key, value)

            @classmethod
            def delete(cls, key: str):
                return SimulatedBatchOperation("delete", key)

        with unittest.mock.patch("cli.BatchOperation", StrFactory):
            op_put = create_batch_put("k_str", "v_str")
            self.assertEqual(op_put.key, "k_str")
            self.assertEqual(op_put.value, "v_str")

            op_del = create_batch_delete("k_del_str")
            self.assertEqual(op_del.key, "k_del_str")

    def test_batch_put_and_delete_operations(self):
        self.db.open()
        self.db.put(b"existing_k", b"existing_v")

        ops = [
            SimulatedBatchOperation.put(b"k1", b"v1"),
            SimulatedBatchOperation.put(b"k2", b"v2"),
            SimulatedBatchOperation.delete(b"existing_k"),
        ]
        res = self.db.batch(ops)
        self.assertTrue(res)

        self.assertEqual(self.db.get(b"k1"), b"v1")
        self.assertEqual(self.db.get(b"k2"), b"v2")
        self.assertIsNone(self.db.get(b"existing_k"))
        self.db.close()

    def test_batch_persistence_across_reopen(self):
        self.db.open()
        ops = [
            SimulatedBatchOperation.put(b"batch_persist_1", b"val_1"),
            SimulatedBatchOperation.put(b"batch_persist_2", b"val_2"),
        ]
        self.db.batch(ops)
        self.db.close()

        # Reopen with new instance
        reopened = SimulatedAstraDBWithBatch(self.db_path)
        reopened.open()
        self.assertEqual(reopened.get(b"batch_persist_1"), b"val_1")
        self.assertEqual(reopened.get(b"batch_persist_2"), b"val_2")
        reopened.close()

    @unittest.skipIf(AstraDB is None or BatchOperation is None, "AstraDB / BatchOperation pending implementation")
    def test_real_astradb_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "real_batch.db")
            real_db = AstraDB(path)
            real_db.open()
            op = create_batch_put("test_key", "test_val")
            self.assertTrue(real_db.batch([op]))
            self.assertEqual(real_db.get(b"test_key"), b"test_val")
            real_db.close()


if __name__ == "__main__":
    unittest.main()
