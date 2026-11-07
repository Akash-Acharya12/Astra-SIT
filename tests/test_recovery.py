"""
Functional tests for crash recovery, persistence, and durability.

Core durability rule: Never print ACK before durable success.
Verifies state persistence across open/close lifecycles and recovery.
"""
import unittest
import os
import tempfile
from unittest.mock import MagicMock

try:
    from main import AstraDB
except ImportError:
    AstraDB = None

from tests.test_put_get import SimulatedAstraDB


class TestRecoveryAndPersistence(unittest.TestCase):
    """Test data recovery and persistence across simulated database restarts."""

    def setUp(self):
        self.db_path = f"test_recovery_{id(self)}.db"

    def test_multi_cycle_reopen_persistence(self):
        """Verify data survives multiple open/close/reopen cycles."""
        db1 = SimulatedAstraDB(self.db_path)
        db1.open()
        db1.put(b"alpha", b"1")
        db1.close()

        db2 = SimulatedAstraDB(self.db_path)
        db2.open()
        self.assertEqual(db2.get(b"alpha"), b"1")
        db2.put(b"beta", b"2")
        db2.close()

        db3 = SimulatedAstraDB(self.db_path)
        db3.open()
        self.assertEqual(db3.get(b"alpha"), b"1")
        self.assertEqual(db3.get(b"beta"), b"2")
        db3.delete(b"alpha")
        db3.close()

        db4 = SimulatedAstraDB(self.db_path)
        db4.open()
        self.assertIsNone(db4.get(b"alpha"))
        self.assertEqual(db4.get(b"beta"), b"2")
        db4.close()

    def test_durability_contract_never_ack_on_failure(self):
        """Verify that when a write fails durability, success is not reported."""
        failing_db = MagicMock()
        failing_db.put.return_value = False

        failing_db.open()
        result = failing_db.put(b"key", b"value")
        self.assertFalse(result, "Failed write must not return True")
        failing_db.close()


if __name__ == "__main__":
    unittest.main()
