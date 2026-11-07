"""
Functional tests for Astra-SIT CLI (cli.py).

Verifies:
- Command parsing for open, get, put, del, batch, dump, check, serve
- Encoding string keys and values as UTF-8
- Decoding bytes safely as UTF-8 (handling invalid UTF-8 without crashing)
- Missing-key behavior (get returning None and del returning 1)
- Batch execution from file and command-line arguments
- Reporting dump() and check() as pending public interfaces
- JSONL serve protocol:
  - PUT and GET
  - GET missing key (NOTFOUND <id>)
  - DELETE existing key (ACK <id>)
  - DELETE missing key (ERROR <id>)
  - BATCH success (ACK <id>)
  - BATCH failure (ERROR <id>)
  - Persistence after closing and reopening
  - Multiple JSONL requests in one process
  - IDs preserved in responses
  - Values containing spaces, quotes, and newlines
  - Malformed JSON handling without crashing
  - Invalid operations handling
  - stdout contains only protocol responses
  - Failed writes never produce success ACK
"""
import unittest
import io
import os
import json
import tempfile
from unittest.mock import MagicMock, patch
import cli
from tests.test_batch import SimulatedBatchOperation, SimulatedAstraDBWithBatch


class TestCLICommands(unittest.TestCase):
    """Test CLI commands and argument handling."""

    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_db.get.return_value = None
        self.mock_db.put.return_value = True
        self.mock_db.delete.return_value = True
        self.mock_db.batch.return_value = True

    @patch("cli.get_db")
    def test_cli_open_command(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with patch("sys.stdout", new_callable=io.StringIO):
            ret = cli.main(["open", "test.db"])
            self.assertEqual(ret, 0)
            self.mock_db.open.assert_called_once()
            self.mock_db.close.assert_called_once()

    @patch("cli.get_db")
    def test_cli_open_reversed_args(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with patch("sys.stdout", new_callable=io.StringIO):
            ret = cli.main(["test.db", "open"])
            self.assertEqual(ret, 0)
            self.mock_db.open.assert_called_once()

    @patch("cli.get_db")
    def test_cli_put_command_success(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["put", "test.db", "my_key", "my_val"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_out.getvalue().strip(), "OK")
        self.mock_db.put.assert_called_once_with(b"my_key", b"my_val")

    @patch("cli.get_db")
    def test_cli_put_command_failure_never_prints_ack_or_ok(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.put.return_value = False
        with patch("sys.stderr", new_callable=io.StringIO) as mock_err:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["put", "test.db", "my_key", "my_val"])
                self.assertEqual(ret, 1)
                self.assertNotIn("OK", mock_out.getvalue())
                self.assertIn("put operation failed durably", mock_err.getvalue())

    @patch("cli.get_db")
    def test_cli_get_existing_key(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.get.return_value = b"my_value"

        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["get", "test.db", "my_key"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_out.getvalue().strip(), "my_value")

        self.mock_db.get.assert_called_once_with(b"my_key")

    @patch("cli.get_db")
    def test_cli_get_nonexistent_key_prints_none(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.get.return_value = None

        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["get", "test.db", "missing"])
            self.assertEqual(ret, 1)
            self.assertEqual(mock_out.getvalue().strip(), "None")

    @patch("cli.get_db")
    def test_cli_get_invalid_utf8_does_not_crash(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.get.return_value = b"\xff\xfe\xaa"

        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["get", "test.db", "binary_key"])
            self.assertEqual(ret, 0)
            self.assertIn("\ufffd", mock_out.getvalue())

    @patch("cli.get_db")
    def test_cli_del_existing_key(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.delete.return_value = True

        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["del", "test.db", "existing_key"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_out.getvalue().strip(), "OK")

        self.mock_db.delete.assert_called_once_with(b"existing_key")

    @patch("cli.get_db")
    def test_cli_del_nonexistent_key_returns_1(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        self.mock_db.delete.return_value = False

        with patch("sys.stderr", new_callable=io.StringIO) as mock_err:
            ret = cli.main(["del", "test.db", "missing_key"])
            self.assertEqual(ret, 1)
            self.assertIn("Key not found", mock_err.getvalue())

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_cli_batch_from_args(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            ret = cli.main(["batch", "test.db", "put", "k1", "v1", "del", "k2"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_out.getvalue().strip(), "OK")

        self.mock_db.batch.assert_called_once()
        ops = self.mock_db.batch.call_args[0][0]
        self.assertEqual(len(ops), 2)
        self.assertEqual(ops[0].op_type, "put")
        self.assertEqual(ops[1].op_type, "delete")

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_cli_batch_from_file_lines(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write("put file_k file_v\ndel file_del_k\n")
            batch_file = f.name

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["batch", "test.db", batch_file])
                self.assertEqual(ret, 0)
                self.assertEqual(mock_out.getvalue().strip(), "OK")
            self.mock_db.batch.assert_called_once()
        finally:
            if os.path.exists(batch_file):
                os.remove(batch_file)

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_cli_batch_from_file_json(self, mock_get_db):
        mock_get_db.return_value = self.mock_db
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write('[{"op": "put", "key": "jk1", "value": "jv1"}, {"op": "del", "key": "jk2"}]')
            batch_file = f.name

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["batch", "test.db", batch_file])
                self.assertEqual(ret, 0)
                self.assertEqual(mock_out.getvalue().strip(), "OK")
            self.mock_db.batch.assert_called_once()
        finally:
            if os.path.exists(batch_file):
                os.remove(batch_file)

    def test_cli_dump_reports_pending_public_interface(self):
        with patch("sys.stderr", new_callable=io.StringIO) as mock_err:
            ret = cli.main(["dump", "test.db"])
            self.assertEqual(ret, 1)
            self.assertIn("dump() public interface is not yet confirmed", mock_err.getvalue())

    def test_cli_check_reports_pending_public_interface(self):
        with patch("sys.stderr", new_callable=io.StringIO) as mock_err:
            ret = cli.main(["check", "test.db"])
            self.assertEqual(ret, 1)
            self.assertIn("check() public interface is not yet confirmed", mock_err.getvalue())


class TestCLIJSONLServeProtocol(unittest.TestCase):
    """Test approved JSONL serve protocol on stdin/stdout."""

    def setUp(self):
        self.db_path = f"test_jsonl_{id(self)}.db"
        self.sim_db = SimulatedAstraDBWithBatch(self.db_path)

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_serve_put_and_get(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        reqs = (
            json.dumps({"id": 1, "op": "put", "key": "name", "value": "Akash"}) + "\n" +
            json.dumps({"id": 2, "op": "get", "key": "name"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ACK 1")
        get_resp = json.loads(lines[1])
        self.assertEqual(get_resp, {"id": 2, "status": "OK", "value": "Akash"})

    @patch("cli.get_db")
    def test_serve_get_missing_key(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        req = json.dumps({"id": 10, "op": "get", "key": "unknown_key"}) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "NOTFOUND 10")

    @patch("cli.get_db")
    def test_serve_delete_existing_key(self, mock_get_db):
        mock_get_db.return_value = self.sim_db
        # Prepopulate key
        self.sim_db.open()
        self.sim_db.put(b"del_key", b"del_val")
        self.sim_db.close()

        req = json.dumps({"id": 20, "op": "delete", "key": "del_key"}) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ACK 20")

    @patch("cli.get_db")
    def test_serve_delete_missing_key(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        req = json.dumps({"id": 21, "op": "delete", "key": "no_such_key"}) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        # Failed delete operation must not produce ACK
        self.assertNotEqual(lines[0], "ACK 21")
        self.assertEqual(lines[0], "ERROR 21")

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_serve_batch_success(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        req = json.dumps({
            "id": 30,
            "op": "batch",
            "operations": [
                {"op": "put", "key": "b1", "value": "v1"},
                {"op": "put", "key": "b2", "value": "v2"},
            ]
        }) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ACK 30")

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_serve_batch_failure(self, mock_get_db):
        # Database batch returns False
        mock_db = MagicMock()
        mock_db.batch.return_value = False
        mock_get_db.return_value = mock_db

        req = json.dumps({
            "id": 31,
            "op": "batch",
            "operations": [{"op": "put", "key": "k", "value": "v"}]
        }) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertNotEqual(lines[0], "ACK 31")
        self.assertEqual(lines[0], "ERROR 31")

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_serve_persistence_after_closing_and_reopening(self, mock_get_db):
        # Run first serve session with PUT
        mock_get_db.return_value = self.sim_db
        req1 = json.dumps({"id": 100, "op": "put", "key": "persist_k", "value": "persist_v"}) + "\n"
        with patch("sys.stdin", io.StringIO(req1)):
            with patch("sys.stdout", new_callable=io.StringIO):
                cli.main(["serve", self.db_path])

        # Run second serve session in new process/instance targeting same path
        new_sim_db = SimulatedAstraDBWithBatch(self.db_path)
        mock_get_db.return_value = new_sim_db
        req2 = json.dumps({"id": 101, "op": "get", "key": "persist_k"}) + "\n"
        with patch("sys.stdin", io.StringIO(req2)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                cli.main(["serve", self.db_path])
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        resp = json.loads(lines[0])
        self.assertEqual(resp, {"id": 101, "status": "OK", "value": "persist_v"})

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_serve_multiple_jsonl_requests_in_one_process(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        reqs = (
            json.dumps({"id": 1, "op": "put", "key": "item1", "value": "val1"}) + "\n" +
            json.dumps({"id": 2, "op": "put", "key": "item2", "value": "val2"}) + "\n" +
            json.dumps({"id": 3, "op": "get", "key": "item1"}) + "\n" +
            json.dumps({"id": 4, "op": "delete", "key": "item2"}) + "\n" +
            json.dumps({"id": 5, "op": "get", "key": "item2"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ACK 1")
        self.assertEqual(lines[1], "ACK 2")
        self.assertEqual(json.loads(lines[2]), {"id": 3, "status": "OK", "value": "val1"})
        self.assertEqual(lines[3], "ACK 4")
        self.assertEqual(lines[4], "NOTFOUND 5")

    @patch("cli.get_db")
    def test_serve_ids_preserved_in_responses(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        # Test various ID forms: int, string, uuid-style
        reqs = (
            json.dumps({"id": 99999, "op": "get", "key": "none"}) + "\n" +
            json.dumps({"id": "req-uuid-1234", "op": "get", "key": "none"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "NOTFOUND 99999")
        self.assertEqual(lines[1], "NOTFOUND req-uuid-1234")

    @patch("cli.get_db")
    def test_serve_values_containing_spaces_quotes_and_newlines(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        complex_val = 'Line 1 with spaces\nLine 2 with "quotes" and \'apostrophes\'\n\tTabbed line 3'
        reqs = (
            json.dumps({"id": 50, "op": "put", "key": "complex_k", "value": complex_val}) + "\n" +
            json.dumps({"id": 51, "op": "get", "key": "complex_k"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ACK 50")
        resp = json.loads(lines[1])
        self.assertEqual(resp["id"], 51)
        self.assertEqual(resp["status"], "OK")
        self.assertEqual(resp["value"], complex_val)

    @patch("cli.get_db")
    def test_serve_malformed_json_and_process_continues(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        reqs = (
            '{"id": 60, "op": "broken json\n' +
            json.dumps({"id": 61, "op": "put", "key": "k", "value": "v"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                with patch("sys.stderr", new_callable=io.StringIO) as mock_err:
                    ret = cli.main(["serve", self.db_path])
                    self.assertEqual(ret, 0)
                    lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]
                    self.assertIn("Malformed JSON", mock_err.getvalue())

        self.assertEqual(lines[0], "ERROR 60")
        self.assertEqual(lines[1], "ACK 61")

    @patch("cli.get_db")
    def test_serve_invalid_operations(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        req = json.dumps({"id": 70, "op": "unknown_operation", "key": "k"}) + "\n"
        with patch("sys.stdin", io.StringIO(req)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                with patch("sys.stderr", new_callable=io.StringIO):
                    ret = cli.main(["serve", self.db_path])
                    self.assertEqual(ret, 0)
                    lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertEqual(lines[0], "ERROR 70")

    @patch("cli.get_db")
    def test_serve_stdout_contains_only_protocol_responses(self, mock_get_db):
        mock_get_db.return_value = self.sim_db

        reqs = (
            json.dumps({"id": 1, "op": "put", "key": "clean_k", "value": "clean_v"}) + "\n" +
            json.dumps({"id": 2, "op": "get", "key": "clean_k"}) + "\n" +
            json.dumps({"id": 3, "op": "delete", "key": "clean_k"}) + "\n" +
            json.dumps({"id": 4, "op": "get", "key": "clean_k"}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                raw_stdout = mock_out.getvalue()

        # Every single non-empty line must match either ACK <id>, NOTFOUND <id>, ERROR <id>, or a JSON object
        for line in raw_stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            is_valid_protocol = (
                line.startswith("ACK ") or
                line.startswith("NOTFOUND ") or
                line.startswith("ERROR ") or
                (line.startswith("{") and line.endswith("}"))
            )
            self.assertTrue(is_valid_protocol, f"Non-protocol output leaked to stdout: {line}")

    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_failed_writes_never_produce_success_ack(self, mock_get_db):
        # Simulate database engine failing write
        failing_db = MagicMock()
        failing_db.put.return_value = False
        failing_db.delete.return_value = False
        failing_db.batch.return_value = False
        mock_get_db.return_value = failing_db

        reqs = (
            json.dumps({"id": 10, "op": "put", "key": "k", "value": "v"}) + "\n" +
            json.dumps({"id": 20, "op": "delete", "key": "k"}) + "\n" +
            json.dumps({"id": 30, "op": "batch", "operations": [{"op": "put", "key": "k", "value": "v"}]}) + "\n"
        )
        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [l.strip() for l in mock_out.getvalue().splitlines() if l.strip()]

        self.assertNotIn("ACK 10", lines)
        self.assertNotIn("ACK 20", lines)
        self.assertNotIn("ACK 30", lines)
        self.assertEqual(lines, ["ERROR 10", "ERROR 20", "ERROR 30"])


    @patch("cli.BatchOperation", SimulatedBatchOperation)
    @patch("cli.get_db")
    def test_write_exceptions_never_produce_success_ack(self, mock_get_db):
        # Simulate durability/I/O failures raised by the database engine.
        failing_db = MagicMock()
        failing_db.put.side_effect = OSError("simulated fsync failure")
        failing_db.delete.side_effect = OSError("simulated fsync failure")
        failing_db.batch.side_effect = OSError("simulated fsync failure")
        mock_get_db.return_value = failing_db

        reqs = (
            json.dumps({"id": 40, "op": "put", "key": "k", "value": "v"}) + "\n" +
            json.dumps({"id": 50, "op": "delete", "key": "k"}) + "\n" +
            json.dumps({
                "id": 60,
                "op": "batch",
                "operations": [{"op": "put", "key": "k", "value": "v"}]
            }) + "\n"
        )

        with patch("sys.stdin", io.StringIO(reqs)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                ret = cli.main(["serve", self.db_path])
                self.assertEqual(ret, 0)
                lines = [
                    line.strip()
                    for line in mock_out.getvalue().splitlines()
                    if line.strip()
                ]

        self.assertEqual(lines, ["ERROR 40", "ERROR 50", "ERROR 60"])
        self.assertNotIn("ACK 40", lines)
        self.assertNotIn("ACK 50", lines)
        self.assertNotIn("ACK 60", lines)

if __name__ == "__main__":
    unittest.main()
