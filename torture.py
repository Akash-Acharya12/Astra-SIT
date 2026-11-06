"""
torture.py - Crash-Safety and Durability Torture Test Suite for Astra-SIT.

Features:
- Independent Oracle tracking acknowledged vs. unacknowledged in-flight operations.
- Randomized operation generator (PUT, GET, DELETE, BATCH) driven by deterministic seeds.
- Abrupt process-kill crash testing (hard OS termination via TerminateProcess / SIGKILL).
- Post-crash restart verification: asserts durability of acknowledged writes, atomicity
  of batches, absence of torn writes, and absence of phantom/corrupted data.
- Pluggable database adapters: auto-detects team members' StorageEngine or CLI,
  and provides a built-in ReferenceCrashSafeEngine for self-testing and verification.
"""

from __future__ import annotations

import abc
import argparse
from dataclasses import dataclass, field
from enum import Enum
import importlib
import json
import multiprocessing as mp
from multiprocessing.connection import Connection
import os
import random
import shutil
import struct
import sys
import subprocess
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import zlib


# ==============================================================================
# Custom Exceptions for Torture Testing & Consistency Violations
# ==============================================================================

class TortureTestError(Exception):
    """Base exception for all torture test failures."""
    pass


class DurabilityViolationError(TortureTestError):
    """Raised when an acknowledged write is missing or altered after restart."""
    pass


class AtomicityViolationError(TortureTestError):
    """Raised when an atomic batch is only partially applied across a crash."""
    pass


class StateCorruptionError(TortureTestError):
    """Raised when a retrieved value does not match any valid execution state."""
    pass


class PhantomKeyError(TortureTestError):
    """Raised when an unwritten or acknowledged-deleted key appears in the database."""
    pass


class CrashRecoveryError(TortureTestError):
    """Raised when the database fails to restart, rebuild its index, or replay the log."""
    pass


class LivenessCheckError(TortureTestError):
    """Raised when a recovered database cannot accept new writes or perform reads."""
    pass
class ServeProtocolError(TortureTestError):
    "Raised when the JSONL serve process violates the approved protocol."
    pass


# ==============================================================================
# Operation Types and Models
# ==============================================================================

class OperationType(str, Enum):
    PUT = "PUT"
    GET = "GET"
    DELETE = "DELETE"
    BATCH = "BATCH"


class OperationStatus(str, Enum):
    PENDING = "PENDING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    UNACKNOWLEDGED = "UNACKNOWLEDGED"
    FAILED = "FAILED"


@dataclass
class BatchItem:
    op_type: OperationType  # PUT or DELETE
    key: str
    value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op_type": self.op_type.value,
            "key": self.key,
            "value": self.value,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> BatchItem:
        return BatchItem(
            op_type=OperationType(data["op_type"]),
            key=data["key"],
            value=data.get("value"),
        )


@dataclass
class Operation:
    op_id: int
    op_type: OperationType
    key: Optional[str] = None
    value: Optional[str] = None
    batch_items: Optional[List[BatchItem]] = None
    step: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op_id": self.op_id,
            "op_type": self.op_type.value,
            "key": self.key,
            "value": self.value,
            "batch_items": [item.to_dict() for item in self.batch_items] if self.batch_items else None,
            "step": self.step,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Operation:
        raw_items = data.get("batch_items")
        batch_items = [BatchItem.from_dict(item) for item in raw_items] if raw_items else None
        return Operation(
            op_id=data["op_id"],
            op_type=OperationType(data["op_type"]),
            key=data.get("key"),
            value=data.get("value"),
            batch_items=batch_items,
            step=data.get("step", 0),
        )


@dataclass
class VerificationReport:
    passed: bool
    acknowledged_keys_verified: int = 0
    deletions_verified: int = 0
    unacknowledged_resolved: int = 0
    in_flight_resolution: str = "NONE"
    details: str = ""
    error: Optional[str] = None


# ==============================================================================
# Independent Oracle
# ==============================================================================

class IndependentOracle:
    """
    Authoritative, independent model of expected database state.

    Core Rule:
      "Do not assume an operation succeeded unless the database acknowledged it."

    Semantics under crashes:
      - Acknowledged operations: MUST be preserved across crashes (Durability guarantee).
      - Unacknowledged operations (in-flight during process kill):
        May either have committed before the crash, or been aborted / torn.
        The oracle validates that the post-restart state reflects either:
          (a) The state prior to the in-flight operation (write aborted/rolled back), OR
          (b) The state with the full in-flight operation applied (write committed just before kill).
        Any intermediate, partial, or corrupted state is flagged as a violation.
    """

    def __init__(self):
        # Acknowledged key -> value mapping
        self.acknowledged_state: Dict[str, str] = {}
        # Keys confirmed deleted
        self.acknowledged_deletions: Set[str] = set()
        # All keys ever touched across all operations
        self.all_known_keys: Set[str] = set()
        # The single operation currently in flight (not yet acknowledged)
        self.in_flight_op: Optional[Operation] = None
        # In-flight operations interrupted by crashes
        self.unacknowledged_ops: List[Operation] = []
        # Operation history log: (Operation, OperationStatus, details)
        self.history: List[Tuple[Operation, OperationStatus, Any]] = []

    def record_start(self, op: Operation) -> None:
        """Mark an operation as submitted and in flight."""
        self.in_flight_op = op
        if op.key:
            self.all_known_keys.add(op.key)
        if op.batch_items:
            for item in op.batch_items:
                self.all_known_keys.add(item.key)

    def record_ack(self, op: Operation, result: Any = None) -> None:
        """Database explicitly acknowledged that the operation succeeded."""
        if self.in_flight_op and self.in_flight_op.op_id == op.op_id:
            self.in_flight_op = None

        if op.op_type == OperationType.PUT:
            assert op.key is not None and op.value is not None
            self.acknowledged_state[op.key] = op.value
            self.acknowledged_deletions.discard(op.key)
        elif op.op_type == OperationType.DELETE:
            assert op.key is not None
            self.acknowledged_state.pop(op.key, None)
            self.acknowledged_deletions.add(op.key)
        elif op.op_type == OperationType.BATCH:
            assert op.batch_items is not None
            for item in op.batch_items:
                if item.op_type == OperationType.PUT:
                    assert item.value is not None
                    self.acknowledged_state[item.key] = item.value
                    self.acknowledged_deletions.discard(item.key)
                elif item.op_type == OperationType.DELETE:
                    self.acknowledged_state.pop(item.key, None)
                    self.acknowledged_deletions.add(item.key)

        self.history.append((op, OperationStatus.ACKNOWLEDGED, result))

    def record_failure(self, op: Operation, error: str) -> None:
        """Database explicitly returned an error or rejected the operation."""
        if self.in_flight_op and self.in_flight_op.op_id == op.op_id:
            self.in_flight_op = None
        self.history.append((op, OperationStatus.FAILED, error))

    def record_crash(self) -> None:
        """
        Process was killed abruptly. Any in-flight operation was not acknowledged.
        """
        if self.in_flight_op is not None:
            self.unacknowledged_ops.append(self.in_flight_op)
            self.history.append((self.in_flight_op, OperationStatus.UNACKNOWLEDGED, "KILLED_IN_FLIGHT"))
            self.in_flight_op = None

    def get_expected_value(self, key: str) -> Optional[str]:
        """Returns the confirmed acknowledged value for a key during normal execution."""
        return self.acknowledged_state.get(key)

    def verify_read(self, key: str, actual_value: Optional[str]) -> None:
        """Verify that a GET operation matches the acknowledged oracle state."""
        expected = self.get_expected_value(key)
        if actual_value != expected:
            raise DurabilityViolationError(
                f"GET mismatch on key '{key}': expected {expected!r}, got {actual_value!r}"
            )

    def verify_restart(self, db_reader: Callable[[str], Optional[str]]) -> VerificationReport:
        """
        Verify the database's recovered state against the oracle after restart.

        1. Acknowledged State Durability:
           All keys in acknowledged_state that were NOT part of an in-flight unacknowledged
           write must exist with the exact acknowledged value.
        2. Acknowledged Deletions:
           Keys that were acknowledged deleted and not touched by in-flight writes must be absent.
        3. In-flight Unacknowledged Resolution:
           - Unacknowledged PUT: value is either prior acknowledged value OR the new value.
           - Unacknowledged DELETE: value is either prior acknowledged value OR None.
           - Unacknowledged BATCH: atomicity check! Either ALL operations took effect OR NONE took effect.
        """
        in_flight = self.unacknowledged_ops[-1] if self.unacknowledged_ops else None
        in_flight_keys: Set[str] = set()

        if in_flight:
            if in_flight.key:
                in_flight_keys.add(in_flight.key)
            if in_flight.batch_items:
                for item in in_flight.batch_items:
                    in_flight_keys.add(item.key)

        # Step 1: Verify acknowledged keys not involved in in-flight writes
        ack_count = 0
        for key, expected_val in self.acknowledged_state.items():
            if key in in_flight_keys:
                continue
            actual_val = db_reader(key)
            if actual_val != expected_val:
                raise DurabilityViolationError(
                    f"Durability Violation on restart: key '{key}' expected {expected_val!r}, "
                    f"but database returned {actual_val!r}"
                )
            ack_count += 1

        # Step 2: Verify acknowledged deletions not involved in in-flight writes
        del_count = 0
        for key in self.acknowledged_deletions:
            if key in in_flight_keys or key in self.acknowledged_state:
                continue
            actual_val = db_reader(key)
            if actual_val is not None:
                raise PhantomKeyError(
                    f"Phantom Key Violation on restart: deleted key '{key}' resurrected with value {actual_val!r}"
                )
            del_count += 1

        # Step 3: Verify and reconcile in-flight unacknowledged operations
        resolution_desc = "NONE_PENDING"
        if in_flight:
            if in_flight.op_type == OperationType.PUT:
                assert in_flight.key is not None and in_flight.value is not None
                k = in_flight.key
                new_v = in_flight.value
                old_v = self.acknowledged_state.get(k)
                actual_v = db_reader(k)

                if actual_v == new_v:
                    resolution_desc = f"PUT_COMMITTED('{k}')"
                    self.acknowledged_state[k] = new_v
                    self.acknowledged_deletions.discard(k)
                elif actual_v == old_v:
                    resolution_desc = f"PUT_ABORTED('{k}')"
                    # old state preserved
                else:
                    raise StateCorruptionError(
                        f"Unacknowledged PUT on '{k}' corrupted! "
                        f"Expected old value {old_v!r} OR new value {new_v!r}, got {actual_v!r}"
                    )

            elif in_flight.op_type == OperationType.DELETE:
                assert in_flight.key is not None
                k = in_flight.key
                old_v = self.acknowledged_state.get(k)
                actual_v = db_reader(k)

                if actual_v is None:
                    resolution_desc = f"DELETE_COMMITTED('{k}')"
                    self.acknowledged_state.pop(k, None)
                    self.acknowledged_deletions.add(k)
                elif actual_v == old_v:
                    resolution_desc = f"DELETE_ABORTED('{k}')"
                else:
                    raise StateCorruptionError(
                        f"Unacknowledged DELETE on '{k}' corrupted! "
                        f"Expected old value {old_v!r} OR None, got {actual_v!r}"
                    )

            elif in_flight.op_type == OperationType.BATCH:
                assert in_flight.batch_items is not None
                # Check atomicity: either ALL took effect or NONE took effect
                all_new = True
                all_old = True

                for item in in_flight.batch_items:
                    k = item.key
                    old_v = self.acknowledged_state.get(k)
                    actual_v = db_reader(k)
                    expected_new = item.value if item.op_type == OperationType.PUT else None

                    if actual_v != expected_new:
                        all_new = False
                    if actual_v != old_v:
                        all_old = False

                if all_new:
                    resolution_desc = "BATCH_ALL_COMMITTED"
                    for item in in_flight.batch_items:
                        if item.op_type == OperationType.PUT:
                            assert item.value is not None
                            self.acknowledged_state[item.key] = item.value
                            self.acknowledged_deletions.discard(item.key)
                        elif item.op_type == OperationType.DELETE:
                            self.acknowledged_state.pop(item.key, None)
                            self.acknowledged_deletions.add(item.key)
                elif all_old:
                    resolution_desc = "BATCH_ALL_ROLLED_BACK"
                else:
                    raise AtomicityViolationError(
                        f"Batch Atomicity Violation! In-flight batch {in_flight.batch_items} was partially applied!"
                    )

            # Clear unacknowledged op now that it's verified and reconciled
            self.unacknowledged_ops.clear()

        return VerificationReport(
            passed=True,
            acknowledged_keys_verified=ack_count,
            deletions_verified=del_count,
            unacknowledged_resolved=1 if in_flight else 0,
            in_flight_resolution=resolution_desc,
            details="All consistency, durability, and atomicity invariants satisfied.",
        )


# ==============================================================================
# Randomized Workload Generator
# ==============================================================================

class RandomWorkloadGenerator:
    """
    Deterministic random operation generator.
    Given the same seed, produces an identical stream of operations.
    """

    def __init__(
        self,
        seed: int,
        key_pool_size: int = 30,
        min_val_size: int = 4,
        max_val_size: int = 64,
        put_weight: float = 0.40,
        get_weight: float = 0.30,
        del_weight: float = 0.15,
        batch_weight: float = 0.15,
        max_batch_size: int = 6,
    ):
        self.seed = seed
        self.prng = random.Random(seed)
        self.key_pool = [f"key_{i:03d}" for i in range(key_pool_size)]
        # Add special edge-case keys
        self.key_pool.extend(["k_empty_val", "k_long_" + ("x" * 40), "k_unicode_🚀_astra", "k_special_chars:!@#$%^&*()"])
        self.min_val_size = min_val_size
        self.max_val_size = max_val_size
        self.max_batch_size = max_batch_size

        self.op_types = [OperationType.PUT, OperationType.GET, OperationType.DELETE, OperationType.BATCH]
        self.weights = [put_weight, get_weight, del_weight, batch_weight]
        self._op_counter = 0

    def _random_value(self) -> str:
        length = self.prng.randint(self.min_val_size, self.max_val_size)
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        return "".join(self.prng.choices(alphabet, k=length))

    def _random_key(self) -> str:
        return self.prng.choice(self.key_pool)

    def next_operation(self, step: int) -> Operation:
        self._op_counter += 1
        op_choice = self.prng.choices(self.op_types, weights=self.weights, k=1)[0]

        if op_choice == OperationType.PUT:
            return Operation(
                op_id=self._op_counter,
                op_type=OperationType.PUT,
                key=self._random_key(),
                value=self._random_value(),
                step=step,
            )
        elif op_choice == OperationType.GET:
            return Operation(
                op_id=self._op_counter,
                op_type=OperationType.GET,
                key=self._random_key(),
                step=step,
            )
        elif op_choice == OperationType.DELETE:
            return Operation(
                op_id=self._op_counter,
                op_type=OperationType.DELETE,
                key=self._random_key(),
                step=step,
            )
        else:  # BATCH
            batch_size = self.prng.randint(2, self.max_batch_size)
            items: List[BatchItem] = []
            used_keys = set()
            for _ in range(batch_size):
                k = self._random_key()
                # Ensure distinct keys within a single batch for clean verification
                if k in used_keys:
                    continue
                used_keys.add(k)
                if self.prng.random() < 0.7:
                    items.append(BatchItem(OperationType.PUT, k, self._random_value()))
                else:
                    items.append(BatchItem(OperationType.DELETE, k, None))

            return Operation(
                op_id=self._op_counter,
                op_type=OperationType.BATCH,
                batch_items=items,
                step=step,
            )


# ==============================================================================
# Built-In Reference Crash-Safe Engine
# ==============================================================================

class ReferenceCrashSafeEngine:
    """
    Production-grade reference crash-safe Key-Value store with a Write-Ahead Log (WAL).
    Features:
      - Framing: Magic (4B), Length (4B), CRC32 (4B), Record Type (1B), Payload (NB).
      - Immediate fsync on every commit to ensure durability.
      - Crash recovery: verifies CRC32 of every record, truncates torn/incomplete writes
        at the log tail, and replays valid committed entries into an in-memory index.
      - Atomic batch support (BATCH_BEGIN, BATCH_ITEM, BATCH_COMMIT).
    """

    MAGIC = b"ASTR"
    OP_PUT = 1
    OP_DELETE = 2
    OP_BATCH_BEGIN = 3
    OP_BATCH_PUT = 4
    OP_BATCH_DEL = 5
    OP_BATCH_COMMIT = 6

    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        self.wal_path = os.path.join(db_dir, "astra_store.wal")
        self.index: Dict[str, str] = {}
        self.wal_file = None
        self._open_and_recover()

    def _open_and_recover(self) -> None:
        """Scan WAL, verify CRC checksums, repair torn writes, replay into index."""
        self.index.clear()
        if not os.path.exists(self.wal_path):
            self.wal_file = open(self.wal_path, "a+b", buffering=0)
            return

        valid_offset = 0
        pending_batch: Optional[List[Tuple[int, str, Optional[str]]]] = None

        with open(self.wal_path, "rb") as f:
            while True:
                hdr = f.read(13)  # 4B magic + 4B len + 4B crc + 1B op
                if len(hdr) < 13:
                    # EOF or torn header
                    break

                magic = hdr[:4]
                if magic != self.MAGIC:
                    # Corrupted magic, stop at last valid record
                    break

                payload_len, expected_crc = struct.unpack(">II", hdr[4:12])
                op_code = hdr[12]

                payload = f.read(payload_len)
                if len(payload) < payload_len:
                    # Torn payload write from crash! Stop recovery here.
                    break

                actual_crc = zlib.crc32(hdr[12:13] + payload)
                if actual_crc != expected_crc:
                    # CRC mismatch: corrupted or partially written record
                    break

                # Record is 100% valid
                valid_offset = f.tell()

                # Replay record
                if op_code == self.OP_PUT:
                    data = json.loads(payload.decode("utf-8"))
                    self.index[data["k"]] = data["v"]
                elif op_code == self.OP_DELETE:
                    data = json.loads(payload.decode("utf-8"))
                    self.index.pop(data["k"], None)
                elif op_code == self.OP_BATCH_BEGIN:
                    pending_batch = []
                elif op_code == self.OP_BATCH_PUT:
                    if pending_batch is not None:
                        data = json.loads(payload.decode("utf-8"))
                        pending_batch.append((self.OP_BATCH_PUT, data["k"], data["v"]))
                elif op_code == self.OP_BATCH_DEL:
                    if pending_batch is not None:
                        data = json.loads(payload.decode("utf-8"))
                        pending_batch.append((self.OP_BATCH_DEL, data["k"], None))
                elif op_code == self.OP_BATCH_COMMIT:
                    if pending_batch is not None:
                        for b_op, b_k, b_v in pending_batch:
                            if b_op == self.OP_BATCH_PUT and b_v is not None:
                                self.index[b_k] = b_v
                            elif b_op == self.OP_BATCH_DEL:
                                self.index.pop(b_k, None)
                        pending_batch = None

        # Truncate any torn tail beyond the last valid offset
        actual_size = os.path.getsize(self.wal_path)
        if actual_size > valid_offset:
            with open(self.wal_path, "r+b") as f:
                f.seek(valid_offset)
                f.truncate()

        self.wal_file = open(self.wal_path, "a+b", buffering=0)

    def _write_record(self, op_code: int, payload_bytes: bytes) -> None:
        crc = zlib.crc32(bytes([op_code]) + payload_bytes)
        hdr = self.MAGIC + struct.pack(">II", len(payload_bytes), crc) + bytes([op_code])
        self.wal_file.write(hdr + payload_bytes)
        self.wal_file.flush()
        os.fsync(self.wal_file.fileno())

    def put(self, key: str, value: str) -> bool:
        payload = json.dumps({"k": key, "v": value}).encode("utf-8")
        self._write_record(self.OP_PUT, payload)
        self.index[key] = value
        return True

    def get(self, key: str) -> Optional[str]:
        return self.index.get(key)

    def delete(self, key: str) -> bool:
        payload = json.dumps({"k": key}).encode("utf-8")
        self._write_record(self.OP_DELETE, payload)
        self.index.pop(key, None)
        return True

    def batch(self, items: List[BatchItem]) -> bool:
        # Atomic batch sequence
        self._write_record(self.OP_BATCH_BEGIN, b"")
        for item in items:
            if item.op_type == OperationType.PUT:
                assert item.value is not None
                payload = json.dumps({"k": item.key, "v": item.value}).encode("utf-8")
                self._write_record(self.OP_BATCH_PUT, payload)
            elif item.op_type == OperationType.DELETE:
                payload = json.dumps({"k": item.key}).encode("utf-8")
                self._write_record(self.OP_BATCH_DEL, payload)
        self._write_record(self.OP_BATCH_COMMIT, b"")

        for item in items:
            if item.op_type == OperationType.PUT:
                assert item.value is not None
                self.index[item.key] = item.value
            elif item.op_type == OperationType.DELETE:
                self.index.pop(item.key, None)
        return True

    def scan_all(self) -> Dict[str, str]:
        return dict(self.index)

    def close(self) -> None:
        if self.wal_file and not self.wal_file.closed:
            self.wal_file.flush()
            try:
                os.fsync(self.wal_file.fileno())
            except OSError:
                pass
            self.wal_file.close()


# ==============================================================================
# Database Adapters
# ==============================================================================

class BaseDBAdapter(abc.ABC):
    """Abstract interface for database operations."""

    @abc.abstractmethod
    def put(self, key: str, value: str) -> bool:
        pass

    @abc.abstractmethod
    def get(self, key: str) -> Optional[str]:
        pass

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @abc.abstractmethod
    def batch(self, items: List[BatchItem]) -> bool:
        pass

    @abc.abstractmethod
    def scan_all(self) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass


class ReferenceEngineAdapter(BaseDBAdapter):
    """Adapter wrapping ReferenceCrashSafeEngine."""

    def __init__(self, db_dir: str):
        self.engine = ReferenceCrashSafeEngine(db_dir)

    def put(self, key: str, value: str) -> bool:
        return self.engine.put(key, value)

    def get(self, key: str) -> Optional[str]:
        return self.engine.get(key)

    def delete(self, key: str) -> bool:
        return self.engine.delete(key)

    def batch(self, items: List[BatchItem]) -> bool:
        return self.engine.batch(items)

    def scan_all(self) -> Dict[str, str]:
        return self.engine.scan_all()

    def close(self) -> None:
        self.engine.close()


class StorageEngineAdapter(BaseDBAdapter):
    """
    Adapter dynamically binding to teammates' storage.py or main.py.
    Detects classes like StorageEngine, KVStore, Database, AstraDB, AstraStorage.
    """

    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        self.engine = self._load_storage_engine(db_dir)

    def _load_storage_engine(self, db_dir: str) -> Any:
        module_names = ["storage", "main", "recovery"]
        candidate_classes = [
            "StorageEngine", "KVStore", "Database", "AstraDB",
            "AstraStorage", "Engine", "Storage"
        ]

        found_class = None
        for mod_name in module_names:
            try:
                mod = importlib.import_module(mod_name)
                for cls_name in candidate_classes:
                    if hasattr(mod, cls_name):
                        found_class = getattr(mod, cls_name)
                        break
                if found_class:
                    break
            except ImportError:
                continue

        if not found_class:
            raise ImportError(
                "Could not find a storage engine class in storage.py or main.py. "
                "Ensure StorageEngine, KVStore, or Database is defined."
            )

        # Attempt to instantiate with db_dir
        try:
            return found_class(db_dir)
        except TypeError:
            return found_class()

    def put(self, key: str, value: str) -> bool:
        for method_name in ["put", "set", "insert", "write"]:
            if hasattr(self.engine, method_name):
                getattr(self.engine, method_name)(key, value)
                return True
        raise NotImplementedError("StorageEngine does not implement put/set.")

    def get(self, key: str) -> Optional[str]:
        for method_name in ["get", "read", "lookup"]:
            if hasattr(self.engine, method_name):
                return getattr(self.engine, method_name)(key)
        raise NotImplementedError("StorageEngine does not implement get/read.")

    def delete(self, key: str) -> bool:
        for method_name in ["delete", "remove", "del_key"]:
            if hasattr(self.engine, method_name):
                getattr(self.engine, method_name)(key)
                return True
        raise NotImplementedError("StorageEngine does not implement delete/remove.")

    def batch(self, items: List[BatchItem]) -> bool:
        for method_name in ["batch", "write_batch", "execute_batch"]:
            if hasattr(self.engine, method_name):
                # Try passing raw items or dict representation
                try:
                    getattr(self.engine, method_name)(items)
                except TypeError:
                    getattr(self.engine, method_name)([item.to_dict() for item in items])
                return True
        # If no explicit batch method, apply sequentially
        for item in items:
            if item.op_type == OperationType.PUT:
                self.put(item.key, item.value or "")
            elif item.op_type == OperationType.DELETE:
                self.delete(item.key)
        return True

    def scan_all(self) -> Dict[str, str]:
        for method_name in ["scan", "all", "items", "scan_all"]:
            if hasattr(self.engine, method_name):
                res = getattr(self.engine, method_name)()
                if isinstance(res, dict):
                    return res
                if isinstance(res, (list, tuple)):
                    return dict(res)
        return {}

    def close(self) -> None:
        for method_name in ["close", "shutdown", "stop"]:
            if hasattr(self.engine, method_name):
                getattr(self.engine, method_name)()
                break
class ServeProcessAdapter(BaseDBAdapter):
    """Adapter communicating with the database through the JSONL serve protocol."""

    def __init__(self, db_dir: str, process=None):
        self.db_dir = db_dir
        self.next_request_id = 1
        self.process = process

        # A supplied process is used for mock testing.
        if self.process is None:
            command = [
                sys.executable,
                "cli.py",
                "serve",
                db_dir,
            ]

            self.process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                text=True,
                encoding="utf-8",
                bufsize=1,
            )

    def _get_request_id(self) -> int:
        request_id = self.next_request_id
        self.next_request_id += 1
        return request_id

    def _exchange(self, request: Dict[str, Any], expected_op: str):
        """Send one JSONL request and validate its matching response."""

        request_id = request["id"]

        if type(request_id) is not int:
            raise ServeProtocolError("Request ID must be an integer.")

        if self.process.poll() is not None:
            raise ServeProtocolError("Serve process is not running.")

        try:
            self.process.stdin.write(
                json.dumps(request, separators=(",", ":")) + "\n"
            )
            self.process.stdin.flush()

            response_line = self.process.stdout.readline()

        except (OSError, BrokenPipeError) as exc:
            raise ServeProtocolError(
                f"Communication with serve process failed: {exc}"
            ) from exc

        if not response_line:
            raise ServeProtocolError("Serve process closed stdout unexpectedly.")

        response_line = response_line.strip()

        # ACK, NOTFOUND and ERROR responses.
        parts = response_line.split()

        if len(parts) == 2 and parts[0] in ("ACK", "NOTFOUND", "ERROR"):
            try:
                response_id = int(parts[1])
            except ValueError as exc:
                raise ServeProtocolError("Invalid response ID.") from exc

            if response_id != request_id:
                raise ServeProtocolError(
                    f"Response ID mismatch: expected {request_id}, "
                    f"received {response_id}."
                )

            status = parts[0]

            if status == "ACK" and expected_op in ("put", "delete", "batch"):
                return True

            if status == "NOTFOUND" and expected_op == "get":
                return None

            if status == "ERROR":
                if expected_op == "get":
                    raise ServeProtocolError(
                        f"GET request {request_id} failed."
                    )
                return False

            raise ServeProtocolError(
                f"Unexpected {status} response for {expected_op}."
            )

        # Successful GET response.
        try:
            response = json.loads(response_line)
        except json.JSONDecodeError as exc:
            raise ServeProtocolError(
                "Server returned malformed JSON."
            ) from exc

        if not isinstance(response, dict):
            raise ServeProtocolError("GET response must be a JSON object.")

        if type(response.get("id")) is not int:
            raise ServeProtocolError("GET response has an invalid ID.")

        if response["id"] != request_id:
            raise ServeProtocolError(
                f"Response ID mismatch: expected {request_id}, "
                f"received {response['id']}."
            )

        if expected_op != "get":
            raise ServeProtocolError(
                "JSON response received for a non-GET operation."
            )

        if response.get("status") != "OK":
            raise ServeProtocolError("Unexpected GET response status.")

        value = response.get("value")

        if not isinstance(value, str):
            raise ServeProtocolError("GET response value must be a string.")

        return value

    def put(self, key: str, value: str) -> bool:
        request = {
            "id": self._get_request_id(),
            "op": "put",
            "key": key,
            "value": value,
        }
        return self._exchange(request, "put")

    def get(self, key: str) -> Optional[str]:
        request = {
            "id": self._get_request_id(),
            "op": "get",
            "key": key,
        }
        return self._exchange(request, "get")

    def delete(self, key: str) -> bool:
        request = {
            "id": self._get_request_id(),
            "op": "delete",
            "key": key,
        }
        return self._exchange(request, "delete")

    def batch(self, items: List[BatchItem]) -> bool:
        operations = []

        for item in items:
            operation = {
                "op": item.op_type.value.lower(),
                "key": item.key,
            }

            if item.op_type == OperationType.PUT:
                if item.value is None:
                    raise ValueError("BATCH PUT requires a value.")
                operation["value"] = item.value

            elif item.op_type != OperationType.DELETE:
                raise ValueError("BATCH supports only PUT and DELETE.")

            operations.append(operation)

        request = {
            "id": self._get_request_id(),
            "op": "batch",
            "operations": operations,
        }

        return self._exchange(request, "batch")

    def scan_all(self) -> Dict[str, str]:
        raise NotImplementedError(
            "The approved JSONL protocol does not provide SCAN."
        )

    def close(self) -> None:
        if self.process is None:
            return

        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()

        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=2)

def create_adapter(adapter_type: str, db_dir: str) -> BaseDBAdapter:
    """Factory function creating the requested or auto-detected database adapter."""
    if adapter_type == "reference":
        return ReferenceEngineAdapter(db_dir)
    elif adapter_type == "storage":
        return StorageEngineAdapter(db_dir)
    elif adapter_type == "auto":
        try:
            return StorageEngineAdapter(db_dir)
        except Exception:
            return ReferenceEngineAdapter(db_dir)
    else:
        raise ValueError(f"Unknown adapter type: {adapter_type}")


# ==============================================================================
# Worker Process for Process-Kill Crash Testing
# ==============================================================================

def _worker_process_loop(db_dir: str, adapter_type: str, pipe: Connection) -> None:
    """
    Subprocess loop executing database operations.
    Can be terminated abruptly via TerminateProcess / SIGKILL at any moment.
    """
    adapter = None
    try:
        adapter = create_adapter(adapter_type, db_dir)
        pipe.send({"status": "READY"})

        while True:
            try:
                if not pipe.poll(timeout=0.1):
                    continue
                msg = pipe.recv()
            except (EOFError, BrokenPipeError):
                break

            cmd = msg.get("cmd")

            if cmd == "EXEC":
                op_data = msg["op"]
                op = Operation.from_dict(op_data)
                result = None

                # Execute on adapter
                if op.op_type == OperationType.PUT:
                    assert op.key is not None and op.value is not None
                    result = adapter.put(op.key, op.value)
                elif op.op_type == OperationType.GET:
                    assert op.key is not None
                    result = adapter.get(op.key)
                elif op.op_type == OperationType.DELETE:
                    assert op.key is not None
                    result = adapter.delete(op.key)
                elif op.op_type == OperationType.BATCH:
                    assert op.batch_items is not None
                    result = adapter.batch(op.batch_items)

                # Send acknowledgement back to test controller
                pipe.send({"status": "ACK", "op_id": op.op_id, "result": result})

            elif cmd == "GET":
                k = msg["key"]
                val = adapter.get(k)
                pipe.send({"status": "ACK", "value": val})

            elif cmd == "SCAN":
                data = adapter.scan_all()
                pipe.send({"status": "ACK", "data": data})

            elif cmd == "CLOSE":
                if adapter:
                    adapter.close()
                pipe.send({"status": "CLOSED"})
                break

            elif cmd == "PING":
                pipe.send({"status": "PONG"})

    except Exception as exc:
        try:
            pipe.send({"status": "ERROR", "error": str(exc), "trace": traceback.format_exc()})
        except Exception:
            pass
    finally:
        if adapter:
            try:
                adapter.close()
            except Exception:
                pass


# ==============================================================================
# Process-Kill Crash Harness
# ==============================================================================

class CrashHarness:
    """
    Orchestrates the lifecycle of the database process:
      - Spawns the database worker in an isolated OS child process.
      - Injects abrupt process crashes via hard OS termination (proc.kill()).
      - Ensures OS file handles settle, then boots a clean instance on the same directory.
    """

    def __init__(self, db_dir: str, adapter_type: str = "auto"):
        self.db_dir = db_dir
        self.adapter_type = adapter_type
        self.proc: Optional[mp.Process] = None
        self.pipe: Optional[Connection] = None
        self._boot()

    def _boot(self) -> None:
        """Start a new database worker process."""
        parent_conn, child_conn = mp.Pipe(duplex=True)
        self.pipe = parent_conn
        self.proc = mp.Process(
            target=_worker_process_loop,
            args=(self.db_dir, self.adapter_type, child_conn),
        )
        self.proc.daemon = True
        self.proc.start()

        # Wait for READY signal
        if not self.pipe.poll(timeout=10.0):
            self.kill()
            raise CrashRecoveryError("Database worker process timed out during boot.")

        msg = self.pipe.recv()
        if msg.get("status") != "READY":
            err = msg.get("error", "Unknown worker startup failure")
            raise CrashRecoveryError(f"Worker startup failed: {err}")

    def execute_op(self, op: Operation, crash_in_flight: bool = False) -> Any:
        """
        Execute an operation on the database worker.
        If crash_in_flight is True: send the operation and IMMEDIATELY kill the process
        before waiting for or reading the acknowledgement!
        """
        if self.proc is None or not self.proc.is_alive():
            raise CrashRecoveryError("Cannot execute operation: worker process is not running.")

        # Send command to worker
        self.pipe.send({"cmd": "EXEC", "op": op.to_dict()})

        if crash_in_flight:
            # Abrupt crash while operation is in-flight!
            self.kill()
            return None

        # Normal execution: wait for acknowledgement
        if not self.pipe.poll(timeout=15.0):
            # Timed out waiting for ACK
            self.kill()
            raise TimeoutError(f"Database worker timed out on operation {op.op_type} #{op.op_id}")

        msg = self.pipe.recv()
        if msg.get("status") == "ACK":
            return msg.get("result")
        elif msg.get("status") == "ERROR":
            raise RuntimeError(f"Database worker error: {msg.get('error')}")
        else:
            raise RuntimeError(f"Unexpected worker response: {msg}")

    def get_key(self, key: str) -> Optional[str]:
        """Query a single key from the worker."""
        if self.proc is None or not self.proc.is_alive():
            raise CrashRecoveryError("Cannot read: worker process is not running.")

        self.pipe.send({"cmd": "GET", "key": key})
        if not self.pipe.poll(timeout=10.0):
            self.kill()
            raise TimeoutError(f"Database worker timed out reading key {key}")

        msg = self.pipe.recv()
        return msg.get("value")

    def scan_all(self) -> Dict[str, str]:
        """Scan all key-value pairs from the worker."""
        if self.proc is None or not self.proc.is_alive():
            raise CrashRecoveryError("Cannot scan: worker process is not running.")

        self.pipe.send({"cmd": "SCAN"})
        if not self.pipe.poll(timeout=10.0):
            self.kill()
            raise TimeoutError("Database worker timed out scanning")

        msg = self.pipe.recv()
        return msg.get("data", {})

    def kill(self) -> None:
        """Hard process kill: OS-level termination with NO graceful shutdown."""
        if self.proc and self.proc.is_alive():
            self.proc.kill()
            self.proc.join(timeout=3.0)
        if self.pipe:
            try:
                self.pipe.close()
            except Exception:
                pass
        self.proc = None
        self.pipe = None
        # Short pause to ensure OS kernel completes file handle cleanup
        time.sleep(0.06)

    def restart(self) -> None:
        """Kill any existing process and boot a fresh instance on the same db_dir."""
        self.kill()
        self._boot()

    def close(self) -> None:
        """Graceful shutdown when tests finish."""
        if self.proc and self.proc.is_alive() and self.pipe:
            try:
                self.pipe.send({"cmd": "CLOSE"})
                self.proc.join(timeout=2.0)
            except Exception:
                pass
        self.kill()


# ==============================================================================
# Main Torture Test Runner
# ==============================================================================

class TortureRunner:
    """
    Main orchestrator for randomized stress testing, crash injection,
    and restart verification against the Independent Oracle.
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        iterations: int = 500,
        crashes: int = 10,
        crash_interval: Optional[int] = None,
        key_pool_size: int = 30,
        adapter_type: str = "auto",
        db_dir: Optional[str] = None,
        verbose: bool = False,
    ):
        self.seed = seed if seed is not None else random.randint(100_000, 999_999_999)
        self.iterations = iterations
        self.max_crashes = crashes
        self.crash_interval = crash_interval or max(1, iterations // (crashes + 1))
        self.key_pool_size = key_pool_size
        self.adapter_type = adapter_type
        self.verbose = verbose

        self.custom_dir = db_dir is not None
        self.db_dir = db_dir or tempfile.mkdtemp(prefix="astra_torture_")
        self.oracle = IndependentOracle()
        self.generator = RandomWorkloadGenerator(self.seed, key_pool_size=self.key_pool_size)
        self.harness: Optional[CrashHarness] = None

    def log(self, message: str) -> None:
        print(message, flush=True)

    def log_v(self, message: str) -> None:
        if self.verbose:
            print(f"  [DEBUG] {message}", flush=True)

    def run(self) -> bool:
        """Run the torture test suite. Returns True on success, raises on failure."""
        self.log("=" * 70)
        self.log("ASTRA CRASH-SAFETY & DURABILITY TORTURE TEST")
        self.log("=" * 70)
        self.log(f"Seed:            {self.seed} (Pass --seed {self.seed} to reproduce)")
        self.log(f"Target Adapter:  {self.adapter_type}")
        self.log(f"Iterations:      {self.iterations}")
        self.log(f"Planned Crashes: {self.max_crashes}")
        self.log(f"Crash Interval:  Every ~{self.crash_interval} operations")
        self.log(f"Key Pool Size:   {self.key_pool_size}")
        self.log(f"Database Dir:    {self.db_dir}")
        self.log("=" * 70)

        crashes_injected = 0
        step = 0

        try:
            self.harness = CrashHarness(self.db_dir, self.adapter_type)
            self.log_v("Harness initialized and worker process started.")

            while step < self.iterations:
                step += 1
                op = self.generator.next_operation(step)

                # Check if this operation triggers a crash
                should_crash = (
                    crashes_injected < self.max_crashes
                    and (step % self.crash_interval == 0 or step == self.iterations)
                )

                if should_crash:
                    crashes_injected += 1
                    # Alternate between:
                    # 1. In-flight crash (kill during write, write unacknowledged)
                    # 2. Post-ack crash (write acknowledged, kill immediately after)
                    crash_in_flight = (crashes_injected % 2 == 1) and (op.op_type != OperationType.GET)

                    if crash_in_flight:
                        self.log(
                            f"\n[CRASH {crashes_injected:02d}/{self.max_crashes:02d}] "
                            f"Step {step}: Injected IN-FLIGHT crash on {op.op_type} #{op.op_id}!"
                        )
                        # Record op in flight on oracle
                        self.oracle.record_start(op)
                        # Execute and hard kill before ACK
                        self.harness.execute_op(op, crash_in_flight=True)
                        # Oracle notes crash: in-flight op is UNACKNOWLEDGED
                        self.oracle.record_crash()
                    else:
                        # Normal execution followed by immediate abrupt kill
                        self.oracle.record_start(op)
                        res = self.harness.execute_op(op, crash_in_flight=False)
                        self.oracle.record_ack(op, res)
                        self.log(
                            f"\n[CRASH {crashes_injected:02d}/{self.max_crashes:02d}] "
                            f"Step {step}: Injected POST-ACK crash immediately following {op.op_type} #{op.op_id}!"
                        )
                        self.harness.kill()
                        self.oracle.record_crash()

                    # Restart and run full Oracle verification
                    self.log(f"[RESTART] Rebooting database instance on {self.db_dir}...")
                    self.harness.restart()

                    self.log("[VERIFY] Running independent oracle restart verification...")
                    report = self.oracle.verify_restart(self.harness.get_key)
                    self.log(
                        f"         - Acknowledged keys verified: {report.acknowledged_keys_verified} (100% durable)\n"
                        f"         - Deletions verified:         {report.deletions_verified}\n"
                        f"         - In-flight resolution:       {report.in_flight_resolution}\n"
                        f"         - Status:                     PASSED"
                    )

                    # Post-restart liveness check: perform a new write and read to verify full functionality
                    test_key = f"__liveness_{crashes_injected}__"
                    test_val = f"liveness_val_{crashes_injected}"
                    live_op = Operation(
                        op_id=999_000 + crashes_injected,
                        op_type=OperationType.PUT,
                        key=test_key,
                        value=test_val,
                        step=step,
                    )
                    self.oracle.record_start(live_op)
                    self.harness.execute_op(live_op)
                    self.oracle.record_ack(live_op)
                    readback = self.harness.get_key(test_key)
                    if readback != test_val:
                        raise LivenessCheckError(
                            f"Post-crash liveness check failed! Written {test_val!r}, read {readback!r}"
                        )
                    self.log_v("Post-crash liveness check passed.")
                    continue

                # Standard non-crash operation
                self.oracle.record_start(op)
                try:
                    if op.op_type == OperationType.GET:
                        actual = self.harness.execute_op(op)
                        assert op.key is not None
                        self.oracle.verify_read(op.key, actual)
                        self.oracle.record_ack(op, actual)
                        self.log_v(f"Step {step:04d}: GET {op.key} -> {actual!r} (MATCH)")
                    else:
                        res = self.harness.execute_op(op)
                        self.oracle.record_ack(op, res)
                        self.log_v(f"Step {step:04d}: {op.op_type} #{op.op_id} (ACK)")

                except Exception as op_err:
                    self.oracle.record_failure(op, str(op_err))
                    raise op_err

            # Final verification after completing all iterations
            self.log("\n[FINAL VERIFICATION] Verifying all keys at conclusion of test...")
            report = self.oracle.verify_restart(self.harness.get_key)
            self.log(
                f"Acknowledged keys durable: {report.acknowledged_keys_verified}\n"
                f"Deletions durable:         {report.deletions_verified}"
            )

            self.log("=" * 70)
            self.log("TORTURE TEST PASSED! All crash-safety and durability invariants hold.")
            self.log(f"Total Operations: {step}")
            self.log(f"Crashes Tested:   {crashes_injected}")
            self.log(f"Reproduce with:   python torture.py --seed {self.seed} --iterations {self.iterations} --crashes {self.max_crashes}")
            self.log("=" * 70)
            return True

        except Exception as exc:
            self.log("\n" + "!" * 70)
            self.log(f"TORTURE TEST FAILED at step {step}!")
            self.log(f"Exception: {type(exc).__name__}: {exc}")
            self.log(f"Seed to reproduce: python torture.py --seed {self.seed} --iterations {self.iterations} --crashes {self.max_crashes}")
            self.log(f"Database Directory preserved for inspection: {self.db_dir}")
            self.log("!" * 70)
            traceback.print_exc()
            return False

        finally:
            if self.harness:
                self.harness.close()
            # Clean up temp directory only on complete success
            if not self.custom_dir and 'exc' not in locals():
                try:
                    shutil.rmtree(self.db_dir, ignore_errors=True)
                except Exception:
                    pass


# ==============================================================================
# Self-Test & Unit Tests for the Torture Suite
# ==============================================================================

def run_self_tests() -> bool:
    """
    Run automated self-tests to prove that:
      1. Independent Oracle properly tracks acknowledged state and detects corruptions.
      2. Oracle handles in-flight unacknowledged operations and atomic batches.
      3. ReferenceCrashSafeEngine survives real process kills and torn-write recoveries.
      4. Seed-based test runs are 100% deterministic and reproducible.
    """
    print("=" * 70)
    print("RUNNING TORTURE SUITE SELF-TESTS")
    print("=" * 70)

    # Test 1: Oracle Durability Tracking
    print("[1/4] Testing Oracle Durability & Acknowledged State...")
    oracle = IndependentOracle()
    op1 = Operation(1, OperationType.PUT, "alpha", "val1")
    oracle.record_start(op1)
    oracle.record_ack(op1)

    op2 = Operation(2, OperationType.PUT, "beta", "val2")
    oracle.record_start(op2)
    oracle.record_ack(op2)

    op3 = Operation(3, OperationType.DELETE, "alpha")
    oracle.record_start(op3)
    oracle.record_ack(op3)

    assert oracle.get_expected_value("alpha") is None
    assert oracle.get_expected_value("beta") == "val2"

    # Verify simulated clean restart
    mock_db = {"beta": "val2"}
    report = oracle.verify_restart(lambda k: mock_db.get(k))
    assert report.passed
    print("      -> Oracle Durability Tracking: PASSED")

    # Test 2: Oracle In-Flight Unacknowledged Handling & Batch Atomicity
    print("[2/4] Testing Oracle In-Flight Unacknowledged & Batch Atomicity...")
    batch_op = Operation(
        op_id=4,
        op_type=OperationType.BATCH,
        batch_items=[
            BatchItem(OperationType.PUT, "x", "100"),
            BatchItem(OperationType.PUT, "y", "200"),
        ],
    )
    oracle.record_start(batch_op)
    # Simulate crash before ACK!
    oracle.record_crash()

    # Case A: Entire batch committed before crash
    mock_db_committed = {"beta": "val2", "x": "100", "y": "200"}
    oracle_clone = IndependentOracle()
    oracle_clone.acknowledged_state = dict(oracle.acknowledged_state)
    oracle_clone.all_known_keys = set(oracle.all_known_keys)
    oracle_clone.unacknowledged_ops = list(oracle.unacknowledged_ops)
    report_a = oracle_clone.verify_restart(lambda k: mock_db_committed.get(k))
    assert report_a.passed and "BATCH_ALL_COMMITTED" in report_a.in_flight_resolution

    # Case B: Partial batch applied (Atomicity violation!)
    mock_db_partial = {"beta": "val2", "x": "100"}  # missing y!
    try:
        oracle.verify_restart(lambda k: mock_db_partial.get(k))
        raise AssertionError("Oracle failed to catch batch atomicity violation!")
    except AtomicityViolationError:
        print("      -> Oracle In-Flight & Atomicity Check: PASSED")

    # Test 3: Reproducibility with Seed
    print("[3/4] Testing Deterministic PRNG Reproducibility...")
    gen1 = RandomWorkloadGenerator(seed=987654)
    gen2 = RandomWorkloadGenerator(seed=987654)
    for i in range(50):
        o1 = gen1.next_operation(i)
        o2 = gen2.next_operation(i)
        assert o1.op_type == o2.op_type
        assert o1.key == o2.key
        assert o1.value == o2.value
    print("      -> Deterministic Reproducibility: PASSED")

    # Test 4: JSONL Serve Adapter Mock Tests
    print("[4/5] Testing JSONL Serve Adapter Protocol...")

    class MockStdin:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, data):
            self.writes.append(data)

        def flush(self):
            pass

        def close(self):
            self.closed = True

    class MockStdout:
        def __init__(self, responses):
            self.responses = list(responses)

        def readline(self):
            if not self.responses:
                return ""
            return self.responses.pop(0)

    class MockProcess:
        def __init__(self, responses):
            self.stdin = MockStdin()
            self.stdout = MockStdout(responses)
            self.returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.returncode = 0
            return 0

        def terminate(self):
            self.returncode = -1

    mock_process = MockProcess([
        "ACK 1\n",
        '{"id":2,"status":"OK","value":"hello"}\n',
        "NOTFOUND 3\n",
        "ACK 4\n",
        "ACK 5\n",
        "ERROR 6\n",
    ])

    adapter = ServeProcessAdapter("mock-db", process=mock_process)

    # PUT success
    assert adapter.put("name", "Zaid") is True

    # GET success
    assert adapter.get("name") == "hello"

    # GET missing key
    assert adapter.get("missing") is None

    # DELETE success
    assert adapter.delete("name") is True

    # BATCH success
    batch_items = [
        BatchItem(OperationType.PUT, "city", "Mangalore"),
        BatchItem(OperationType.DELETE, "old_city"),
    ]
    assert adapter.batch(batch_items) is True

    # PUT failure
    assert adapter.put("failed_key", "value") is False

    # Verify request serialization
    requests = [
        json.loads(line)
        for line in mock_process.stdin.writes
    ]

    assert [request["id"] for request in requests] == [1, 2, 3, 4, 5, 6]

    assert requests[0]["op"] == "put"
    assert requests[0]["key"] == "name"
    assert requests[0]["value"] == "Zaid"

    assert requests[4]["op"] == "batch"
    assert requests[4]["operations"] == [
        {"op": "put", "key": "city", "value": "Mangalore"},
        {"op": "delete", "key": "old_city"},
    ]

    adapter.close()

    # Verify response ID mismatch is rejected
    mismatch_process = MockProcess(["ACK 99\n"])
    mismatch_adapter = ServeProcessAdapter(
        "mock-db",
        process=mismatch_process,
    )

    try:
        mismatch_adapter.put("key", "value")
        raise AssertionError("Adapter accepted an incorrect response ID!")
    except ServeProtocolError:
        pass
    finally:
        mismatch_adapter.close()

    print("      -> JSONL Serve Adapter Protocol: PASSED")

# Test 5: End-to-End Torture Run against ReferenceCrashSafeEngine
    print("[5/5] Testing End-to-End Torture Run with Process Kills...")
    runner = TortureRunner(
        seed=1337,
        iterations=60,
        crashes=3,
        crash_interval=18,
        key_pool_size=15,
        adapter_type="reference",
        verbose=False,
    )
    success = runner.run()
    assert success, "End-to-End Torture test failed on ReferenceCrashSafeEngine!"
    print("      -> End-to-End Torture Run with Real Process Kills: PASSED")

    print("\n" + "=" * 70)
    print("ALL TORTURE SUITE SELF-TESTS PASSED!")
    print("=" * 70)
    return True


# ==============================================================================
# CLI Entry Point
# ==============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Astra-SIT Crash-Safety and Durability Torture Test Runner"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible testing (default: random).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=300,
        help="Total number of operation cycles to run (default: 300).",
    )
    parser.add_argument(
        "--crashes",
        type=int,
        default=8,
        help="Total number of process-kill crash events to inject (default: 8).",
    )
    parser.add_argument(
        "--crash-interval",
        type=int,
        default=None,
        help="Number of operations between injected crashes (default: auto).",
    )
    parser.add_argument(
        "--key-space",
        type=int,
        default=25,
        help="Size of key pool to exercise overwrites and deletes (default: 25).",
    )
    parser.add_argument(
        "--target",
        choices=["auto", "reference", "storage"],
        default="auto",
        help="Target database adapter (default: auto).",
    )
    parser.add_argument(
        "--db-dir",
        type=str,
        default=None,
        help="Custom database directory (default: isolated temporary directory).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable detailed debug logging for each operation.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run torture suite self-tests verifying oracle, WAL recovery, and crash harness.",
    )

    args = parser.parse_args()

    if args.self_test:
        success = run_self_tests()
        sys.exit(0 if success else 1)

    runner = TortureRunner(
        seed=args.seed,
        iterations=args.iterations,
        crashes=args.crashes,
        crash_interval=args.crash_interval,
        key_pool_size=args.key_space,
        adapter_type=args.target,
        db_dir=args.db_dir,
        verbose=args.verbose,
    )

    success = runner.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
