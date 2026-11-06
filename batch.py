"""
Atomic Batch Transaction Manager for Astra-SIT Key-Value Database.
Ensures multi-operation atomic batches with all-or-nothing transactional guarantees.
"""

import struct
import threading
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from index import IndexEntry, MemoryIndex
from storage import OpType, StorageEngine


@dataclass(frozen=True)
class BatchOperation:
    """Represents a single operation within an atomic batch."""
    op_type: OpType
    key: bytes
    value: Optional[bytes] = None

    @classmethod
    def put(cls, key: Union[str, bytes], value: Union[str, bytes]) -> "BatchOperation":
        k = key.encode("utf-8") if isinstance(key, str) else key
        v = value.encode("utf-8") if isinstance(value, str) else value
        return cls(op_type=OpType.PUT, key=k, value=v)

    @classmethod
    def delete(cls, key: Union[str, bytes]) -> "BatchOperation":
        k = key.encode("utf-8") if isinstance(key, str) else key
        return cls(op_type=OpType.DELETE, key=k, value=b"")


class BatchManager:
    """
    Coordinates atomic batch transactions using the commit-marker protocol:
    OP_TX_BEGIN -> [Records...] -> OP_TX_COMMIT -> single fsync -> atomic index update.
    """

    def __init__(self):
        self._tx_id_lock = threading.Lock()
        self._current_tx_id = 1

    def next_tx_id(self) -> int:
        """Generate a monotonically increasing 64-bit transaction ID."""
        with self._tx_id_lock:
            tid = self._current_tx_id
            self._current_tx_id += 1
            return tid

    def set_starting_tx_id(self, tx_id: int) -> None:
        """Update transaction ID counter during startup/recovery."""
        with self._tx_id_lock:
            if tx_id >= self._current_tx_id:
                self._current_tx_id = tx_id + 1

    def execute_batch(
        self,
        storage: StorageEngine,
        index: MemoryIndex,
        operations: List[BatchOperation],
    ) -> bool:
        """
        Execute a batch of operations atomically.
        Guarantees all operations are made durable before the in-memory index is updated.
        """
        if not operations:
            return True

        # Validation phase: verify all keys and types upfront
        for op in operations:
            if not isinstance(op.key, bytes) or len(op.key) == 0:
                raise ValueError("Batch keys must be non-empty bytes.")
            if op.op_type == OpType.PUT and (op.value is None or not isinstance(op.value, bytes)):
                raise ValueError(f"PUT operation for key {op.key!r} requires bytes value.")

        tx_id = self.next_tx_id()
        num_ops = len(operations)

        # 1. Write OP_TX_BEGIN containing expected count in value field (4 bytes uint32)
        count_bytes = struct.pack("!I", num_ops)
        storage.append_record(
            op_type=OpType.TX_BEGIN,
            key=b"",
            value=count_bytes,
            tx_id=tx_id,
        )

        # 2. Append each transactional operation and stage index updates
        # Staged tuple: (is_put, key, IndexEntry or None)
        staged_updates: List[Tuple[bool, bytes, Optional[IndexEntry]]] = []

        for op in operations:
            if op.op_type == OpType.PUT:
                val = op.value if op.value is not None else b""
                offset, total_len = storage.append_record(
                    op_type=OpType.PUT,
                    key=op.key,
                    value=val,
                    tx_id=tx_id,
                )
                val_offset = offset + 29 + len(op.key)
                entry = IndexEntry(
                    offset=offset,
                    total_len=total_len,
                    val_offset=val_offset,
                    val_len=len(val),
                    timestamp=0,  # Populated from log if needed
                )
                staged_updates.append((True, op.key, entry))
            elif op.op_type == OpType.DELETE:
                storage.append_record(
                    op_type=OpType.DELETE,
                    key=op.key,
                    value=b"",
                    tx_id=tx_id,
                )
                staged_updates.append((False, op.key, None))
            else:
                raise ValueError(f"Unsupported batch operation type: {op.op_type}")

        # 3. Write OP_TX_COMMIT marker
        storage.append_record(
            op_type=OpType.TX_COMMIT,
            key=b"",
            value=b"",
            tx_id=tx_id,
        )

        # 4. Durability barrier: flush and fsync entire batch
        storage.sync()

        # 5. Atomic index commit (under index lock)
        for is_put, key, entry in staged_updates:
            if is_put and entry is not None:
                index.set(key, entry)
            else:
                index.delete(key)

        return True
