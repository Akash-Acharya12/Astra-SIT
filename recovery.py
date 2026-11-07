"""
Crash Recovery Engine for Astra-SIT Key-Value Database.
Sequentially scans the log, validates record integrity with CRC32,
atomically replays committed transactions, rolls back uncommitted batches,
and safely truncates only incomplete or torn records at the tail of the log.
"""

import os
import struct
from typing import Dict, List, Optional, Tuple

from index import IndexEntry, MemoryIndex
from storage import DatabaseCorruptedError, OpType, RecordCodec, StorageEngine


class RecoveryError(DatabaseCorruptedError):
    """Raised when log recovery encounters unrecoverable corruption."""
    pass


class RecoveryManager:
    """
    Reconstructs the in-memory hash index from persistent append-only log storage.
    Enforces atomic batch commits, CRC validation, and safe EOF truncation boundaries.
    """

    @classmethod
    def recover(cls, storage: StorageEngine) -> Tuple[MemoryIndex, int]:
        """
        Scan data.db from byte 0 to EOF.
        - Validates CRC32 of all complete records.
        - Standalone records (tx_id == 0) are applied immediately.
        - Batch records (tx_id > 0) are applied ONLY after a valid OP_TX_COMMIT.
        - Incomplete batches at EOF and torn writes are safely truncated.
        - Mid-log corruptions and complete records with invalid CRC raise RecoveryError.

        Returns:
            Tuple[MemoryIndex, int]: (rebuilt_index, safe_truncation_point)
        """
        log_path = storage.file_path
        if not os.path.exists(log_path):
            return MemoryIndex(), 0

        file_size = os.path.getsize(log_path)
        if file_size == 0:
            return MemoryIndex(), 0

        index = MemoryIndex()
        # Active transactions: tx_id -> {'expected_count': int, 'records': list, 'begin_offset': int}
        active_transactions: Dict[int, Dict] = {}
        safe_truncation_point = 0
        offset = 0

        with open(log_path, "rb") as f:
            while offset < file_size:
                # 1. Check if at least a full 29-byte header is available
                remaining_bytes = file_size - offset
                if remaining_bytes < RecordCodec.HEADER_SIZE:
                    # Torn header at EOF
                    break

                f.seek(offset)
                header_bytes = f.read(RecordCodec.HEADER_SIZE)
                if len(header_bytes) < RecordCodec.HEADER_SIZE:
                    # Premature EOF reading header
                    break

                try:
                    crc, timestamp, tx_id, op_type, key_len, val_len = (
                        RecordCodec.decode_header(header_bytes)
                    )
                except (ValueError, struct.error):
                    # Malformed header
                    if offset + RecordCodec.HEADER_SIZE < file_size:
                        raise RecoveryError(
                            f"Malformed record header mid-log at offset {offset}"
                        )
                    # At EOF, treat as torn header
                    break

                expected_record_len = RecordCodec.HEADER_SIZE + key_len + val_len

                if key_len > 1024 or val_len > 1048576:
                    if offset + RecordCodec.HEADER_SIZE < file_size:
                        raise RecoveryError(
                            f"Header length limits exceeded at offset {offset}: key_len={key_len}, val_len={val_len}"
                        )
                    break

                # 2. Check if entire payload is present on disk
                if offset + expected_record_len > file_size:
                    # Torn payload at EOF
                    break

                payload_bytes = f.read(key_len + val_len)
                if len(payload_bytes) != key_len + val_len:
                    # Incomplete payload read at EOF
                    break

                key = payload_bytes[:key_len]
                val = payload_bytes[key_len:]

                # 3. Verify CRC32
                computed_crc = RecordCodec.compute_crc(
                    timestamp, tx_id, op_type, key, val
                )
                if crc != computed_crc:
                    # All bytes were present on disk, but CRC is invalid
                    # This is data corruption, not a simple torn write
                    raise RecoveryError(
                        f"CRC mismatch at offset {offset}: stored=0x{crc:08X}, computed=0x{computed_crc:08X}"
                    )

                val_offset = offset + RecordCodec.HEADER_SIZE + key_len

                # 4. Process Valid Record
                if tx_id == 0:
                    if active_transactions:
                        raise RecoveryError(
                            f"Standalone operation while transaction {list(active_transactions.keys())} is active at offset {offset}"
                        )
                    # Standalone auto-commit operation
                    if op_type == OpType.PUT:
                        entry = IndexEntry(
                            offset=offset,
                            total_len=expected_record_len,
                            val_offset=val_offset,
                            val_len=val_len,
                            timestamp=timestamp,
                        )
                        index.set(key, entry)
                        safe_truncation_point = offset + expected_record_len
                    elif op_type == OpType.DELETE:
                        index.delete(key)
                        safe_truncation_point = offset + expected_record_len
                    else:
                        raise RecoveryError(
                            f"Unexpected standalone operation type {op_type} at offset {offset}"
                        )

                else:
                    # Transactional batch operation (tx_id > 0)
                    if op_type == OpType.TX_BEGIN:
                        if active_transactions:
                            raise RecoveryError(
                                f"Overlapping transaction while {list(active_transactions.keys())} is active at offset {offset}"
                            )
                        if tx_id in active_transactions:
                            raise RecoveryError(
                                f"Duplicate TX_BEGIN for tx_id {tx_id} at offset {offset}"
                            )
                        if len(val) != 4:
                            raise RecoveryError(
                                f"Malformed TX_BEGIN at offset {offset}: expected 4-byte count"
                            )
                        expected_count = struct.unpack("!I", val)[0]
                        active_transactions[tx_id] = {
                            "expected_count": expected_count,
                            "records": [],
                            "begin_offset": offset,
                        }
                        # safe_truncation_point is NOT advanced here

                    elif op_type in (OpType.PUT, OpType.DELETE):
                        if tx_id not in active_transactions:
                            raise RecoveryError(
                                f"Transactional record with unknown tx_id {tx_id} at offset {offset}"
                            )
                        entry = (
                            IndexEntry(
                                offset=offset,
                                total_len=expected_record_len,
                                val_offset=val_offset,
                                val_len=val_len,
                                timestamp=timestamp,
                            )
                            if op_type == OpType.PUT
                            else None
                        )
                        tx_info = active_transactions[tx_id]
                        tx_info["records"].append((op_type, key, entry))
                        if len(tx_info["records"]) > tx_info["expected_count"]:
                            raise RecoveryError(
                                f"Transaction {tx_id} exceeded declared count of {tx_info['expected_count']} at offset {offset}"
                            )

                    elif op_type == OpType.TX_COMMIT:
                        if tx_id not in active_transactions:
                            raise RecoveryError(
                                f"TX_COMMIT for unknown tx_id {tx_id} at offset {offset}"
                            )
                        tx_info = active_transactions.pop(tx_id)
                        if len(tx_info["records"]) != tx_info["expected_count"]:
                            raise RecoveryError(
                                f"Transaction {tx_id} count mismatch on commit: expected {tx_info['expected_count']}, got {len(tx_info['records'])}"
                            )

                        # Atomically apply all committed records to the index
                        for staged_op, staged_key, staged_entry in tx_info["records"]:
                            if staged_op == OpType.PUT and staged_entry is not None:
                                index.set(staged_key, staged_entry)
                            elif staged_op == OpType.DELETE:
                                index.delete(staged_key)

                        # Batch is now fully committed and durable
                        safe_truncation_point = offset + expected_record_len

                    elif op_type == OpType.TX_ABORT:
                        # Explicit abort marker
                        active_transactions.pop(tx_id, None)
                        safe_truncation_point = offset + expected_record_len

                    else:
                        raise RecoveryError(
                            f"Unknown opcode {op_type} at offset {offset}"
                        )

                offset += expected_record_len

        return index, safe_truncation_point

    @classmethod
    def get_max_tx_id(cls, storage: StorageEngine) -> int:
        """Scan the retained log and return the highest committed tx_id."""
        log_path = storage.file_path
        if not os.path.exists(log_path):
            return 0
        file_size = os.path.getsize(log_path)
        if file_size == 0:
            return 0

        max_tx_id = 0
        offset = 0
        with open(log_path, "rb") as f:
            while offset + RecordCodec.HEADER_SIZE <= file_size:
                f.seek(offset)
                header_bytes = f.read(RecordCodec.HEADER_SIZE)
                if len(header_bytes) < RecordCodec.HEADER_SIZE:
                    break
                try:
                    _, _, tx_id, _, key_len, val_len = RecordCodec.decode_header(
                        header_bytes
                    )
                except (ValueError, struct.error):
                    break

                rec_len = RecordCodec.HEADER_SIZE + key_len + val_len
                if offset + rec_len > file_size:
                    break

                if tx_id > max_tx_id:
                    max_tx_id = tx_id
                offset += rec_len

        return max_tx_id


__all__ = ["RecoveryManager", "RecoveryError"]
