"""Crash recovery for the append-only Astra-SIT storage log.

The storage layer owns record decoding and CRC validation.  This module only
replays complete, valid records into a MemoryIndex and reports the byte offset
that the caller may safely retain.  It deliberately does not truncate or sync the storage file; the caller must
truncate to the returned boundary before accepting new writes.
"""

from __future__ import annotations

import struct
from typing import Any

from index import IndexEntry, MemoryIndex
from storage import DatabaseCorruptedError, OpType, RecordCodec


class RecoveryError(DatabaseCorruptedError):
    """Raised when the log contains corruption or an invalid transaction."""


class RecoveryManager:
    """Rebuild a ``MemoryIndex`` from the storage engine's append-only log."""

    @staticmethod
    def recover(storage: Any) -> tuple[MemoryIndex, int]:
        """Recover the latest committed state and return its safe byte boundary.

        A partial final header/payload is treated as a torn write.  If that
        partial record belongs to an open transaction, the rollback boundary
        is the transaction's TX_BEGIN record, so none of its operations can
        become visible.  CRC failures and other malformed records are errors;
        they are never silently treated as a torn tail.

        The caller is responsible for truncating the storage file to the
        returned ``safe_truncation_point`` before allowing subsequent writes.
        """
        index = MemoryIndex()
        offset = 0
        safe_truncation_point = 0
        pending_transaction: dict[str, Any] | None = None

        while True:
            eof = storage.get_eof()
            if offset == eof:
                break
            if offset > eof:
                raise RecoveryError(
                    f"Storage EOF moved behind recovery offset {offset} (EOF {eof})"
                )

            try:
                record = storage.read_record(offset)
            except ValueError as exc:
                message = str(exc)
                rollback_point = (
                    pending_transaction["start_offset"]
                    if pending_transaction is not None
                    else safe_truncation_point
                )

                if message.startswith("Incomplete record header at offset "):
                    # A short remainder cannot contain another complete header,
                    # so this is an incomplete final header, not a middle-log
                    # record that recovery can safely skip.
                    if eof - offset < RecordCodec.HEADER_SIZE:
                        return index, rollback_point
                    raise RecoveryError(
                        f"Incomplete header reported before the end of the log "
                        f"at offset {offset}"
                    ) from exc

                if message.startswith("Incomplete record payload at offset "):
                    # read_record has already decoded the header and determined
                    # that its declared key/value payload extends past EOF.
                    return index, rollback_point

                if message.startswith("CRC mismatch at offset "):
                    raise RecoveryError(
                        f"CRC corruption at offset {offset}; recovery stopped"
                    ) from exc

                raise RecoveryError(
                    f"Unable to read record at offset {offset}: {message}"
                ) from exc

            # Enforce sequential scanning and sane record boundaries.  These
            # checks prevent a broken decoder result from advancing the scan.
            if record.offset != offset:
                raise RecoveryError(
                    f"Record offset mismatch: expected {offset}, got {record.offset}"
                )
            if record.total_len <= 0:
                raise RecoveryError(f"Invalid record length at offset {offset}")

            record_end = offset + record.total_len
            if record_end > eof:
                raise RecoveryError(
                    f"Decoded record extends beyond EOF at offset {offset}"
                )

            try:
                op_type = OpType(record.op_type)
            except (TypeError, ValueError) as exc:
                raise RecoveryError(
                    f"Unknown operation type at offset {offset}: {record.op_type!r}"
                ) from exc

            tx_id = record.tx_id

            if op_type == OpType.PUT or op_type == OpType.DELETE:
                if tx_id == 0:
                    if pending_transaction is not None:
                        raise RecoveryError(
                            f"Standalone operation encountered inside transaction "
                            f"{pending_transaction['tx_id']} at offset {offset}"
                        )
                    RecoveryManager._apply_operation(index, record, op_type)
                    safe_truncation_point = record_end
                else:
                    if (
                        pending_transaction is None
                        or pending_transaction["tx_id"] != tx_id
                    ):
                        raise RecoveryError(
                            f"Transactional operation without matching TX_BEGIN "
                            f"for transaction {tx_id} at offset {offset}"
                        )
                    pending_transaction["operations"].append((record, op_type))

            elif op_type == OpType.TX_BEGIN:
                if tx_id == 0:
                    raise RecoveryError(
                        f"TX_BEGIN has transaction ID 0 at offset {offset}"
                    )
                if pending_transaction is not None:
                    raise RecoveryError(
                        f"Nested/interleaved TX_BEGIN at offset {offset}; "
                        f"transaction {pending_transaction['tx_id']} is still open"
                    )
                if len(record.value) != 4:
                    raise RecoveryError(
                        f"TX_BEGIN at offset {offset} must contain a 4-byte "
                        f"expected-operation count"
                    )

                expected_count = struct.unpack("!I", record.value)[0]
                pending_transaction = {
                    "tx_id": tx_id,
                    "start_offset": offset,
                    "expected_count": expected_count,
                    "operations": [],
                }

            elif op_type == OpType.TX_COMMIT:
                if tx_id == 0 or pending_transaction is None:
                    raise RecoveryError(
                        f"TX_COMMIT without an open transaction at offset {offset}"
                    )
                if pending_transaction["tx_id"] != tx_id:
                    raise RecoveryError(
                        f"TX_COMMIT transaction ID mismatch at offset {offset}: "
                        f"expected {pending_transaction['tx_id']}, got {tx_id}"
                    )
                if record.key or record.value:
                    raise RecoveryError(
                        f"TX_COMMIT at offset {offset} must have an empty key and value"
                    )

                actual_count = len(pending_transaction["operations"])
                expected_count = pending_transaction["expected_count"]
                if actual_count != expected_count:
                    raise RecoveryError(
                        f"Transaction {tx_id} operation-count mismatch: "
                        f"expected {expected_count}, found {actual_count}"
                    )

                # Publish the batch only after its matching commit and count
                # validation have both been observed.
                for operation_record, operation_type in pending_transaction[
                    "operations"
                ]:
                    RecoveryManager._apply_operation(
                        index, operation_record, operation_type
                    )

                pending_transaction = None
                safe_truncation_point = record_end

            elif op_type == OpType.TX_ABORT:
                if tx_id == 0 or pending_transaction is None:
                    raise RecoveryError(
                        f"TX_ABORT without an open transaction at offset {offset}"
                    )
                if pending_transaction["tx_id"] != tx_id:
                    raise RecoveryError(
                        f"TX_ABORT transaction ID mismatch at offset {offset}: "
                        f"expected {pending_transaction['tx_id']}, got {tx_id}"
                    )

                # An explicit abort closes the transaction without publishing
                # any buffered operations.  The abort record itself is valid,
                # so the log is safe through its end.
                pending_transaction = None
                safe_truncation_point = record_end

            else:  # Defensive: all currently defined OpType values are handled.
                raise RecoveryError(
                    f"Unsupported operation type {op_type!r} at offset {offset}"
                )

            offset = record_end

        # A complete but uncommitted transaction at EOF must be rolled back
        # from TX_BEGIN, not merely from the last operation record.
        if pending_transaction is not None:
            safe_truncation_point = pending_transaction["start_offset"]

        return index, safe_truncation_point

    @staticmethod
    def get_max_tx_id(storage: Any) -> int:
        """Return the highest transaction ID in the retained, valid log."""
        _, safe_truncation_point = RecoveryManager.recover(storage)
        eof = storage.get_eof()

        if safe_truncation_point != eof:
            raise RecoveryError(
                "Storage must be truncated to its safe recovery boundary "
                f"before restoring transaction IDs "
                f"(safe boundary {safe_truncation_point}, EOF {eof})"
            )

        offset = 0
        max_tx_id = 0

        while offset < eof:
            try:
                record = storage.read_record(offset)
            except ValueError as exc:
                raise RecoveryError(
                    f"Unable to read retained record at offset {offset}: {exc}"
                ) from exc

            if record.offset != offset:
                raise RecoveryError(
                    f"Record offset mismatch: expected {offset}, "
                    f"got {record.offset}"
                )

            if record.total_len <= 0:
                raise RecoveryError(
                    f"Invalid record length at offset {offset}"
                )

            record_end = offset + record.total_len
            if record_end > eof:
                raise RecoveryError(
                    f"Retained record extends beyond EOF at offset {offset}"
                )

            max_tx_id = max(max_tx_id, record.tx_id)
            offset = record_end

        if offset != eof:
            raise RecoveryError(
                f"Recovery scan did not end at EOF: offset {offset}, EOF {eof}"
            )

        return max_tx_id

    @staticmethod
    def _apply_operation(index: MemoryIndex, record: Any, op_type: OpType) -> None:
        """Apply one already-validated standalone or committed operation."""
        if op_type == OpType.PUT:
            entry = IndexEntry(
                offset=record.offset,
                total_len=record.total_len,
                val_offset=(
                    record.offset
                    + RecordCodec.HEADER_SIZE
                    + len(record.key)
                ),
                val_len=len(record.value),
                timestamp=record.timestamp,
            )
            index.set(record.key, entry)
        elif op_type == OpType.DELETE:
            index.delete(record.key)
        else:
            raise RecoveryError(
                f"Cannot apply non-data operation {op_type!r} to the index"
            )
