"""
Crash-Safe Log Compactor for Astra-SIT Key-Value Database.
Reclaims disk space by rewriting live records to a temporary file,
verifying checksums, catching up tail mutations, and executing an
atomic os.replace() cutover.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from index import IndexEntry, MemoryIndex
from storage import OpType, RecordCodec, StorageEngine


class Compactor:
    """
    Executes crash-safe two-phase compaction for Astra-SIT log storage.

    Phase 1: Background Snapshot & Rewrite
    - Takes a snapshot of live keys from MemoryIndex.
    - Reads values from data.db and writes fresh OP_PUT records to data.db.compact.
    - Flushes and fsyncs data.db.compact.

    Phase 2: Tail Mutation Catch-up & Atomic Cutover
    - Replays any mutations written between snapshot_eof and current_eof.
    - Fsyncs data.db.compact and closes its handle.
    - Closes all storage handles to satisfy Windows file-locking rules.
    - Atomically replaces data.db with data.db.compact via os.replace().
    - Fsyncs directory metadata on POSIX.
    - Re-opens storage log and updates MemoryIndex with new offsets.
    """

    @classmethod
    def clean_interrupted_compaction(cls, db_dir: str) -> bool:
        """
        Clean up leftover temporary compaction files from an earlier interrupted run.
        Returns True if a stale compact file was removed.
        """
        compact_path = os.path.join(os.path.abspath(db_dir), "data.db.compact")
        if os.path.exists(compact_path):
            try:
                os.remove(compact_path)
                return True
            except OSError:
                pass
        return False

    @classmethod
    def compact(
        cls,
        db_dir: str,
        index: MemoryIndex,
        storage: StorageEngine,
    ) -> Dict[str, Any]:
        """
        Execute compaction on the active database log.

        Parameters:
            db_dir: Directory containing database files.
            index: The active MemoryIndex instance.
            storage: The active StorageEngine instance.

        Returns:
            Dictionary with compaction metrics:
            initial_size, final_size, reclaimed_bytes, live_keys, duration_sec.
        """
        if storage._is_closed:
            raise RuntimeError("Cannot compact a closed StorageEngine.")

        start_time = time.perf_counter()
        log_path = storage.file_path
        compact_path = log_path + ".compact"

        # 1. Clean up any leftover temporary file from a previously interrupted compaction
        if os.path.exists(compact_path):
            try:
                os.remove(compact_path)
            except OSError as exc:
                raise RuntimeError(
                    f"Unable to remove existing compaction file '{compact_path}': {exc}"
                ) from exc

        initial_size = os.path.getsize(log_path) if os.path.exists(log_path) else 0

        # 2. Phase 1: Snapshot live index entries and record EOF
        snapshot_eof = storage.get_eof()
        live_items: List[Tuple[bytes, IndexEntry]] = index.items()

        new_entries: Dict[bytes, IndexEntry] = {}
        new_offset = 0
        records_written = 0

        compact_file = None
        cutover_done = False

        try:
            # 3. Open temporary compaction log
            compact_file = open(compact_path, "wb")

            # 4. Rewrite live records into temporary file
            for key, entry in live_items:
                # Read value directly from current log
                val = storage.read_value(entry.val_offset, entry.val_len)

                # Encode clean OP_PUT record (standalone tx_id = 0)
                data = RecordCodec.encode(
                    op_type=OpType.PUT,
                    key=key,
                    value=val,
                    tx_id=0,
                    timestamp=entry.timestamp,
                )
                compact_file.write(data)

                val_offset = new_offset + RecordCodec.HEADER_SIZE + len(key)
                new_entries[key] = IndexEntry(
                    offset=new_offset,
                    total_len=len(data),
                    val_offset=val_offset,
                    val_len=len(val),
                    timestamp=entry.timestamp,
                )
                new_offset += len(data)
                records_written += 1

            # 5. Flush and fsync temporary compacted file
            compact_file.flush()
            os.fsync(compact_file.fileno())

            # 6. Phase 2: Tail Mutation Catch-up
            # Check if any new records were appended during Phase 1
            current_eof = storage.get_eof()
            if current_eof > snapshot_eof:
                replay_offset = snapshot_eof
                while replay_offset < current_eof:
                    rec = storage.read_record(replay_offset)
                    if rec.op_type == OpType.PUT:
                        # Check if this key's latest version is still live
                        current_entry = index.get(rec.key)
                        if current_entry and current_entry.offset == rec.offset:
                            data = RecordCodec.encode(
                                op_type=OpType.PUT,
                                key=rec.key,
                                value=rec.value,
                                tx_id=0,
                                timestamp=rec.timestamp,
                            )
                            compact_file.write(data)
                            val_offset = new_offset + RecordCodec.HEADER_SIZE + len(rec.key)
                            new_entries[rec.key] = IndexEntry(
                                offset=new_offset,
                                total_len=len(data),
                                val_offset=val_offset,
                                val_len=len(rec.value),
                                timestamp=rec.timestamp,
                            )
                            new_offset += len(data)
                            records_written += 1
                    elif rec.op_type == OpType.DELETE:
                        new_entries.pop(rec.key, None)

                    replay_offset += rec.total_len

                compact_file.flush()
                os.fsync(compact_file.fileno())

            # 7. Close temporary compact file handle before replacement
            compact_file.close()
            compact_file = None

            # 8. Close storage engine handles to comply with Windows file-sharing rules
            # (Windows prohibits os.replace if any open file handles to target exist)
            storage.close()

            # 9. Atomic File Replacement (os.replace)
            os.replace(compact_path, log_path)
            cutover_done = True

            # 10. Sync parent directory metadata on POSIX
            if sys.platform != "win32":
                try:
                    dir_fd = os.open(db_dir, os.O_RDONLY)
                    os.fsync(dir_fd)
                    os.close(dir_fd)
                except OSError:
                    pass

            # 11. Reopen storage engine at new compacted file
            storage.open()

            # 12. Atomically update in-memory index to point to compacted file offsets
            index.clear()
            for k, ent in new_entries.items():
                index.set(k, ent)

        except Exception as exc:
            # Handle failure during compaction
            if compact_file and not compact_file.closed:
                try:
                    compact_file.close()
                except OSError:
                    pass

            # If failure happened before atomic replace, remove partial .compact file
            if not cutover_done and os.path.exists(compact_path):
                try:
                    os.remove(compact_path)
                except OSError:
                    pass

            # Ensure storage is reopened if it was closed before error
            if storage._is_closed:
                try:
                    storage.open()
                except Exception:
                    pass

            raise exc

        final_size = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        reclaimed_bytes = max(0, initial_size - final_size)
        elapsed_sec = time.perf_counter() - start_time

        return {
            "initial_size": initial_size,
            "final_size": final_size,
            "reclaimed_bytes": reclaimed_bytes,
            "live_keys": len(new_entries),
            "records_written": records_written,
            "duration_sec": elapsed_sec,
        }
