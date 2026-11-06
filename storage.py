"""
Append-only Binary Storage Engine for Astra-SIT Key-Value Database.
Handles binary record serialization, CRC32 validation, file I/O, and durability.
"""

import enum
import io
import os
import struct
import threading
import time
import zlib
from dataclasses import dataclass
from typing import Optional, Tuple


# --- Exceptions ---
class AstraError(Exception):
    """Base exception for all Astra-SIT errors."""
    pass


class KeyNotFoundError(AstraError):
    """Raised when a requested key does not exist."""
    pass


class DatabaseCorruptedError(AstraError):
    """Raised when record CRC fails or mid-log structure is damaged."""
    pass


class DatabaseLockedError(AstraError):
    """Raised when another process holds the exclusive database lock."""
    pass


class TransactionError(AstraError):
    """Raised when a batch transaction fails validation or execution."""
    pass


class DatabaseClosedError(AstraError):
    """Raised when an operation is attempted on a closed database instance."""
    pass


class OpType(enum.IntEnum):
    """Operation types supported by Astra-SIT."""
    PUT = 1
    DELETE = 2
    TX_BEGIN = 3
    TX_COMMIT = 4
    TX_ABORT = 5



@dataclass(frozen=True)
class Record:
    """Decoded record containing full metadata and payload."""
    crc: int
    timestamp: int
    tx_id: int
    op_type: OpType
    key: bytes
    value: bytes
    offset: int
    total_len: int


class RecordCodec:
    """
    Encodes and decodes binary records with CRC32 data integrity verification.

    Binary Layout:
    +---------------+------------------+-----------------+---------------+----------------+------------------+---------------------+-----------------------+
    | CRC32 (4B)    | Timestamp (8B)   | TxID (8B)       | OpType (1B)   | KeyLen (4B)    | ValLen (4B)      | Key (KeyLen bytes)  | Value (ValLen bytes)  |
    +---------------+------------------+-----------------+---------------+----------------+------------------+---------------------+-----------------------+
    |<------------------------- Fixed Header (29 Bytes) -------------------------->|<----------------- Variable Payload ------------------>|
                    |<-------------------------------- Covered by CRC32 -------------------------------------------------------------------->|
    """

    HEADER_FORMAT = "!IQQBII"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 29 bytes
    PAYLOAD_HEADER_FORMAT = "!QQBII"
    PAYLOAD_HEADER_SIZE = struct.calcsize(PAYLOAD_HEADER_FORMAT)  # 25 bytes

    @classmethod
    def compute_crc(cls, timestamp: int, tx_id: int, op_type: int, key: bytes, value: bytes) -> int:
        """Compute CRC32 checksum over the payload header, key, and value bytes."""
        payload_header = struct.pack(
            cls.PAYLOAD_HEADER_FORMAT,
            timestamp,
            tx_id,
            int(op_type),
            len(key),
            len(value),
        )
        data = payload_header + key + value
        return zlib.crc32(data) & 0xFFFFFFFF

    @classmethod
    def encode(
        cls,
        op_type: OpType,
        key: bytes,
        value: bytes,
        tx_id: int = 0,
        timestamp: Optional[int] = None,
    ) -> bytes:
        """Encode a record into its binary on-disk representation."""
        if timestamp is None:
            timestamp = time.time_ns() // 1000  # Microseconds since epoch

        crc = cls.compute_crc(timestamp, tx_id, op_type, key, value)
        header = struct.pack(
            cls.HEADER_FORMAT,
            crc,
            timestamp,
            tx_id,
            int(op_type),
            len(key),
            len(value),
        )
        return header + key + value

    @classmethod
    def decode_header(cls, header_bytes: bytes) -> Tuple[int, int, int, OpType, int, int]:
        """Decode the 29-byte fixed header."""
        if len(header_bytes) < cls.HEADER_SIZE:
            raise ValueError(f"Header too short: expected {cls.HEADER_SIZE} bytes, got {len(header_bytes)}")
        crc, timestamp, tx_id, op_type_int, key_len, val_len = struct.unpack(
            cls.HEADER_FORMAT, header_bytes
        )
        return crc, timestamp, tx_id, OpType(op_type_int), key_len, val_len


class StorageEngine:
    """
    Manages low-level append-only log file I/O.
    Provides thread-safe appends, durability fsync, and thread-local reading.
    """

    def __init__(self, file_path: str):
        self.file_path = os.path.abspath(file_path)
        self._write_file: Optional[io.BufferedRandom] = None
        self._write_lock = threading.RLock()
        self._local = threading.local()
        self._open_read_handles: list = []
        self._is_closed = True

    def open(self) -> None:
        """Open or create the append-only storage log file."""
        with self._write_lock:
            # Ensure parent directory exists
            parent = os.path.dirname(self.file_path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)

            # Open file in read/write binary mode; create if not exists
            if not os.path.exists(self.file_path):
                with open(self.file_path, "wb"):
                    pass

            self._write_file = open(self.file_path, "r+b")
            self._write_file.seek(0, os.SEEK_END)
            self._is_closed = False

    def _get_read_handle(self) -> io.BufferedReader:
        """Get or create a thread-local file handle for safe concurrent reads."""
        handle = getattr(self._local, "handle", None)
        if handle is None or handle.closed:
            handle = open(self.file_path, "rb")
            self._local.handle = handle
            with self._write_lock:
                self._open_read_handles.append(handle)
        return handle

    def append_record(
        self,
        op_type: OpType,
        key: bytes,
        value: bytes,
        tx_id: int = 0,
        timestamp: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Append a binary record to the end of the log.
        Returns (offset, total_record_len).
        """
        with self._write_lock:
            if self._is_closed or self._write_file is None:
                raise RuntimeError("StorageEngine is closed.")

            data = RecordCodec.encode(op_type, key, value, tx_id, timestamp)
            self._write_file.seek(0, os.SEEK_END)
            offset = self._write_file.tell()
            self._write_file.write(data)
            return offset, len(data)

    def read_value(self, val_offset: int, val_len: int) -> bytes:
        """Read a value directly from the log without parsing the entire record."""
        handle = self._get_read_handle()
        handle.seek(val_offset)
        data = handle.read(val_len)
        if len(data) != val_len:
            raise IOError(f"Unexpected EOF reading value at offset {val_offset}: expected {val_len} bytes, got {len(data)}")
        return data

    def read_record(self, offset: int) -> Record:
        """Read and decode a full record at a specific file offset, validating CRC32."""
        handle = self._get_read_handle()
        handle.seek(offset)
        header_bytes = handle.read(RecordCodec.HEADER_SIZE)
        if len(header_bytes) < RecordCodec.HEADER_SIZE:
            raise ValueError(f"Incomplete record header at offset {offset}")

        crc, timestamp, tx_id, op_type, key_len, val_len = RecordCodec.decode_header(header_bytes)
        payload = handle.read(key_len + val_len)
        if len(payload) != key_len + val_len:
            raise ValueError(f"Incomplete record payload at offset {offset}")

        key = payload[:key_len]
        val = payload[key_len:]

        # Validate CRC32
        expected_crc = RecordCodec.compute_crc(timestamp, tx_id, op_type, key, val)
        if crc != expected_crc:
            raise ValueError(
                f"CRC mismatch at offset {offset}: stored=0x{crc:08X}, computed=0x{expected_crc:08X}"
            )

        total_len = RecordCodec.HEADER_SIZE + key_len + val_len
        return Record(
            crc=crc,
            timestamp=timestamp,
            tx_id=tx_id,
            op_type=op_type,
            key=key,
            value=val,
            offset=offset,
            total_len=total_len,
        )

    def sync(self) -> None:
        """Flush and fsync write file descriptor to guarantee physical durability."""
        with self._write_lock:
            if self._write_file and not self._write_file.closed:
                self._write_file.flush()
                os.fsync(self._write_file.fileno())

    def truncate(self, size: int) -> None:
        """Truncate the log file to a given size (e.g. to drop torn EOF writes)."""
        with self._write_lock:
            if self._write_file and not self._write_file.closed:
                self._write_file.truncate(size)
                self._write_file.seek(size)
                self.sync()

    def get_eof(self) -> int:
        """Return the current end-of-file byte offset."""
        with self._write_lock:
            if self._write_file and not self._write_file.closed:
                self._write_file.seek(0, os.SEEK_END)
                return self._write_file.tell()
            return 0

    def close(self) -> None:
        """Flush, fsync, and close all file handles."""
        with self._write_lock:
            if self._is_closed:
                return
            if self._write_file and not self._write_file.closed:
                self._write_file.flush()
                try:
                    os.fsync(self._write_file.fileno())
                except OSError:
                    pass
                self._write_file.close()
                self._write_file = None

            # Close all thread-local read handles
            for handle in self._open_read_handles:
                try:
                    if not handle.closed:
                        handle.close()
                except OSError:
                    pass
            self._open_read_handles.clear()

            # Clear current thread's handle
            if hasattr(self._local, "handle"):
                try:
                    if self._local.handle and not self._local.handle.closed:
                        self._local.handle.close()
                except OSError:
                    pass
                self._local.handle = None

            self._is_closed = True
