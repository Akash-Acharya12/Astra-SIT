"""
In-memory Hash Index for Astra-SIT Key-Value Database.
Maintains key -> IndexEntry mapping in memory for O(1) point lookups.
"""

from dataclasses import dataclass
import threading
from typing import Optional, List, Tuple, Dict


@dataclass(frozen=True)
class IndexEntry:
    """Represents the on-disk location and metadata for a key's current value."""
    offset: int        # Byte offset in the log file where the record starts
    total_len: int     # Total byte length of the record (header + key + val)
    val_offset: int    # Byte offset where the value starts (offset + 29 + key_len)
    val_len: int       # Byte length of the value
    timestamp: int     # Timestamp of the record in microseconds


class MemoryIndex:
    """
    Thread-safe in-memory hash index.
    Stores key (bytes) -> IndexEntry mappings.
    """

    def __init__(self):
        self._index: Dict[bytes, IndexEntry] = {}
        self._lock = threading.RLock()

    def set(self, key: bytes, entry: IndexEntry) -> None:
        """Add or update an index entry for a key."""
        with self._lock:
            self._index[key] = entry

    def get(self, key: bytes) -> Optional[IndexEntry]:
        """Retrieve the index entry for a key. Returns None if absent."""
        with self._lock:
            return self._index.get(key)

    def delete(self, key: bytes) -> bool:
        """
        Remove a key from the index (e.g. upon tombstone).
        Returns True if the key was present, False otherwise.
        """
        with self._lock:
            return self._index.pop(key, None) is not None

    def contains(self, key: bytes) -> bool:
        """Check if a key exists in the index."""
        with self._lock:
            return key in self._index

    def items(self) -> List[Tuple[bytes, IndexEntry]]:
        """Return a snapshot of all active (key, IndexEntry) pairs."""
        with self._lock:
            return list(self._index.items())

    def keys(self) -> List[bytes]:
        """Return a snapshot of all active keys."""
        with self._lock:
            return list(self._index.keys())

    def size(self) -> int:
        """Return the number of live keys in the index."""
        with self._lock:
            return len(self._index)

    def clear(self) -> None:
        """Reset the index."""
        with self._lock:
            self._index.clear()

    def clone(self) -> "MemoryIndex":
        """Create a thread-safe snapshot copy of the index."""
        with self._lock:
            new_idx = MemoryIndex()
            new_idx._index = dict(self._index)
            return new_idx
