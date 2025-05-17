import os
import tempfile
import time
from typing import Any, Dict, Optional, Union

from sqlitedict import SqliteDict

DEFAULT_CACHE_TTL = 600
CACHE_FILENAME = "netaudio_mdns_cache.sqlite"


class MdnsCache:
    """
    A cache for mDNS discovery results using SqliteDict with TTL support.
    """

    def __init__(self, ttl: int = DEFAULT_CACHE_TTL, cache_dir: Optional[str] = None):
        """
        Initializes the MdnsCache.

        Args:
            ttl: Time-to-live for cache entries in seconds.
            cache_dir: Directory to store the cache file. Defaults to a system-specific temporary directory.
        """
        self.ttl = ttl
        if cache_dir is None:
            cache_dir = tempfile.gettempdir()

        self.cache_file_path = os.path.join(cache_dir, CACHE_FILENAME)

        os.makedirs(cache_dir, exist_ok=True)

        self._db = SqliteDict(self.cache_file_path, autocommit=True)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves an item from the cache if it exists and is not stale.

        Args:
            key: The key for the cache entry (e.g., service name).

        Returns:
            The cached data dictionary if found and valid, otherwise None.
        """
        if key not in self._db:
            return None

        entry: Union[Dict[str, Any], None] = self._db.get(key)

        if (
            entry is None
            or not isinstance(entry, dict)
            or "last_seen" not in entry
            or "data" not in entry
        ):
            self.delete(key)
            return None

        last_seen_timestamp = entry.get("last_seen", 0)
        if not isinstance(last_seen_timestamp, (int, float)):
            self.delete(key)
            return None

        if time.time() - last_seen_timestamp > self.ttl:
            self.delete(key)
            return None

        return entry.get("data")

    def set(self, key: str, value: Dict[str, Any]) -> None:
        """
        Adds or updates an item in the cache with the current timestamp.

        Args:
            key: The key for the cache entry.
            value: The data dictionary to cache.
        """
        entry = {"data": value, "last_seen": time.time()}
        self._db[key] = entry

    def delete(self, key: str) -> None:
        """
        Deletes an item from the cache.

        Args:
            key: The key of the item to delete.
        """
        if key in self._db:
            del self._db[key]

    def clear(self) -> None:
        """
        Clears all items from the cache.
        """
        self._db.clear()

    def close(self) -> None:
        """
        Closes the database connection.
        """
        if hasattr(self, "_db") and self._db is not None:
            self._db.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
