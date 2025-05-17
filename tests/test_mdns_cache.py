import os
import tempfile
import time
import unittest
from unittest.mock import patch

from netaudio.common.mdns_cache import CACHE_FILENAME, DEFAULT_CACHE_TTL, MdnsCache


class TestMdnsCache(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache_dir = self.temp_dir.name
        self.test_ttl = 2
        self.cache = MdnsCache(ttl=self.test_ttl, cache_dir=self.cache_dir)
        self.cache_file_path = self.cache.cache_file_path

    def tearDown(self):
        self.cache.close()
        self.temp_dir.cleanup()

    def test_01_set_and_get_fresh_entry(self):
        """Test setting and getting a cache entry before it expires."""
        key = "device1.local."
        data = {"ip": "192.168.1.1", "name": "Device1", "model": "ModelX"}
        self.cache.set(key, data)
        retrieved_data = self.cache.get(key)
        self.assertIsNotNone(retrieved_data)
        self.assertEqual(retrieved_data, data)

    def test_02_get_non_existent_entry(self):
        """Test getting a non-existent cache entry."""
        retrieved_data = self.cache.get("nonexistent.local.")
        self.assertIsNone(retrieved_data)

    @patch("time.time")
    def test_03_get_stale_entry(self, mock_time):
        """Test that a cache entry becomes stale after TTL."""
        key = "device2.local."
        data = {"ip": "192.168.1.2", "name": "Device2", "model": "ModelY"}

        initial_timestamp = 1000.0
        mock_time.return_value = initial_timestamp

        self.cache.set(key, data)
        retrieved_data = self.cache.get(key)
        self.assertEqual(
            retrieved_data, data, "Should retrieve fresh data immediately after set"
        )

        mock_time.return_value = initial_timestamp + self.test_ttl + 1

        retrieved_data_stale = self.cache.get(key)
        self.assertIsNone(retrieved_data_stale, "Should return None for stale data")

        self.assertIsNone(
            self.cache._db.get(key), "Stale entry should be removed from DB on access"
        )

    def test_04_set_updates_existing_entry_and_timestamp(self):
        """Test that setting an existing key updates its value and timestamp."""
        key = "device3.local."
        data1 = {"ip": "192.168.1.3", "name": "Device3_v1"}
        data2 = {"ip": "192.168.1.3", "name": "Device3_v2"}

        with patch("time.time") as mock_time:
            # First set
            mock_time.return_value = 1000.0
            self.cache.set(key, data1)
            entry1 = self.cache._db.get(key)
            self.assertEqual(entry1["data"], data1)
            self.assertEqual(entry1["last_seen"], 1000.0)

            mock_time.return_value = 1000.0 + self.test_ttl / 2
            self.cache.set(key, data2)
            entry2 = self.cache._db.get(key)
            self.assertEqual(entry2["data"], data2, "Data should be updated")
            self.assertEqual(
                entry2["last_seen"],
                1000.0 + self.test_ttl / 2,
                "Timestamp should be updated",
            )

            retrieved_data = self.cache.get(key)
            self.assertEqual(retrieved_data, data2, "Get should return updated data")

    def test_05_delete_entry(self):
        """Test deleting a cache entry."""
        key = "device4.local."
        data = {"ip": "192.168.1.4"}
        self.cache.set(key, data)
        self.assertIsNotNone(self.cache.get(key), "Entry should exist before delete")

        self.cache.delete(key)
        self.assertIsNone(self.cache.get(key), "Entry should not exist after delete")
        self.assertIsNone(
            self.cache._db.get(key), "Entry should be removed from DB after delete"
        )

    def test_06_clear_cache(self):
        """Test clearing all entries from the cache."""
        self.cache.set("dev1", {"ip": "1.1.1.1"})
        self.cache.set("dev2", {"ip": "2.2.2.2"})
        self.assertTrue(
            len(list(self.cache._db.keys())) > 0,
            "Cache should have entries before clear",
        )

        self.cache.clear()
        self.assertEqual(
            len(list(self.cache._db.keys())), 0, "Cache should be empty after clear"
        )
        self.assertIsNone(self.cache.get("dev1"), "dev1 should be None after clear")
        self.assertIsNone(self.cache.get("dev2"), "dev2 should be None after clear")

    def test_07_cache_persists_to_file_and_can_be_reloaded(self):
        """Test that cache data is written to file and can be reloaded by another instance."""
        key = "persistent_device.local."
        data = {"ip": "10.0.0.1", "name": "Persistent"}
        self.cache.set(key, data)
        original_last_seen = self.cache._db.get(key)["last_seen"]
        self.cache.close()

        reloaded_cache = MdnsCache(ttl=self.test_ttl, cache_dir=self.cache_dir)
        retrieved_data = reloaded_cache.get(key)
        self.assertIsNotNone(retrieved_data)
        self.assertEqual(retrieved_data, data)

        reloaded_entry = reloaded_cache._db.get(key)
        self.assertEqual(reloaded_entry["last_seen"], original_last_seen)
        reloaded_cache.close()

    @patch("time.time")
    def test_08_get_stale_entry_deletes_it(self, mock_time):
        """Test that getting a stale entry not only returns None but also deletes it from the DB."""
        key = "stale_and_delete.local."
        data = {"ip": "192.168.1.5"}

        current_time = 1000.0
        mock_time.return_value = current_time
        self.cache.set(key, data)
        self.assertIsNotNone(self.cache._db.get(key), "Entry should be in DB after set")

        mock_time.return_value = current_time + self.test_ttl + 1
        self.assertIsNone(self.cache.get(key), "Stale entry should return None")
        self.assertIsNone(
            self.cache._db.get(key), "Stale entry should be deleted from DB after get"
        )

    def test_09_default_ttl_usage(self):
        """Test that MdnsCache uses DEFAULT_CACHE_TTL if no ttl is provided."""
        specific_test_cache_dir = os.path.join(
            self.cache_dir, "subdir_for_default_ttl_test"
        )
        os.makedirs(specific_test_cache_dir, exist_ok=True)

        cache_with_default_ttl = MdnsCache(cache_dir=specific_test_cache_dir)

        self.assertEqual(cache_with_default_ttl.ttl, DEFAULT_CACHE_TTL)

        cache_with_default_ttl.set("default_ttl_test_key", {"data": "test_value"})
        self.assertIsNotNone(cache_with_default_ttl.get("default_ttl_test_key"))

        cache_with_default_ttl.close()

    def test_10_context_manager(self):
        """Test that the cache can be used as a context manager and closes properly."""
        temp_dir_ctx = tempfile.TemporaryDirectory()

        with MdnsCache(ttl=1, cache_dir=temp_dir_ctx.name) as ctx_cache:
            ctx_cache.set("ctx_key", {"data": "test"})
            self.assertIsNotNone(ctx_cache.get("ctx_key"))
            self.assertTrue(
                hasattr(ctx_cache._db, "conn") and ctx_cache._db.conn is not None
            )

        expected_cache_file = os.path.join(temp_dir_ctx.name, CACHE_FILENAME)
        self.assertTrue(os.path.exists(expected_cache_file))
        temp_dir_ctx.cleanup()

    def test_11_invalid_entry_format_in_db(self):
        """Test that malformed entries in the DB are handled gracefully (deleted)."""
        key = "malformed.local."

        self.cache._db[key] = {
            "nodata": True,
            "wrong_timestamp_field": "abc",
        }
        self.assertIsNone(
            self.cache.get(key),
            "Malformed entry should be treated as not found and deleted.",
        )
        self.assertIsNone(
            self.cache._db.get(key),
            "Malformed entry should be deleted from DB upon access.",
        )

        key2 = "bad_timestamp.local."
        self.cache._db[key2] = {
            "data": {"ip": "1.2.3.4"},
            "last_seen": "not_a_timestamp",
        }
        self.assertIsNone(
            self.cache.get(key2),
            "Entry with bad timestamp should be treated as not found and deleted.",
        )
        self.assertIsNone(
            self.cache._db.get(key2),
            "Entry with bad timestamp should be deleted from DB upon access.",
        )

    def test_11_cache_flushing_on_ttl_zero(self):
        pass


if __name__ == "__main__":
    unittest.main()
