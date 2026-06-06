"""Unit tests for load_tracking_table DAG.

Covers: verify_postgres_schema, create_or_verify_index, prepare_chunks,
and the shared pure helpers (calculate_chunks, derive_id, build_bulk_body,
get_client_config, get_opensearch_client).
"""
import os
import unittest
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis.strategies import integers, text, lists, tuples, fixed_dictionaries, datetimes, none, one_of

try:
    import dags.load_tracking_table as dag_mod
    import dags.pg_to_os_shared as shared
except ModuleNotFoundError:
    import load_tracking_table as dag_mod
    import pg_to_os_shared as shared


# ── Shared helpers ─────────────────────────────────────────────────────────────

class TestGetClientConfig(unittest.TestCase):
    def test_empty_host_raises(self):
        with self.assertRaises(ValueError):
            shared.get_client_config("", 9200, "admin", "admin", False)

    def test_port_boundaries(self):
        c1 = shared.get_client_config("localhost", 1, "admin", "admin", False)
        self.assertEqual(c1["hosts"][0]["port"], 1)
        c2 = shared.get_client_config("localhost", 65535, "admin", "admin", False)
        self.assertEqual(c2["hosts"][0]["port"], 65535)
        with self.assertRaises(ValueError):
            shared.get_client_config("localhost", 0, "admin", "admin", False)
        with self.assertRaises(ValueError):
            shared.get_client_config("localhost", 65536, "admin", "admin", False)


class TestGetOpenSearchClient(unittest.TestCase):
    @patch.object(shared, "OpenSearch")
    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self, mock_os):
        shared.get_opensearch_client()
        config = mock_os.call_args[1]
        self.assertEqual(config["hosts"], [{"host": "localhost", "port": 9200}])
        self.assertEqual(config["http_auth"], ("admin", "admin"))
        self.assertFalse(config["use_ssl"])

    @patch.object(shared, "OpenSearch")
    @patch.dict(os.environ, {"OPENSEARCH_USE_SSL": "true"})
    def test_ssl_enabled(self, mock_os):
        shared.get_opensearch_client()
        config = mock_os.call_args[1]
        self.assertTrue(config["use_ssl"])
        self.assertFalse(config["verify_certs"])


# ── create_or_verify_index ────────────────────────────────────────────────────

class TestCreateOrVerifyIndex(unittest.TestCase):
    @patch.object(dag_mod, "get_opensearch_client")
    def test_index_already_exists(self, mock_get):
        client = MagicMock()
        client.indices.exists.return_value = True
        mock_get.return_value = client
        dag_mod.create_or_verify_index.function()
        client.indices.create.assert_not_called()

    @patch.object(dag_mod, "get_opensearch_client")
    def test_index_created_when_missing(self, mock_get):
        client = MagicMock()
        client.indices.exists.return_value = False
        mock_get.return_value = client
        dag_mod.create_or_verify_index.function()
        client.indices.create.assert_called_once_with(
            index=shared.OS_INDEX_NAME, body=shared.index_body
        )

    @patch.object(dag_mod, "get_opensearch_client")
    def test_exception_propagates(self, mock_get):
        client = MagicMock()
        client.indices.exists.side_effect = Exception("API down")
        mock_get.return_value = client
        with self.assertRaises(Exception, msg="API down"):
            dag_mod.create_or_verify_index.function()


# ── get_eligible_user_chunks ──────────────────────────────────────────────────

class TestGetEligibleUserChunks(unittest.TestCase):
    @patch.object(dag_mod, "PostgresHook")
    def test_zero_eligible_users_returns_empty(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook

        res = dag_mod.get_eligible_user_chunks.function()
        self.assertEqual(res, [])

    @patch.object(dag_mod, "PostgresHook")
    def test_multiple_chunks_exact_divisor(self, mock_hook_cls):
        hook = MagicMock()
        # 200 users = 2 chunks of 100
        hook.get_records.return_value = [(i,) for i in range(1, 2 * shared.CHUNK_SIZE + 1)]
        mock_hook_cls.return_value = hook

        res = dag_mod.get_eligible_user_chunks.function()

        self.assertEqual(len(res), 2)
        self.assertEqual(len(res[0]), shared.CHUNK_SIZE)
        self.assertEqual(res[0][0], 1)
        self.assertEqual(res[1][0], 101)

    @patch.object(dag_mod, "PostgresHook")
    def test_db_exception_propagates(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.side_effect = Exception("DB error")
        mock_hook_cls.return_value = hook

        with self.assertRaises(Exception, msg="DB error"):
            dag_mod.get_eligible_user_chunks.function()

# ── prepare_chunk ─────────────────────────────────────────────────────────────

class TestPrepareChunk(unittest.TestCase):
    @patch.object(dag_mod, "PostgresHook")
    def test_empty_chunk_returns_zero(self, mock_hook_cls):
        res = dag_mod.prepare_chunk.function([])
        self.assertEqual(res, {"users": 0, "addresses_inserted": 0})
        mock_hook_cls.assert_not_called()

    @patch("time.sleep")
    @patch.object(dag_mod, "PostgresHook")
    def test_retry_recovery_succeeds_on_second_attempt(self, mock_hook_cls, mock_sleep):
        hook = MagicMock()
        # 2 users, 2 addresses
        hook.get_records.return_value = [(1, 10, 'permanent'), (2, 20, 'office')]
        mock_hook_cls.return_value = hook
        
        # Fail the first insert, succeed on the second
        hook.run.side_effect = [Exception("Transient DB Lock"), None]

        res = dag_mod.prepare_chunk.function([1, 2])

        self.assertEqual(res, {"users": 2, "addresses_inserted": 2})
        # hook.run called twice (1 failure, 1 success)
        self.assertEqual(hook.run.call_count, 2)
        # sleep called once for 5 seconds
        mock_sleep.assert_called_once_with(5)

    @patch("time.sleep")
    @patch.object(dag_mod, "PostgresHook")
    def test_retry_exhaustion_raises_exception(self, mock_hook_cls, mock_sleep):
        hook = MagicMock()
        hook.get_records.return_value = [(1, 10, 'permanent')]
        mock_hook_cls.return_value = hook
        
        # Fail all 3 attempts
        hook.run.side_effect = Exception("Persistent DB Error")

        with self.assertRaises(Exception, msg="Persistent DB Error"):
            dag_mod.prepare_chunk.function([1])

        # hook.run called 3 times, sleep called 2 times
        self.assertEqual(hook.run.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


# ── Property-based tests (shared helpers) ─────────────────────────────────────

class TestPropertyBased(unittest.TestCase):



    @settings(deadline=None)
    @given(pk=one_of(integers(), text()))
    def test_p3_document_id_is_deterministic(self, pk):
        row1 = (pk, "Alice", "a@x.com", "active", "2026-01-01", [])
        row2 = (pk, "Bob",   "b@x.com", "inactive", None,        [])
        self.assertEqual(shared.derive_id(row1), shared.derive_id(row2))
        self.assertEqual(shared.derive_id(row1), str(pk))

    @settings(deadline=None)
    @given(rows=lists(tuples(
        integers(), text(), text(), text(),
        one_of(datetimes(), none()),
        lists(fixed_dictionaries({
            "id": integers(), "type": text(), "address": text(),
            "long": one_of(integers(), none()), "lati": one_of(integers(), none()),
        })),
    ), min_size=1))
    def test_p4_bulk_body_always_uses_index_action(self, rows):
        body = shared.build_bulk_body(rows, "users")
        for i in range(0, len(body), 2):
            self.assertIn("index", body[i])
            self.assertNotIn("create", body[i])
            self.assertNotIn("update", body[i])
            self.assertNotIn("delete", body[i])

    @settings(deadline=None)
    @given(port=one_of(integers(max_value=0), integers(min_value=65536)))
    def test_p5_invalid_port_raises_value_error(self, port):
        with self.assertRaises(ValueError):
            shared.get_client_config("localhost", port, "admin", "admin", False)

    @settings(deadline=None)
    @given(
        host=one_of(integers(), text()).filter(lambda x: str(x) != ""),
        port=integers(min_value=1, max_value=65535),
    )
    def test_p6_valid_config_includes_host_and_port(self, host, port):
        cfg = shared.get_client_config(str(host), port, "u", "p", False)
        self.assertEqual(cfg["hosts"][0]["host"], str(host))
        self.assertEqual(cfg["hosts"][0]["port"], port)


if __name__ == "__main__":
    unittest.main()
