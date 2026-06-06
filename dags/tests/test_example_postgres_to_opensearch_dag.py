import os
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import integers, text, lists, tuples, one_of, just, fixed_dictionaries, datetimes, none

# Fallback module import to support local execution pathways
try:
    import dags.example_postgres_to_opensearch_dag as dag_mod
except ModuleNotFoundError:
    import example_postgres_to_opensearch_dag as dag_mod

class TestGetOpenSearchClient(unittest.TestCase):
    @patch.object(dag_mod, 'OpenSearch')
    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self, mock_opensearch):
        dag_mod.get_opensearch_client()
        mock_opensearch.assert_called_once()
        config = mock_opensearch.call_args[1]
        self.assertEqual(config["hosts"], [{"host": "localhost", "port": 9200}])
        self.assertEqual(config["http_auth"], ("admin", "admin"))
        self.assertFalse(config["use_ssl"])
        self.assertNotIn("verify_certs", config)

    @patch.object(dag_mod, 'OpenSearch')
    @patch.dict(os.environ, {"OPENSEARCH_USE_SSL": "true"})
    def test_ssl_enabled(self, mock_opensearch):
        dag_mod.get_opensearch_client()
        config = mock_opensearch.call_args[1]
        self.assertTrue(config["use_ssl"])
        self.assertFalse(config["verify_certs"])

    def test_resolved_host_empty(self):
        with self.assertRaises(ValueError):
            dag_mod.get_client_config("", 9200, "admin", "admin", False)

    def test_port_boundaries(self):
        # 1 and 65535 are valid
        c1 = dag_mod.get_client_config("localhost", 1, "admin", "admin", False)
        self.assertEqual(c1["hosts"][0]["port"], 1)
        c2 = dag_mod.get_client_config("localhost", 65535, "admin", "admin", False)
        self.assertEqual(c2["hosts"][0]["port"], 65535)
        # 0 and 65536 are invalid
        with self.assertRaises(ValueError):
            dag_mod.get_client_config("localhost", 0, "admin", "admin", False)
        with self.assertRaises(ValueError):
            dag_mod.get_client_config("localhost", 65536, "admin", "admin", False)


class TestCreateOrVerifyIndex(unittest.TestCase):
    @patch.object(dag_mod, 'get_opensearch_client')
    def test_create_or_verify_index_exists(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_get_client.return_value = mock_client
        
        dag_mod.create_or_verify_index.function()
        
        mock_client.indices.exists.assert_called_once_with(index=dag_mod.OS_INDEX_NAME)
        mock_client.indices.create.assert_not_called()

    @patch.object(dag_mod, 'get_opensearch_client')
    def test_create_or_verify_index_not_exists(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_get_client.return_value = mock_client
        
        dag_mod.create_or_verify_index.function()
        
        mock_client.indices.exists.assert_called_once_with(index=dag_mod.OS_INDEX_NAME)
        mock_client.indices.create.assert_called_once_with(index=dag_mod.OS_INDEX_NAME, body=dag_mod.index_body)

    @patch.object(dag_mod, 'get_opensearch_client')
    def test_create_or_verify_index_exception(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.indices.exists.side_effect = Exception("API failure")
        mock_get_client.return_value = mock_client
        
        with self.assertRaises(Exception) as context:
            dag_mod.create_or_verify_index.function()
        self.assertIn("API failure", str(context.exception))


class TestPrepareChunks(unittest.TestCase):
    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_zero_rows(self, mock_pg_hook):
        """No eligible users → empty chunk list, no tracking insert."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []
        mock_pg_hook.return_value = mock_hook

        res = dag_mod.prepare_chunks.function()

        self.assertEqual(res, [])
        mock_hook.get_records.assert_called_once()
        mock_hook.run.assert_not_called()

    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_single_chunk(self, mock_pg_hook):
        """5 000 eligible users → 1 chunk; tracking rows inserted."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(i,) for i in range(1, 5001)]
        mock_pg_hook.return_value = mock_hook

        res = dag_mod.prepare_chunks.function()

        self.assertEqual(res, [{"offset": 0, "limit": 5000}])
        mock_hook.run.assert_called_once()

    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_exact_divisor(self, mock_pg_hook):
        """20 000 eligible users → 2 equal chunks."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(i,) for i in range(1, 20001)]
        mock_pg_hook.return_value = mock_hook

        res = dag_mod.prepare_chunks.function()

        self.assertEqual(res, [
            {"offset": 0, "limit": 10000},
            {"offset": 10000, "limit": 10000}
        ])

    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_remainder(self, mock_pg_hook):
        """25 000 eligible users → 2 full chunks + 1 remainder chunk."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(i,) for i in range(1, 25001)]
        mock_pg_hook.return_value = mock_hook

        res = dag_mod.prepare_chunks.function()

        self.assertEqual(res, [
            {"offset": 0, "limit": 10000},
            {"offset": 10000, "limit": 10000},
            {"offset": 20000, "limit": 5000}
        ])

    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_tracking_insert_is_idempotent(self, mock_pg_hook):
        """hook.run is called once with an INSERT … ON CONFLICT DO NOTHING statement."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(1,), (2,), (3,)]
        mock_pg_hook.return_value = mock_hook

        dag_mod.prepare_chunks.function()

        self.assertEqual(mock_hook.run.call_count, 1)
        sql_called = mock_hook.run.call_args[0][0]
        self.assertIn("ON CONFLICT", sql_called)
        self.assertIn("DO NOTHING", sql_called)

    @patch.object(dag_mod, 'PostgresHook')
    def test_prepare_chunks_exception(self, mock_pg_hook):
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = Exception("DB error")
        mock_pg_hook.return_value = mock_hook

        with self.assertRaises(Exception) as context:
            dag_mod.prepare_chunks.function()
        self.assertIn("DB error", str(context.exception))


class TestSyncChunk(unittest.TestCase):
    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_zero_rows(self, mock_pg_hook, mock_get_client):
        """When no user IDs are returned for the chunk window, skip and return zero counts."""
        mock_hook = MagicMock()
        # First get_records call returns user IDs; empty → early return
        mock_hook.get_records.return_value = []
        mock_pg_hook.return_value = mock_hook

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        chunk = {"offset": 0, "limit": 10000}
        res = dag_mod.sync_chunk.function(chunk)

        self.assertEqual(res, {"chunks_processed": 1, "docs_indexed": 0, "errors": 0})
        mock_client.bulk.assert_not_called()

    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_success(self, mock_pg_hook, mock_get_client):
        """Happy path: user indexed and verified via _mget → COMPLETED."""
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [
            [(1,)],  # eligible user IDs in this chunk
            [(1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6),
              [{"id": 1, "type": "permanent", "address": "mumbai - 400001",
                "long": 12.403, "lati": 2.494}])],  # full data row
        ]
        mock_pg_hook.return_value = mock_hook

        mock_client = MagicMock()
        mock_client.bulk.return_value = {"errors": False, "items": []}
        # _mget returns the doc for verification
        mock_client.mget.return_value = {
            "docs": [{
                "_id": "1",
                "found": True,
                "_source": {"addresses": [{"type": "permanent"}]}
            }]
        }
        mock_get_client.return_value = mock_client

        chunk = {"offset": 0, "limit": 10000}
        res = dag_mod.sync_chunk.function(chunk)

        self.assertEqual(res, {"chunks_processed": 1, "docs_indexed": 1, "errors": 0})
        mock_client.bulk.assert_called_once()
        mock_client.mget.assert_called_once()
        bulk_body = mock_client.bulk.call_args[1]["body"]
        self.assertEqual(bulk_body[0]["index"]["_id"], "1")
        self.assertEqual(bulk_body[1]["name"], "Alice")
        self.assertEqual(bulk_body[1]["addresses"][0]["address"], "mumbai - 400001")
        # Verify COMPLETED update was called
        update_calls = [str(call) for call in mock_hook.run.call_args_list]
        self.assertTrue(any("COMPLETED" in c for c in update_calls))

    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_verification_failure(self, mock_pg_hook, mock_get_client):
        """If OpenSearch doc is not found after indexing → error count=1, FAILED tracking."""
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [
            [(1,)],
            [(1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6),
              [{"id": 1, "type": "permanent", "address": "mumbai - 400001",
                "long": 12.403, "lati": 2.494}])],
        ]
        mock_pg_hook.return_value = mock_hook

        mock_client = MagicMock()
        mock_client.bulk.return_value = {"errors": False, "items": []}
        mock_client.mget.return_value = {
            "docs": [{"_id": "1", "found": False}]
        }
        mock_get_client.return_value = mock_client

        chunk = {"offset": 0, "limit": 10000}
        res = dag_mod.sync_chunk.function(chunk)

        self.assertEqual(res["errors"], 1)
        # FAILED status must be written back to tracking
        update_calls = [str(call) for call in mock_hook.run.call_args_list]
        self.assertTrue(any("FAILED" in c for c in update_calls))

    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_doc_errors(self, mock_pg_hook, mock_get_client):
        """Bulk index error for a doc (HTTP 400) → RuntimeError raised."""
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [
            [(1,)],
            [(1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6), [])],
        ]
        mock_pg_hook.return_value = mock_hook

        mock_client = MagicMock()
        mock_client.bulk.return_value = {
            "errors": True,
            "items": [
                {"index": {"_id": "1", "status": 400, "error": {"reason": "bad value mapping"}}}
            ]
        }
        mock_get_client.return_value = mock_client

        chunk = {"offset": 0, "limit": 10000}
        with self.assertRaises(RuntimeError) as context:
            dag_mod.sync_chunk.function(chunk)
        self.assertIn("failed bulk indexing", str(context.exception))

    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_postgres_exception(self, mock_pg_hook):
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = Exception("PG connection reset")
        mock_pg_hook.return_value = mock_hook

        chunk = {"offset": 0, "limit": 10000}
        with self.assertRaises(Exception) as context:
            dag_mod.sync_chunk.function(chunk)
        self.assertIn("PG connection reset", str(context.exception))

    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_sync_chunk_opensearch_exception(self, mock_pg_hook, mock_get_client):
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [
            [(1,)],
            [(1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6), [])],
        ]
        mock_pg_hook.return_value = mock_hook

        mock_client = MagicMock()
        mock_client.bulk.side_effect = Exception("OS connection Timeout")
        mock_get_client.return_value = mock_client

        chunk = {"offset": 0, "limit": 10000}
        with self.assertRaises(Exception) as context:
            dag_mod.sync_chunk.function(chunk)
        self.assertIn("OS connection Timeout", str(context.exception))


class TestSummariseSync(unittest.TestCase):
    def test_summarise_sync_success(self):
        results = [
            {"chunks_processed": 1, "docs_indexed": 10, "errors": 0},
            {"chunks_processed": 1, "docs_indexed": 5, "errors": 0}
        ]
        # Should not raise exception
        dag_mod.summarise_sync.function(results)

    def test_summarise_sync_failures(self):
        results = [
            {"chunks_processed": 1, "docs_indexed": 10, "errors": 0},
            {"chunks_processed": 1, "docs_indexed": 5, "errors": 1}
        ]
        with self.assertRaises(RuntimeError) as context:
            dag_mod.summarise_sync.function(results)
        self.assertIn("Sync completed with 1 errors across 1 chunks", str(context.exception))


class TestPropertyBasedTests(unittest.TestCase):

    # Feature: postgres-to-opensearch-dag, Property 1: Chunk specs are non-overlapping and cover all rows
    @settings(deadline=None)
    @given(
        total=integers(min_value=1, max_value=10_000_000),
        chunk_size=integers(min_value=1, max_value=10_000)
    )
    def test_p1_chunk_specs(self, total, chunk_size):
        specs = dag_mod.calculate_chunks(total, chunk_size)
        self.assertTrue(len(specs) > 0)
        
        # Verify contiguous & non-overlapping offset properties
        self.assertEqual(specs[0]["offset"], 0)
        for i in range(1, len(specs)):
            expected_offset = specs[i-1]["offset"] + specs[i-1]["limit"]
            self.assertEqual(specs[i]["offset"], expected_offset)
            
        # Verify complete coverage
        self.assertEqual(sum(s["limit"] for s in specs), total)
        
        # Verify remainder limit property
        remainder = total % chunk_size
        if remainder != 0:
            self.assertEqual(specs[-1]["limit"], remainder)
        else:
            self.assertEqual(specs[-1]["limit"], chunk_size)

    # Feature: postgres-to-opensearch-dag, Property 2: Empty-table short-circuit
    @settings(deadline=None)
    @given(chunk_size=integers(min_value=1, max_value=10_000))
    def test_p2_empty_table(self, chunk_size):
        specs = dag_mod.calculate_chunks(0, chunk_size)
        self.assertEqual(specs, [])

    # Feature: postgres-to-opensearch-dag, Property 3: Document ID is deterministic and stateless
    @settings(deadline=None)
    @given(pk=one_of(integers(), text()))
    def test_p3_deterministic_id(self, pk):
        row1 = (pk, "Alice", "alice@example.com", "active", "2026-06-06")
        row2 = (pk, "Bob", "bob@example.com", "inactive", "2026-06-05")
        
        id1 = dag_mod.derive_id(row1)
        id2 = dag_mod.derive_id(row2)
        
        self.assertEqual(id1, id2)
        self.assertEqual(id1, str(pk))

    # Feature: postgres-to-opensearch-dag, Property 4: Idempotent bulk action type
    @settings(deadline=None)
    @given(rows=lists(tuples(
        integers(),
        text(),
        text(),
        text(),
        one_of(datetimes(), none()),
        lists(fixed_dictionaries({
            "id": integers(),
            "type": text(),
            "address": text(),
            "long": one_of(integers(), none()),
            "lati": one_of(integers(), none())
        }))
    ), min_size=1))
    def test_p4_action_type(self, rows):
        body = dag_mod.build_bulk_body(rows, "users")
        # Every action description (odd index items in body list) must be of "index" type
        for i in range(0, len(body), 2):
            action_descriptor = body[i]
            self.assertIn("index", action_descriptor)
            self.assertNotIn("create", action_descriptor)
            self.assertNotIn("update", action_descriptor)
            self.assertNotIn("delete", action_descriptor)

    # Feature: postgres-to-opensearch-dag, Property 5: Chunk result dict always contains required keys
    @settings(deadline=None)
    @given(chunk=fixed_dictionaries({"offset": integers(min_value=0), "limit": integers(min_value=0)}))
    @patch.object(dag_mod, 'get_opensearch_client')
    @patch.object(dag_mod, 'PostgresHook')
    def test_p5_result_shape(self, mock_pg_hook, mock_get_client, chunk):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []
        mock_pg_hook.return_value = mock_hook
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        res = dag_mod.sync_chunk.function(chunk)
        self.assertIsInstance(res, dict)
        self.assertIn("chunks_processed", res)
        self.assertIn("docs_indexed", res)
        self.assertIn("errors", res)
        self.assertTrue(res["chunks_processed"] >= 0)
        self.assertTrue(res["docs_indexed"] >= 0)
        self.assertTrue(res["errors"] >= 0)

    # Feature: postgres-to-opensearch-dag, Property 6: Port validation rejects out-of-range values
    @settings(deadline=None)
    @given(port=one_of(integers(max_value=0), integers(min_value=65536)))
    def test_p6_port_validation(self, port):
        with self.assertRaises(ValueError):
            dag_mod.get_client_config("localhost", port, "admin", "admin", False)

    # Feature: postgres-to-opensearch-dag, Property 7: OpenSearch client defaults are applied for empty/absent env vars
    @settings(deadline=None)
    @given(
        host=one_of(just(""), just(None)),
        port=one_of(just(""), just(None)),
        username=one_of(just(""), just(None)),
        password=one_of(just(""), just(None))
    )
    @patch.object(dag_mod, 'OpenSearch')
    def test_p7_client_defaults(self, mock_opensearch, host, port, username, password):
        def env_side_effect(key, default=None):
            if key == "OPENSEARCH_HOST":
                return host
            elif key == "OPENSEARCH_PORT":
                return port
            elif key == "OPENSEARCH_USERNAME":
                return username
            elif key == "OPENSEARCH_PASSWORD":
                return password
            elif key == "OPENSEARCH_USE_SSL":
                return "false"
            return default
            
        with patch('os.getenv', side_effect=env_side_effect):
            dag_mod.get_opensearch_client()
            mock_opensearch.assert_called_once()
            config = mock_opensearch.call_args[1]
            self.assertEqual(config["hosts"], [{"host": "localhost", "port": 9200}])
            self.assertEqual(config["http_auth"], ("admin", "admin"))
