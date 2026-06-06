"""Unit tests for migrate_user_data DAG.

Covers: get_pending_chunks, sync_chunk (all steps), summarise_sync.
"""
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis.strategies import integers, fixed_dictionaries, lists

try:
    import dags.migrate_user_data as dag_mod
    import dags.pg_to_os_shared as shared
except ModuleNotFoundError:
    import migrate_user_data as dag_mod
    import pg_to_os_shared as shared


# ── get_pending_chunks ────────────────────────────────────────────────────────

class TestGetPendingChunks(unittest.TestCase):
    @patch.object(dag_mod, "PostgresHook")
    def test_no_pending_users_returns_empty(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook

        res = dag_mod.get_pending_chunks.function()

        self.assertEqual(res, [])

    @patch.object(dag_mod, "PostgresHook")
    def test_returns_correct_chunks(self, mock_hook_cls):
        hook = MagicMock()
        # 150 pending users → 2 chunks with CHUNK_SIZE=100
        hook.get_records.return_value = [(i,) for i in range(1, 151)]
        mock_hook_cls.return_value = hook

        res = dag_mod.get_pending_chunks.function()

        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], list(range(1, 101)))
        self.assertEqual(res[1], list(range(101, 151)))

    @patch.object(dag_mod, "PostgresHook")
    def test_queries_new_and_failed_status(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook

        dag_mod.get_pending_chunks.function()

        sql = hook.get_records.call_args[0][0]
        self.assertIn("NEW", sql)
        self.assertIn("FAILED", sql)

    @patch.object(dag_mod, "PostgresHook")
    def test_db_exception_propagates(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.side_effect = Exception("DB timeout")
        mock_hook_cls.return_value = hook

        with self.assertRaises(Exception, msg="DB timeout"):
            dag_mod.get_pending_chunks.function()


# ── sync_chunk ────────────────────────────────────────────────────────────────

class TestSyncChunk(unittest.TestCase):

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_empty_chunk_window_skips_indexing(self, mock_hook_cls, mock_get_client):
        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook

        client = MagicMock()
        mock_get_client.return_value = client

        res = dag_mod.sync_chunk.function([])

        self.assertEqual(res, {"chunks_processed": 1, "docs_indexed": 0, "errors": 0})
        client.bulk.assert_not_called()

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_happy_path_completed(self, mock_hook_cls, mock_get_client):
        """All docs indexed and verified → COMPLETED status written, errors=0."""
        hook = MagicMock()
        hook.get_records.return_value = [
            (1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6),
              [{"id": 1, "type": "permanent", "address": "mumbai - 400001",
                "long": 72.88, "lati": 19.08}])
        ]
        mock_hook_cls.return_value = hook

        client = MagicMock()
        client.bulk.return_value = {"errors": False, "items": []}
        client.mget.return_value = {
            "docs": [{"_id": "1", "found": True,
                      "_source": {"addresses": [{"type": "permanent"}]}}]
        }
        mock_get_client.return_value = client

        res = dag_mod.sync_chunk.function([1])

        self.assertEqual(res["errors"], 0)
        self.assertEqual(res["docs_indexed"], 1)
        client.bulk.assert_called_once()
        client.mget.assert_called_once()
        # COMPLETED update must be issued
        calls = [str(c) for c in hook.run.call_args_list]
        self.assertTrue(any("COMPLETED" in c for c in calls))
        # No FAILED update
        self.assertFalse(any("FAILED" in c for c in calls))

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_verification_failure_writes_failed_status(self, mock_hook_cls, mock_get_client):
        """Doc indexed but not found in verification → FAILED status, errors=1."""
        hook = MagicMock()
        hook.get_records.return_value = [
            (1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6),
              [{"id": 1, "type": "permanent", "address": "mumbai - 400001",
                "long": 72.88, "lati": 19.08}])
        ]
        mock_hook_cls.return_value = hook

        client = MagicMock()
        client.bulk.return_value = {"errors": False, "items": []}
        client.mget.return_value = {"docs": [{"_id": "1", "found": False}]}
        mock_get_client.return_value = client

        res = dag_mod.sync_chunk.function([1])

        self.assertEqual(res["errors"], 1)
        calls = [str(c) for c in hook.run.call_args_list]
        self.assertTrue(any("FAILED" in c for c in calls))
        self.assertFalse(any("COMPLETED" in c for c in calls))

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_bulk_doc_error_raises_runtime_error(self, mock_hook_cls, mock_get_client):
        """A doc-level bulk failure (HTTP 400) must raise RuntimeError."""
        hook = MagicMock()
        hook.get_records.return_value = [
            (1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6), [])
        ]
        mock_hook_cls.return_value = hook

        client = MagicMock()
        client.bulk.return_value = {
            "errors": True,
            "items": [{"index": {"_id": "1", "status": 400,
                                  "error": {"reason": "mapper error"}}}],
        }
        mock_get_client.return_value = client

        with self.assertRaises(RuntimeError) as ctx:
            dag_mod.sync_chunk.function([1])
        self.assertIn("failed bulk indexing", str(ctx.exception))

    @patch.object(dag_mod, "PostgresHook")
    def test_postgres_exception_in_step1_propagates(self, mock_hook_cls):
        hook = MagicMock()
        hook.get_records.side_effect = Exception("PG reset")
        mock_hook_cls.return_value = hook

        with self.assertRaises(Exception, msg="PG reset"):
            dag_mod.sync_chunk.function([1])

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_opensearch_bulk_exception_propagates(self, mock_hook_cls, mock_get_client):
        hook = MagicMock()
        hook.get_records.return_value = [
            (1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6), [])
        ]
        mock_hook_cls.return_value = hook

        client = MagicMock()
        client.bulk.side_effect = Exception("OS timeout")
        mock_get_client.return_value = client

        with self.assertRaises(Exception, msg="OS timeout"):
            dag_mod.sync_chunk.function([1])

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_step1_updates_migration_tracking(self, mock_hook_cls, mock_get_client):
        """Step 1 must mark the exact provided IDs as IN_PROGRESS in migration_tracking."""
        hook = MagicMock()
        # Mock empty returns for step 2 to stop early
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook
        mock_get_client.return_value = MagicMock()

        dag_mod.sync_chunk.function([1, 2, 3])

        calls = [str(c) for c in hook.run.call_args_list]
        self.assertTrue(any("UPDATE migration_tracking" in c for c in calls))
        self.assertTrue(any("IN_PROGRESS" in c for c in calls))
        self.assertTrue(any("IN (1, 2, 3)" in c for c in calls))

    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_address_type_mismatch_marks_failed(self, mock_hook_cls, mock_get_client):
        """Address type mismatch between Postgres and OpenSearch → FAILED."""
        hook = MagicMock()
        hook.get_records.return_value = [
            (1, "Alice", "alice@example.com", "active", datetime(2026, 6, 6),
              [{"id": 1, "type": "permanent", "address": "x", "long": 0, "lati": 0},
               {"id": 2, "type": "office",    "address": "y", "long": 0, "lati": 0}])
        ]
        mock_hook_cls.return_value = hook

        client = MagicMock()
        client.bulk.return_value = {"errors": False, "items": []}
        # OpenSearch only has one type, not two
        client.mget.return_value = {
            "docs": [{"_id": "1", "found": True,
                      "_source": {"addresses": [{"type": "permanent"}]}}]
        }
        mock_get_client.return_value = client

        res = dag_mod.sync_chunk.function([1])

        self.assertEqual(res["errors"], 1)
        calls = [str(c) for c in hook.run.call_args_list]
        self.assertTrue(any("FAILED" in c for c in calls))


# ── summarise_sync ────────────────────────────────────────────────────────────

class TestSummariseSync(unittest.TestCase):
    def test_all_success_does_not_raise(self):
        results = [
            {"chunks_processed": 1, "docs_indexed": 100, "errors": 0},
            {"chunks_processed": 1, "docs_indexed": 50,  "errors": 0},
        ]
        dag_mod.summarise_sync.function(results)  # no exception

    def test_partial_failure_raises_runtime_error(self):
        results = [
            {"chunks_processed": 1, "docs_indexed": 100, "errors": 0},
            {"chunks_processed": 1, "docs_indexed": 50,  "errors": 3},
        ]
        with self.assertRaises(RuntimeError) as ctx:
            dag_mod.summarise_sync.function(results)
        self.assertIn("3 errors", str(ctx.exception))

    def test_none_results_are_skipped(self):
        results = [None, {"chunks_processed": 1, "docs_indexed": 10, "errors": 0}]
        dag_mod.summarise_sync.function(results)  # no exception


# ── Property-based ────────────────────────────────────────────────────────────

class TestPropertyBased(unittest.TestCase):

    @settings(deadline=None)
    @given(chunk=lists(integers(min_value=1, max_value=1000), max_size=100))
    @patch.object(dag_mod, "get_opensearch_client")
    @patch.object(dag_mod, "PostgresHook")
    def test_result_always_has_required_keys(self, mock_hook_cls, mock_get_client, chunk):
        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_cls.return_value = hook
        mock_get_client.return_value = MagicMock()

        res = dag_mod.sync_chunk.function(chunk)

        self.assertIsInstance(res, dict)
        for key in ("chunks_processed", "docs_indexed", "errors"):
            self.assertIn(key, res)
            self.assertGreaterEqual(res[key], 0)


if __name__ == "__main__":
    unittest.main()
