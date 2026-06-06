"""DAG 2 of 2 — PostgreSQL → OpenSearch pipeline.

Responsibilities
----------------
* Read pending user IDs from migration_tracking (status IN ('NEW', 'FAILED')).
* Split them into bounded chunks and distribute across Celery workers via
  dynamic task mapping (sync_chunk.expand).
* Each chunk: marks users IN_PROGRESS → fetches Postgres rows → sub-batched
  _bulk index → concurrent _mget verification → batch-marks COMPLETED / FAILED.
* Summarise total indexed, verified, and errors across all chunks.

Prerequisite
------------
Run load_tracking_table DAG first (or trigger it via TriggerDagRunOperator).
This DAG reads from migration_tracking, so it is safe to re-run:
  - COMPLETED rows are skipped automatically.
  - FAILED rows are retried on the next run.
"""
from __future__ import annotations

import json as _json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    # This path is recognized by Pyright and pytest (when running from project root)
    from dags.pg_to_os_shared import (
        OS_INDEX_NAME,
        PG_SOURCE_TABLE,
        CHUNK_SIZE,
        MAX_CONCURRENT_CHUNKS,
        BULK_BATCH_SIZE,
        VERIFY_BATCH_SIZE,
        MAX_VERIFY_WORKERS,
        default_args,
        build_bulk_body,
        get_opensearch_client,
    )
except ModuleNotFoundError:
    # This path is used by Airflow at runtime (dags/ is in sys.path)
    from pg_to_os_shared import (  # type: ignore
        OS_INDEX_NAME,
        PG_SOURCE_TABLE,
        CHUNK_SIZE,
        MAX_CONCURRENT_CHUNKS,
        BULK_BATCH_SIZE,
        VERIFY_BATCH_SIZE,
        MAX_VERIFY_WORKERS,
        default_args,
        build_bulk_body,
        get_opensearch_client,
    )


# ── Task: compute chunk list from migration_tracking ──────────────────────────
@task
def get_pending_chunks() -> list[list[int]]:
    """
    Read migration_tracking for users with status NEW or FAILED (supports reruns),
    split the user IDs into bounded chunk lists, and return them for dynamic mapping.
    compute offset/limit chunk descriptors, and return them for dynamic mapping.

    Why read from migration_tracking instead of XCom from DAG 1?
    - Decouples scheduling: DAG 2 can run independently / be re-triggered.
    - Handles reruns: FAILED rows are automatically retried.
    - COMPLETED rows are never re-processed.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        rows = hook.get_records("""
            SELECT DISTINCT user_id
            FROM migration_tracking
            WHERE migration_status IN ('NEW', 'FAILED')
            ORDER BY user_id
        """)

        total = len(rows)
        if total == 0:
            print("No pending users in migration_tracking. Nothing to migrate.")
            return []

        all_user_ids = [r[0] for r in rows]
        chunks = [
            all_user_ids[i : i + CHUNK_SIZE]
            for i in range(0, total, CHUNK_SIZE)
        ]
        print(f"Found {total} pending users (NEW/FAILED). Chunks to process: {len(chunks)}.")
        return chunks

    except Exception as e:
        print(f"Error in get_pending_chunks: {e}")
        raise


# ── Task: sync one chunk ──────────────────────────────────────────────────────
@task(task_id="sync_chunk", max_active_tis_per_dag=MAX_CONCURRENT_CHUNKS)
def sync_chunk(chunk_user_ids: list[int]) -> dict:
    """
    For a given list of pending user_ids:

      Step 1 — Mark IN_PROGRESS in migration_tracking.
      Step 2 — Fetch user + address data via correlated subquery.
      Step 3 — Sub-batched _bulk insert (BULK_BATCH_SIZE rows per call).
      Step 4 — Concurrent _mget verification (VERIFY_BATCH_SIZE × MAX_VERIFY_WORKERS).
      Step 5 — Batch-update migration_tracking: COMPLETED or FAILED.
    """
    if not chunk_user_ids:
        return {"chunks_processed": 1, "docs_indexed": 0, "errors": 0}

    chunk_size_actual = len(chunk_user_ids)
    ids_list = ", ".join(str(uid) for uid in chunk_user_ids)

    # ── Step 1: resolve user IDs and mark IN_PROGRESS ─────────────────────────
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'IN_PROGRESS',
                updated_at       = NOW()
            WHERE user_id IN ({ids_list})
        """)
        print(f"Marked {chunk_size_actual} users as IN_PROGRESS.")

    except Exception as e:
        print(f"Error marking IN_PROGRESS: {e}")
        raise

    # ── Step 2: fetch full user + address data via correlated subquery ─────────
    try:
        # Correlated subquery: paginate users first, then fetch their addresses.
        # Avoids a GROUP BY full-table scan on the join.
        query = f"""
            SELECT
                u.id,
                u.name,
                u.email,
                u.status,
                u.created_at,
                (
                    SELECT json_agg(
                        (SELECT cols FROM (SELECT id, type, address, long, lati) AS cols)
                    )
                    FROM addresses
                    WHERE addresses.user_id = u.id
                ) AS address
            FROM {PG_SOURCE_TABLE} u
            WHERE u.id IN ({ids_list})
            ORDER BY u.id
        """
        rows = hook.get_records(query)
    except Exception as e:
        print(f"Error fetching Postgres records: {e}")
        raise

    if not rows:
        return {"chunks_processed": 1, "docs_indexed": 0, "errors": 0}

    # ── Step 3: sub-batched _bulk insert ──────────────────────────────────────
    client = get_opensearch_client()
    bulk_failed_doc_ids: set[str] = set()
    total_indexed = 0
    num_bulk_batches = math.ceil(len(rows) / BULK_BATCH_SIZE)

    for batch_idx in range(num_bulk_batches):
        batch_rows = rows[batch_idx * BULK_BATCH_SIZE : (batch_idx + 1) * BULK_BATCH_SIZE]
        try:
            bulk_body = build_bulk_body(batch_rows, OS_INDEX_NAME)
            response = client.bulk(body=bulk_body)
        except Exception as e:
            print(f"Bulk-batch {batch_idx}: error — {e}")
            raise

        if response.get("errors", False):
            for item in response.get("items", []):
                for _, details in item.items():
                    if details.get("status", 200) >= 300:
                        doc_id = details.get("_id")
                        reason = details.get("error", {}).get("reason", "unknown")
                        bulk_failed_doc_ids.add(str(doc_id))
                        print(f"  Batch {batch_idx} — doc {doc_id} failed: {reason}")

        batch_success = len(batch_rows) - len(
            [i for i in response.get("items", [])
             for _, d in i.items() if d.get("status", 200) >= 300]
        )
        total_indexed += batch_success
        print(
            f"Bulk-batch {batch_idx + 1}/{num_bulk_batches}: "
            f"{batch_success}/{len(batch_rows)} docs indexed."
        )

    if bulk_failed_doc_ids:
        raise RuntimeError(
            f"{len(bulk_failed_doc_ids)} documents failed bulk indexing."
        )

    print(f"Bulk-indexed {total_indexed} docs across {num_bulk_batches} batches.")

    # ── Step 4: concurrent batched _mget verification ──────────────────────────
    # Build expected address-type map from Postgres data
    expected_types_map: dict[int, list[str]] = {}
    for row in rows:
        uid = row[0]
        raw_addr = row[5]
        if isinstance(raw_addr, str):
            raw_addr = _json.loads(raw_addr)
        if raw_addr is None:
            raw_addr = []
        expected_types_map[uid] = sorted(
            set(addr.get("type") for addr in raw_addr if addr.get("type"))
        )

    all_user_ids = list(expected_types_map.keys())
    verify_batches = [
        all_user_ids[i : i + VERIFY_BATCH_SIZE]
        for i in range(0, len(all_user_ids), VERIFY_BATCH_SIZE)
    ]

    verified_ids: list[int] = []
    failed_ids: list[int] = []
    verification_failures: list[tuple] = []

    def _verify_batch(batch_user_ids: list[int]) -> list[tuple]:
        """_mget a batch of doc IDs and return (user_id, ok, reason) tuples."""
        results: list[tuple] = []
        try:
            mget_resp = client.mget(
                body={"ids": [str(uid) for uid in batch_user_ids]},
                index=OS_INDEX_NAME,
            )
        except Exception as exc:
            for uid in batch_user_ids:
                results.append((uid, False, f"_mget error: {exc}"))
            return results

        for doc in mget_resp.get("docs", []):
            uid = int(doc.get("_id", 0))
            if not doc.get("found", False):
                results.append((uid, False, "document not found in OpenSearch"))
                continue
            indexed_types = sorted(
                set(a.get("type") for a in doc.get("_source", {}).get("addresses", []) if a.get("type"))
            )
            expected = expected_types_map.get(uid, [])
            if indexed_types != expected:
                results.append((uid, False, f"type mismatch — expected {expected}, got {indexed_types}"))
            else:
                results.append((uid, True, "ok"))
        return results

    # Run _mget batches concurrently
    with ThreadPoolExecutor(max_workers=MAX_VERIFY_WORKERS) as executor:
        futures = {executor.submit(_verify_batch, batch): batch for batch in verify_batches}
        for future in as_completed(futures):
            for uid, ok, reason in future.result():
                if ok:
                    verified_ids.append(uid)
                else:
                    failed_ids.append(uid)
                    verification_failures.append((uid, reason))

    print(
        f"Verification done via {len(verify_batches)} _mget batch(es) "
        f"({MAX_VERIFY_WORKERS} workers) — verified={len(verified_ids)}, failed={len(failed_ids)}."
    )

    # ── Step 5: batch-update migration_tracking ────────────────────────────────
    if verified_ids:
        completed_list = ", ".join(str(uid) for uid in verified_ids)
        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'COMPLETED',
                updated_at       = NOW()
            WHERE user_id IN ({completed_list})
        """)
        print(f"Marked {len(verified_ids)} users as COMPLETED.")

    if failed_ids:
        failed_list = ", ".join(str(uid) for uid in failed_ids)
        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'FAILED',
                updated_at       = NOW()
            WHERE user_id IN ({failed_list})
        """)
        print(f"Marked {len(failed_ids)} users as FAILED.")
        for uid, reason in verification_failures:
            print(f"  - user_id {uid}: {reason}")

    error_count = len(verification_failures)
    print(f"Chunk done — indexed: {total_indexed}, verified: {len(verified_ids)}, errors: {error_count}.")
    return {
        "chunks_processed": 1,
        "docs_indexed": total_indexed,
        "errors": error_count,
    }


# ── Task: summarise all chunk results ─────────────────────────────────────────
@task
def summarise_sync(chunk_results: list[dict]) -> None:
    """Aggregate per-chunk stats and raise on any errors."""
    total_chunks = total_docs = total_errors = failed_chunks = 0

    for res in chunk_results:
        if not res:
            continue
        total_chunks += res.get("chunks_processed", 0)
        total_docs   += res.get("docs_indexed", 0)
        err           = res.get("errors", 0)
        total_errors += err
        if err > 0:
            failed_chunks += 1

    print("Sync summary:")
    print(f"  Total chunks processed : {total_chunks}")
    print(f"  Total documents indexed: {total_docs}")
    print(f"  Total errors           : {total_errors}")

    if total_errors == 0:
        print(f"SUCCESS: all {total_docs} documents indexed and verified.")
    else:
        print(f"Failed chunks: {failed_chunks}")
        raise RuntimeError(
            f"Sync completed with {total_errors} errors across {failed_chunks} chunks."
        )


# ── DAG 2: migrate_user_data ──────────────────────────────────────────────────
@dag(
    dag_id="migrate_user_data",
    default_args=default_args,
    description=(
        "Step 2 of 2: Read pending users from migration_tracking and bulk-index "
        "them into OpenSearch with per-user COMPLETED / FAILED lifecycle tracking."
    ),
    schedule=None,          # triggered manually or after load_tracking_table
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "opensearch", "migration", "sync"],
)
def migrate_user_data_dag():
    chunks_list = get_pending_chunks()
    results     = sync_chunk.expand(chunk_user_ids=chunks_list)
    summary     = summarise_sync(results)  # type: ignore[bad-argument-type]

    chunks_list >> results >> summary


# Instantiate
migrate_user_data = migrate_user_data_dag()
