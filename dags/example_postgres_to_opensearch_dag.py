from __future__ import annotations

import os
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from opensearchpy import OpenSearch

# Constants
PG_SOURCE_TABLE = "users"
OS_INDEX_NAME = "users"
CHUNK_SIZE = 100
MAX_CONCURRENT_CHUNKS = 3
BULK_BATCH_SIZE = 100        # rows per _bulk API call within a chunk
VERIFY_BATCH_SIZE = 100      # doc IDs per _mget API call during verification
MAX_VERIFY_WORKERS = 4       # concurrent _mget threads during verification

# Default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Index mapping body
index_body = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "keyword"},
            "email": {"type": "keyword"},
            "status": {"type": "keyword"},
            "created_at": {"type": "date"},
            "addresses": {
                "type": "nested",
                "properties": {
                    "id": {"type": "integer"},
                    "type": {"type": "keyword"},
                    "address": {"type": "text"},
                    "long": {"type": "double"},
                    "lati": {"type": "double"}
                }
            }
        }
    }
}

# Pure Helpers
def calculate_chunks(total: int, chunk_size: int) -> list[dict]:
    if total <= 0:
        return []
    num_chunks = math.ceil(total / chunk_size)
    return [
        {
            "offset": i * chunk_size,
            "limit": min(chunk_size, total - i * chunk_size)
        }
        for i in range(num_chunks)
    ]

def derive_id(row: tuple) -> str:
    return str(row[0])

def build_bulk_body(rows: list, index_name: str) -> list:
    import json
    body = []
    for row in rows:
        action = {"index": {"_index": index_name, "_id": derive_id(row)}}
        addresses = row[5]
        if isinstance(addresses, str):
            addresses = json.loads(addresses)
        if addresses is None:
            addresses = []
        doc = {
            "id": row[0],
            "name": row[1],
            "email": row[2],
            "status": row[3],
            "created_at": row[4].isoformat() if row[4] is not None else None,
            "addresses": addresses
        }
        body.append(action)
        body.append(doc)
    return body

def get_client_config(host, port, username, password, use_ssl) -> dict:
    if not host:
        raise ValueError("OpenSearch host cannot be empty")
    try:
        port_int = int(port)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid OpenSearch port: {port}")
    if port_int < 1 or port_int > 65535:
        raise ValueError(f"OpenSearch port must be in range 1-65535, got: {port_int}")

    config = {
        "hosts": [{"host": host, "port": port_int}],
        "http_auth": (username, password),
        "use_ssl": use_ssl,
        "timeout": 30
    }
    if use_ssl:
        config["verify_certs"] = False
    return config

def get_opensearch_client() -> OpenSearch:
    host = os.getenv("OPENSEARCH_HOST")
    if host is None or host == "":
        host = "localhost"

    port_raw = os.getenv("OPENSEARCH_PORT")
    if port_raw is None or port_raw == "":
        port = 9200
    else:
        port = port_raw

    username = os.getenv("OPENSEARCH_USERNAME")
    if username is None or username == "":
        username = "admin"

    password = os.getenv("OPENSEARCH_PASSWORD")
    if password is None or password == "":
        password = "admin"

    use_ssl_raw = os.getenv("OPENSEARCH_USE_SSL", "false")
    use_ssl = use_ssl_raw.lower() in ("true", "1", "yes")

    config = get_client_config(host, port, username, password, use_ssl)
    return OpenSearch(**config)

# Task definitions at module level for unit testing
@task
def verify_postgres_schema():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                status VARCHAR(20) NOT NULL DEFAULT 'active',
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        hook.run("""
            INSERT INTO users (name, email, status, created_at)
            SELECT
                'User ' || i,
                'user.' || i || '@example.com',
                CASE WHEN i % 10 = 0 THEN 'inactive' WHEN i % 15 = 0 THEN 'suspended' ELSE 'active' END,
                NOW() - (i || ' minutes')::interval
            FROM generate_series(1, 1000) AS i
            ON CONFLICT (email) DO NOTHING;
        """)

        hook.run("""
            CREATE TABLE IF NOT EXISTS addresses (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                type VARCHAR(50) NOT NULL,
                address VARCHAR(255) NOT NULL,
                long DOUBLE PRECISION,
                lati DOUBLE PRECISION
            );
        """)

        hook.run("""
            INSERT INTO addresses (user_id, type, address, long, lati)
            SELECT
                u.id,
                addr.type,
                (ARRAY[
                    'mumbai - 400001',
                    'bangalore - 560032',
                    'delhi - 110001',
                    'chennai - 600001',
                    'hyderabad - 500001',
                    'kolkata - 700001',
                    'pune - 411001',
                    'ahmedabad - 380001',
                    'jaipur - 302001',
                    'lucknow - 226001'
                ])[1 + ((u.id + addr.idx) % 10)],
                (ARRAY[72.8777, 77.5946, 77.2090, 80.2707, 78.4867, 88.3639, 73.8567, 72.5714, 75.7873, 80.9462])[1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001,
                (ARRAY[19.0760, 12.9716, 28.6139, 13.0827, 17.3850, 22.5726, 18.5204, 23.0225, 26.9124, 26.8467])[1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001
            FROM users u
            CROSS JOIN LATERAL (
                SELECT 'permanent' AS type, 1 AS idx
                UNION ALL
                SELECT 'office' AS type, 2 AS idx
                UNION ALL
                -- Only generate a 3rd address ('temporary') for users where u.id % 2 = 0
                SELECT 'temporary' AS type, 3 AS idx
                WHERE u.id % 2 = 0
            ) AS addr
            WHERE NOT EXISTS (SELECT 1 FROM addresses LIMIT 1);
        """)

        # Create migration tracking table to capture per-user migration lifecycle
        hook.run("""
            CREATE TABLE IF NOT EXISTS migration_tracking (
                user_id          INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                migration_status VARCHAR(20) NOT NULL DEFAULT 'NEW',
                created_at       TIMESTAMP  NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMP  NOT NULL DEFAULT NOW()
            );
        """)

        print("Postgres schema, seed records, and migration_tracking table verified successfully.")
    except Exception as e:
        print(f"Error verifying Postgres schema/seed records: {e}")
        raise

@task
def create_or_verify_index():
    try:
        client = get_opensearch_client()
        if not client.indices.exists(index=OS_INDEX_NAME):
            client.indices.create(index=OS_INDEX_NAME, body=index_body)
            print(f"Created index {OS_INDEX_NAME} with mapping successfully.")
        else:
            print(f"Index {OS_INDEX_NAME} already exists. Skipping creation.")
    except Exception as e:
        print(f"Error checking/creating index {OS_INDEX_NAME}: {e}")
        raise

@task
def prepare_chunks() -> list[dict]:
    """
    1. Query all eligible users (those with at least one address) ordered by id.
    2. Batch-insert every user_id into migration_tracking with status NEW
       (ON CONFLICT DO NOTHING makes this idempotent across re-runs).
    3. Return a list of offset/limit chunk descriptors for dynamic task mapping.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Fetch all user ids that have at least one address, ordered for stable pagination
        eligible_rows = hook.get_records("""
            SELECT DISTINCT u.id
            FROM users u
            WHERE EXISTS (SELECT 1 FROM addresses a WHERE a.user_id = u.id)
            ORDER BY u.id
        """)

        total = len(eligible_rows)
        if total == 0:
            print(f"No eligible users found in {PG_SOURCE_TABLE}. Nothing to migrate.")
            return []

        # Batch-insert tracking rows with status NEW (idempotent)
        values_clause = ", ".join(f"({row[0]}, 'NEW')" for row in eligible_rows)
        hook.run(f"""
            INSERT INTO migration_tracking (user_id, migration_status)
            VALUES {values_clause}
            ON CONFLICT (user_id) DO NOTHING
        """)
        print(f"Inserted/verified {total} NEW rows in migration_tracking.")

        chunks = calculate_chunks(total, CHUNK_SIZE)
        print(f"Total eligible users: {total}. Total chunks: {len(chunks)}.")
        return chunks
    except Exception as e:
        print(f"Error in prepare_chunks: {e}")
        raise

@task(task_id="sync_chunk", max_active_tis_per_dag=MAX_CONCURRENT_CHUNKS)
def sync_chunk(chunk: dict) -> dict:
    """
    For a given offset/limit window:
      1. Mark the users in this chunk as IN_PROGRESS in migration_tracking.
      2. Fetch user + address data using a correlated subquery (no GROUP BY / full-scan).
      3. Bulk-index documents into OpenSearch.
      4. Verify each user's document exists in OpenSearch with all expected address types.
      5. Mark verified users as COMPLETED in migration_tracking.
    """
    import json as _json
    offset = chunk.get("offset", 0)
    limit = chunk.get("limit", CHUNK_SIZE)
    chunk_num = offset // CHUNK_SIZE

    # ── Step 1: fetch chunk user IDs and mark IN_PROGRESS ──────────────────
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        id_rows = hook.get_records(f"""
            SELECT DISTINCT u.id
            FROM users u
            WHERE EXISTS (SELECT 1 FROM addresses a WHERE a.user_id = u.id)
            ORDER BY u.id
            LIMIT {limit} OFFSET {offset}
        """)

        if not id_rows:
            print(f"Chunk {chunk_num}: no users found at offset {offset}. Skipping.")
            return {"chunks_processed": 1, "docs_indexed": 0, "errors": 0}

        chunk_user_ids = [r[0] for r in id_rows]
        ids_list = ", ".join(str(uid) for uid in chunk_user_ids)

        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'IN_PROGRESS',
                updated_at       = NOW()
            WHERE user_id IN ({ids_list})
        """)
        print(f"Chunk {chunk_num}: marked {len(chunk_user_ids)} users as IN_PROGRESS.")
    except Exception as e:
        print(f"Chunk {chunk_num}: error fetching IDs or marking IN_PROGRESS: {e}")
        raise

    # ── Step 2: fetch full user + address data via correlated subquery ──────
    try:
        # Correlated subquery avoids GROUP BY full-table scan.
        # Only the chunked users are driven through the outer query; addresses are
        # fetched per-user via the inner correlated SELECT.
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
        print(f"Chunk {chunk_num}: error fetching Postgres records (offset={offset}, limit={limit}): {e}")
        raise

    if not rows:
        return {"chunks_processed": 1, "docs_indexed": 0, "errors": 0}

    # ── Step 3: sub-batched bulk-index into OpenSearch ─────────────────────────
    # Split rows into batches of BULK_BATCH_SIZE so each _bulk call is bounded.
    # Individual document errors are collected; a full batch exception aborts the chunk.
    client = get_opensearch_client()
    bulk_failed_doc_ids: set[str] = set()   # doc IDs that failed in any batch
    total_indexed = 0
    num_bulk_batches = math.ceil(len(rows) / BULK_BATCH_SIZE)

    for batch_idx in range(num_bulk_batches):
        batch_rows = rows[batch_idx * BULK_BATCH_SIZE : (batch_idx + 1) * BULK_BATCH_SIZE]
        try:
            bulk_body = build_bulk_body(batch_rows, OS_INDEX_NAME)
            response = client.bulk(body=bulk_body)
        except Exception as e:
            print(f"Chunk {chunk_num}, bulk-batch {batch_idx}: error executing OpenSearch bulk write: {e}")
            raise

        if response.get("errors", False):
            for item in response.get("items", []):
                for action_type, details in item.items():
                    if details.get("status", 200) >= 300:
                        doc_id = details.get("_id")
                        error_reason = details.get("error", {}).get("reason", "unknown error")
                        bulk_failed_doc_ids.add(str(doc_id))
                        print(f"  Chunk {chunk_num} bulk-batch {batch_idx} — Doc ID {doc_id} failed: {error_reason}")

        batch_success = len(batch_rows) - len(
            [i for i in response.get("items", [])
             for _, d in i.items() if d.get("status", 200) >= 300]
        )
        total_indexed += batch_success
        print(f"Chunk {chunk_num} bulk-batch {batch_idx + 1}/{num_bulk_batches}: "
              f"{batch_success}/{len(batch_rows)} docs indexed.")

    if bulk_failed_doc_ids:
        raise RuntimeError(
            f"Chunk {chunk_num}: {len(bulk_failed_doc_ids)} documents failed bulk indexing."
        )

    print(f"Chunk {chunk_num}: bulk-indexed {total_indexed} documents across {num_bulk_batches} batches.")

    # ── Step 4: concurrent batched _mget verification ─────────────────────────
    # Build expected_types map: {user_id: sorted list of address types from PG}
    expected_types_map: dict[int, list[str]] = {}
    for row in rows:
        user_id = row[0]
        raw_addresses = row[5]
        if isinstance(raw_addresses, str):
            raw_addresses = _json.loads(raw_addresses)
        if raw_addresses is None:
            raw_addresses = []
        expected_types_map[user_id] = sorted(
            set(addr.get("type") for addr in raw_addresses if addr.get("type"))
        )

    all_user_ids = list(expected_types_map.keys())
    # Split into VERIFY_BATCH_SIZE batches for _mget
    verify_batches = [
        all_user_ids[i : i + VERIFY_BATCH_SIZE]
        for i in range(0, len(all_user_ids), VERIFY_BATCH_SIZE)
    ]

    verified_ids: list[int] = []
    failed_ids: list[int] = []
    verification_failures: list[tuple] = []

    def _verify_batch(batch_user_ids: list[int]) -> list[tuple]:
        """Fetch a batch of docs via _mget and return (user_id, ok, reason) tuples."""
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
            indexed_addresses = doc.get("_source", {}).get("addresses", [])
            indexed_types = sorted(
                set(addr.get("type") for addr in indexed_addresses if addr.get("type"))
            )
            expected = expected_types_map.get(uid, [])
            if indexed_types != expected:
                results.append((
                    uid, False,
                    f"address type mismatch — expected {expected}, got {indexed_types}"
                ))
            else:
                results.append((uid, True, "ok"))
        return results

    # Run _mget batches concurrently up to MAX_VERIFY_WORKERS threads
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
        f"Chunk {chunk_num}: verification done via {len(verify_batches)} _mget batch(es) "
        f"({MAX_VERIFY_WORKERS} workers) — "
        f"verified={len(verified_ids)}, failed={len(failed_ids)}."
    )

    # ── Step 5: batch-update migration_tracking for COMPLETED and FAILED ───────
    if verified_ids:
        completed_list = ", ".join(str(uid) for uid in verified_ids)
        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'COMPLETED',
                updated_at       = NOW()
            WHERE user_id IN ({completed_list})
        """)
        print(f"Chunk {chunk_num}: marked {len(verified_ids)} users as COMPLETED.")

    if failed_ids:
        failed_list = ", ".join(str(uid) for uid in failed_ids)
        hook.run(f"""
            UPDATE migration_tracking
            SET migration_status = 'FAILED',
                updated_at       = NOW()
            WHERE user_id IN ({failed_list})
        """)
        print(f"Chunk {chunk_num}: marked {len(failed_ids)} users as FAILED.")
        for uid, reason in verification_failures:
            print(f"  - user_id {uid}: {reason}")

    error_count = len(verification_failures)
    print(f"Chunk {chunk_num} done — indexed: {total_indexed}, verified: {len(verified_ids)}, errors: {error_count}.")
    return {
        "chunks_processed": 1,
        "docs_indexed": total_indexed,
        "errors": error_count
    }

@task
def summarise_sync(chunk_results: list[dict]) -> None:
    total_chunks = 0
    total_docs = 0
    total_errors = 0
    failed_chunks_count = 0

    for res in chunk_results:
        if not res:
            continue
        total_chunks += res.get("chunks_processed", 0)
        total_docs += res.get("docs_indexed", 0)
        err = res.get("errors", 0)
        total_errors += err
        if err > 0:
            failed_chunks_count += 1

    print(f"Sync summary:")
    print(f"  Total chunks processed: {total_chunks}")
    print(f"  Total documents indexed: {total_docs}")
    print(f"  Total errors: {total_errors}")

    if total_errors == 0:
        print(f"SUCCESS: Successfully indexed {total_docs} documents.")
    else:
        print(f"Failed chunks count: {failed_chunks_count}")
        raise RuntimeError(f"Sync completed with {total_errors} errors across {failed_chunks_count} chunks.")

# DAG definition
@dag(
    dag_id="example_postgres_to_opensearch_dag",
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "postgres", "opensearch"],
)
def postgres_to_opensearch_dag():
    verify_db = verify_postgres_schema()
    create_index = create_or_verify_index()
    chunks_list = prepare_chunks()
    results = sync_chunk.expand(chunk=chunks_list)
    summary = summarise_sync(results)  # type: ignore[bad-argument-type]  # Airflow resolves XComArg to list[dict] at runtime

    [verify_db, create_index] >> chunks_list >> results >> summary

# Instantiate the DAG
example_postgres_to_opensearch_dag = postgres_to_opensearch_dag()
