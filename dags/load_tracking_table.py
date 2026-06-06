"""DAG 1 of 2 — PostgreSQL → OpenSearch pipeline.

Responsibilities
----------------
* Ensure the Postgres schema (users, addresses, migration_tracking) is ready.
* Ensure the OpenSearch index exists with the correct mapping.
* Discover all eligible users (those with ≥1 address) and register them in
  migration_tracking with status = NEW (idempotent; skips existing rows).
* Return chunk descriptors via XCom so the migrate_user_data DAG can consume them.

Downstream
----------
Run migrate_user_data after this DAG completes. That DAG reads migration_tracking
for NEW / FAILED rows and drives the actual indexing.
"""
from __future__ import annotations

import time
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
        default_args,
        index_body,
        calculate_chunks,
        get_opensearch_client,
    )
except ModuleNotFoundError:
    # This path is used by Airflow at runtime (dags/ is in sys.path)
    from pg_to_os_shared import (  # type: ignore
        OS_INDEX_NAME,
        PG_SOURCE_TABLE,
        CHUNK_SIZE,
        MAX_CONCURRENT_CHUNKS,
        default_args,
        index_body,
        calculate_chunks,
        get_opensearch_client,
    )


# ── Task: verify Postgres schema & seed data ──────────────────────────────────
@task
def verify_postgres_schema():
    """Create users / addresses / migration_tracking tables and seed demo data."""
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

        # The user requested to "reset sequence" so we ensure a completely clean slate
        # by truncating and restarting identity before inserting the mock data.
        hook.run("TRUNCATE TABLE users CASCADE;")

        hook.run("""
            INSERT INTO users (name, email, status, created_at)
            SELECT
                'User ' || i,
                'user.' || i || '@example.com',
                CASE WHEN i % 10 = 0 THEN 'inactive'
                     WHEN i % 15 = 0 THEN 'suspended'
                     ELSE 'active' END,
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
                    'mumbai - 400001','bangalore - 560032','delhi - 110001',
                    'chennai - 600001','hyderabad - 500001','kolkata - 700001',
                    'pune - 411001','ahmedabad - 380001','jaipur - 302001','lucknow - 226001'
                ])[1 + ((u.id + addr.idx) % 10)],
                (ARRAY[72.8777,77.5946,77.2090,80.2707,78.4867,88.3639,73.8567,72.5714,75.7873,80.9462])
                    [1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001,
                (ARRAY[19.0760,12.9716,28.6139,13.0827,17.3850,22.5726,18.5204,23.0225,26.9124,26.8467])
                    [1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001
            FROM users u
            CROSS JOIN LATERAL (
                SELECT 'permanent' AS type, 1 AS idx
                UNION ALL SELECT 'office', 2
                UNION ALL SELECT 'temporary', 3 WHERE u.id % 2 = 0
            ) AS addr
            WHERE NOT EXISTS (SELECT 1 FROM addresses LIMIT 1);
        """)

        # migration_tracking: per-user lifecycle (NEW → IN_PROGRESS → COMPLETED/FAILED)
        hook.run("""
            CREATE TABLE IF NOT EXISTS migration_tracking (
                user_id          INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                address_id       INTEGER NOT NULL REFERENCES addresses(id) ON DELETE CASCADE,
                type             VARCHAR(50) NOT NULL,
                migration_status VARCHAR(20) NOT NULL DEFAULT 'NEW',
                created_at       TIMESTAMP  NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMP  NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, address_id)
            );
        """)

        print("Postgres schema, seed data, and migration_tracking table verified.")
    except Exception as e:
        print(f"Error in verify_postgres_schema: {e}")
        raise


# ── Task: create or verify OpenSearch index ───────────────────────────────────
@task
def create_or_verify_index():
    """Create the OpenSearch index with nested-address mapping if it does not exist."""
    try:
        client = get_opensearch_client()
        if not client.indices.exists(index=OS_INDEX_NAME):
            client.indices.create(index=OS_INDEX_NAME, body=index_body)
            print(f"Created index '{OS_INDEX_NAME}' with mapping.")
        else:
            print(f"Index '{OS_INDEX_NAME}' already exists. Skipping creation.")
    except Exception as e:
        print(f"Error in create_or_verify_index: {e}")
        raise


# ── Task: populate tracking table and return chunk descriptors ────────────────
@task
def prepare_chunks() -> list[dict]:
    """
    1. Query all eligible user IDs (users with ≥1 address) ordered by id.
    2. Batch-insert them into migration_tracking with status NEW
       (ON CONFLICT DO NOTHING → safe to re-run).
    3. Return [{offset, limit}, …] chunk descriptors via XCom.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        eligible_rows = hook.get_records("""
            SELECT a.user_id, a.id as address_id, a.type
            FROM addresses a
            ORDER BY a.user_id, a.id
        """)

        total = len(eligible_rows)
        if total == 0:
            print(f"No eligible users found in '{PG_SOURCE_TABLE}'. Nothing to migrate.")
            return []

        # 3. Sub-batched Inserts with Retries
        batches = [
            eligible_rows[i : i + CHUNK_SIZE]
            for i in range(0, total, CHUNK_SIZE)
        ]

        def _insert_batch_with_retry(batch_idx: int, batch_rows: list[tuple]) -> None:
            # Instantiate thread-local hook to avoid concurrent connection issues
            thread_hook = PostgresHook(postgres_conn_id="postgres_default")
            values_clause = ", ".join(f"({row[0]}, {row[1]}, '{row[2]}', 'NEW')" for row in batch_rows)
            query = f"""
                INSERT INTO migration_tracking (user_id, address_id, type, migration_status)
                VALUES {values_clause}
                ON CONFLICT (user_id, address_id) DO NOTHING
            """

            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    thread_hook.run(query)
                    print(f"Batch {batch_idx + 1}/{len(batches)}: Successfully registered {len(batch_rows)} users (status=NEW).")
                    break  # Success! Exit the retry loop
                except Exception as e:
                    if attempt < max_retries:
                        print(f"Batch {batch_idx + 1} failed on attempt {attempt}: {e}. Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        print(f"Batch {batch_idx + 1} failed after {max_retries} attempts. Halting task.")
                        raise

        # Execute batches concurrently
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CHUNKS) as executor:
            futures = {
                executor.submit(_insert_batch_with_retry, batch_idx, batch_rows): batch_idx
                for batch_idx, batch_rows in enumerate(batches)
            }
            for future in as_completed(futures):
                # If any future raised an exception, it will re-raise here.
                future.result()

        chunks = calculate_chunks(total, CHUNK_SIZE)
        print(f"Total eligible addresses: {total}. Completed tracking inserts across {len(batches)} batches using {MAX_CONCURRENT_CHUNKS} threads.")
        return chunks

    except Exception as e:
        print(f"Error in prepare_chunks: {e}")
        raise


# ── DAG 1: load_tracking_table ────────────────────────────────────────────────
@dag(
    dag_id="load_tracking_table",
    default_args=default_args,
    description=(
        "Step 1 of 2: Verify Postgres schema, ensure OpenSearch index, "
        "and register all eligible users in migration_tracking (status=NEW)."
    ),
    schedule=None,          # triggered manually or by an orchestrator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "opensearch", "migration", "setup"],
)
def load_tracking_table_dag():
    verify_db = verify_postgres_schema()
    create_index = create_or_verify_index()
    chunks_list = prepare_chunks()

    [verify_db, create_index] >> chunks_list


# Instantiate
load_tracking_table = load_tracking_table_dag()
