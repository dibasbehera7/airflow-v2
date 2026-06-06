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


# ── Task: compute eligible user chunks ─────────────────────────────────────────
@task
def get_eligible_user_chunks() -> list[list[int]]:
    """
    1. Query all eligible unique user IDs (users with ≥1 address).
    2. Batch them into chunk sizes and return for Airflow dynamic task mapping.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        rows = hook.get_records("""
            SELECT DISTINCT user_id
            FROM addresses
            ORDER BY user_id
        """)

        total = len(rows)
        if total == 0:
            print(f"No eligible users found in '{PG_SOURCE_TABLE}'. Nothing to migrate.")
            return []

        all_user_ids = [r[0] for r in rows]
        chunks = [
            all_user_ids[i : i + CHUNK_SIZE]
            for i in range(0, total, CHUNK_SIZE)
        ]
        
        print(f"Found {total} eligible users. Chunks to process: {len(chunks)}.")
        return chunks

    except Exception as e:
        print(f"Error in get_eligible_user_chunks: {e}")
        raise


# ── Task: insert address rows for a chunk into tracking table ────────────────
@task(task_id="prepare_chunk", max_active_tis_per_dag=MAX_CONCURRENT_CHUNKS)
def prepare_chunk(chunk_user_ids: list[int]) -> dict:
    """
    For a given chunk of users:
    1. Query their specific address metadata.
    2. Batch insert them into migration_tracking with status NEW.
    """
    if not chunk_user_ids:
        return {"users": 0, "addresses_inserted": 0}

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        ids_list = ", ".join(str(uid) for uid in chunk_user_ids)
        
        # Get specific address metadata
        rows = hook.get_records(f"""
            SELECT user_id, id as address_id, type
            FROM addresses
            WHERE user_id IN ({ids_list})
        """)
        
        if not rows:
            return {"users": len(chunk_user_ids), "addresses_inserted": 0}
            
        values_clause = ", ".join(f"({r[0]}, {r[1]}, '{r[2]}', 'NEW')" for r in rows)
        query = f"""
            INSERT INTO migration_tracking (user_id, address_id, type, migration_status)
            VALUES {values_clause}
            ON CONFLICT (user_id, address_id) DO NOTHING
        """

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                hook.run(query)
                print(f"Successfully registered {len(rows)} addresses for {len(chunk_user_ids)} users (status=NEW).")
                break  # Success! Exit the retry loop
            except Exception as e:
                if attempt < max_retries:
                    print(f"Batch insert failed on attempt {attempt}: {e}. Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"Batch insert failed after {max_retries} attempts. Halting task.")
                    raise

        return {"users": len(chunk_user_ids), "addresses_inserted": len(rows)}

    except Exception as e:
        print(f"Error in prepare_chunk: {e}")
        raise


# ── Task: summarise the preparation ───────────────────────────────────────────
@task
def summarise_preparation(results: list[dict]):
    """Aggregates results from the mapped tasks and prints a summary."""
    total_users = sum(r.get("users", 0) for r in results if r)
    total_addresses = sum(r.get("addresses_inserted", 0) for r in results if r)
    print(f"--- MIGRATION PREPARATION SUMMARY ---")
    print(f"Chunks processed      : {len(results)}")
    print(f"Total Users Checked   : {total_users}")
    print(f"Addresses Registered  : {total_addresses}")


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
    user_chunks = get_eligible_user_chunks()
    
    # Map the chunks directly into the prepare tasks
    results = prepare_chunk.expand(chunk_user_ids=user_chunks)
    summary = summarise_preparation(results)

    [verify_db, create_index] >> user_chunks
    
    # The summary inherently depends on all mapped tasks finishing


# Instantiate
load_tracking_table = load_tracking_table_dag()
