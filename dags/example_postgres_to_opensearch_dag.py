from __future__ import annotations

import os
import math
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from opensearchpy import OpenSearch

# Constants
PG_SOURCE_TABLE = "users"
OS_INDEX_NAME = "users"
CHUNK_SIZE = 10_000
MAX_CONCURRENT_CHUNKS = 3

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
            "created_at": {"type": "date"}
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
    body = []
    for row in rows:
        action = {"index": {"_index": index_name, "_id": derive_id(row)}}
        doc = {
            "id": row[0],
            "name": row[1],
            "email": row[2],
            "status": row[3],
            "created_at": row[4].isoformat() if row[4] is not None else None
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
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        result = hook.get_first(f"SELECT COUNT(*) FROM {PG_SOURCE_TABLE}")
        total = result[0] if result else 0
        if total == 0:
            print(f"Table {PG_SOURCE_TABLE} contains zero rows. Zero rows found.")
            return []
        
        chunks = calculate_chunks(total, CHUNK_SIZE)
        print(f"Total row count in {PG_SOURCE_TABLE}: {total}. Total chunks: {len(chunks)}.")
        return chunks
    except Exception as e:
        print(f"Error querying table {PG_SOURCE_TABLE} row count: {e}")
        raise

@task(task_id="sync_chunk", max_active_tis_per_dag=MAX_CONCURRENT_CHUNKS)
def sync_chunk(chunk: dict) -> dict:
    offset = chunk.get("offset", 0)
    limit = chunk.get("limit", CHUNK_SIZE)
    chunk_num = offset // CHUNK_SIZE
    
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = hook.get_records(f"SELECT * FROM {PG_SOURCE_TABLE} LIMIT {limit} OFFSET {offset}")
    except Exception as e:
        print(f"Error fetching Postgres records for chunk {chunk_num} (offset {offset}, limit {limit}): {e}")
        raise

    if not rows:
        return {"chunks_processed": 1, "docs_indexed": 0, "errors": 0}
        
    try:
        bulk_body = build_bulk_body(rows, OS_INDEX_NAME)
        client = get_opensearch_client()
        response = client.bulk(body=bulk_body)
    except Exception as e:
        print(f"Error executing OpenSearch bulk write for chunk {chunk_num}: {e}")
        raise
        
    if response.get("errors", False):
        failed_docs = []
        for item in response.get("items", []):
            for action_type, details in item.items():
                if details.get("status", 200) >= 300:
                    doc_id = details.get("_id")
                    error_reason = details.get("error", {}).get("reason", "unknown error")
                    failed_docs.append((doc_id, error_reason))
                    
        print(f"Bulk indexing failed in chunk {chunk_num} for documents:")
        for doc_id, reason in failed_docs:
            print(f"  - Doc ID {doc_id}: {reason}")
            
        raise RuntimeError(f"Failed to index {len(failed_docs)} documents in chunk {chunk_num}.")
        
    print(f"Chunk {chunk_num} synced successfully: {len(rows)} documents indexed.")
    return {"chunks_processed": 1, "docs_indexed": len(rows), "errors": 0}

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
    create_index = create_or_verify_index()
    chunks_list = prepare_chunks()
    results = sync_chunk.expand(chunk=chunks_list)
    summary = summarise_sync(results)  # type: ignore[bad-argument-type]  # Airflow resolves XComArg to list[dict] at runtime
    
    create_index >> chunks_list >> results >> summary

# Instantiate the DAG
example_postgres_to_opensearch_dag = postgres_to_opensearch_dag()
