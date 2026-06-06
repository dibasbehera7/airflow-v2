"""Shared constants, index mapping, and pure helper functions used by both
the load_tracking_table and migrate_user_data DAGs.

Do NOT import airflow.sdk or PostgresHook here — this module must remain
importable in unit tests without a running Airflow installation.
"""
from __future__ import annotations

import math
import os

from opensearchpy import OpenSearch

# ── Pipeline constants ────────────────────────────────────────────────────────
PG_SOURCE_TABLE = "users"
OS_INDEX_NAME = "users"
CHUNK_SIZE = 100             # eligible users per Airflow task instance
MAX_CONCURRENT_CHUNKS = 3    # max parallel sync_chunk task instances per DAG run
BULK_BATCH_SIZE = 100        # rows per _bulk API call within a chunk
VERIFY_BATCH_SIZE = 100      # doc IDs per _mget API call during verification
MAX_VERIFY_WORKERS = 4       # concurrent _mget threads during verification

# ── OpenSearch index mapping ──────────────────────────────────────────────────
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
                    "lati": {"type": "double"},
                },
            },
        }
    }
}

# ── Airflow default_args ──────────────────────────────────────────────────────
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Pure helpers ──────────────────────────────────────────────────────────────

def calculate_chunks(total: int, chunk_size: int) -> list[dict]:
    """Return a list of {offset, limit} dicts that cover [0, total)."""
    if total <= 0:
        return []
    num_chunks = math.ceil(total / chunk_size)
    return [
        {
            "offset": i * chunk_size,
            "limit": min(chunk_size, total - i * chunk_size),
        }
        for i in range(num_chunks)
    ]


def derive_id(row: tuple) -> str:
    """Return the OpenSearch document _id for a row (row[0] = user PK)."""
    return str(row[0])


def build_bulk_body(rows: list, index_name: str) -> list:
    """Build a flat list of [action_meta, doc, action_meta, doc, …] for _bulk."""
    import json

    body: list = []
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
            "addresses": addresses,
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
        "timeout": 30,
    }
    if use_ssl:
        config["verify_certs"] = False
    return config


def get_opensearch_client() -> OpenSearch:
    host = os.getenv("OPENSEARCH_HOST") or "localhost"
    port_raw = os.getenv("OPENSEARCH_PORT")
    port = port_raw if port_raw else 9200
    username = os.getenv("OPENSEARCH_USERNAME") or "admin"
    password = os.getenv("OPENSEARCH_PASSWORD") or "admin"
    use_ssl = os.getenv("OPENSEARCH_USE_SSL", "false").lower() in ("true", "1", "yes")

    config = get_client_config(host, port, username, password, use_ssl)
    return OpenSearch(**config)
