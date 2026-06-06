# Airflow PostgreSQL to OpenSearch Migration

This project provides a robust, scalable, and resilient Apache Airflow pipeline to migrate relational data (Users and their Addresses) from a PostgreSQL database into a document-oriented OpenSearch index. 

The migration features a dedicated lifecycle tracking table (`migration_tracking`) that monitors the status of every single record at a per-address granularity, allowing the pipeline to seamlessly resume from failures without re-processing completed data.

## 🏗️ Architecture

The migration is split across two decoupled Airflow DAGs:

### 1. Setup & Preparation DAG (`load_tracking_table.py`)
**Purpose**: Initializes the environment and prepares the migration backlog.
- **`verify_postgres_schema`**: Bootstraps the PostgreSQL database. Creates `users` and `addresses` tables, seeds them with 1000 mock users and their addresses, and creates the `migration_tracking` table (with `user_id`, `address_id`, `type`, and `migration_status`).
- **`create_or_verify_index`**: Connects to OpenSearch and initializes the `users` index with the correct nested mappings.
- **`get_eligible_user_chunks`**: Fetches all distinct `user_id`s with addresses and chunks them securely over XCom.
- **`prepare_chunk`** (Dynamically Mapped): Airflow workers process the specific user IDs assigned to them in parallel. They query the address data and insert tracking rows with a targeted linear retry mechanism (3 attempts, 5-second backoff) to gracefully handle transient database locks.
- **`summarise_preparation`**: Aggregates mapped task outputs to summarize total users and tracking rows registered.

### 2. Synchronization DAG (`migrate_user_data.py`)
**Purpose**: The core migration engine that moves data and validates it.
- **`get_pending_chunks`**: Queries the `migration_tracking` table for `DISTINCT user_id`s that have addresses with a `NEW` or `FAILED` status, and breaks them into explicit chunk arrays.
- **`sync_chunk`** (Dynamically Mapped):
  1. Updates the tracking table to `IN_PROGRESS` for the targeted user chunk.
  2. Executes a highly-efficient correlated subquery to fetch the User and JSON-aggregate all their Addresses.
  3. Bulk indexes the documents into OpenSearch (`_bulk` API).
  4. Concurrently verifies the indexed data using the `_mget` API to ensure OpenSearch accurately recorded all address types.
  5. Updates the tracking table to `COMPLETED` (if successful) or `FAILED` (if the document is missing or mismatched).
- **`summarise_sync`**: Aggregates the results of all parallel chunks and outputs a final migration report.

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Apache Airflow (or Airflow TaskFlow API environment)
- PostgreSQL Database
- OpenSearch Cluster

### Installation
1. **Clone the repository** and set up your virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
2. **Install Dependencies**:
   ```bash
   pip install apache-airflow opensearch-py pytest hypothesis
   ```
3. **Configure Airflow Connections**:
   Ensure you have an Airflow connection set up for PostgreSQL. By default, the DAGs look for `postgres_default`.

4. **Configure OpenSearch**:
   The `pg_to_os_shared.py` module looks for the `opensearch_default` connection in Airflow. Alternatively, it falls back to `localhost:9200` with `admin:admin`. 

## 🏃‍♂️ Steps to Execute

1. **Start Airflow**:
   Ensure your Airflow Scheduler and Webserver are running.
   ```bash
   airflow standalone
   ```

2. **Trigger Setup (`load_tracking_table`)**:
   - Go to the Airflow UI.
   - Trigger the `load_tracking_table` DAG. 
   - Wait for it to complete. You should see logs indicating 1000 users were seeded and registered in the `migration_tracking` table.

3. **Trigger Migration (`migrate_user_data`)**:
   - Trigger the `migrate_user_data` DAG.
   - You can monitor the dynamic task mapping expanding in real-time.
   - If any chunk fails (e.g., OpenSearch timeout), the specific address rows will be marked `FAILED`. Re-triggering the DAG will **only** pick up and retry the `FAILED` and `NEW` rows!

## 🧪 Testing

The project includes a comprehensive, property-based test suite built with `pytest`, `unittest.mock`, and `hypothesis` to validate both the helper functions and the Airflow tasks.

To run the tests, ensure `PYTHONPATH` points to the project root and run:
```bash
set PYTHONPATH=.
pytest dags/tests/ -v
```
*(On Linux/macOS: `export PYTHONPATH=. && pytest dags/tests/ -v`)*

The test suite covers:
- Chunking logic (offsets, exact divisors, limits).
- Sub-batched SQL insertions and targeted retry loops.
- `_mget` concurrent verification logic.
- Graceful exception propagation and OpenSearch configurations.
