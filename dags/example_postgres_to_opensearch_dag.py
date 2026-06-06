"""
DEPRECATED — this monolithic DAG has been split into two separate DAGs.

  DAG 1 (setup)  : dags/load_tracking_table.py   → dag_id="load_tracking_table"
  DAG 2 (migrate): dags/migrate_user_data.py      → dag_id="migrate_user_data"
  Shared helpers : dags/pg_to_os_shared.py

This file is kept for reference only and does NOT register any DAG.
It can be safely deleted once both new DAGs are confirmed working in Airflow.
"""
# No DAG is instantiated here — Airflow will ignore this file.
