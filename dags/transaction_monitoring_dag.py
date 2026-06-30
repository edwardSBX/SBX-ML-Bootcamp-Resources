"""
transaction_monitoring_dag.py

Airflow DAG for instructional transaction monitoring demo.

Pipeline:
  1. Extract joined user + transaction data from PostgreSQL
  2. Compute rolling 30-day transaction sum per user (pandas)
  3. Filter rows where rolling_30d_sum >= 10,000 USD
  4. Load flagged rows into high_value_monitored table
"""

from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}

ROLLING_WINDOW_DAYS = 30
THRESHOLD_USD = 10_000


def extract_transform_load(**context):
    """Extract, compute rolling sum feature, filter, and load flagged transactions."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        extract_sql = """
            SELECT
                t.txn_id,
                t.user_id,
                u.first_name,
                u.last_name,
                u.user_age,
                u.user_income,
                u.occupation,
                t.txn_amount,
                t.transaction_type,
                t.txn_timestamp
            FROM transactions t
            JOIN users u ON t.user_id = u.user_id
            ORDER BY t.user_id, t.txn_timestamp
        """
        df = pd.read_sql(extract_sql, conn)
        if df.empty:
            print("No transactions found. Run generate_fake_data.py first.")
            return

        df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"])
        df = df.sort_values(["user_id", "txn_timestamp"])

        def rolling_sum_per_user(group):
            group = group.set_index("txn_timestamp").sort_index()
            group["rolling_30d_sum"] = (
                group["txn_amount"]
                .rolling(f"{ROLLING_WINDOW_DAYS}D", min_periods=1)
                .sum()
            )
            return group.reset_index()

        df = df.groupby("user_id", group_keys=False).apply(rolling_sum_per_user)
        flagged = df[df["rolling_30d_sum"] >= THRESHOLD_USD].copy()
        flagged["flagged_at"] = datetime.utcnow()

        print(
            f"Processed {len(df)} transactions; "
            f"flagged {len(flagged)} rows (rolling {ROLLING_WINDOW_DAYS}d sum >= ${THRESHOLD_USD:,})."
        )

        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS high_value_monitored;")
        cur.execute(
            """
            CREATE TABLE high_value_monitored (
                txn_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(20) NOT NULL,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                user_age INTEGER,
                user_income NUMERIC(12, 2),
                occupation VARCHAR(20),
                txn_amount NUMERIC(12, 2) NOT NULL,
                transaction_type VARCHAR(20),
                txn_timestamp TIMESTAMP NOT NULL,
                rolling_30d_sum NUMERIC(14, 2) NOT NULL,
                flagged_at TIMESTAMP NOT NULL
            );
            """
        )

        if not flagged.empty:
            rows = [
                (
                    row.txn_id,
                    row.user_id,
                    row.first_name,
                    row.last_name,
                    int(row.user_age),
                    float(row.user_income),
                    row.occupation,
                    float(row.txn_amount),
                    row.transaction_type,
                    row.txn_timestamp,
                    float(row.rolling_30d_sum),
                    row.flagged_at,
                )
                for row in flagged.itertuples(index=False)
            ]
            execute_values(
                cur,
                """
                INSERT INTO high_value_monitored (
                    txn_id, user_id, first_name, last_name, user_age, user_income,
                    occupation, txn_amount, transaction_type, txn_timestamp,
                    rolling_30d_sum, flagged_at
                ) VALUES %s
                """,
                rows,
            )

        conn.commit()
        cur.close()
    finally:
        conn.close()


default_args = {
    "owner": "demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="transaction_monitoring_demo",
    default_args=default_args,
    description="Rolling 30-day >= $10,000 transaction monitoring rule",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transaction-monitoring", "demo"],
) as dag:
    extract_transform_load_task = PythonOperator(
        task_id="extract_transform_load_high_value_flags",
        python_callable=extract_transform_load,
    )