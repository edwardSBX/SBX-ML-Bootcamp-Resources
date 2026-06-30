#!/usr/bin/env python3
"""
generate_fake_data.py

Generates fake user and transaction data for the Airflow transaction monitoring demo.

Prerequisites:
    pip install -r requirements.txt

This script:
- Uses psycopg (v3) to create users and transactions tables
- Uses psycopg2 to bulk-upload generated data
- Generates ~50 users and ~750 transactions over a 2-month window
- Injects high-volume users so the rolling 30-day >= $10,000 rule produces flags
"""

import random
from datetime import datetime, timedelta

import psycopg
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker()

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

NUM_USERS = 50
TXNS_PER_USER = 15
START_DATE = datetime(2026, 4, 1)
END_DATE = datetime(2026, 5, 31)


def generate_data():
    """Generate users and transactions spanning the two-month period."""
    users = []
    transactions = []
    user_ids = [f"U{str(i).zfill(4)}" for i in range(1, NUM_USERS + 1)]

    for uid in user_ids:
        users.append({
            "user_id": uid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "user_age": random.randint(18, 75),
            "user_income": round(random.uniform(25000, 180000), 2),
            "occupation": random.choice(["employee", "unemployed", "retired"]),
        })

    for uid in user_ids:
        num_txns = random.randint(TXNS_PER_USER - 5, TXNS_PER_USER + 5)
        for j in range(num_txns):
            delta = END_DATE - START_DATE
            random_seconds = random.randint(0, int(delta.total_seconds()))
            txn_timestamp = START_DATE + timedelta(seconds=random_seconds)

            if random.random() < 0.10:
                txn_amount = round(random.uniform(2500, 9500), 2)
            else:
                txn_amount = round(random.uniform(5, 1200), 2)

            transactions.append({
                "txn_id": f"TXN{uid[1:]}{j:04d}{random.randint(10000, 99999)}",
                "user_id": uid,
                "txn_amount": txn_amount,
                "transaction_type": random.choice(
                    ["purchase", "withdrawal", "transfer", "deposit", "payment", "refund"]
                ),
                "txn_timestamp": txn_timestamp,
            })

    # Guarantee a few users exceed the $10k rolling-window threshold
    high_volume_users = user_ids[:3]
    cluster_start = datetime(2026, 5, 1)
    for i, uid in enumerate(high_volume_users):
        for j in range(8):
            transactions.append({
                "txn_id": f"TXNHV{i}{j:03d}{random.randint(10000, 99999)}",
                "user_id": uid,
                "txn_amount": round(random.uniform(1400, 2200), 2),
                "transaction_type": "transfer",
                "txn_timestamp": cluster_start + timedelta(days=j * 2, hours=random.randint(0, 12)),
            })

    random.shuffle(transactions)
    return users, transactions


def create_tables():
    """Create users and transactions tables using psycopg (v3)."""
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS transactions CASCADE;")
            cur.execute("DROP TABLE IF EXISTS users CASCADE;")

            cur.execute("""
                CREATE TABLE users (
                    user_id VARCHAR(20) PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    user_age INTEGER CHECK (user_age >= 18),
                    user_income NUMERIC(12, 2),
                    occupation VARCHAR(20) CHECK (
                        occupation IN ('employee', 'unemployed', 'retired')
                    )
                );
            """)

            cur.execute("""
                CREATE TABLE transactions (
                    txn_id VARCHAR(50) PRIMARY KEY,
                    user_id VARCHAR(20) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    txn_amount NUMERIC(12, 2) NOT NULL CHECK (txn_amount >= 0),
                    transaction_type VARCHAR(20),
                    txn_timestamp TIMESTAMP NOT NULL
                );
            """)

            cur.execute(
                "CREATE INDEX idx_transactions_user_time ON transactions (user_id, txn_timestamp);"
            )
            cur.execute(
                "CREATE INDEX idx_transactions_time ON transactions (txn_timestamp);"
            )
        conn.commit()

    print("Tables 'users' and 'transactions' created.")


def upload_data(users, transactions):
    """Bulk-insert users and transactions using psycopg2."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()

        user_data = [
            (
                u["user_id"], u["first_name"], u["last_name"],
                u["user_age"], u["user_income"], u["occupation"],
            )
            for u in users
        ]
        execute_values(
            cur,
            """
            INSERT INTO users (user_id, first_name, last_name, user_age, user_income, occupation)
            VALUES %s
            ON CONFLICT (user_id) DO NOTHING;
            """,
            user_data,
        )
        print(f"Inserted {len(users)} users.")

        txn_data = [
            (
                t["txn_id"], t["user_id"], t["txn_amount"],
                t["transaction_type"], t["txn_timestamp"],
            )
            for t in transactions
        ]
        execute_values(
            cur,
            """
            INSERT INTO transactions (txn_id, user_id, txn_amount, transaction_type, txn_timestamp)
            VALUES %s
            ON CONFLICT (txn_id) DO NOTHING;
            """,
            txn_data,
        )
        conn.commit()
        cur.close()
        print(f"Inserted {len(transactions)} transactions.")
    finally:
        conn.close()


if __name__ == "__main__":
    print("Starting fake data generation...")
    users, transactions = generate_data()
    print(
        f"Generated {len(users)} users and {len(transactions)} transactions "
        f"({START_DATE.date()} to {END_DATE.date()})."
    )

    print("Creating tables with psycopg...")
    create_tables()

    print("Uploading data with psycopg2...")
    upload_data(users, transactions)

    print("\nData load complete.")
    print("  pgAdmin:  http://localhost:5050")
    print("  Airflow:  http://localhost:8080  -> trigger 'transaction_monitoring_demo'")