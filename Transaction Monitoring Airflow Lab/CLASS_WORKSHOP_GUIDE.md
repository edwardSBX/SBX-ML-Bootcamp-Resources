# Transaction Monitoring with Airflow — Class Workshop Guide

**Audience:** Students building their own transaction monitoring (TM) pipeline  
**Time:** ~90–120 minutes (lecture + hands-on)  
**Goal:** Reproduce the demo stack, then implement a **custom TM rule** of your choice

---

## What you will build

A small but realistic AML-style monitoring pipeline:

```
Fake data script  →  PostgreSQL  →  Airflow DAG  →  Flagged results table
(generate_fake_data.py)   (users, transactions)   (your TM rule)   (tm_alerts)
```

| Component | Tool | Role |
|-----------|------|------|
| Database | PostgreSQL 13 | Stores users, transactions, and alert output |
| Orchestration | Apache Airflow 2.10 | Runs the monitoring pipeline on a schedule |
| DB browser | pgAdmin 4 | Inspect data and verify your rule worked |
| Data loader | Python + psycopg/psycopg2 | Creates schema and loads fake transactions |
| Rule engine | Python + pandas (in DAG) | Computes features and filters alerts |

**Reference rule (instructor demo):** Flag transactions where a user's **rolling 30-day sum ≥ $10,000**.

**Your task:** Replace that rule with any TM logic you choose (see [Part 6](#part-6-design-your-own-tm-rule)).

---

## Prerequisites

Install before class:

| Software | Verify with |
|----------|-------------|
| Docker + Docker Compose | `docker --version` and `docker compose version` |
| Python 3.10+ | `python3 --version` |
| Git (optional) | `git --version` |

**System requirements:** 8 GB RAM recommended. First Airflow boot takes 2–3 minutes.

---

## Project folder structure

Create a project directory (e.g. `tm-airflow-lab`) with this layout:

```
tm-airflow-lab/
├── docker-compose.yaml      # Step 2
├── requirements.txt         # Step 3
├── generate_fake_data.py    # Step 4
├── dags/
│   └── transaction_monitoring_dag.py   # Step 5 (customize in Step 6)
├── logs/                    # Auto-created; Airflow writes here
└── plugins/                 # Empty for now
```

```bash
mkdir -p tm-airflow-lab/{dags,logs,plugins}
cd tm-airflow-lab
```

---

## Part 1 — Understand the data model

Before writing code, agree on two source tables.

### `users` table

| Column | Type | Example |
|--------|------|---------|
| `user_id` | VARCHAR (PK) | `U0001` |
| `first_name` | VARCHAR | `Jane` |
| `last_name` | VARCHAR | `Smith` |
| `user_age` | INTEGER | `34` |
| `user_income` | NUMERIC | `75000.00` |
| `occupation` | VARCHAR | `employee`, `unemployed`, or `retired` |

### `transactions` table

| Column | Type | Example |
|--------|------|---------|
| `txn_id` | VARCHAR (PK) | `TXN00010001` |
| `user_id` | VARCHAR (FK → users) | `U0001` |
| `txn_amount` | NUMERIC (USD) | `450.00` |
| `transaction_type` | VARCHAR | `purchase`, `withdrawal`, `transfer`, etc. |
| `txn_timestamp` | TIMESTAMP | `2026-05-15 14:32:00` |

### Output table (created by your DAG)

Name it something meaningful, e.g. `tm_alerts` or `high_value_monitored`. It should store:
- The original transaction + user fields you need for review
- The **feature(s)** your rule computed
- A `flagged_at` timestamp

---

## Part 2 — Launch the Docker stack

Create `docker-compose.yaml` with four services:

1. **postgres** — application database  
2. **pgadmin** — web UI for SQL  
3. **airflow-init** — one-time DB migration + admin user  
4. **airflow-webserver** + **airflow-scheduler** — pipeline runtime  

Copy the working file from this repo, or build it service by service:

```bash
docker compose up -d
```

### Wait for startup

First boot installs Python packages inside Airflow containers. **Do not open the UI immediately.**

```bash
# Watch until webserver is healthy (may take 2–3 min)
docker compose ps

# Or poll the health endpoint
curl http://localhost:8080/health
```

### Access points

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `admin` |
| PostgreSQL (from host) | `localhost:5432` | `airflow` / `airflow`, db `airflow` |

> **pgAdmin tip:** When registering the server, use host **`postgres`** (not `localhost`). See [PGADMIN_INSTRUCTIONS.md](PGADMIN_INSTRUCTIONS.md).


## Part 3 — Python environment (host machine)

The data generator runs **on your laptop**, not inside Airflow.

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt`:

```
psycopg[binary]>=3.1
psycopg2-binary>=2.9
faker>=24.0
pandas>=2.0
```

**Why two PostgreSQL drivers?**
- **psycopg (v3)** — creates tables (as required by the lab spec)
- **psycopg2** — bulk-inserts rows (as required by the lab spec)

---

## Part 4 — Generate and load fake data

Create `generate_fake_data.py` with three responsibilities:

### 4.1 Generate fake records

Required fields per transaction (joined with user data in the DAG):
- `txn_id`, `user_id`, `first_name`, `last_name`, `user_age`, `user_income`, `occupation`
- `txn_amount`, `transaction_type`, `txn_timestamp`

Spread timestamps across a **two-month window** (e.g. April 1 – May 31, 2026).

**Tip:** Inject a few users with clustered high-value transactions so your rule produces visible alerts during the demo.

### 4.2 Create tables (psycopg)

```python
import psycopg

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",   # psycopg uses "dbname", not "database"
    "user": "airflow",
    "password": "airflow",
}

with psycopg.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE users (...);")
        cur.execute("CREATE TABLE transactions (...);")
    conn.commit()
```

### 4.3 Upload data (psycopg2)

```python
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(**DB_CONFIG)
# bulk INSERT users, then transactions
```

### 4.4 Run it

```bash
python generate_fake_data.py
```

### 4.5 Verify in pgAdmin

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM transactions;

SELECT t.*, u.first_name, u.last_name
FROM transactions t
JOIN users u ON t.user_id = u.user_id
ORDER BY t.txn_timestamp DESC
LIMIT 10;
```

---

## Part 5 — Build the Airflow DAG

Create `dags/transaction_monitoring_dag.py`.

Every TM pipeline in this lab follows the same **ETL pattern**:

```
Extract  →  Transform (compute feature + filter)  →  Load (write alerts)
```

### 5.1 DAG skeleton

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_transform_load(**context):
    # YOUR LOGIC HERE
    pass

with DAG(
    dag_id="transaction_monitoring_demo",   # rename to match your rule
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["transaction-monitoring"],
) as dag:
    PythonOperator(
        task_id="run_tm_rule",
        python_callable=extract_transform_load,
    )
```

### 5.2 Database connection inside the DAG

Airflow containers talk to Postgres over the Docker network:

```python
DB_CONFIG = {
    "host": "postgres",      # service name from docker-compose — NOT localhost
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}
```

### 5.3 Extract — SQL join

```python
extract_sql = """
    SELECT
        t.txn_id, t.user_id, t.txn_amount, t.transaction_type, t.txn_timestamp,
        u.first_name, u.last_name, u.user_age, u.user_income, u.occupation
    FROM transactions t
    JOIN users u ON t.user_id = u.user_id
"""
df = pd.read_sql(extract_sql, conn)
```

### 5.4 Transform — reference rule (rolling 30-day sum)

```python
df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"])
df = df.sort_values(["user_id", "txn_timestamp"])

def rolling_sum_per_user(group):
    group = group.set_index("txn_timestamp").sort_index()
    group["rolling_30d_sum"] = group["txn_amount"].rolling("30D", min_periods=1).sum()
    return group.reset_index()

df = df.groupby("user_id", group_keys=False).apply(rolling_sum_per_user)
flagged = df[df["rolling_30d_sum"] >= 10_000]
```

### 5.5 Load — write to output table

Create a table matching your rule's output columns, then `INSERT` flagged rows.

### 5.6 Run the DAG

1. Open http://localhost:8080  
2. Find your DAG in the list (refresh if needed — scheduler scans every ~30s)  
3. **Unpause** the DAG (toggle on the left)  
4. Click **Trigger DAG** (play button)  
5. Open the task → **Log** — look for `SUCCESS` and your `Processed X; flagged Y` message  

### 5.7 Verify alerts

```sql
SELECT COUNT(*) FROM high_value_monitored;   -- or your table name

SELECT user_id, txn_amount, rolling_30d_sum, txn_timestamp
FROM high_value_monitored
ORDER BY rolling_30d_sum DESC
LIMIT 20;
```

---

## Part 6 — Design your own TM rule

This is the creative part. Keep the same **infrastructure** (Docker, tables, DAG structure) and only change the **transform + filter** logic and output table schema.

### Rule design worksheet

Fill this out before coding:

| Question | Your answer |
|----------|-------------|
| Rule name | e.g. "Large single withdrawal" |
| What are you monitoring? | Amount? Frequency? Velocity? User profile? |
| What feature will you compute? | e.g. `txn_amount`, `txn_count_7d`, `amount_to_income_ratio` |
| What is the threshold? | e.g. `>= 5000`, `> 10 transactions`, `ratio > 0.5` |
| Output table name | e.g. `large_withdrawal_alerts` |

### Example rules (pick one or invent your own)

#### Rule A — Large single transaction

**Logic:** Flag any transaction where `txn_amount >= 5000`.

```python
flagged = df[df["txn_amount"] >= 5000].copy()
flagged["rule_name"] = "large_single_txn"
```

*Easy to implement; good baseline.*

---

#### Rule B — High amount relative to income

**Logic:** Flag when `txn_amount > 0.25 * user_income` (transaction exceeds 25% of stated annual income).

```python
df["amount_to_income_ratio"] = df["txn_amount"] / df["user_income"]
flagged = df[df["amount_to_income_ratio"] > 0.25].copy()
```

*Uses customer profile data — common in real TM.*

---

#### Rule C — Rapid-fire transactions (velocity)

**Logic:** Flag users with **5 or more transactions within any 24-hour window**.

```python
def count_24h_window(group):
    group = group.set_index("txn_timestamp").sort_index()
    group["txn_count_24h"] = group["txn_id"].rolling("24H").count()
    return group.reset_index()

df = df.groupby("user_id", group_keys=False).apply(count_24h_window)
flagged = df[df["txn_count_24h"] >= 5].copy()
```

*Detects structuring / smurfing patterns.*

---

#### Rule D — Unemployed user, high spending

**Logic:** Flag transactions over $1,000 where `occupation = 'unemployed'`.

```python
flagged = df[
    (df["occupation"] == "unemployed") & (df["txn_amount"] >= 1000)
].copy()
```

*Simple rule combining behavioral + profile signals.*

---

#### Rule E — Withdrawal spike (rolling 7-day)

**Logic:** Sum of `withdrawal` type transactions in 7 days ≥ $3,000.

```python
withdrawals = df[df["transaction_type"] == "withdrawal"].copy()

def rolling_withdrawal_sum(group):
    group = group.set_index("txn_timestamp").sort_index()
    group["withdrawal_sum_7d"] = group["txn_amount"].rolling("7D", min_periods=1).sum()
    return group.reset_index()

withdrawals = withdrawals.groupby("user_id", group_keys=False).apply(rolling_withdrawal_sum)
flagged = withdrawals[withdrawals["withdrawal_sum_7d"] >= 3000].copy()
```

---

#### Rule F — Senior citizen, large transfer

**Logic:** Flag `transfer` transactions ≥ $2,000 where `user_age >= 65`.

```python
flagged = df[
    (df["user_age"] >= 65)
    & (df["transaction_type"] == "transfer")
    & (df["txn_amount"] >= 2000)
].copy()
```

*Elder fraud monitoring angle.*

---

### Customization checklist

When you change the rule, update **all** of these:

- [ ] Feature calculation in `extract_transform_load()`
- [ ] Filter condition (`flagged = df[...]`)
- [ ] `print()` message describing your rule
- [ ] Output table `CREATE TABLE` columns (include your new feature columns)
- [ ] `INSERT` column list
- [ ] DAG `dag_id` and `description` strings
- [ ] (Optional) Fake data generator — add transactions that trigger **your** rule

### Tune your fake data

If your DAG flags 0 rows, your data may not satisfy the rule. Edit `generate_fake_data.py` to inject targeted records:

```python
# Example: guarantee a large single transaction for testing Rule A
transactions.append({
    "txn_id": "TXN_TEST_LARGE",
    "user_id": "U0001",
    "txn_amount": 7500.00,
    "transaction_type": "withdrawal",
    "txn_timestamp": datetime(2026, 5, 10, 12, 0, 0),
})
```

Re-run `python generate_fake_data.py`, then re-trigger the DAG.

---

## Part 7 — End-to-end checklist

Use this to confirm your project is complete before presenting.

### Infrastructure
- [ ] `docker compose up -d` runs without errors
- [ ] Airflow UI loads at http://localhost:8080
- [ ] pgAdmin connects to server host `postgres`

### Data
- [ ] `users` table has rows
- [ ] `transactions` table has rows spanning ~2 months
- [ ] Timestamps and amounts look realistic

### Pipeline
- [ ] DAG appears in Airflow (no import errors in scheduler logs)
- [ ] DAG run completes with **SUCCESS**
- [ ] Task log shows processed count and flagged count
- [ ] Output/alert table exists in pgAdmin
- [ ] Alert rows match your rule logic (spot-check 3–5 rows manually)

### Presentation
- [ ] You can explain your rule in one sentence
- [ ] You can show the SQL extract, pandas transform, and filter
- [ ] You can demo a live re-run (trigger DAG → refresh pgAdmin)

---

## Part 8 — Presenting your demo (5-minute template)

Suggested flow for each student:

1. **Problem (30s)** — "My rule detects ___ because ___"
2. **Architecture (30s)** — Show diagram: data → Postgres → Airflow → alerts table
3. **Data (1m)** — pgAdmin: show `users` / `transactions` sample rows
4. **Rule (2m)** — Walk through your pandas logic in the DAG file
5. **Results (1m)** — pgAdmin: `SELECT * FROM your_alert_table LIMIT 10`
6. **Wrap-up (30s)** — How you'd improve it in production (incremental loads, alerts, tuning)

---

## Part 9 — Instructor quick reference

### Suggested 90-minute agenda

| Time | Activity |
|------|----------|
| 0:00–0:15 | Lecture: what TM is, why orchestration matters |
| 0:15–0:25 | Live demo of finished project (your working copy) |
| 0:25–0:35 | Students: `docker compose up -d`, verify UIs |
| 0:35–0:50 | Students: data generator + pgAdmin verification |
| 0:50–1:10 | Students: DAG setup, run reference rule |
| 1:10–1:25 | Students: customize TM rule |
| 1:25–1:30 | Volunteers present; Q&A |

### Commands to have on the board

```bash
docker compose up -d
docker compose ps
docker compose logs airflow-webserver --tail 30
source .venv/bin/activate && python generate_fake_data.py
curl http://localhost:8080/health
```

### Grading rubric (suggested)

| Criteria | Points |
|----------|--------|
| Stack runs (Airflow + Postgres + pgAdmin) | 20 |
| Data loaded correctly (both tables) | 20 |
| DAG runs successfully | 20 |
| Custom TM rule implemented (not copy of demo rule) | 25 |
| Can explain rule and show flagged results | 15 |

---

## Troubleshooting reference

| Error | Cause | Solution |
|-------|-------|----------|
| `invalid connection option "database"` (psycopg) | psycopg v3 uses `dbname` | Change key to `"dbname": "airflow"` |
| Connection refused from `generate_fake_data.py` | Postgres not running | `docker compose up -d`; wait for healthy |
| DAG not visible | Scheduler hasn't parsed yet | Wait 30s; check `docker compose logs airflow-scheduler` |
| 0 flagged rows | Data doesn't trigger your rule | Inject test transactions; lower threshold temporarily |
| `No transactions found` in DAG log | Data not loaded | Re-run `generate_fake_data.py` |
| pandas SQLAlchemy warning | Cosmetic | Safe to ignore for this lab |

---

## Further reading

- [Apache Airflow Docker docs](https://airflow.apache.org/docs/docker-stack/index.html)
- [pandas rolling windows](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rolling.html)
- [PGADMIN_INSTRUCTIONS.md](PGADMIN_INSTRUCTIONS.md) — connecting pgAdmin step by step

---

**You have everything you need to reproduce the demo and make it your own. Pick a rule, make the data prove it works, and show the alerts.**
