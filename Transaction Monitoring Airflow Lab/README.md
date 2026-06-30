# Airflow Transaction Monitoring Demo

This repository contains a complete, self-contained instructional demo showing how to use **Apache Airflow** for transaction monitoring / AML-style rules on top of PostgreSQL.

> **Teaching this in class?** See **[CLASS_WORKSHOP_GUIDE.md](CLASS_WORKSHOP_GUIDE.md)** for step-by-step build instructions and ideas for custom TM rules.

## What it demonstrates

1. **Dockerized stack**: Airflow 2.10 + PostgreSQL 13 + pgAdmin 4 (single `docker-compose.yaml`)
2. **Data generation**: Realistic fake users + transactions spanning a 2-month window (April–May 2026)
3. **Table creation** with proper schema, foreign keys, and indexes (done via `psycopg2`)
4. **Airflow DAG**: Extracts data → computes a **rolling 30-day transaction sum per user** (pandas) → filters rows where `rolling_30d_sum >= 10000` → loads into a monitoring table

The rule encoded: *"Flag any transaction where the user's total transaction volume in any rolling 30-day window reaches or exceeds $10,000 USD."*

---

## Quick Start (5–10 minutes)

### 1. Start the stack

```bash
cd /path/to/artifacts
docker compose up -d
```

Wait 60–120 seconds for everything to become healthy (first run the webserver/scheduler automatically run `airflow db migrate`).

**Access points**:
- **Airflow UI**: http://localhost:8080 → login `admin` / `admin`
- **pgAdmin**: http://localhost:5050 → login `admin@admin.com` / `admin`
- **PostgreSQL**: `localhost:5432` (user `airflow`, password `airflow`, database `airflow`)

### 2. Generate and load fake data

Make sure you have the Python dependencies on your host machine:

```bash
pip install psycopg2-binary faker pandas
```

Then run:

```bash
python generate_fake_data.py
```

This will:
- Connect to the Docker PostgreSQL
- Drop & recreate `users` and `transactions` tables
- Insert ~50 users and ~750 transactions with realistic names, ages, incomes, occupations, amounts, and timestamps spread over April 1 – May 31, 2026
- Some high-value transactions are included so the monitoring rule will trigger examples

You can verify in pgAdmin — see **[PGADMIN_INSTRUCTIONS.md](PGADMIN_INSTRUCTIONS.md)** for full setup steps.

Quick connection summary:
- pgAdmin URL: http://localhost:5050 (`admin@admin.com` / `admin`)
- Server host: **`postgres`** (not `localhost` — pgAdmin runs inside Docker)
- Port: `5432` | Database: `airflow` | User/Pass: `airflow` / `airflow`

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM transactions;
SELECT * FROM transactions ORDER BY txn_timestamp DESC LIMIT 10;
```

### 3. Run the Airflow DAG

1. In the Airflow UI, find the DAG named **`transaction_monitoring_demo`**
2. Unpause it (toggle switch)
3. Trigger it manually (play button) or wait for the daily schedule
4. Watch the task `extract_transform_load_high_value_flags` succeed (green)

After the run completes, query the results in pgAdmin:

```sql
-- See all flagged high-value activity
SELECT 
    user_id,
    first_name || ' ' || last_name AS customer,
    txn_amount,
    rolling_30d_sum,
    txn_timestamp,
    transaction_type
FROM high_value_monitored
ORDER BY rolling_30d_sum DESC, txn_timestamp;
```

You should see rows where the rolling 30-day sum met or exceeded the $10,000 threshold.

---

## Files Overview

| File | Purpose |
|------|---------|
| `docker-compose.yaml` | Launches Airflow (webserver + scheduler), PostgreSQL, and pgAdmin4. Includes volume mounts for `dags/`, `logs/`, `plugins/`. Installs `psycopg2-binary`, `pandas`, and Postgres provider automatically. |
| `generate_fake_data.py` | Standalone script that creates the two source tables and populates them with fake data using `psycopg2` + `faker`. |
| `dags/transaction_monitoring_dag.py` | The Airflow DAG. Uses `pandas` rolling window per user to implement the business rule and writes results to `high_value_monitored`. |
| `README.md` | This file. |

---

## Key Technical Points (for learning)

- **Why one PostgreSQL?** Simplicity for the demo. In real life you would often have a separate analytics/warehouse DB.
- **Rolling window in pandas**: `df.groupby('user_id').apply(...)` + `.rolling('30D')` on a datetime index is the idiomatic way.
- **Why SequentialExecutor?** Keeps the compose file small (no Redis/Celery needed). Fine for demos and small workloads.
- **Connection inside DAG**: Uses direct `psycopg2.connect(host='postgres', ...)` so it works inside the Docker network without extra Airflow Connection setup in the UI.
- **Idempotency**: The DAG truncates the target table on each run (demo convenience). A production version would be incremental.
- **Extensibility ideas**:
  - Add Great Expectations or SQL data quality checks
  - Send Slack/Email alerts when new high-value flags appear
  - Use `PostgresOperator` + raw SQL for the transform (no pandas)
  - Incremental processing using `max(flagged_at)` or watermark tables
  - Feature store pattern or model inference step

---

## Troubleshooting

- **DAG not appearing?** Make sure `./dags/transaction_monitoring_dag.py` exists and `docker compose logs airflow-scheduler` shows no import errors.
- **Connection refused to postgres from script?** Confirm `docker compose ps` shows `postgres` healthy and port 5432 is published.
- **No rows in high_value_monitored?** The fake data generator injects ~10% high-value txns. Re-run `generate_fake_data.py` or lower the threshold temporarily in the DAG for testing.
- **Permission / ownership issues on Linux?** The compose file sets `AIRFLOW_UID=50000` (or your host UID). You may need `sudo chown -R 50000:0 logs/ dags/ plugins/` after first run.
- **Airflow webserver unhealthy?** Give it more time on first boot (it runs `airflow db migrate`).

---

## Next Steps / Production Hardening

- Switch to `CeleryExecutor` + Redis for real parallelism
- Add separate read-replica or analytics DB for the heavy transforms
- Implement incremental / CDC-style loading
- Add alerting and audit logging
- Parameterize the threshold (`10000`) and window (`30D`) via DAG params or Variables
- Containerize the data generator or turn it into an Airflow task itself

This demo gives you a realistic, runnable foundation for building production-grade transaction monitoring pipelines with Airflow.

Happy monitoring! 🚀