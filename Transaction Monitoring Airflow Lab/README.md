# Airflow Transaction Monitoring Demo

This folder is a complete, self-contained instructional demo showing how to use **Apache Airflow** for transaction monitoring / AML-style rules on top of PostgreSQL.

> **Teaching this in class?** See **[CLASS_WORKSHOP_GUIDE.md](CLASS_WORKSHOP_GUIDE.md)** for step-by-step build instructions and ideas for custom TM rules.

## What it demonstrates

1. **Dockerized stack**: Airflow 2.10 + PostgreSQL 13 + pgAdmin 4 (single `docker-compose.yaml`)
2. **Data generation**: Realistic fake users + transactions spanning a 2-month window (April–May 2026)
3. **Table creation** with proper schema, foreign keys, and indexes (done via `psycopg`)
4. **Airflow DAG**: Extracts data → computes a **rolling 30-day transaction sum per user** (pandas) → filters rows where `rolling_30d_sum >= 10000` → loads into a monitoring table

The rule encoded: *"Flag any transaction where the user's total transaction volume in any rolling 30-day window reaches or exceeds $10,000 USD."*

---

## Prerequisites

| Tool | Check |
|------|-------|
| Docker + Docker Compose v2 | `docker --version` and `docker compose version` |
| Python 3.10+ | `python3 --version` |
| Git | `git --version` |

**System requirements:** 8 GB RAM recommended. First Airflow boot takes 2–3 minutes while Docker images download and pip packages install inside the containers.

---

## Setup (clone → run)

### 1. Get the project

Clone the repository and enter this folder:

```bash
git clone <your-repo-url>
cd "Transaction Monitoring Airflow Lab"
```

Or, if you already have the repo, just `cd` into this directory.

### 2. Configure environment for Docker

Docker Compose reads a `.env` file automatically. Create one from the template:

```bash
cp .env.example .env
```

**Linux users** — set your host user ID so Airflow can write to mounted `logs/`, `dags/`, and `plugins/` folders:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
echo "_PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary pandas apache-airflow-providers-postgres" >> .env
```

macOS and Windows can keep the defaults in `.env.example` (`AIRFLOW_UID=50000`).

### 3. Create a Python virtual environment (host machine)

The data generator runs on your host and connects to PostgreSQL in Docker. Install dependencies into a venv:

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Start the Docker stack

```bash
docker compose up -d
```

Wait 2–3 minutes on first run. Verify all services are healthy:

```bash
docker compose ps
```

You should see `airflow-init` as **Exited (0)** and `airflow-webserver`, `airflow-scheduler`, `postgres`, and `pgadmin4` as **Up (healthy)**.

**Access points:**
- **Airflow UI**: http://localhost:8080 → login `admin` / `admin`
- **pgAdmin**: http://localhost:5050 → login `admin@admin.com` / `admin`
- **PostgreSQL**: `localhost:5432` (user `airflow`, password `airflow`, database `airflow`)

### 5. Generate and load fake data

With the venv still activated and Docker running:

```bash
python generate_fake_data.py
```

This will:
- Connect to the Docker PostgreSQL instance on `localhost:5432`
- Drop and recreate `users` and `transactions` tables
- Insert ~50 users and ~750 transactions with realistic data
- Inject high-volume users so the monitoring rule produces flagged rows

Verify in pgAdmin — see **[PGADMIN_INSTRUCTIONS.md](PGADMIN_INSTRUCTIONS.md)** for connection steps.

Quick SQL checks:

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM transactions;
SELECT * FROM transactions ORDER BY txn_timestamp DESC LIMIT 10;
```

### 6. Run the Airflow DAG

1. Open http://localhost:8080 and log in (`admin` / `admin`)
2. Find the DAG **`transaction_monitoring_demo`**
3. Unpause it (toggle switch)
4. Trigger it manually (play button) or wait for the daily schedule
5. Watch the task `extract_transform_load_high_value_flags` turn green

After the run completes, query results in pgAdmin:

```sql
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

---

## Files Overview

| File | Purpose |
|------|---------|
| `docker-compose.yaml` | Launches Airflow (webserver + scheduler), PostgreSQL, and pgAdmin4 |
| `.env.example` | Template for Docker Compose environment variables (copy to `.env`) |
| `requirements.txt` | Python packages for the host-side data generator (`venv`) |
| `generate_fake_data.py` | Creates source tables and populates them with fake data |
| `dags/transaction_monitoring_dag.py` | Airflow DAG implementing the rolling-window monitoring rule |
| `CLASS_WORKSHOP_GUIDE.md` | Step-by-step lab instructions for students |
| `PGADMIN_INSTRUCTIONS.md` | How to connect pgAdmin to PostgreSQL |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `airflow-init` exits with code 1 | Ensure `_PIP_ADDITIONAL_REQUIREMENTS: ''` is set on the init service (already in `docker-compose.yaml`). Run `docker compose down` then `docker compose up -d` again. |
| DAG not appearing | Check `./dags/transaction_monitoring_dag.py` exists. Run `docker compose logs airflow-scheduler --tail 50` for import errors. |
| Connection refused from `generate_fake_data.py` | Confirm `docker compose ps` shows `postgres` as healthy and port 5432 is published. |
| No rows in `high_value_monitored` | Re-run `generate_fake_data.py`. The script injects high-volume users; ~10% of random txns are also large. |
| Permission errors on Linux (`logs/`, `dags/`) | Set `AIRFLOW_UID=$(id -u)` in `.env`, then `docker compose down && docker compose up -d`. |
| Airflow webserver unhealthy on first boot | Wait 2–3 minutes — webserver installs pip packages on first start. Check `docker compose logs airflow-webserver --tail 30`. |
| `ModuleNotFoundError` running data script | Activate the venv: `source venv/bin/activate`, then `pip install -r requirements.txt`. |

### Clean restart

```bash
docker compose down --volumes
docker compose up -d
```

This wipes the PostgreSQL volume and re-runs DB migration on next start.

---

## Next Steps / Production Hardening

- Switch to `CeleryExecutor` + Redis for real parallelism
- Build a custom Airflow image with dependencies baked in (instead of `_PIP_ADDITIONAL_REQUIREMENTS`)
- Add separate read-replica or analytics DB for heavy transforms
- Implement incremental / CDC-style loading
- Add alerting and audit logging
- Parameterize the threshold (`10000`) and window (`30D`) via DAG params or Variables

This demo gives you a realistic, runnable foundation for building production-grade transaction monitoring pipelines with Airflow.