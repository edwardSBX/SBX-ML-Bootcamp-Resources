# pgAdmin Connection Instructions

Use these steps to connect pgAdmin to the PostgreSQL database launched by Docker Compose in this project.

## Prerequisites

Make sure the stack is running:

```bash
cd "/home/eduardo/Desktop/TM System"
docker compose up -d
```

Confirm PostgreSQL is healthy:

```bash
docker compose ps
```

You should see `postgres` with status **healthy**.

---

## Step 1 — Log in to pgAdmin

1. Open your browser and go to: **http://localhost:5050**
2. Log in with:
   - **Email:** `admin@admin.com`
   - **Password:** `admin`

---

## Step 2 — Register the PostgreSQL server

1. In the left sidebar, right-click **Servers** → **Register** → **Server…**
2. Fill in the tabs below.

### General tab

| Field | Value |
|-------|-------|
| **Name** | `TM System Postgres` *(any friendly name)* |

### Connection tab

| Field | Value |
|-------|-------|
| **Host name/address** | `postgres` |
| **Port** | `5432` |
| **Maintenance database** | `airflow` |
| **Username** | `airflow` |
| **Password** | `airflow` |
| **Save password?** | ✅ Yes *(recommended for the demo)* |

> **Important:** Use `postgres` as the host — **not** `localhost`.
>
> pgAdmin runs inside Docker on the same network as the database. The hostname `postgres` is the Docker service name from `docker-compose.yaml`. Using `localhost` inside pgAdmin would point back to the pgAdmin container itself, not your database.

3. Click **Save**.

You should now see the server listed under **Servers** in the left panel.

---

## Step 3 — Browse the database

Expand the tree in the left sidebar:

```
Servers
 └── TM System Postgres
      └── Databases
           └── airflow
                └── Schemas
                     └── public
                          └── Tables
```

After running `generate_fake_data.py`, you should see:

| Table | Description |
|-------|-------------|
| `users` | Customer profile data |
| `transactions` | Raw transaction records |
| `high_value_monitored` | Flagged rows *(created after running the Airflow DAG)* |

To view data: right-click a table → **View/Edit Data** → **All Rows**.

---

## Step 4 — Run sample queries

Right-click the `airflow` database → **Query Tool**, then run:

```sql
-- Row counts
SELECT COUNT(*) AS user_count FROM users;
SELECT COUNT(*) AS txn_count FROM transactions;

-- Recent transactions with customer names
SELECT
    t.txn_id,
    u.first_name,
    u.last_name,
    t.txn_amount,
    t.transaction_type,
    t.txn_timestamp
FROM transactions t
JOIN users u ON t.user_id = u.user_id
ORDER BY t.txn_timestamp DESC
LIMIT 20;

-- Flagged high-value activity (after Airflow DAG run)
SELECT
    user_id,
    first_name || ' ' || last_name AS customer,
    txn_amount,
    rolling_30d_sum,
    txn_timestamp
FROM high_value_monitored
ORDER BY rolling_30d_sum DESC;
```

Click the **▶ Execute** button (or press F5) to run.

---

## Connection reference (quick copy)

| Setting | Value |
|---------|-------|
| pgAdmin URL | http://localhost:5050 |
| pgAdmin login | `admin@admin.com` / `admin` |
| Server host | `postgres` |
| Server port | `5432` |
| Database | `airflow` |
| DB username | `airflow` |
| DB password | `airflow` |

---

## Troubleshooting

### "Could not connect to server: Connection refused"

- Confirm PostgreSQL is running: `docker compose ps`
- Restart if needed: `docker compose restart postgres`
- Make sure you used host `postgres`, not `localhost`

### "password authentication failed for user airflow"

- Double-check username and password are both `airflow`
- If you changed credentials in `docker-compose.yaml`, use those instead

### Tables are missing

- Run the data generator first:
  ```bash
  source .venv/bin/activate
  python generate_fake_data.py
  ```
- Refresh the table list: right-click **Tables** → **Refresh**

### `high_value_monitored` table does not exist

- That table is created by the Airflow DAG, not the data generator
- In Airflow UI (http://localhost:8080), unpause and trigger `transaction_monitoring_demo`
- Refresh tables in pgAdmin after the DAG succeeds