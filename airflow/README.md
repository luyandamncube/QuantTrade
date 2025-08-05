
## Airflow Data Ingestion



## Environment Setup

1. Install WSL2 + Ubuntu

- Open PowerShell as Administrator and run:

```bash
wsl --install
wsl --install -d Ubuntu
```
- Restart your PC after the install.

2. Open Ubuntu and update packages
```bash
wsl -d Ubuntu
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv -y
```

3. Install Python 3.11
```bash
sudo apt update
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev -y
```

4. Make script executable and run
```bash
chmod +x install_airflow.sh
./install_airflow.sh
```

5. Create and activate a Python virtual environment
```bash
cd ~
python3.11 -m venv airflow_venv
source airflow_venv/bin/activate
```

6. Run airflow
```bash
# Start scheduler (in one terminal)
airflow scheduler

# Start webserver (in another terminal or background)
airflow webserver --port 8080
```

7. Open browser and go to
```bash
http://localhost:8080
```

## Airflow commands 


### Environment setup
```bash
airflow db init                   # Initialize metadata DB (run once)
airflow db upgrade                # Upgrade DB schema if needed
airflow users create              # Create admin user
```
### Summary Flow
- Save duckdb to QuantTrade\data\processed\alpaca\minute.duckdb

```bash
            +----------------+
            | Fetch Alpaca   |
            | 1-min bars     |
            +-------+--------+
                    |
                    v
      +-------------+--------------+
      | Load existing CSV (if any) |
      +-------------+--------------+
                    |
                    v
    +-------------------------------+
    | Merge + deduplicate by index |
    +-------------------------------+
           |                     |
           v                     v
     Save as CSV         Save to DuckDB table
                         - minute_SPY
                         - minute_QQQ
                         ...

```

### DAG Management
```bash
airflow dags list                 # List all discovered DAGs
airflow dags list-import-errors  # List all discovered DAGs (with compile errors)
airflow dags show <dag_id>       # Visualize DAG structure (ASCII tree)
airflow dags trigger <dag_id>    # Trigger a DAG run manually
airflow dags pause <dag_id>      # Pause DAG (stops scheduling)
airflow dags unpause <dag_id>    # Unpause DAG (enables scheduling)
```

### Task Management
```bash
airflow tasks list <dag_id>              # List tasks in a DAG
airflow tasks test <dag_id> <task_id> <execution_date>
                                        # Run task *without* dependencies or scheduler
```
### DAG & Task State Maintenance
```bash
airflow tasks clear <dag_id> --start-date <date> --end-date <date>
                                        # Clear task instance states
airflow dags delete <dag_id>            # Delete all DAG runs + metadata

```
### Monitoring & Logs
```bash
airflow tasks logs <dag_id> <task_id> <execution_date>
                                        # View logs for a specific task
```
### Scheduler & Webserver
```bash
airflow scheduler               # Start the scheduler (restarts DAG parsing)
airflow webserver               # Launch the Airflow web UI
```