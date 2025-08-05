#!/bin/bash

# ✅ Step 0: Activate virtual environment (update this if needed)
python3 -m venv airflow_venv
source airflow_venv/bin/activate

# ✅ Step 1: Set environment variable for Airflow
export AIRFLOW_HOME=~/airflow

# ✅ Step 2: Install Airflow with constraints
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1,2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Apache Airflow $AIRFLOW_VERSION for Python $PYTHON_VERSION..."
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# ✅ Step 3: Initialize the Airflow database
echo "Initializing Airflow database..."
airflow db init

# ✅ Step 4: Create an admin user
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Luyanda \
    --lastname Mncube \
    --role Admin \
    --email admin@example.com \
    --password admin

# ✅ Final message
echo "✅ Airflow installed and configured!"
echo "Run the following in separate terminals:"
echo "1. source airflow_venv/bin/activate"
echo "2. airflow webserver --port 8080"
echo "3. airflow scheduler"
