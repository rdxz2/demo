# Virtual env installation

```sh
conda create -n xz2demoairflow312 python=3.12 -y
conda activate xz2demoairflow312

AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt --constraint "${CONSTRAINT_URL}"
```

**_Important! to install new package, use the constraint file_**

```sh
pip install __PACKAGE__ --constraint "${CONSTRAINT_URL}"
```

# Database setup

## Airflow metadata database

```sql
CREATE USER airflow WITH PASSWORD '12321' LOGIN;

CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow
GRANT ALL ON SCHEMA public TO airflow;
```

# Airflow service setup

```sh
ln -s __REPO_DIR__/airflow/webserver_config.py __AIRFLOW_HOME__/
```

Init DB

```sh
airflow db init
```

# Run Airflow locally

Run scheduler

```sh
airflow scheduler
```

Run webserver

```sh
airflow webserver
```

Create admin user locally (DB auth)

```sh
airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@somesite.site
```

# Run Airflow as system service

Web server

```sh
sudo ln -s __REPO_DIR__/airflow/airflow-webserver.service /etc/systemd/system/airflow-webserver.service

sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver.service
sudo systemctl start airflow-webserver.service
```

Scheduler

```sh
sudo ln -s __REPO_DIR__/airflow/airflow-scheduler.service /etc/systemd/system/airflow-scheduler.service

sudo systemctl daemon-reload
sudo systemctl enable airflow-scheduler.service
sudo systemctl start airflow-scheduler.service
```

# Assign admin role for new user login

```sh
airflow users add-role --email __EMAIL__ --role Admin
```
