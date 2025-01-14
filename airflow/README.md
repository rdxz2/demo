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
CREATE USER airflow_readonly WITH PASSWORD '12321' LOGIN;

CREATE DATABASE airflow;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow
-- Publication
CREATE PUBLICATION "airflow" FOR ALL TABLES;
SELECT PG_CREATE_REPLICATION_SLOTS('airflow', 'pgoutput');
GRANT USAGE ON SCHEMA public TO repl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl;
-- The owner
GRANT USAGE ON SCHEMA public TO airflow;
GRANT ALL ON SCHEMA public TO airflow;
-- Readonly
GRANT USAGE ON SCHEMA public TO airflow_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO airflow_readonly;
```

# Run Airflow locally

Database initialization

```sh
airflow db init
```

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
airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@some.site
```
