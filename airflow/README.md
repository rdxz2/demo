# Virtual env installation

```sh
conda create -n xz2demoairflow312 python=3.12 -y
conda activate xz2demoairflow312

AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
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
GRANT ALL ON SCHEMA public TO airflow;
```

# Running airflow

```sh
ln -s /home/ubuntu/repos/xz2/demo/airflow/dags /home/ubuntu/airflow
ln -s /home/ubuntu/repos/xz2/demo/airflow/plugins /home/ubuntu/airflow

airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@rdxz2.site
```
