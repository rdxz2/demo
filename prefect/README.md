# Virtual env installation

```sh
conda create -n xz2demoprefect312 python=3.12 -y
conda activate xz2demoprefect312
pip install -r requirements.txt

prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:12321@localhost:5432/prefect"
```

# Database setup

## Prefect metadata database

```sql
CREATE USER prefect WITH PASSWORD '12321' LOGIN;
CREATE DATABASE prefect OWNER prefect;
```

# Running prefect server

```sh
conda activate xz2demoprefect312
prefect server start
```
