# PostgreSQL installation

```sh
docker volume create pg__data

docker run \
    --name pg \
    -d \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=12321 \
    -v pg__data:/var/lib/postgresql/data \
    postgres:16
```

## Airflow

### Airflow metadata database

```sql
CREATE USER airflow WITH PASSWORD '12321' LOGIN;

CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow
GRANT ALL ON SCHEMA public TO airflow;
```
