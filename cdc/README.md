# Virtual env installation

```sh
conda create -n xz2democdc312 python=3.12 -y
conda activate xz2democdc312
pip install -r requirements.txt
```

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

## Streamed database

```sql
ALTER SYSTEM SET wal_level = logical;
SHOW wal_level;

CREATE USER repl WITH PASSWORD '12321';
ALTER ROLE repl WITH REPLICATION LOGIN;
```

```sh
docker restart pg
```

```sql
CREATE DATABASE stream;
\c stream
CREATE PUBLICATION publ_stream FOR ALL TABLES;

-- Replication slot will be automatically created by the streamer
```

## Replication metadata database

```sql
CREATE DATABASE repl OWNER repl;
```

# Redis installation

For Discord bot utilization

```sh
docker volume create rd__data

docker run \
    --name rd \
    -d \
    -p 6379:6379 \
    -v rd__data:/data \
    redis:7.4 \
    redis-server \
    --save 60 1
```
