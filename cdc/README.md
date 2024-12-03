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

## Replication

```sql
CREATE USER repl WITH PASSWORD '12321';
CREATE DATABASE stream;

ALTER ROLE repl WITH REPLICATION LOGIN;
ALTER SYSTEM SET wal_level = logical;
```

```sh
docker restart pg
```

```sql
\c stream

CREATE PUBLICATION publ_stream FOR ALL TABLES;

-- Replication slot will be automatically created by the streamer
```

## Replication schema

```sql
CREATE DATABASE repl OWNER repl;
```