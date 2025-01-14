# Deployment

## General setup

```sh
sudo timedatectl set-timezone Asia/Jakarta

# Register access key
vim ~/.ssh/access-key-bitbucket  # Or generate the access key, need to register this access key into BitBucket

cat > ~/.ssh/config <<EOF
Host bitbucket.org
HostName bitbucket.org
User git
IdentityFile ~/.ssh/access-key-bitbucket
IdentitiesOnly yes
EOF
chmod 400 ~/.ssh/config

# Output file for cdc logs
mkdir -p cdc/output cdc/dockerlogs

# Install python
sudo apt install -y python3.12 python3.12-venv
mkdir ~/venv

# Install java
sudo apt update -y && sudo apt install -y default-jre

# Install certbot
sudo apt install -y certbot

# Clone git repository
git clone git@bitbucket.org:xz2/demo.git

# Install PostgreSQL 16
bash <<EOF
set -e
sudo apt install curl ca-certificates
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc

sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

sudo apt update -y
sudo apt install -y postgresql-16
EOF

# Install docker
bash <<EOF
set -e
sudo apt update -y && sudo apt install -y ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update -y && sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo usermod -aG docker $USER && newgrp docker
EOF
```

## PostgreSQL

```sh
sudo su - postgres -c psql
```

```sql
ALTER SYSTEM SET wal_level = logical;
SHOW wal_level;
```

```sh
sudo systemctl restart postgresql.service
sudo su - postgres -c psql
```

```sql
CREATE USER airflow WITH PASSWORD '12321' LOGIN;
CREATE USER airflow_readonly WITH PASSWORD '12321' LOGIN;
CREATE USER metabase WITH PASSWORD '12321' LOGIN;
CREATE USER metabase_readonly WITH PASSWORD '12321' LOGIN;
CREATE USER repl WITH PASSWORD '12321' LOGIN REPLICATION;
CREATE USER repl_readonly WITH PASSWORD '12321' LOGIN;

CREATE DATABASE airflow;
CREATE DATABASE metabase;
CREATE DATABASE cdc;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;
GRANT ALL PRIVILEGES ON DATABASE cdc TO repl;

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

\c metabase
-- Publication
CREATE PUBLICATION "metabase" FOR ALL TABLES;
SELECT PG_CREATE_REPLICATION_SLOTS('metabase', 'pgoutput');
GRANT USAGE ON SCHEMA public TO repl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl;
-- The owner
GRANT USAGE ON SCHEMA public TO metabase;
GRANT ALL ON SCHEMA public TO metabase;
-- Readonly
GRANT USAGE ON SCHEMA public TO metabase_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO metabase_readonly;
-- Create trigger to alter table replica identity to full for tables that does not have primary key
CREATE OR REPLACE FUNCTION f__set_replica_identity_full()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    has_pk boolean;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF obj.object_type = 'table' THEN
            -- Check if the table has a primary key
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = format('%I.%I', obj.schema_name, obj.objid::regclass)::regclass
                  AND contype = 'p'
            ) INTO has_pk;

            -- Apply REPLICA IDENTITY FULL only if no primary key exists
            IF NOT has_pk THEN
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', obj.schema_name, obj.objid::regclass);
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
DROP EVENT TRIGGER IF EXISTS tg__create_table__set_replica_identity_full;
CREATE EVENT TRIGGER tg__create_table__set_replica_identity_full
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE')
EXECUTE FUNCTION f__set_replica_identity_full();

\c cdc
-- Master
GRANT USAGE ON SCHEMA public TO repl;
GRANT ALL ON SCHEMA public TO repl;
-- Readonly
GRANT USAGE ON SCHEMA public TO repl_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_readonly;
```

## CDC docker image

```sh
cd cdc
docker build -t xz2-demo-cdc-streamer:__VERSION__ -t xz2-demo-cdc-streamer:latest --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.streamer .
docker build -t xz2-demo-cdc-uploader:__VERSION__ -t xz2-demo-cdc-uploader:latest --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.uploader .
```

## Metabase

Site name: **metabase.rdxz2.site**

### Install SSL certificate

```sh
sudo certbot certonly --manual --preferred-challenges dns
```

When asked for site, type **metabase.rdxz2.site**, it will generate a token, copy it

- Go to Hostinger > Select **rdxz2.site** > DNS / Nameservers, add a new record
  - Type: **TXT**
  - Name: **\_acme-challenge**
  - Content: **_Paste the token_**
- Go to https://dnschecker.org/, search for **\_acme-challenge.metabase.rdxz2.site** using type **TXT** -> should display checklist along with the provided token
- Go back to the terminal, press ENTER to finalize

```sh
sudo cat /etc/letsencrypt/live/metabase.rdxz2.site/fullchain.pem
# Copy the value into load balancer configuration: Certificate

sudo cat /etc/letsencrypt/live/metabase.rdxz2.site/privkey.pem
# Copy the value into load balancer configuration: Private key
```

### Create external regional application load balancer

**_Create the load balancer_**

## Create OAuth2.0 Client ID

- Name: **Metabase**
- Authorized JavaScript origins
  - URIs1: **https://metabase.rdxz2.site**
- Authorized redirect URIs
  - URIs1: **https://metabase.rdxz2.site**

### Configure A record

- Go to Hostinger > Select **rdxz2.site** > DNS / Nameservers, add a new record
  - Type: **A**
  - Name: **metabase**
  - Content: **_Paste load balancer IP address_**

### Run CDC service

```sh
docker run -d --name cdc-uploader-metabase -v /home/ubuntu/demo/cdc/.env.metabase:/app/.env -v /home/ubuntu/cdc/dockerlogs:/app/logs -v /home/ubuntu/cdc/output:/app/output -v /home/ubuntu/sa/xz2-demo-cdc-9fc7663fdfac.json:/app/sa.json --network host xz2-demo-cdc-uploader
docker run -d --name cdc-streamer-metabase -v /home/ubuntu/demo/cdc/.env.metabase:/app/.env -v /home/ubuntu/cdc/dockerlogs:/app/logs -v /home/ubuntu/cdc/output:/app/output -v /home/ubuntu/sa/xz2-demo-cdc-9fc7663fdfac.json:/app/sa.json --network host xz2-demo-cdc-streamer
```

- Go to https://dnschecker.org/, search for **metabase.rdxz2.site** using type **A** -> should display checklist

### Run metabase service

```sh
cd ~/demo/metabase

wget https://downloads.metabase.com/v0.52.2/metabase.jar

sudo ln -s $(pwd)/metabase.service /etc/systemd/system/metabase.service
sudo systemctl daemon-reload
sudo systemctl enable metabase.service
sudo systemctl start metabase.service
```

## Airflow

Site name: **airflow.rdxz2.site**

### Install SSL certificate

```sh
sudo certbot certonly --manual --preferred-challenges dns
```

When asked for site, type **airflow.rdxz2.site**, it will generate a token, copy it

- Go to Hostinger > Select **airflow.rdxz2.site** > DNS / Nameservers, add a new record
  - Type: **TXT**
  - Name: **\_acme-challenge**
  - Content: **_Paste the token_**
- Go to https://dnschecker.org/, search for **\_acme-challenge.airflow.rdxz2.site** using type **TXT** -> should display checklist along with the provided token
- Go back to the terminal, press ENTER to finalize

```sh
sudo cat /etc/letsencrypt/live/airflow.rdxz2.site/fullchain.pem
# Copy the value into load balancer configuration: Certificate

sudo cat /etc/letsencrypt/live/airflow.rdxz2.site/privkey.pem
# Copy the value into load balancer configuration: Private key
```

### Create external regional application load balancer

**_Create the load balancer_**

## Create OAuth2.0 Client ID

- Name: **Metabase**
- Authorized JavaScript origins
  - URIs1: **https://airflow.rdxz2.site**
- Authorized redirect URIs
  - URIs1: **https://airflow.rdxz2.site/oauth-authorized/google**

### Configure A record

- Go to Hostinger > Select **airflow.rdxz2.site** > DNS / Nameservers, add a new record
  - Type: **A**
  - Name: **airflow**
  - Content: **_Paste load balancer IP address_**
- Go to https://dnschecker.org/, search for **airflow.rdxz2.site** using type **A** -> should display checklist

### Run airflow service

```sh
python3.12 -m venv ~/venv/airflow
source ~/venv/airflow/bin/activate

cd ~/demo/airflow

AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt --constraint "${CONSTRAINT_URL}"

ln -s $(pwd)/airflow/webserver_config.py ~/airflow/webserver_config.py

airflow db init
deactivate

sudo ln -s $(pwd)/airflow-webserver.service /etc/systemd/system/airflow-webserver.service
sudo ln -s $(pwd)/airflow-scheduler.service /etc/systemd/system/airflow-scheduler.service
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver.service
sudo systemctl start airflow-webserver.service
sudo systemctl enable airflow-scheduler.service
sudo systemctl start airflow-scheduler.service

# Assign the first admin role
airflow users add-role --email __EMAIL__ --role Admin
```

### Run CDC service

```sh
docker run -d --name cdc-uploader-airflow -v /home/ubuntu/demo/cdc/.env.airflow:/app/.env -v /home/ubuntu/cdc/dockerlogs:/app/logs -v /home/ubuntu/cdc/output:/app/output -v /home/ubuntu/sa/xz2-demo-cdc-9fc7663fdfac.json:/app/sa.json --network host xz2-demo-cdc-uploader
docker run -d --name cdc-streamer-airflow -v /home/ubuntu/demo/cdc/.env.airflow:/app/.env -v /home/ubuntu/cdc/dockerlogs:/app/logs -v /home/ubuntu/cdc/output:/app/output -v /home/ubuntu/sa/xz2-demo-cdc-9fc7663fdfac.json:/app/sa.json --network host xz2-demo-cdc-streamer
```
