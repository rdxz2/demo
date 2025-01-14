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

# Handful scripts

```sh
# Build CDC services
cd cdc
docker build -t xz2-demo-cdc-streamer:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.streamer .
docker build -t xz2-demo-cdc-uploader:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.uploader .
docker build -t xz2-demo-cdc-producer:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.producer .

# Run CDC services (stream database)
cd cdc
docker rm -f cdc-streamer-stream && docker run --name cdc-streamer-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1
docker rm -f cdc-uploader-stream && docker run --name cdc-uploader-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1
docker rm -f cdc-producer-stream && docker run --name cdc-producer-stream -v ./.env.stream:/app/.env --network host xz2-demo-cdc-producer:v0.0.1

# Run CDC services (metabase database)
cd cdc
docker rm -f cdc-streamer-metabase && docker run --name cdc-streamer-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1
docker rm -f cdc-uploader-metabase && docker run --name cdc-uploader-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1

# Stop all CDC services
docker ps --filter name=cdc-* -q | xargs docker stop

# Start CDC service (previously created)
docker start -a cdc-streamer-stream
docker start -a cdc-uploader-stream
docker start -a cdc-producer-stream
docker start -a cdc-streamer-metabase
docker start -a cdc-uploader-metabase

# Run metabase
cd metabase
./run.sh

# Run prefect server
cd prefect
prefect server start
```

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
```

Install python

```sh
sudo apt install -y python3.12 python3.12-venv
```

Install certbot

```sh
sudo apt install -y certbot
```

Install PostgreSQL

```sh
sudo apt install curl ca-certificates
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc

sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

sudo apt update -y
sudo apt install -y postgresql-16
```

Clone git repository

```sh
git clone git@bitbucket.org:xz2/demo.git
```

## Metabase

Site name: **metabase.rdxz2.site**

### Install SSL certificate

```sh
sudo certbot certonly --manual --preferred-challenges dns
```

When asked for site, type **metabase.rdxz2.site**, it will generate a token, copy it

- Go to Hostinger > Select **metabase.rdxz2.site** > DNS / Nameservers, add a new record
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

**Create the load balancer**

## Create OAuth2.0 Client ID

- Name: **Metabase**
- Authorized JavaScript origins
  - URIs1: **https://metabase.rdxz2.site**
- Authorized redirect URIs
  - URIs1: **https://metabase.rdxz2.site**

### Configure A record

- Go to Hostinger > Select **metabase.rdxz2.site** > DNS / Nameservers, add a new record
  - Type: **A**
  - Name: **metabase**
  - Content: **_Paste load balancer IP address_**
- Go to https://dnschecker.org/, search for **metabase.rdxz2.site** using type **A** -> should display checklist

### Run metabase service

See: [Metabase section](./metabase/README.md)

### Renew SSL certificate

```sh
sudo crontab -e

0 0 * * * certbot renew --quiet
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

**Create the load balancer**

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

See: [Airflow section](./airflow/README.md)
