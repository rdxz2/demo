# Virtual env installation

```sh
conda create -n xz2demoprefect312 python=3.12 -y
conda activate xz2demoprefect312
pip install -r requirements.txt

prefect config set PREFECT_API_URL='https://prefect.rdxz2.site/api'
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:12321@localhost:5432/prefect"
prefect config set PREFECT_LOGGING_LEVEL="INFO"
```

# Database setup

## Prefect metadata database

```sql
CREATE USER prefect WITH PASSWORD '12321' LOGIN;
CREATE DATABASE prefect OWNER prefect;
```

# Run Prefect locally

The web server

```sh
prefect server start
```

## Managing worker pools (worker processes)

Creating work pool

```sh
prefect work-pool create __WORK_POOL_NAME__
```

Starting work pool

```sh
prefect worker start --pool __WORK_POOL_NAME__
```

## Deploying a flow

Based on the [deployment file](./prefect.yaml)

```sh
# All flows
prefect deploy --all

# Or a single flow
prefect deploy --name __FLOW_NAME__
```

# Run Prefect as system service

Web server

```sh
sudo ln -s /path/to/prefect-server.service /etc/systemd/system/prefect-server.service

sudo systemctl daemon-reload
sudo systemctl enable prefect-server.service
sudo systemctl start prefect-server.service
```

Worker

```sh
sudo ln -s /path/to/prefect-worker.service /etc/systemd/system/prefect-worker.service

sudo systemctl daemon-reload
sudo systemctl enable prefect-worker.service
sudo systemctl start prefect-worker.service
```
