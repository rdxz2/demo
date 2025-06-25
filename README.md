# Hello

My name is Richard and I am a data engineer working with ELT pipelines and infrastructure on daily-basis.

This demo project will showcase my various capabilities and understanding as a data engineer:

- Advanced programming skill and packaging using **Python**.
- OLTP, database management and WAL log streaming using **PostgreSQL**.
- OLAP, datawarehouse management and data transformation using **BigQuery** and **dbt**.
- Data visualization using **Metabase**, including self-hosted service and management.
- Data orchestration and scheduling using **Airflow**, including self-hosted service and management.
- Containerization using **Docker**.
- CI/CD utilizing **GHA** and **Ansible**.
- Third-party library usage with **Discord**.
- Infrastructure management on top of **GCP**.

Table of contents

- [Hello](#hello)
- [What is this project?](#what-is-this-project)
- [Part 1 Producer](#part-1-producer)
- [Part 2 CDC](#part-2-cdc)
  - [Part 2a Streamer](#part-2a-streamer)
  - [Part 2b Uploader](#part-2b-uploader)
  - [Part 3c Merger](#part-3c-merger)
- [Part 3 Airflow](#part-3-airflow)
  - [Part 3a Public data](#part-3a-public-data)
  - [Part 3b dbt](#part-3b-dbt)
- [Part 4 Metabase](#part-4-metabase)
- [Part xx Deployment workflow](#part-xx-deployment-workflow)
- [Appendix 1 Setup from scratch](#appendix-1-setup-from-scratch)
  - [Setup Docker network](#setup-docker-network)
  - [Setup PostgreSQL database](#setup-postgresql-database)
    - [Create replication user](#create-replication-user)
    - [Setup "dummy" application database, with replication](#setup-dummy-application-database-with-replication)
    - [Setup Metabase database, with replication](#setup-metabase-database-with-replication)
  - [Setup CDC](#setup-cdc)
- [Running container](#running-container)
  - [Setup Airflow](#setup-airflow)

# What is this project?

This project is about utilizing various well-known technologies in the data engineering field. It is composed of Python scripts, shell scripts, YAML configurations, and SQL queries throughout different products. The goal is to get those things tied up and synergize with each other, solving common business questions: **_how do I store all of these messy data coming from various sources into a single place, get business insights and act based on it?_**

The project consists of different components and several flows, as described in the image below.

_[image placeholder]_

This readme file will explains all of the running components and how all of them relate to each other.

# Part 1 Producer

# Part 2 CDC

CDC (Change Data Capture)

## Part 2a Streamer

## Part 2b Uploader

## Part 3c Merger

# Part 3 Airflow

## Part 3a Public data

## Part 3b dbt

# Part 4 Metabase

# Part xx Deployment workflow

# Appendix 1 Setup from scratch

This section will show all the commands needs to build the project from scratch. Although most of the deployment process can be automated, we need some knowledge on what the automation scripts is actually doing in the background, which can be learnt through manual executions.

## Setup Docker network

```sh
docker network create demo
```

## Setup PostgreSQL database

Create container

```sh
docker volume create demo-pg
docker run --name demo-pg -d -p 40000:5432 -e POSTGRES_PASSWORD=12321 -v demo-pg:/var/lib/postgresql/data --network demo postgres:17
docker exec -it demo-pg psql -U postgres
```

Change WAL level into logical

```sql
alter system set wal_level = logical;
```

Restart container to load new config

```sh
docker restart demo-pg
```

Verify new config loaded

```sh
docker exec -it demo-pg psql -U postgres -c "show wal_level"  # Should print logical
```

### Create replication user

```sh
docker exec -it demo-pg psql -U postgres
```

```sql
create user repl with password '12321' login replication;
```

### Setup "dummy" application database, with replication

```sh
docker exec -it demo-pg psql -U postgres
```

```sql
create user dummy with password '12321' login;
create database dummydb owner dummy;

\c dummydb
create publication "repl" for all tables;
select pg_create_logical_replication_slot('repl_dummydb', 'pgoutput');  -- Name must be unique across DB instance
```

### Setup Metabase database, with replication

```sh
docker exec -it demo-pg psql -U postgres
```

```sql
create user metabase with password '12321' login;
create database metabasedb owner metabase;

\c metabasedb
create publication "repl" for all tables;
select pg_create_logical_replication_slot('repl_metabasedb', 'pgoutput');  -- Name must be unique across DB instance
```

## Setup CDC

# Running container

```sh
docker run -itd --name=demo-producer -v /home/ubuntu/repos/demo/producer/pg.json:/app/pg.json --network demo demo-producer:0.0.1
docker run -itd --name=demo-streamer -v /home/ubuntu/repos/demo/cdc/outputs:/app/outputs -v /home/ubuntu/repos/demo/cdc/logs:/app/logs -v /home/ubuntu/repos/demo/cdc/ageless-aura-462704-t4-a13ab9de1b8a-cdc.json:/app/sa.json -v /home/ubuntu/repos/demo/cdc/.env.dummydb:/app/.env --network demo demo-streamer:0.0.1
docker run -itd --name=demo-uploader -v /home/ubuntu/repos/demo/cdc/outputs:/app/outputs -v /home/ubuntu/repos/demo/cdc/logs:/app/logs -v /home/ubuntu/repos/demo/cdc/ageless-aura-462704-t4-a13ab9de1b8a-cdc.json:/app/sa.json -v /home/ubuntu/repos/demo/cdc/.env.dummydb:/app/.env --network demo demo-uploader:0.0.1
```

## Setup Airflow
