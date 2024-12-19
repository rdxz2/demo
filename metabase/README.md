# Installation

Download Metabase binary file

```sh
wget https://downloads.metabase.com/v0.52.2/metabase.jar
```

Install Java

```sh
sudo apt update -y && sudo apt install -y default-jre
```

# Database setup

## Metabase metadata database

```sql
CREATE USER metabase WITH PASSWORD '12321' LOGIN;
CREATE DATABASE metabase OWNER metabase;
\c metabase
CREATE PUBLICATION "metabase" FOR ALL TABLES;
```

# Run Metabase

```sql
source .env.sh
java -jar metabase.jar
```
