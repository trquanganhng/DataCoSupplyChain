# DataCoSupplyChain
Dockerized Data Ingestion and Visualization with Python, PostgreSQL, pgAdmin, and Tableau

## Docker Commands
#### MacOS
### Running Postgres and pgAdmin

#### Create a network
```bash
docker network create pgnetwork-SCD
```

#### Running Postgres
```
docker run -it \
  -e POSTGRES_USER="admin" \
  -e POSTGRES_PASSWORD="admin" \
  -e POSTGRES_DB="DataCoSupplyChain" \
  -v $(pwd)/DataCoSupplyChain_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pgnetwork-SCD \
  --name pgdatabase-SCD \
  postgres:14
```
##### Test connection
```bash
pgcli -h localhost -p 5432 -u admin -d DataCoSupplyChain
```

#### Running pgAdmin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="admin" \
  -p 8080:80 \
  --network=pgnetwork-SCD \
  --name pgadmin-SCD \
  dpage/pgadmin4
```
#### Running locally
```
URL="https://github.com/trquanganhng/DataCoSupplyChain/releases/download/Download/DataCoSupplyChainDataset.csv"

python ingest_data.py \
  --user=admin \
  --password=admin \
  --host=localhost \
  --port=5432 \
  --db=DataCoSupplyChain \
  --url=${URL} \
```
#### Build docker image
```bash
docker build -t data_ingest:v001 .
```

## Usage

#### Use Docker-Compose to run Postgres and pgAdmin together
```bash
docker-compose up -d
```

#### Run the built docker image
```
URL="https://github.com/trquanganhng/DataCoSupplyChain/releases/download/Download/DataCoSupplyChainDataset.csv"

docker run -it \
  --network=pgnetwork-SCD \
  data_ingest:v001 \
  --user=admin \
  --password=admin \
  --host=pgdatabase-SCD \
  --port=5432 \
  --db=DataCoSupplyChain \
  --url=${URL} \
```
