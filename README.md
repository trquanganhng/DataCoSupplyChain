# DataCoSupplyChain
Dockerized Data Ingestion and Visualization with Python, PostgreSQL, pgAdmin, and Tableau

### Dataset
Source: https://data.mendeley.com/datasets/8gx2fvg2k6/5

URL = https://data.mendeley.com/public-files/datasets/8gx2fvg2k6/files/72784be5-36d3-44fe-b75d-0edbf1999f65/file_downloaded

Downloading the data
```bash
wget https://github.com/trquanganhng/DataCoSupplyChain/releases/download/Download/DataCoSupplyChainDataset.csv
```

### Description
I have split the Dataset into multiple tables based on the file "DescriptionDataCoSupplyChain.csv"(https://github.com/trquanganhng/DataCoSupplyChain/releases/download/Download/DescriptionDataCoSupplyChain.csv).

The dataset will be separated into tables as follows (https://github.com/trquanganhng/DataCoSupplyChain/files/15068615/DataCoSupplyChain.pdf)

After successfully inputting data into the Database. I created a few Dashboards from these data using Tableau.

Summary: https://github.com/trquanganhng/DataCoSupplyChain/files/15068619/Visualization.pdf

Folders: https://github.com/trquanganhng/DataCoSupplyChain/tree/d679b9d043dc24caf51cc14f5795855f40610ec7/Visualization

## Usage
#### MacOS

#### Clone Repository
```bash
git clone https://github.com/trquanganhng/DataCoSupplyChain.git
```
### Running Postgres and pgAdmin

#### Create a network
```bash
docker network create pgnetwork
```

#### Running Postgres
```
docker run -it \
  -e POSTGRES_USER="admin" \
  -e POSTGRES_PASSWORD="admin" \
  -e POSTGRES_DB="DataCoSupplyChain" \
  -v $(pwd)/DataCoSupplyChain_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pgnetwork \
  --name pgdatabase \
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
  --network=pgnetwork \
  --name pgadmin \
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
docker build -t ingest_data:v001 .
```
#### Run the built docker image
```
URL="https://github.com/trquanganhng/DataCoSupplyChain/releases/download/Download/DataCoSupplyChainDataset.csv"

docker run -it \
  --network=pgnetwork \
  ingest_data:v001 \
  --user=admin \
  --password=admin \
  --host=pgdatabase \
  --port=5432 \
  --db=DataCoSupplyChain \
  --url=${URL} \
```
