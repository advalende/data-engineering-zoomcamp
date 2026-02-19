# data-engineering-zoomcamp
Learning about data engineering with Datatalksclub


## Postgres Database

```bash
docker run -it --rm \
  --network=pg-network \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18
```

```bash 
uv add sqlalchemy "psycopg[binary,pool]"
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```

## Test ingestion script

```bash
uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host='localhost' \
  --pg-port=5432 \
  --pg-db='ny_taxi' \
  --target-table='yellow_taxi_data' \
  --start-date='2019-02' \
  --end-date='2019-03'
```

## Dockerizing ingestion script

```bash
docker run -it --rm \
  taxi_ingest:v001 \
  --network=pg-network \
  --pg-user=root \
  --pg-pass=root \
  --pg-host='localhost' \
  --pg-port=5432 \
  --pg-db='ny_taxi' \
  --target-table='yellow_taxi_data' \
  --start-date='2019-01' \
  --end-date='2019-01'
```