#!/usr/bin/env bash

## bash script to run the ingestion container
docker run -it --rm \
  --network=pipeline_pg-network \
  taxi_ingest:v001 \
  --year=2021 \
  --month=1 \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=pgdatabase \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --chunksize=100000 \
  --target-table=yellow_taxi_trips