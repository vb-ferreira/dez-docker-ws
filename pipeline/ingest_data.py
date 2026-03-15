#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from tqdm import tqdm
from sqlalchemy import create_engine

# LOAD DATA

# Lê os dados
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)

n_rows = df.shape[1]

# INGESTION

# Parâmetros para a linha de comando
@click.command()
@click.option('--year', default=2021, help='Year')
@click.option('--month', default=1, help='Month')
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_trips', help='Target table name')
@click.option('--chunksize', default=100000, help='Chunk size')
def run(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    # Cria conexão com a base de dados
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Cria uma tabela vazia usando o cabeçalho do df (o mesmo qque fazer um CREATE TABLE)
    df.head(n=0).to_sql(name=f'yellow_taxi_trips_{year}_{month}', con=engine, if_exists='replace')

    # Calcula o número de chunks (considerando o tamanho de 100_000 para cada)
    n_chunks = (n_rows // chunksize) + 1

    # Cria um iterador
    df_iter = pd.read_csv(
        prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Loop de ingestão
    first = True

    for df_chunk in tqdm(df_iter, total=n_chunks):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))

    print('Goodbye!')

if __name__ == '__main__':
    run()
