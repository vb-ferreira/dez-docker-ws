#!/usr/bin/env python
# coding: utf-8

import pandas as pd
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

def run():
    # Variáveis para parametrização
    year = 2021
    month = 1
    pg_user = "root"  
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = '5432'
    pg_db = "ny_taxi"
    table_name = 'yellow_taxi_data'
    chunksize = 100000

    # Cria conexão com a base de dados
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Cria uma tabela vazia usando o cabeçalho do df (o mesmo que fazer um CREATE TABLE)
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

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
                name=table_name,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))

    print('Goodbye!')

if __name__ == '__main__':
    run()
