#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, Integer, Float, Text, DateTime

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    # Download the Parquet file
    parq_name = 'output.parquet'
    os.system(f"curl {url} -o {parq_name}")

    # Connect to PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the Parquet file
    df = pq.read_table(parq_name).to_pandas()

    # Define the schema with SQLAlchemy types
    schema = {
        'VendorID': Integer(),
        'tpep_pickup_datetime': DateTime(),
        'tpep_dropoff_datetime': DateTime(),
        'passenger_count': Float(),
        'trip_distance': Float(),
        'RatecodeID': Float(),
        'store_and_fwd_flag': Text(),
        'PULocationID': Integer(),
        'DOLocationID': Integer(),
        'payment_type': Integer(),
        'fare_amount': Float(),
        'extra': Float(),
        'mta_tax': Float(),
        'tip_amount': Float(),
        'tolls_amount': Float(),
        'improvement_surcharge': Float(),
        'total_amount': Float(),
        'congestion_surcharge': Float(),
        'airport_fee': Float()
    }

    # Write DataFrame to PostgreSQL with manual schema definition
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, dtype=schema)

    print('Data ingestion completed.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest Parquet to Postgres")
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='name of the table in db')
    parser.add_argument('--url', help='url of the file')
    args = parser.parse_args()
    main(args)
