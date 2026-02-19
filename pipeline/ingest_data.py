import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--start-date', default=None, help='Start date [YYYY-MM]')
@click.option('--end-date', default=None, help='End date [YYYY-MM]')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, start_date, end_date):

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

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    for year_month in pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y-%m'):
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
        file_name = f'yellow_tripdata_{year_month}.csv.gz'
        url = f'{prefix}/yellow_tripdata_{year_month}.csv.gz'

        print(f'Processing {file_name}...\n')

        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=100_000
        )

        row_counter = 0
        for df_chunk in tqdm(df_iter):

            row_counter += df_chunk.shape[0]

            # Insert chunk
            df_chunk['VendorID'] = df_chunk['VendorID'].fillna(0)
            df_chunk.to_sql(
                name="yellow_taxi_data",
                con=engine,
                if_exists="append"
            )
            print(f'\t{row_counter:,} rows inserted.')

        print(f'Done processing {file_name}!!!\n')

if __name__ == "__main__":
    print('Ingestion of data started...')
    main()
    print('Ingestion complete.')
