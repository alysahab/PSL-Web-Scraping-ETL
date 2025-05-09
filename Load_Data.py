import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


class Load:
    def __init__(self):
        """Initialize database connection with env credentials"""
        load_dotenv(override=True)
        USER = os.getenv('USER')
        PASSWORD = os.getenv('PASSWORD')
        HOST = os.getenv('HOST')
        DBNAME = os.getenv('DBNAME')

        try:
            self.engine = create_engine(
                f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DBNAME}',
                pool_size=10,
                max_overflow=20
            )
        except Exception as e:
            print(f'Connection Error: {e}')
            self.engine = None

    def load_data(self):
        """Batch load transformed data to AWS RDS"""
        datasets = {
            'batting_data': 'Data/batting_records_cleaned.csv',
            'bowling_data': 'Data/bowling_records_cleaned.csv',
            'players_metadata': 'Data/players_metadata_cleaned.csv'
        }

        for table_name, file_path in datasets.items():
            print(f'Loading {table_name}...')
            df = pd.read_csv(file_path)
            df.to_sql(
                name=table_name,
                con=self.engine,
                index=False,
                if_exists='replace',
                method='multi',      # faster insertion then default
                chunksize=1000
            )
            print(f'{table_name} loaded successfully')



# AWS Setup
# load_dotenv()
# con = mysql.connector.connect(
#     user=os.getenv('USER'),
#     host=os.getenv('HOST'),
#     password=os.getenv('PASSWORD'),
#     port=int(os.getenv('PORT'))
# )
#
# with con.cursor() as mycursor:
#     mycursor.execute('CREATE DATABASE IF NOT EXISTS psl')
#     print('PSL database created')