import io
import datetime
import logging

import numpy as np
import psycopg2
import psycopg2.extras
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs

from utils import db_connect
from pathlib import Path
import os

PROJECT_ROOT = str(Path(os.path.abspath(__file__)).parents[1])

WX_DATA = PROJECT_ROOT + '/wx_data'
YLD_DATA = PROJECT_ROOT + '/yld_data'

WX_SCHEMA = 'wx_schema'
YLD_SCHEMA = 'yld_schema'
WX_TABLE = 'wx_data'
YLD_TABLE = 'yld_data'
AVG_TABLE = 'avg_table'

WX_COLS = ['DATE', 'MAX_TEMP_1_10_DEG_C', 'MIN_TEMP_1_10_DEG_C', 'PRECIPITATION_1_10_MM']
YLD_COLS = ['YEAR', 'TOTAL_CORN_GRAIN_YIELD_MT']


class Ingestor():
    def __init__(self, schema, table):
        self.schema = schema
        self.table = table

    def ingest_data(self, query):
        """
        Reads data from local dir, ingests into Postgres db
        """
        logging.info('Ingesting data: ' + self.table)
        data_is_wx = self.table == 'wx_data'
        keys, data_dir, data_cols = (['FILE', 'DATE'], WX_DATA, WX_COLS) if data_is_wx \
            else (['YEAR'], YLD_DATA, YLD_COLS)

        # concatenate all csvs in dir into df
        file_list = [file for file in os.listdir(data_dir) if file.endswith('.txt')]
        df = pd.concat([self.ingest_data_helper(file, data_dir, data_cols) for file in file_list])
        df = df.drop_duplicates(keys)

        # only keep filename for wx_data, rename
        if data_is_wx:
            df.rename(columns={'FILE': 'STATION_ID'}, inplace=True)
            df['DATE'] = pd.to_datetime(df['DATE'].astype(str), format='%Y%m%d')
        else:
            df.drop('FILE', axis=1, inplace=True)

        self.create_table(self.table, query)
        self.upload_to_db(df)

    def ingest_data_helper(self, file, data_dir, cols):
        """
        Reads csv as DataFrame, adds FILE col
        Returns: Pandas DataFrame
        """
        df = pd.read_csv(f'{data_dir}/{file}', sep='\t', names=cols)
        df['FILE'] = Path(file).stem
        return df

    def create_table(self, table, query):
        """
        Creates schema and table in Postgres if not exists
        """
        logging.info(f'Creating schema (if not exists): {self.schema}')
        session = db_connect()
        cur = session.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema}')

        logging.info(f'Creating table (if not exists): {self.schema}.{table}')

        cur.execute(query)
        session.commit()
        cur.close()
        session.close()

    def upload_to_db(self, df):
        """
        Writes df to Postgres db, gets rows added count
        """
        logging.info(f'Writing to table: {self.schema}.{self.table}')
        psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
        session = db_connect()
        cur = session.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f'SELECT COUNT(*) FROM {self.schema}.{self.table}')
        prev_count = cur.fetchone()['count']

        # store new csv in memory and insert
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, date_format='%Y-%m-%d')
        buffer.seek(0)

        query = f'''
        BEGIN;
        CREATE TEMP TABLE tmp_table 
        (LIKE {self.schema}.{self.table} INCLUDING DEFAULTS)
        ON COMMIT DROP;

        COPY tmp_table({','.join(df.columns)}) FROM STDOUT CSV HEADER;

        INSERT INTO {self.schema}.{self.table}
        SELECT *
        FROM tmp_table
        ON CONFLICT DO NOTHING;
        COMMIT;
        '''
        cur.copy_expert(query, buffer)
        session.commit()

        cur.execute(f'SELECT COUNT(*) FROM {self.schema}.{self.table}')
        new_count = cur.fetchone()['count']
        logging.info(f'Rows added: {str(new_count - prev_count)}')
        cur.close()
        session.close()

    def generate_avg_table(self):
        """
        Creates average table, fills with filtered data
        """
        logging.info('Creating averages table (if not exists)')

        query = f'''
        CREATE TABLE IF NOT EXISTS {self.schema}.{AVG_TABLE}(
            STATION_ID VARCHAR(20),
            YEAR int4,
            AVG_MAX_TEMP int4,
            AVG_MIN_TEMP int4,
            TOTAL_PRECIPITATION int4,
            PRIMARY KEY(STATION_ID,YEAR)
        );
        '''
        self.create_table(AVG_TABLE, query)

        logging.info('Writing to averages table')
        session = db_connect()
        cur = session.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = f'''
        INSERT INTO {self.schema}.{AVG_TABLE}
        (STATION_ID, YEAR, AVG_MAX_TEMP, AVG_MIN_TEMP, TOTAL_PRECIPITATION)
        SELECT
            STATION_ID,
            EXTRACT(YEAR FROM DATE) as YYYY,
            AVG(MAX_TEMP_1_10_DEG_C) FILTER (WHERE MAX_TEMP_1_10_DEG_C <> -9999) / 10,
            AVG(MIN_TEMP_1_10_DEG_C) FILTER (WHERE MIN_TEMP_1_10_DEG_C <> -9999) / 10,
            SUM(PRECIPITATION_1_10_mm) FILTER (WHERE PRECIPITATION_1_10_mm <> -9999) / 100
        FROM
            {self.schema}.{self.table}
        GROUP BY
            STATION_ID,
            EXTRACT(YEAR FROM DATE)
        ON CONFLICT DO NOTHING;
        '''
        cur.execute(query)
        session.commit()
        cur.close()
        session.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(asctime)s: %(filename)s: %(lineno)d : %(message)s',
        datefmt='%d-%b-%y %H:%M:%S')

    query_wx = f'''
    CREATE TABLE IF NOT EXISTS {WX_SCHEMA}.{WX_TABLE}(
        STATION_ID VARCHAR(20),
        DATE date,
        MIN_TEMP_1_10_DEG_C int4,
        MAX_TEMP_1_10_DEG_C int4,
        PRECIPITATION_1_10_mm int4,
        PRIMARY KEY(STATION_ID,DATE)
    );
    '''
    query_yld = f'''
    CREATE TABLE IF NOT EXISTS {YLD_SCHEMA}.{YLD_TABLE}(
        YEAR int4 PRIMARY KEY,
        TOTAL_CORN_GRAIN_YIELD_MT int4
    );
    '''

    start_time = datetime.datetime.now()
    logging.info(f"Starting to ingest weather data at {start_time}")
    Ingestion_Obj_WX = Ingestor(WX_SCHEMA, WX_TABLE)
    Ingestion_Obj_WX.ingest_data(query_wx)
    logging.info(f"Completed ingesting weather data at {datetime.datetime.now()}")
    logging.info(f"Total time to ingest weather data {datetime.datetime.now() - start_time}")

    start_time = datetime.datetime.now()
    logging.info(f"Starting to ingest yield data at {start_time}")
    Ingestion_Obj_YLD = Ingestor(YLD_SCHEMA, YLD_TABLE)
    Ingestion_Obj_YLD.ingest_data(query_yld)
    logging.info(f"Completed ingesting yield data at {datetime.datetime.now()}")
    logging.info(f"Total time to ingest yield data {datetime.datetime.now() - start_time}")

    start_time = datetime.datetime.now()
    logging.info(f"Starting to compute averages {start_time}")
    Ingestion_Obj_WX.generate_avg_table()
    logging.info(f"Completed computing averages at {datetime.datetime.now()}")
    logging.info(f"Total time to computer averages {datetime.datetime.now() - start_time}")

    logging.info("Ingestion of data is complete")