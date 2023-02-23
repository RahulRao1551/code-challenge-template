import configparser

import psycopg2
import psycopg2.extras

config = configparser.ConfigParser()
config.read('config.ini')


def db_connect():
    pg_conn = psycopg2.connect(user=config['database']['USERNAME'], \
        password=config['database']['PASSWORD'], host=config['database']['HOST'], \
        port=config['database']['PORT'], dbname=config['database']['DATABASE'])
    return pg_conn


def generate_where_clause(where_lst):
    where_lst = [x for x in where_lst if x[1] is not None]

    # adjusts query based on num of params
    if len(where_lst) == 0:
        return str()
    str_temp = f" WHERE {where_lst[0][0]} = '{where_lst[0][1]}'"
    if len(where_lst) == 1:
        return str_temp
    return str_temp + f" AND {where_lst[1][0]} = '{where_lst[1][1]}'"


def get_data(schema,table,where_clause,pagination_clause,count=False):
    selection = '*'
    if count:
        selection = 'COUNT(*)'
    session = db_connect()
    cur = session.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    query = f'''SELECT {selection} FROM {schema}.{table}{where_clause}{pagination_clause};'''
    cur.execute(query)
    res = cur.fetchall()

    cur.close()
    session.close()
    return res
