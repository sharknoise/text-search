#!/usr/bin/env python
import ast
from os import getenv

import pandas
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# configuration
HOST = getenv('PG_HOST')
DATABASE = getenv('PG_DATABASE')
USER = getenv('PG_USER')
PASSWORD = getenv('PG_PASSWORD')
CSV_SOURCE = 'https://drive.google.com/uc?export=download&confirm=s5vl&id=1O5rOunfzkkF4vIZXk3WCbb6A2XpRPDt1'

pg_connection = psycopg2.connect(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port='5432',
    sslmode='require',
)
pg_connection.autocommit = True
pg_cursor = pg_connection.cursor()

with open('schema.sql', 'r') as schema_file:
    pg_cursor.execute(schema_file.read())

data = pandas.read_csv(CSV_SOURCE, header='infer')

# prepare for inserting into PostgreSQL as a varchar array
data['rubrics'] = data['rubrics'].apply(ast.literal_eval)

# dataframe ids start from 0, we want ids in db to start from 1
data.index += 1

for row in data.itertuples():
    pg_cursor.execute(
        '''
        INSERT INTO posts (id, rubrics, text, created_date)
        VALUES (%s, %s, %s, %s)
        ''',
        [row.Index, row.rubrics, row.text, row.created_date],
    )
pg_connection.close()
