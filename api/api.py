from os import getenv

import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from flask import Flask, g
from flask_restx import Api, Resource

load_dotenv()

HOST = getenv('PG_HOST')
DATABASE = getenv('PG_DATABASE')
USER = getenv('PG_USER')
PASSWORD = getenv('PG_PASSWORD')
ES_URL = getenv('ES_URL')

app = Flask(__name__)
api = Api(app)

es = Elasticsearch(ES_URL)


def connect_pg():
    """Connect to the database."""
    pg_connection = psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port='5432',
        sslmode='require',
    )
    pg_connection.autocommit = True
    return pg_connection


def get_pg():
    """Open the database connection."""
    if not hasattr(g, "pg_connection"):
        g.pg_connection = connect_pg()
    return g.pg_connection


@app.teardown_appcontext
def close_pg(error):
    """Close the database connection."""
    if hasattr(g, "pg_connection"):
        g.pg_connection.close()


@api.route('/search/<query>')
class DataSearch(Resource):
    def get(self, query):
        """Find the records in Postgres indexed with Elasticsearch.

        Matches the exact phrase from the query."""
        es_search_results = es.search(
            index='posts',
            size=20,
            body={'query': {'match_phrase': {'text': query}}}
        )
        ids = tuple(
            hit['_source']['id'] for hit in es_search_results['hits']['hits']
        )
        if ids:
            pg_results = []
            pg_connection = get_pg()
            pg_cursor = pg_connection.cursor()
            pg_cursor.execute(
                'SELECT * FROM posts WHERE id IN %s ORDER BY created_date',
                [ids],
            )
            pg_results = pg_cursor.fetchall()
            pg_headers = [x[0] for x in pg_cursor.description]
            json_pg_results = []
            for i in range(len(pg_results)):
                json_pg_results.append(dict(zip(pg_headers, pg_results[i])))
                json_pg_results[i]['created_date'] = str(
                    json_pg_results[i]['created_date']
                )
            return json_pg_results
        else:
            return []


if __name__ == '__main__':
    app.run(debug=True)