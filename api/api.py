from datetime import datetime
from os import getenv

import elasticsearch
import psycopg2
from dotenv import load_dotenv
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

es = elasticsearch.Elasticsearch(ES_URL)


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


def get_pg_cursor():
    """Get the cursor to execute SQL queries."""
    if not hasattr(g, "pg_connection"):
        g.pg_connection = connect_pg()
    return g.pg_connection.cursor()


@app.teardown_appcontext
def close_pg(error):
    """Close the database connection."""
    if hasattr(g, "pg_connection"):
        g.pg_connection.close()


@api.route('/posts/<int:post_id>')
@api.response(404, 'Post not found')
@api.response(200, 'Success')
class DataEntry(Resource):
    def get(self, post_id):
        """Get the Postgres record by id."""
        pg_cursor = get_pg_cursor()
        pg_cursor.execute('SELECT * FROM posts WHERE id = %s', [post_id])
        row = pg_cursor.fetchone()
        if row is None:
            return {'post not found': post_id}, 404
        prepared_row = []
        for element in row:
            if isinstance(element, datetime):
                prepared_row.append(str(element))
            else:
                prepared_row.append(element)
        return prepared_row, 200

    def delete(self, post_id):
        """Delete the Postgres record and its ES document by id."""
        pg_cursor = get_pg_cursor()
        try:
            es.delete(index='posts', id=post_id)
            pg_cursor.execute('DELETE FROM posts WHERE id = %s', [post_id])
        except elasticsearch.exceptions.NotFoundError:
            return {'post not found': post_id}, 404
        return {'post deleted': post_id}, 200


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
            pg_cursor = get_pg_cursor()
            pg_cursor.execute(
                'SELECT * FROM posts WHERE id IN %s ORDER BY created_date',
                [ids],
            )
            pg_results = pg_cursor.fetchall()
            pg_headers = [x[0] for x in pg_cursor.description]
            prepared_pg_results = []
            for i in range(len(pg_results)):
                prepared_pg_results.append(
                    dict(zip(pg_headers, pg_results[i]))
                )
                prepared_pg_results[i]['created_date'] = str(
                    prepared_pg_results[i]['created_date']
                )
            return prepared_pg_results, 200
        else:
            return [], 200


if __name__ == '__main__':
    app.run(debug=True)
