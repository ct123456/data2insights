from flask import Flask, Response
from api.repositories.zip_code_repository import ZipCodeRepository
from api.repositories.provider_repository import ProviderRepository
from api.models.db_config import DbConfig

import psycopg2
import json

app = Flask(__name__)


@app.route('/')
def index():
    return "It works!"

@app.route('/provider/<npi>')
def get_provider(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    provider_repository = ProviderRepository(connection)
    results = provider_repository.get(npi)
    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/institution/<npi>')
def get_institution(npi):


@app.route('/zipcode/<zipcode>')
def get_zipcode(zipcode):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    results = zipcode_repository.get(zipcode)
    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True)