from flask import Flask, Response, request
from api.repositories.zip_code_repository import ZipCodeRepository
from api.repositories.provider_repository import ProviderRepository
from api.repositories.institution_repository import InstitutionRepository
from api.models.db_config import DbConfig

import psycopg2
import json

app = Flask(__name__)


@app.route('/')
def index():
    return "It works!"


@app.route('/api/provider/<npi>')
def get_provider(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    provider_repository = ProviderRepository(connection)
    results = provider_repository.get(npi)
    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/institution/<npi>')
def get_institution(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    institution_repository = InstitutionRepository(connection)
    results = institution_repository.get(npi)
    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/zipcode')
def get_all_zipcodes():

    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)

    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    results = zipcode_repository.get_all(limit, offset)

    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/zipcode/<zipcode>')
def get_zipcode(zipcode):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    provider_repository = ProviderRepository(connection)

    results = zipcode_repository.get(zipcode)
    hcp_results = provider_repository.get_by_zipcode(zipcode)

    connection.close()
    return Response(json.dumps({'items': results, 'providers': hcp_results}), mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True)