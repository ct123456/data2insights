from flask import Flask, Response, request
from flask import render_template, send_file
from api.repositories.zip_code_repository import ZipCodeRepository
from api.repositories.provider_repository import ProviderRepository
from api.repositories.institution_repository import InstitutionRepository
from api.models.db_config import DbConfig

import psycopg2
import json

app = Flask(__name__, template_folder='../app/templates', static_url_path='')


@app.route('/')
def index():
    return "It works!"


@app.route('/data/zipcode_50000.json')
def send_data():
    return send_file('/home/ubuntu/data2insights/api/data/zipcode_50000.json')


@app.route('/app')
def map_view():
    zipcode = request.args.get('zipcode', 10001)
    threshold = request.args.get('threshold', 1000)

    zoom_level = 14 if zipcode is not 10001 else 4
    zoom_level = request.args.get('zoom', zoom_level)

    return render_template('map_view.html', zipcode=zipcode, threshold=threshold, zoom_level=zoom_level)


@app.route('/app/provider/<npi>')
def provider_view(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    provider_repository = ProviderRepository(connection)

    hcp_result = provider_repository.get(npi)

    connection.close()

    return render_template('provider_view.html', provider=hcp_result[0])


@app.route('/app/institution/<npi>')
def institution_view(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    institution_repository = InstitutionRepository(connection)

    hco_result = institution_repository.get(npi)

    connection.close()

    return render_template('institution_view.html', institution=hco_result[0])


@app.route('/app/zipcode/<zipcode>')
def zip_code_view(zipcode):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    provider_repository = ProviderRepository(connection)
    institution_repository = InstitutionRepository(connection)

    zipcode_results = zipcode_repository.get(zipcode)
    hcp_results = provider_repository.get_by_zipcode(zipcode)
    hco_results = institution_repository.get_by_zipcode(zipcode)

    connection.close()

    return render_template('zipcode_view.html', zipcode=zipcode_results, providers=hcp_results, institutions=hco_results)


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