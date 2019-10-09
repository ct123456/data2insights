from flask import Flask, Response, request
from flask import render_template, send_file
from api.repositories.zip_code_repository import ZipCodeRepository
from api.repositories.provider_repository import ProviderRepository
from api.repositories.institution_repository import InstitutionRepository
from api.repositories.medicare_repository import MedicareRepository
from api.repositories.provider_address_repository import ProviderAddressRepository
from api.repositories.institution_address_repository import InstitutionAddressRepository
from api.models.db_config import DbConfig

import psycopg2
import json

app = Flask(__name__, template_folder='../app/templates', static_url_path='')


@app.route('/data/zipcode_50000.json')
def send_data():
    return send_file('/home/ubuntu/data2insights/api/data/zipcode_50000.json')


@app.route('/')
@app.route('/app')
def map_view():
    zipcode = request.args.get('zipcode', 67214)
    threshold = request.args.get('threshold', 1000)

    zoom_level = 14 if zipcode is not 67214 else 4
    zoom_level = request.args.get('zoom', zoom_level)

    return render_template('map_view.html', zipcode=zipcode, threshold=threshold, zoom_level=zoom_level)


@app.route('/app/provider/<npi>')
def provider_view(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    provider_repository = ProviderRepository(connection)
    provider_address_repository = ProviderAddressRepository(connection)
    medicare_repository = MedicareRepository(connection)

    hcp_result = provider_repository.get(npi)
    hcp_address_result = provider_address_repository.get_by_npi(npi)
    medicare_results = medicare_repository.get_by_npi(npi)

    connection.close()

    return render_template('provider_view.html', provider=hcp_result[0], addresses=hcp_address_result, medicare=medicare_results)


@app.route('/app/institution/<npi>')
def institution_view(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    institution_repository = InstitutionRepository(connection)

    hco_result = institution_repository.get(npi)

    connection.close()

    return render_template('institution_view.html', institution=hco_result[0])


@app.route('/app/zipcode/<zipcode>')
def zip_code_view(zipcode):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    provider_repository = ProviderRepository(connection)
    institution_repository = InstitutionRepository(connection)

    zipcode_results = zipcode_repository.get(zipcode)
    hcp_results = provider_repository.get_by_zipcode(zipcode, limit=20)
    hco_results = institution_repository.get_by_zipcode(zipcode, limit=20)

    connection.close()

    return render_template('zipcode_view.html', zipcode=zipcode_results, providers=hcp_results, institutions=hco_results)


@app.route('/api/provider/<npi>')
def get_provider(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    provider_repository = ProviderRepository(connection)
    results = provider_repository.get(npi)
    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/institution/<npi>')
def get_institution(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
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
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    results = zipcode_repository.get_all(limit, offset)

    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/zipcode/<zipcode>')
def get_zipcode(zipcode):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    zipcode_repository = ZipCodeRepository(connection)
    provider_repository = ProviderRepository(connection)

    results = zipcode_repository.get(zipcode)
    hcp_results = provider_repository.get_by_zipcode(zipcode)

    connection.close()
    return Response(json.dumps({'items': results, 'providers': hcp_results}), mimetype='application/json')


@app.route('/api/medicare/<npi>')
def get_medicare_by_npi(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    medicare_repository = MedicareRepository(connection)

    results = medicare_repository.get_by_npi(npi)

    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/provider_address/<npi>')
def get_provider_address_by_npi(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    provider_address_repository = ProviderAddressRepository(connection)

    results = provider_address_repository.get_by_npi(npi)

    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


@app.route('/api/institution_address/<npi>')
def get_institution_address_by_npi(npi):
    db_config = DbConfig()
    connection = psycopg2.connect(host=db_config.host, database='healthcare2',
                                  user=db_config.user, password=db_config.password)

    institution_address_repository = InstitutionAddressRepository(connection)

    results = institution_address_repository.get_by_npi(npi)

    connection.close()
    return Response(json.dumps({'items': results}), mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True)