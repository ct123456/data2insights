class InstitutionRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get(self, npi):

        query = """
        SELECT 
            npi, 
            entity_type_code, 
            provider_organization_name_legal_business_name "name", 
            provider_first_line_business_mailing_address "address1", 
            provider_second_line_business_mailing_address "address2", 
            provider_business_mailing_address_city_name "city", 
            provider_business_mailing_address_state_name "state", 
            zip5 "zip_code"
        FROM npi_hco hco
        WHERE hco.npi='{npi}'
        """.format(
            npi=npi
        )

        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        items = []
        for row in results:
            items.append(
                {
                    "npi": row[0],
                    "entity_type_code": row[1],
                    "name": row[2],
                    "address1": row[3],
                    "address2": row[4],
                    "city": row[5],
                    "state": row[6],
                    "zip_code": row[7]
                }
            )

        return items

    def get_by_zipcode(self, zipcode, limit=100, offset=0):
        query = """
        SELECT 
            npi, 
            entity_type_code, 
            provider_organization_name_legal_business_name "name", 
            provider_first_line_business_mailing_address "address1", 
            provider_second_line_business_mailing_address "address2", 
            provider_business_mailing_address_city_name "city", 
            provider_business_mailing_address_state_name "state", 
            zip5 "zip_code"
        FROM npi_hco hco
        WHERE hco.zip5='{zipcode}'
        LIMIT {limit}
        OFFSET {offset}
        ORDER BY npi DESC
        """.format(
            zipcode=zipcode,
            limit=limit,
            offset=offset
        )

        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        items = []
        for row in results:
            items.append(
                {
                    "npi": row[0],
                    "entity_type_code": row[1],
                    "name": row[2],
                    "address1": row[3],
                    "address2": row[4],
                    "city": row[5],
                    "state": row[6],
                    "zip_code": row[7]
                }
            )

        return items
