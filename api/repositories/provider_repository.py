class ProviderRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get(self, npi):

        query = """
        SELECT 
            npi,
            provider_first_name first_name,
            provider_middle_name middle_name,
            provider_last_name_legal_name last_name,
            provider_credential_text credentials,
            provider_first_line_business_mailing_address address1,
            provider_business_mailing_address_city_name city,
            provider_business_mailing_address_state_name state,
            zip5 zip_code,
            provider_gender_code gender
        FROM npi_hcp hcp
        WHERE hcp.npi='{npi}'
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
                    "first_name": row[1],
                    "middle_name": row[2],
                    "last_name": row[3],
                    "credentials": row[4],
                    "address1": row[5],
                    "city": row[6],
                    "state": row[7],
                    "zip_code": row[8],
                    "gender": row[9]
                }
            )

        return items
