class ProviderAddressRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_by_npi(self, npi, limit=100, offset=0):
        query = """
                SELECT 
                    npi,
                    address1,
                    address2,
                    city,
                    state,
                    zip_code
                FROM provider_address pa
                WHERE pa.npi='{npi}'
                ORDER BY state DESC
                LIMIT {limit}
                OFFSET {offset}
                """.format(
            npi=npi,
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
                    "address1": row[1],
                    "address2": row[2],
                    "city": row[3],
                    "state": row[4],
                    "zip_code": row[5]
                }
            )

        return items
