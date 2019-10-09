class ProviderRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get(self, npi):

        query = """
        SELECT 
            npi,
            first_name,
            middle_name,
            last_name,
            suffix,
            credentials,
            specialty,
            gender,
            medicare_count,
            CASE WHEN score > 0 THEN score ELSE 0 END score,
            zip_code
        FROM provider hcp
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
                    "suffix": row[4],
                    "credentials": row[5],
                    "specialty": row[6],
                    "gender": row[7],
                    "medicare_count": row[8],
                    "score": row[9],
                    "zip_code": row[10]
                }
            )

        return items

    def get_by_zipcode(self, zipcode, limit=100, offset=0):
        query = """
                SELECT 
                    npi,
                    first_name,
                    middle_name,
                    last_name,
                    suffix,
                    credentials,
                    specialty,
                    gender,
                    medicare_count,
                    CASE WHEN score > 0 THEN score ELSE 0 END score,
                    zip_code
                FROM provider hcp
                WHERE hcp.zip_code='{zipcode}'
                ORDER BY score DESC, npi ASC
                LIMIT {limit}
                OFFSET {offset}
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
                    "first_name": row[1],
                    "middle_name": row[2],
                    "last_name": row[3],
                    "suffix": row[4],
                    "credentials": row[5],
                    "specialty": row[6],
                    "gender": row[7],
                    "medicare_count": row[8],
                    "score": row[9],
                    "zip_code": row[10]
                }
            )

        return items
