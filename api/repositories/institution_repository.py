class InstitutionRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get(self, npi):

        query = """
        SELECT 
            "npi", 
            "name", 
            "medicare_count",
            "score",
            "zip_code"
        FROM institution hco
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
                    "name": row[1],
                    "medicare_count": row[2],
                    "score": row[3],
                    "zip_code": row[4]
                }
            )

        return items

    def get_by_zipcode(self, zipcode, limit=100, offset=0):
        query = """
        SELECT 
            "npi", 
            "name", 
            "medicare_count",
            "score",
            "zip_code"
        FROM institution hco
        WHERE hco.zip_code='{zipcode}'
        ORDER BY npi DESC
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
                    "name": row[1],
                    "medicare_count": row[2],
                    "score": row[3],
                    "zip_code": row[4]
                }
            )

        return items
