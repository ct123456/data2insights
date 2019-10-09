class MedicareRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_by_npi(self, npi, limit=100, offset=0):
        query = """
                SELECT 
                    npi,
                    hcpcs_code,
                    hcpcs_description,
                    line_service_count,
                    zip_code,
                FROM medicare m
                WHERE m.npi='{npi}'
                ORDER BY hcpcs_code DESC
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
                    "hcpcs_code": row[1],
                    "hcpcs_description": row[2],
                    "line_service_count": row[3],
                    "zip_code": row[4]
                }
            )

        return items
