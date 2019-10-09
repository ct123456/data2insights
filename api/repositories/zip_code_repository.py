class ZipCodeRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_all(self, limit=100, offset=0):
        query = """
                SELECT 
                    zip_code, 
                    state, 
                    latitude, 
                    longitude,  
                    county_fips, 
                    county_name,
                    substring(county_fips, 1, 2) state_code, 
                    substring(county_fips, 2, 3) fips,  
                    provider_count, 
                    medicare_count,
                    score
                FROM neighborhood z
                LIMIT {limit}
                OFFSET {offset}
                """.format(limit=limit, offset=offset)

        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        items = []
        for row in results:
            items.append(
                {
                    "zip_code": row[0],
                    "state": row[1],
                    "lat": row[2],
                    "lng": row[3],
                    "county_fips": row[4],
                    "county_name": row[5],
                    "state_code": row[6],
                    "fips": row[7],
                    "hcp_count": row[8],
                    "hco_count": row[9],
                    "medicare_count": row[10],
                    "score": row[11]
                }
            )

        return items

    def get(self, zipcode):

        query = """
        SELECT 
            zip_code, 
            state, 
            latitude, 
            longitude,  
            county_fips, 
            county_name,
            substring(county_fips, 1, 2) state_code, 
            substring(county_fips, 2, 3) fips,  
            provider_count, 
            medicare_count,
            score
        FROM neighborhood z
        WHERE u.zip_code='{zipcode}'
        """.format(
            zipcode=zipcode
        )

        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        items = []
        for row in results:
            items.append(
                {
                    "zip_code": row[0],
                    "state": row[1],
                    "lat": row[2],
                    "lng": row[3],
                    "county_fips": row[4],
                    "county_name": row[5],
                    "state_code": row[6],
                    "fips": row[7],
                    "hcp_count": row[8],
                    "hco_count": row[9],
                    "medicare_count": row[10],
                    "score": row[11]
                }
            )

        return items
