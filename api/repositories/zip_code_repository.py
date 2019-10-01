class ZipCodeRepository(object):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get(self, zipcode):

        query = """
        SELECT 
            zip, 
            county_name, 
            population, 
            city, 
            state_id, 
            county_fips, 
            substring(county_fips, 1, 2) state_code, 
            substring(county_fips, 2, 3) fips, 
            lat, 
            lng, 
            hcp_count, 
            hco_count
        FROM zipcode z
        JOIN uszips u ON z.zip5=u.zip
        WHERE u.zip='{zipcode}'
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
                    "zip": row[0],
                    "county_name": row[1],
                    "population": row[2],
                    "city": row[3],
                    "state_id": row[4],
                    "county_fips": row[5],
                    "state_code": row[6],
                    "fips": row[7],
                    "lat": row[8],
                    "lng": row[9],
                    "hcp_count": row[10],
                    "hco_count": row[11],
                }
            )

        return items
