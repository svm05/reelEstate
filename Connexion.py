import pymysql

class Dbconnect(object):
    def __init__(self):
        self.dbconection = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='realestate')
        self.dbcursor = self.dbconection.cursor()
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        self.dbcursor.execute("""
            CREATE TABLE IF NOT EXISTS house (
                id INT AUTO_INCREMENT PRIMARY KEY,
                location TEXT,
                status TEXT,
                price TEXT,
                owner TEXT,
                bed TEXT,
                bath TEXT,
                sqft TEXT,
                sqft_lot TEXT
            )
        """)
        self.dbconection.commit()

    def commit_db(self):
        self.dbconection.commit()

    def close_db(self):
        self.dbcursor.close()
        self.dbconection.close()