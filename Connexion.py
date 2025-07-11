import sqlite3

class Dbconnect(object):
    def __init__(self):
        self.dbconection = sqlite3.connect('realestate.db')
        self.dbcursor = self.dbconection.cursor()
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        self.dbcursor.execute("""
            CREATE TABLE IF NOT EXISTS house (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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