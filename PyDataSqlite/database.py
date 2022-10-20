"""
    This module handle database based on slqlite

"""

import sqlite3
from dataclasses import dataclass, field


@dataclass
class Database:
    """
        Class that handles database
        
        The 'pathname' variable can also contain the exact database path
    """
    pathname : str = field(default=':memory:')
    _conn : sqlite3.Connection = field(default=":memory:", repr=False)

    def __post_init__(self):
        try:
            self._conn = sqlite3.connect(self.pathname)
        except sqlite3.Error as err:
            print(err, f'({self.pathname})')

    def __del__(self):
        if isinstance(self._conn, sqlite3.Connection):
            self._conn.close()

    def run(self, sql : str):
        """
            Here execute sql statement
        """
        return None

    @staticmethod
    def create_conn(pathname : str):
        """
            Static method for quick connection creation
        """
        return sqlite3.connect(pathname)

if __name__ == "__main__":
    # db = Database('test.sqlite')
    db = Database()
    print(db._conn)
    
    # input("trava >>")