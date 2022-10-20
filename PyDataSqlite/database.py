"""
    This module handle database based on slqlite

"""

import sqlite3
from dataclasses import dataclass, field


@dataclass
class Database:
    """
        Class that handles database
        
        The 'name' variable can also contain the exact database path
    """
    name : str = field(default=':memory:')
    _conn : sqlite3.Connection = field(repr=False, init=False)

    def __pos_init__(self):
        self._conn = sqlite3.connect(self.name)

if __name__ == "__main__":
    db = Database('test.sqlite')
    print(db)