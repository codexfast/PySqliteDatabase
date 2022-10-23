"""
    This module handle database based on slqlite

"""

from importlib.metadata import metadata
import sqlite3
from dataclasses import dataclass, field

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

@dataclass
class Database(metaclass=Singleton):    
    
    """
        Class that handles database
        
        The 'pathname' variable can also contain the exact database path
    """

    EQUAL = "="
    NOT_EQUAL = "<>"
    LESS = "<"
    GREATER = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="

    ALL = "ALL"
    ANY = "ANY"
    AND = "AND"	 
    ANY = "ANY"	 
    BETWEEN = "BETWEEN"	 
    EXISTS = "EXISTS"	
    IN = "IN"	
    LIKE = "LIKE"	
    NOT = "NOT"	
    OR = "OR"

    __instance = None
    pathname : str = field(default=':memory:')
    # _conn : sqlite3.Connection = field(default=":memory:", repr=False)

    def __post_init__(self):
        try:
            self._conn = sqlite3.connect(self.pathname)
        except sqlite3.Error as err:
            print(err, f'({self.pathname})')
            exit()

    def __del__(self):
        if isinstance(self._conn, sqlite3.Connection):
            self._conn.close()

    def run(self, sql: str) -> sqlite3.Cursor:
        """
            Here execute sql statement
        """

        cur = self._conn.cursor()

        try:
            return cur.execute(sql)
        except sqlite3.OperationalError as err:
            print("Sql statement has a error", f'({err})')
            exit()

    def insert(self, table: str, values: tuple):

        """
            Insert values
        """
        assert type(values) == tuple

        bindings = "?" * len(values)
        sql = f'INSERT INTO {table} VALUES ({",".join(bindings)})'

        cur = self._conn.cursor()

        try:
            cur.execute(sql, values)
            self._conn.commit()
            
            return cur.lastrowid
        except sqlite3.Error as err:
            print("Insert error, ", err)
            exit()

    def select(self, table: str, where: dict | str = '', column: list|str = '*'):
        """
            Select values

            column: 
        """
        values = []
        bindings = []

        if type(column) == list:
            column = ','.join(column)

        if type(where) == dict:
            
            bindings = list(map(lambda k: f'{k}=?' ,where.keys()))
            values = list(map(lambda k: where[k] ,where.keys()))

            where = f'WHERE {",".join(bindings)}'

        sql = f'SELECT {column} FROM {table} {where}'

        try:
            cur = self._conn.cursor()
            cur.execute(sql, values)

            return cur.fetchall()
        except sqlite3.Error as err:
            print("Select error", err)
            exit()

    def delete(self):
        pass

    @staticmethod
    def create_conn(pathname: str):
        """
            Static method for quick connection creation
        """
        return sqlite3.connect(pathname)

if __name__ == "__main__":
    # db = Database('test.sqlite')
    db = Database(pathname="t.sqlite")
    db2 = Database()

    print(db)
    print(db2)
    
    # db.run("CREATE TABLE car (m TEXT, name INTERGER, year INTERGER)")

    # db.insert("car", (None, 'Uno', 2005, ))
    # db.insert("car", (None, 'Fusca', 1967, ))
    # db.insert("car", (None, 'Fusion', 2013, ))

    # print(db.select('car', column=['year'], where={'name':'Uno'}))

    # print(db.run("CREATE TABLE car (name TEXT)"))
    # print(db.run("INSERT INTO car VALUES ('Fusca')"))
    # print(db.run("SELECT * FROM car").fetchall())

    """
        
        db.create_table()
        db.drop_table()

        db.insert()
        db.select()
        db.delete()

        db.backup()

    """

    
    # input("trava >>")