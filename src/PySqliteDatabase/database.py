"""
    This module handle database based on slqlite

"""

from dataclasses import dataclass
from typing import List

import io
import os
import time
import sqlite3
import pathlib

def sqliteError(func):
    def inner(ref, *args, **kargs):
        try:
            return func(ref, *args, **kargs)
        except sqlite3.Error as err:
            print('Sqlite error, ',err)
            exit()

    return inner

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

@dataclass(frozen=True)
class SqliteOperator:

    EQUAL = " = "
    NOT_EQUAL = " <> "
    LESS = " < "
    GREATER = " > "
    LESS_EQUAL = " <= "
    GREATER_EQUAL = " >= "

    ALL = " ALL "
    ANY = " ANY "
    AND = " AND " 	 
    ANY =  " ANY "	 
    BETWEEN = " BETWEEN "	 
    EXISTS = " EXISTS "	
    IN = " IN "	
    LIKE = " LIKE "	
    NOT = " NOT "	
    OR = " OR "

    ASC = "ASC"
    DESC = "DESC"

@dataclass(frozen=True)
class SqliteTypes:

    INTEGER = "INTEGER"
    NUMERIC = "NUMERIC"
    TEXT = "TEXT"
    REAL = "REAL"
    BLOB = "BLOB"
    DATETIME = "DATETIME "

class SqliteEngine:

    PRIMARY_KEY = "PRIMARY KEY"
    NOT_NULL = "NOT NULL"
    UNIQUE = "UNIQUE"
    CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"

    @staticmethod
    def column(name: str, type_: SqliteTypes, primary_key: bool = False, not_null: bool = False, unique: bool = False, default = False):

        if default:
            if type(default) == str:
                if default != 'CURRENT_TIMESTAMP':
                    default = f"DEFAULT '{default}'"
                else:
                    default = f"DEFAULT {default}"

            elif type(default) == int:
                default = f"DEFAULT {default}"
        else:
            default = False

        column = f"""{name} {type_} {default if default else ''} {SqliteEngine.PRIMARY_KEY if primary_key else ''} {SqliteEngine.NOT_NULL if not_null else ''} {SqliteEngine.UNIQUE if unique else ''}"""

        return column

    @staticmethod
    def create(table: str, columns: list[str]):
        return f'CREATE TABLE IF NOT EXISTS {table} ({",".join(columns)})'

class SqliteWhere:
    @staticmethod
    def equal(key: str, value: str) -> str:
        if value != None:
            return f"{key} = '{value}'"
        else:
            return f"{key} IS NULL"

    @staticmethod
    def like(key: str, value: str) -> str:
        """
            value: %value%
        """
        return f"{key}{SqliteOperator.LIKE}'{value}'"

    @staticmethod
    def  in_(key: str, value: tuple) -> str:
        """
            value: tuple('value1', value2, ... )
        """

        return f"{key}{SqliteOperator.IN}({','.join(map(str, value))})"

    @staticmethod
    def between(key: str, low_ex: str|int, high_ex: str|int, not_between: bool = False ) -> str:
        """
            value: tuple('value1', value2, ... )
        """

        return f"{key} {SqliteOperator.NOT if not_between else ''} {SqliteOperator.BETWEEN} {low_ex}{SqliteOperator.AND}{high_ex}"



    @staticmethod
    def where(*args: 'SqliteWhere') -> str:
        return f"WHERE {' '.join(args)}"

class Database(metaclass=Singleton):    
    
    """
        Class that handles database
        
        The 'pathname' variable can also contain the exact database path
    """

    def __init__(self, pathname: str = ':memory:'):
    # def __init__(self, pathname: str = ':memory:', _conn: sqlite3.Connection = None):

        self.pathname: str = pathname
        self._conn: sqlite3.Connection = None

        try:
            self._conn = sqlite3.connect(self.pathname)
        
        except sqlite3.Error as err:
            print(err, f'({self.pathname})')
            exit()


    def __del__(self):
        if hasattr(self, '_conn'):
            if isinstance(self._conn, sqlite3.Connection):
                self._conn.close()

    @sqliteError
    def backup(self, pathname: str):
        """
            pathname: Path name to backup

            Here is created dump sql
        """
        print(f'{"Backup":-^25}')

        bkp_name = pathlib.PurePath(pathname).with_suffix(f'.{time.time_ns()}_dump.sql')
        os.makedirs(os.path.dirname(bkp_name), exist_ok=True)

        with io.open(bkp_name, 'w') as f:
            for lines in self._conn.iterdump():
                f.write(f'{lines}\n')
        print(f'Backup saved in: {pathlib.Path(bkp_name).parent.absolute()}')
        # print(f'{"":-^25}')

    @sqliteError
    def restore(self, pathname: str):
        """
            pathname: Path name to restore

            here is read sql statements in dump
        """
        print(f'{"Restore":-^25}')

        with io.open(pathname, 'r') as f:
            sql = f.read()
            self._conn.executescript(sql)

        print(f'Restoration completed from: {pathname}')

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

    def insert(self, table: str, values: tuple, columns = False):

        """
            Insert values

            table: 'mytable'
            values: (None, name, 18)
            columns: ('id', 'name')

            It is mandatory to inform the columns when working with default values ​​in the tables,
            otherwise the script will return an error
        """
        assert type(values) == tuple

        bindings = "?" * len(values)

        if not columns:

            sql = f'INSERT INTO {table} VALUES ({",".join(bindings)})'
        else:
            sql = f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join(bindings)})'

        cur = self._conn.cursor()

        try:
            cur.execute(sql, values)
            self._conn.commit()
            
            return cur.lastrowid
        except sqlite3.Error as err:

            print("Insert error, ", err)
            exit()

    def select(self, table: str, where: SqliteWhere = '', column: list|str = '*', order_by: tuple[str] = (), limit: int = -1) -> List[tuple] | tuple :
        
        """
            Select values

            column: ['name', 'year', ... ]
            where: SqliteWhere (Class) statement ->.

                SqliteWhere.where(
                        SqliteWhere.equal('name', 'Josh')
                        SqliteOperator.AND,
                        SqliteWhere.equal('year', 2000)
                    )

            order_by: {'name'} or {'name', 'DESC'}
        """

        assert type(limit) == int
        
        values = []

        if type(column) == list:
            column = ','.join(column)
        else:
            column = '*'

        if order_by != () and type(order_by) == tuple:
            order_by = list(order_by)
            if len(order_by)>1:
                if (order_by[1] == SqliteOperator.ASC) or (order_by[1] == SqliteOperator.DESC):
                    order_by = f"{order_by[0]} {order_by[1]}"
                else:
                    raise Exception("'ORDER BY' and different from ASC or DESC ")
            else:
                    order_by = order_by[0]
        else:
            order_by = ''

        sql = f'''
            SELECT {column} 
            FROM {table}
            {where}
            {"ORDER BY " + order_by if order_by else '' }
            {"LIMIT " + str(limit) if limit > 0 else ''}
        '''

        try:
            cur = self._conn.cursor()
            cur.execute(sql, values)

            fetch = cur.fetchall()
            if len(fetch) == 1:
                return fetch[0]
            
            return fetch if len(fetch) > 1 else None
        except sqlite3.Error as err:
            print("Select error", err)
            exit()

    def delete(self, table: str, where: SqliteWhere):
        """'DELETE' only 'WHERE' stmt"""

        if where == '':
            raise ValueError('Where is empty')

        sql = f"""DELETE FROM {table} {where}"""

        try:
            self._conn.execute(sql)
            self._conn.commit()
        except sqlite3.Error as err:
            print("Delete error, ", err)

    def get_columns(self, table: str) -> List[str]:
        cur = self.run(f"SELECT * FROM {table} LIMIT 1")

        return list(map(lambda x: x[0], cur.description))

    @sqliteError
    def drop_table(self, table: str) -> None:
        self._conn.execute(f'DROP TABLE {table}')
        self._conn.commit()

    @sqliteError
    def create_table(self, table: str, columns: list[SqliteEngine.column] ) -> None:
        """
            Create table
        """

        assert type(columns) == list
        assert columns != []

        self._conn.execute(SqliteEngine.create(
            table=table,
            columns=columns
        ))
        self._conn.commit()

    @staticmethod
    def create_conn(pathname: str):
        """
            Static method for quick connection creation
        """
        return sqlite3.connect(pathname)

if __name__ == "__main__":
  
    """
        
        db.create_table()
        db.drop_table()

        db.insert()
        db.select()
        db.delete()

        db.backup()

    """