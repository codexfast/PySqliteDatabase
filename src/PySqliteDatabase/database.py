"""
    This module handle database based on slqlite

"""

import io
import os
import time
import sqlite3
import pathlib
import colorama as cl

def in_apostrophe(txt): return f"""'{txt}'""" 

def sqliteError(func):
    def inner(ref, *args, **kargs):
        try:
            return func(ref, *args, **kargs)
        except sqlite3.Error as err:
            print(f'[*] Sqlite error: {cl.Fore.RED}{err}{cl.Style.RESET_ALL}')
            exit()

    return inner

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class SqliteOperator:

    EQUAL = "="
    NOT_EQUAL = "<>"
    LESS = "<"
    GREATER = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="

    ALL = "ALL"
    ANY = "ANY"
    AND = "AND"
    ANY =  "ANY"
    BETWEEN = "BETWEEN"
    EXISTS = "EXISTS"
    IN = "IN"
    LIKE = "LIKE"
    NOT = "NOT"
    OR = "OR"

    ASC = "ASC"
    DESC = "DESC"

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
    def value(key, v):
        return f"{key}={v if type(v) is int else str(in_apostrophe(v))}"

    @staticmethod
    def create(table: str, columns: list[str]):
        return f'CREATE TABLE IF NOT EXISTS {table} ({",".join(columns)})'

class SqliteWhere():

    """
        Usage:
            where = SqliteWhere(
                        SqliteWhere.equal('name', 'Josh'),
                        SqliteOperator.AND,
                        SqliteWhere.equal('age', 55),
                    )

            print(where) -> WHERE name = 'Josh' AND age = '55'
    """

    def __init__(self, *args: 'SqliteWhere') -> None:
        self.where = f"WHERE {' '.join(args)}"
    
    def __repr__(self) -> str:
        return self.where

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


class Database(metaclass=Singleton):

    """
        Class that handles database

        The 'pathname' variable can also contain the exact database path
    """

    def __init__(self, pathname: str = ':memory:', check_same_thread: bool = False):

        self.pathname: str = pathname
        self._conn: sqlite3.Connection = None

        try:
            self._conn = sqlite3.connect(self.pathname, check_same_thread=check_same_thread)

            print(f'[*] connection successfully established on: {cl.Fore.GREEN}{self.pathname}{cl.Style.RESET_ALL}')

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

    def select(self, table: str, where: SqliteWhere = '', column: list|str = '*', order_by: tuple[str] = (), limit: int = -1) -> list[tuple] | tuple :

        """
            Select values

            column: ['name', 'year', ... ]
            where: SqliteWhere (Class) statement ->.

                SqliteWhere (
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

    @sqliteError
    def update(self, table: str, updata: list[str], where: SqliteWhere, order_by: str = '', limit: int = -1 ):
        """UPDATE only 'WHERE' stmt"""

        if where == '':
            raise ValueError('Where is empty')

        sql = f"""UPDATE {table} SET {','.join(updata)} {where}"""

        if order_by != '':
            sql = f"{sql} ORDER BY {order_by}"

        if limit > 0:
            sql = f"{sql} LIMIT {limit}"

        cur = self._conn.cursor()
        cur.execute(sql)

        return self._conn.commit()

    def delete(self, table: str, where: SqliteWhere):
        """'DELETE' only 'WHERE' stmt"""

        if where == '':
            raise ValueError('Where is empty')

        sql = f"""DELETE FROM {table} {where}"""

        try:
            cur = self._conn.cursor()
            cur.execute(sql)

            return self._conn.commit()
        except sqlite3.Error as err:
            print("Delete error, ", err)

    def get_columns(self, table: str) -> list[str]:
        cur = self.run(f"SELECT * FROM {table} LIMIT 1")

        return list(map(lambda x: x[0], cur.description))

    @sqliteError
    def drop_table(self, table: str) -> None:
        """
            Drop table
        """
        
        cur = self._conn.cursor()
        cur.execute(f'DROP TABLE {table}')

        return self._conn.commit()

    @sqliteError
    def create_table(self, table: str, columns: list[SqliteEngine.column] ) -> None:
        """
            Create table
        """

        assert type(columns) == list
        assert columns != []

        cur = self._conn.cursor()
        cur.execute(SqliteEngine.create(
            table=table,
            columns=columns
        ))

        return self._conn.commit()

    @staticmethod
    def create_conn(pathname: str):
        """
            Static method for quick connection creation
        """
        return sqlite3.connect(pathname)

if __name__ == "__main__":

    """

        db.create_table() ok
        db.drop_table() ok

        db.insert() ok
        db.select() ok
        db.delete() ok

        db.backup() ok

    """
