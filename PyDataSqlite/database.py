"""
    This module handle database based on slqlite

"""
from ast import Raise
from dataclasses import dataclass
import sqlite3


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

class SqliteEngine:
    @staticmethod
    def column():
        pass

class SqliteWhere:
    @staticmethod
    def equal(key: str, value: str):
        if value != None:
            return f"{key} = '{value}'"
        else:
            return f"{key} IS NULL"

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

            table: 'mytable'
            values: (None, name, 18)
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

    def select(self, table: str, where: SqliteWhere = '', column: list|str = '*', order_by: tuple[str] = (), limit: int = -1):
        
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
            limit: 121
            extras: extra instructions ->.
                Coming soon
        """

        assert type(limit) == int
        
        values = []

        if type(column) == list:
            column = ','.join(column)
        else:
            column = '*'

        if order_by != () and type(order_by) == tuple:
            order_by = list(order_by)
            print(SqliteOperator.ASC)
            print(order_by[1])
            print(order_by)
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

            print(f'Tables: {list(map(lambda x: x[0], cur.description))}')

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
    db = Database('test.sqlite')
    
    # db.run("CREATE TABLE IF NOT EXISTS car (id INTEGER PRIMARY KEY NOT NULL, model TEXT, make TEXT, year INTEGER, category TEXT)")

    # db.insert("car", (None, 'Fiat', 'Uno', 1998, 'silver'))
    # db.insert("car", (None, 'Fiat', 'Bravo', 2002, 'black'))
    # db.insert("car", (None, 'VW', 'Jetta', 2022, 'red'))
    # db.insert("car", (None, 'Renault', 'Cleo', 2007, 'white'))
    # db.insert("car", (None, 'Chevrolet', 'Onix',2022, None))


    # result = db.select(table='car', where=SqliteWhere.where(
    #     SqliteWhere.equal('year', 2022),
    #     SqliteOperator.AND,
    #     SqliteWhere.equal('name', 'Onix')
    # ))

    # with open('PyDataSqlite/car.json') as c:
    #     data = json.load(c)
    #     for i in data['results']:
    #         db.insert("car", (None, i['Make'], i['Model'],i['Year'], i['Category']))


    result = db.select(table='car', limit=5, where=SqliteWhere.where(
        SqliteWhere.equal('model','BMW'),
        SqliteOperator.AND,
        SqliteWhere.equal('year','2000')
    ))

    #   sabrujm H \,SMprint(result)

    # db.insert(table='car', values=(None, 'BMW custom', 'Z8', 2022))

    """
        
        db.create_table()
        db.drop_table()

        db.insert()
        db.select()
        db.delete()

        db.backup()

    """

    
    # input("trava >>")